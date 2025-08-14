"""
- DCM Job Processor -
This flask app implements the 'Job Processor'-API (see
`openapi.yaml` in the sibling-package `dcm_job_processor_api`).
"""

from typing import Optional
from time import time, sleep

from flask import Flask
from dcm_common.db import KeyValueStoreAdapter, SQLAdapter
from dcm_common.orchestration import (
    ScalableOrchestrator, get_orchestration_controls
)
from dcm_common.services import DefaultView, ReportView
from dcm_common.services import extensions as common_extensions

from dcm_job_processor.config import AppConfig
from dcm_job_processor.views import ProcessView
from dcm_job_processor.models import Report
from dcm_job_processor import extensions


def app_factory(
    config: AppConfig,
    queue: Optional[KeyValueStoreAdapter] = None,
    registry: Optional[KeyValueStoreAdapter] = None,
    db: Optional[SQLAdapter] = None,
    as_process: bool = False,
    block: bool = False,
):
    """
    Returns a flask-app-object.

    config -- app config derived from `AppConfig`
    queue -- queue adapter override
             (default None; use `MemoryStore`)
    registry -- registry adapter override
                (default None; use `MemoryStore`)
    db -- database adapter
          (default None; uses `config.db`)
    as_process -- whether the app is intended to be run as process via
                  `app.run`; if `True`, startup tasks like starting
                  orchestration-daemon are prepended to `app.run`
                  instead of being run when this factory is executed
                  (default False)
    block -- whether to block execution until all extensions are ready
             (up to 10 seconds); only relevant if not `as_process`
             (default False)
    """

    app = Flask(__name__)
    app.config.from_object(config)

    # create Orchestrator and OrchestratedView-class
    orchestrator = ScalableOrchestrator(
        queue=queue or config.queue,
        registry=registry or config.registry,
        nprocesses=config.ORCHESTRATION_PROCESSES,
    )
    view = ProcessView(
        config=config,
        report_type=Report,
        orchestrator=orchestrator,
        context=ProcessView.NAME
    )

    # register extensions
    if config.ALLOW_CORS:
        common_extensions.cors(app)
    notifications_loader = common_extensions.notifications_loader(
        app, config, as_process
    )
    orchestrator_loader = common_extensions.orchestration_loader(
        app, config, orchestrator, "Job Processor", as_process,
        [
            common_extensions.ExtensionEventRequirement(
                notifications_loader.ready,
                "connection to notification-service",
            )
        ],

    )
    db_loader = common_extensions.db_loader(app, config, config.db, as_process)
    db_init_loader = extensions.db_init_loader(
        app,
        config,
        db or config.db,
        as_process,
        [
            common_extensions.ExtensionEventRequirement(
                db_loader.ready, "database connection made"
            )
        ],
    )

    def ready():
        """Define condition for readiness."""
        return (
            (
                not config.ORCHESTRATION_AT_STARTUP
                or orchestrator_loader.ready.is_set()
            )
            and (db_loader.ready.is_set() and db_init_loader.ready.is_set())
        )

    # block until ready
    if block and not as_process:
        time0 = time()
        while not ready() and time() - time0 < 10:
            sleep(0.01)

    # register orchestrator-controls blueprint
    if getattr(config, "TESTING", False) or config.ORCHESTRATION_CONTROLS_API:
        app.register_blueprint(
            get_orchestration_controls(
                orchestrator,
                orchestrator_loader.data,
                orchestrator_settings={
                    "interval": config.ORCHESTRATION_ORCHESTRATOR_INTERVAL,
                },
                daemon_settings={
                    "interval": config.ORCHESTRATION_DAEMON_INTERVAL,
                }
            ),
            url_prefix="/"
        )

    # register blueprints
    app.register_blueprint(
        DefaultView(config, ready=ready).get_blueprint(),
        url_prefix="/"
    )
    app.register_blueprint(
        view.get_blueprint(),
        url_prefix="/"
    )
    app.register_blueprint(
        ReportView(config, orchestrator).get_blueprint(),
        url_prefix="/"
    )

    return app
