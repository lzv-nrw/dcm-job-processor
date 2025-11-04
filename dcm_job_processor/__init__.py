"""
- DCM Job Processor -
This flask app implements the 'Job Processor'-API (see
`openapi.yaml` in the sibling-package `dcm_job_processor_api`).
"""

from time import time, sleep

from flask import Flask
from dcm_common.services import DefaultView, ReportView
from dcm_common.services import extensions as common_extensions

from dcm_job_processor.config import AppConfig
from dcm_job_processor.views import ProcessView
from dcm_job_processor import extensions


def app_factory(
    config: AppConfig,
    as_process: bool = False,
    block: bool = False,
):
    """
    Returns a flask-app-object.

    config -- app config derived from `AppConfig`
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

    # create OrchestratedView-class
    view = ProcessView(config=config)
    view.register_job_types()

    # register extensions
    if config.ALLOW_CORS:
        app.extensions["cors"] = common_extensions.cors_loader(app)
    app.extensions["orchestra"] = common_extensions.orchestra_loader(
        app, config, config.worker_pool, "Job Processor", as_process
    )
    app.extensions["db"] = common_extensions.db_loader(
        app, config, config.db, as_process
    )
    app.extensions["db_init"] = extensions.db_init_loader(
        app,
        config,
        config.db,
        as_process,
        [
            common_extensions.ExtensionEventRequirement(
                app.extensions["db"].ready, "database connection made"
            )
        ],
    )

    def ready():
        """Define condition for readiness."""
        return (
            not config.ORCHESTRA_AT_STARTUP
            or app.extensions["orchestra"].ready.is_set()
        ) and (
            app.extensions["db"].ready.is_set()
            and app.extensions["db_init"].ready.is_set()
        )

    # block until ready
    if block and not as_process:
        time0 = time()
        while not ready() and time() - time0 < 10:
            sleep(0.01)

    # register blueprints
    app.register_blueprint(
        DefaultView(config, ready=ready).get_blueprint(), url_prefix="/"
    )
    app.register_blueprint(view.get_blueprint(), url_prefix="/")
    app.register_blueprint(ReportView(config).get_blueprint(), url_prefix="/")

    return app
