"""
Process View-class definition
"""

from typing import Optional, Mapping
import sys
from uuid import uuid4
from threading import Lock

from flask import Blueprint, jsonify, Response, request
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext
from dcm_common.util import now
from dcm_common.orchestra import JobConfig, JobContext, JobInfo, Token
from dcm_common import services

from dcm_job_processor.config import AppConfig
from dcm_job_processor.models import (
    TriggerType,
    JobContext as ProcessorJobContext,
    Stage,
    JobConfig as ProcessorJobConfig,
    Report,
)
from dcm_job_processor.handlers import process_handler
from dcm_job_processor.components import Processor
from dcm_job_processor.components.service_adapter import (
    ImportIEsAdapter,
    ImportIPsAdapter,
    BuildIPAdapter,
    ValidationAdapter,
    ValidationMetadataAdapter,
    ValidationPayloadAdapter,
    BuildSIPAdapter,
    TransferAdapter,
    IngestAdapter,
    PrepareIPAdapter,
)


class ProcessView(services.OrchestratedView):
    """View-class for job-processing."""

    NAME = "process"

    def __init__(self, config: AppConfig, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)

        # initialize components
        self.processor = Processor()

    def register_job_types(self):
        self.config.worker_pool.register_job_type(
            self.NAME, self.process, Report
        )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        @bp.route("/process", methods=["POST"])
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process job_config
            handler=process_handler,
            json=flask_json,
        )
        def process(
            job_config: ProcessorJobConfig,
            context: Optional[ProcessorJobContext] = None,
            token: Optional[str] = None,
            callback_url: Optional[str] = None,
        ):
            """Handle request for processing job."""
            # either use provided token value or generate random token anew
            _token = Token(value=token or str(uuid4()))

            # first, attempt to write dummy to database
            try:
                self.config.db.insert(
                    "jobs",
                    {
                        "token": _token.value,
                        "job_config_id": (
                            None if context is None else context.job_config_id
                        ),
                        "user_triggered": (
                            None if context is None else context.user_triggered
                        ),
                        "datetime_triggered": (
                            None
                            if context is None
                            else context.datetime_triggered
                        ),
                        "trigger_type": (
                            None
                            if context is None
                            else (
                                None
                                if context.trigger_type is None
                                else context.trigger_type.value
                            )
                        ),
                    },
                ).eval()
            except ValueError as exc_info:
                # if failed, check whether this is due to the record
                # already existing; if not, an unknown problem occured
                if (
                    self.config.db.get_row(
                        "jobs", _token.value, ["token"]
                    ).eval()
                    is None
                ):
                    return Response(
                        f"Submission rejected: {exc_info}",
                        mimetype="text/plain",
                        status=502,
                    )
                # TODO: the database should have a col for the JSON of the
                # original request to allow checking whether the request was
                # identical here (like in other dcm-microservices)
            else:
                # then submit to controller
                try:
                    self.config.controller.queue_push(
                        _token.value,
                        JobInfo(
                            JobConfig(
                                self.NAME,
                                original_body=request.json,
                                request_body={
                                    "job_config": job_config.json,
                                    "context": (
                                        {} if context is None else context.json
                                    ),
                                    "callback_url": callback_url,
                                },
                            ),
                            report=Report(
                                host=request.host_url, args=request.json
                            ),
                        ),
                    )
                # pylint: disable=broad-exception-caught
                except Exception as exc_info:
                    return Response(
                        f"Submission rejected: {exc_info}",
                        mimetype="text/plain",
                        status=500,
                    )

            return jsonify(_token.json), 201

        def post_abort_hook(token: str) -> None:
            """
            Check if info-object in database is still marked as running.
            (This is only the case if the job did not run before abort.)
            """
            info_db = self.config.db.get_row("jobs", token).eval()
            if info_db is not None:
                if info_db.get("status") in [None, "queued", "running"]:
                    # registry should be the most up-to-date
                    try:
                        info_registry = self.config.controller.get_info(token)
                    except ValueError as exc_info:
                        print(
                            "Error while aborting, could not fetch info from "
                            + f"registry: {exc_info}",
                            file=sys.stderr,
                        )
                        info_registry = {"report": info_db.get("report", {})}

                    # handle potential edge-cases
                    if info_registry.get("report") is None:
                        info_registry["report"] = {}
                    if info_registry["report"].get("progress") is None:
                        info_registry["report"]["progress"] = {
                            "verbose": "aborted",
                            "numeric": 0,
                        }
                    info_registry["report"]["progress"]["status"] = "aborted"

                    self.config.db.update(
                        "jobs",
                        {
                            "token": token,
                            "status": "aborted",
                            "report": info_registry["report"],
                            "datetime_ended": info_db.get("datetime_ended")
                            or now(True).isoformat(),
                        },
                    ).eval()

        self._register_abort_job(
            bp, "/process", post_abort_hook=post_abort_hook
        )

    def process(
        self,
        context: JobContext,
        info: JobInfo,
    ):
        """Job instructions for the '/process' endpoint."""

        job_config = ProcessorJobConfig.from_json(
            info.config.request_body["job_config"]
        )
        job_execution_context = ProcessorJobContext.from_json(
            info.config.request_body["context"]
        )
        info.report.log.set_default_origin("Job Processor")

        # initialize service-adapters
        # service-adapter are based on urllib3 and the connection-pooling used
        # here is generally not compatible with multiprocessing (causes threads
        # to get deadlocked); these adapters therefore need to be initialized
        # in the same process they are used in
        for stage, type_, host in (
            (
                Stage.IMPORT_IES.value,
                ImportIEsAdapter,
                self.config.IMPORT_MODULE_HOST,
            ),
            (
                Stage.IMPORT_IPS.value,
                ImportIPsAdapter,
                self.config.IMPORT_MODULE_HOST,
            ),
            (
                Stage.BUILD_IP.value,
                BuildIPAdapter,
                self.config.IP_BUILDER_HOST,
            ),
            (Stage.VALIDATION.value, ValidationAdapter, ""),
            (
                Stage.VALIDATION_METADATA.value,
                ValidationMetadataAdapter,
                self.config.IP_BUILDER_HOST,
            ),
            (
                Stage.VALIDATION_PAYLOAD.value,
                ValidationPayloadAdapter,
                self.config.OBJECT_VALIDATOR_HOST,
            ),
            (
                Stage.PREPARE_IP.value,
                PrepareIPAdapter,
                self.config.PREPARATION_MODULE_HOST,
            ),
            (
                Stage.BUILD_SIP.value,
                BuildSIPAdapter,
                self.config.SIP_BUILDER_HOST,
            ),
            (
                Stage.TRANSFER.value,
                TransferAdapter,
                self.config.TRANSFER_MODULE_HOST,
            ),
            (Stage.INGEST.value, IngestAdapter, self.config.BACKEND_HOST),
        ):
            stage.adapter = type_(
                host,
                interval=self.config.REQUEST_POLL_INTERVAL,
                timeout=self.config.PROCESS_TIMEOUT,
                request_timeout=self.config.REQUEST_TIMEOUT,
                max_retries=self.config.PROCESS_REQUEST_MAX_RETRIES,
                retry_interval=self.config.PROCESS_REQUEST_RETRY_INTERVAL,
            )

        # re-initialize database-adapter
        # since we are in a separate process, we need to use a new db-adapter
        # (new connections, to be specific) as pscopg-connections must not be
        # shared across processes
        self.config.init_adapter()
        if not self.config.db.pool.is_open:
            self.config.db.pool.init_pool()

        info.report.progress.verbose = "starting processor"
        info.report.log.log(
            LoggingContext.EVENT,
            body="Starting processor for job "
            + f"'{job_config.from_.value.identifier} > "
            + f"{job_config.to.value.identifier if job_config.to else 'ingest'}'.",
        )
        context.push()

        # patch context to work with threading
        context_lock = Lock()
        original_context = JobContext(
            context.push, context.add_child, context.remove_child
        )

        def patched_push():
            with context_lock:
                original_context.push()

        def patched_add_child(child):
            with context_lock:
                original_context.add_child(child)

        def patched_remove_child(id_: str):
            with context_lock:
                original_context.remove_child(id_)

        context = JobContext(
            patched_push, patched_add_child, patched_remove_child
        )

        # write initial record to database
        self.config.db.update(
            "jobs",
            {
                "token": info.report.token.value,
                "datetime_started": now(True).isoformat(),
            },
        ).eval()

        def on_update(row: Optional[Mapping] = None):
            """Callback that writes current report to database."""
            self.config.db.update(
                "jobs",
                {
                    "token": info.report.token.value,
                    "status": info.report.progress.status.value,
                    "success": info.report.data.success,
                    "report": info.report.json,
                }
                | (row or {}),
            ).eval()

        # run job
        self.processor.process(info, context, job_config, on_update=on_update)

        info.report.progress.verbose = "evaluate results"
        context.push()

        info.report.data.success = all(
            record.success for record in info.report.data.records.values()
        )
        info.report.log.log(
            LoggingContext.INFO,
            body=f"Job has been {'' if info.report.data.success else 'un'}successful.",
        )
        context.push()

        # finalize report and update records in database
        if job_execution_context.trigger_type is not TriggerType.TEST:
            # this part is not done in a single transaction to allow a
            # partial failure to happen
            for report_id, record in info.report.data.records.items():
                if report_id == "<bootstrap>":
                    continue
                # perform stage-specific post-processing
                # (e.g. extract identifiers and store in record)
                for stage_id, stage_info in record.stages.items():
                    Stage.from_string(
                        stage_id
                    ).value.adapter.post_process_record(
                        services.APIResult(
                            stage_info.completed,
                            stage_info.success,
                            info.report.children.get(stage_info.log_id),
                        ),
                        record,
                    )

                t = self.config.db.insert(
                    "records",
                    {
                        "job_token": info.report.token.value,
                        "success": record.success,
                        "report_id": report_id,
                        "external_id": record.external_id,
                        "origin_system_id": record.origin_system_id,
                        "sip_id": record.sip_id,
                        "ie_id": record.ie_id,
                        "datetime_processed": (
                            now().isoformat() if record.success else None
                        ),
                    },
                )
                if not t.success:
                    print(
                        f"Unable to create record '{report_id}' for token "
                        + f"'{info.report.token.value}' "
                        + f"(config '{job_execution_context.job_config_id}'): "
                        + t.msg,
                        file=sys.stderr,
                    )
        info.report.progress.complete()
        on_update({"datetime_ended": now(True).isoformat()})
