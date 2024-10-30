"""
Process View-class definition
"""

from typing import Optional

from flask import Blueprint, jsonify
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext as Context
from dcm_common.orchestration import JobConfig, Job, Children
from dcm_common import services

from dcm_job_processor.config import AppConfig
from dcm_job_processor.models import (
    Stage, JobConfig as ProcessorJobConfig, Report
)
from dcm_job_processor.handlers import process_handler
from dcm_job_processor.components import Processor
from dcm_job_processor.components.service_adapter import (
    ImportIEsAdapter, ImportIPsAdapter, BuildIPAdapter, ValidationAdapter,
    ValidationMetadataAdapter, ValidationPayloadAdapter, BuildSIPAdapter,
    TransferAdapter, IngestAdapter,
)


class ProcessView(services.OrchestratedView):
    """View-class for job-processing."""
    NAME = "process"

    def __init__(
        self, config: AppConfig, *args, **kwargs
    ) -> None:
        super().__init__(config, *args, **kwargs)

        # initialize components
        self.processor = Processor()

        # link adapter instances to Stages
        for stage, type_, host in (
            (Stage.IMPORT_IES.value, ImportIEsAdapter, config.IMPORT_MODULE_HOST),
            (Stage.IMPORT_IPS.value, ImportIPsAdapter, config.IMPORT_MODULE_HOST),
            (Stage.BUILD_IP.value, BuildIPAdapter, config.IP_BUILDER_HOST),
            (Stage.VALIDATION.value, ValidationAdapter, ""),
            (Stage.VALIDATION_METADATA.value, ValidationMetadataAdapter, config.IP_BUILDER_HOST),
            (Stage.VALIDATION_PAYLOAD.value, ValidationPayloadAdapter, config.OBJECT_VALIDATOR_HOST),
            (Stage.BUILD_SIP.value, BuildSIPAdapter, config.SIP_BUILDER_HOST),
            (Stage.TRANSFER.value, TransferAdapter, config.TRANSFER_MODULE_HOST),
            (Stage.INGEST.value, IngestAdapter, config.BACKEND_HOST),
        ):
            stage.adapter = type_(
                host, interval=0.1, timeout=config.PROCESS_TIMEOUT
            )
        # configure abort-routes
        for stage, url, rule in (
            (Stage.IMPORT_IES, config.IMPORT_MODULE_HOST, "/import"),
            (Stage.IMPORT_IPS, config.IMPORT_MODULE_HOST, "/import"),
            (Stage.BUILD_IP, config.IP_BUILDER_HOST, "/build"),
            (Stage.VALIDATION_METADATA, config.IP_BUILDER_HOST, "/validate"),
            (Stage.VALIDATION_PAYLOAD, config.OBJECT_VALIDATOR_HOST, "/validate"),
            (Stage.BUILD_SIP, config.SIP_BUILDER_HOST, "/build"),
            (Stage.TRANSFER, config.TRANSFER_MODULE_HOST, "/transfer"),
            (Stage.INGEST, config.BACKEND_HOST, "/ingest"),
        ):
            stage.value.url = url
            stage.value.abort_path = rule

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
            id_: Optional[str] = None,
            callback_url: Optional[str] = None
        ):
            """Handle request for processing job."""
            token = self.orchestrator.submit(
                JobConfig(
                    request_body={
                        "job_config": job_config.json,
                        "id": id_,
                        "callback_url": callback_url
                    },
                    context=self.NAME
                )
            )
            return jsonify(token.json), 201

        self._register_abort_job(bp, "/process")

    def get_job(self, config: JobConfig) -> Job:
        return Job(
            cmd=lambda push, data, children: self.process(
                push, data, children,
                job_config=ProcessorJobConfig.from_json(
                    config.request_body["job_config"]
                ),
            ),
            hooks={
                "startup": services.default_startup_hook,
                "success": services.default_success_hook,
                "fail": services.default_fail_hook,
                "abort": services.default_abort_hook,
                "completion": services.termination_callback_hook_factory(
                    config.request_body.get("callback_url", None),
                )
            },
            name="Job Processor"
        )

    def process(
        self, push, report: Report, children: Children,
        job_config: ProcessorJobConfig
    ):
        """
        Job instructions for the '/process' endpoint.

        Orchestration standard-arguments:
        push -- (orchestration-standard) push `report` to host process
        report -- (orchestration-standard) common report-object shared
                  via `push`
        children -- (orchestration-standard) `ChildJob`-registry shared
                    via `push`

        Keyword arguments:
        job_config -- job configuration details
        """

        report.progress.verbose = ("starting processor")
        report.log.log(
            Context.EVENT,
            body="Starting processor for job "
            + f"'{job_config.from_.value.identifier} > "
            + f"{job_config.to.value.identifier if job_config.to else 'ingest'}'."
        )
        push()

        self.processor.process(report.data, push, children, job_config)

        report.progress.verbose = ("evaluate results")
        push()

        report.data.success = all(
            record.success for record in report.data.records.values()
        )
        report.log.log(
            Context.INFO,
            body=f"Job has been {'' if report.data.success else 'un'}successful."
        )
        push()
