"""
Process View-class definition
"""

from typing import Optional, Mapping, Any
import sys
from dataclasses import dataclass, field
from uuid import uuid4
from threading import Lock, Thread
from time import sleep
from datetime import datetime, timedelta
from traceback import format_exc

from flask import Blueprint, jsonify, Response, request
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext, Logger
from dcm_common.util import now
from dcm_common.orchestra import JobConfig, JobContext, JobInfo, Token
from dcm_common.orchestra.models import ChildJob
from dcm_common import services

from dcm_job_processor.config import AppConfig
from dcm_job_processor.models import (
    JobContext as JPJobContext,
    Stage,
    JobConfig as JPJobConfig,
    Report,
    Record,
    RecordStageInfo,
    RecordStatus,
)
from dcm_job_processor.handlers import process_handler
from dcm_job_processor.components.service_adapter import (
    ServiceAdapter,
    ImportIEsAdapter,
    ImportIPsAdapter,
    BuildIPAdapter,
    ValidationMetadataAdapter,
    ValidationPayloadAdapter,
    BuildSIPAdapter,
    TransferAdapter,
    IngestAdapter,
    PrepareIPAdapter,
)


@dataclass
class Job:
    """Record-class representing the current state of a job."""

    queued: list[Record] = field(default_factory=list)
    processing: list[Record] = field(default_factory=list)
    completed: list[Record] = field(default_factory=list)


class ProcessView(services.OrchestratedView):
    """
    View-class for job-processing.

    Method call tree during job-execution:
    ├─ initialize_service_adapters
    ├─ reinitialize_database_adapter
    ├─ load_template_and_job_config
    ├─ get_threaded_job_context
    ├─ collect_resumable_records
    ├─ import_new_records
    │  ├─ get_next_stage
    │  ├─ run_stage
    │  ├─ get_record_status
    │  └─ execute_record_post_stage
    └─ run
       ├─ loop maintenance
       │  (move records between stages queue/running/finished)
       └─ run_record (as thread)
          ├─ get_record_status
          ├─ get_next_stage
          └─ run_stage (as thread)
             └─ execute_record_post_stage
                └─ link_record_to_ie
    """

    NAME = "process"

    def __init__(self, config: AppConfig, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)

        self.adapters: dict[Stage, ServiceAdapter] = {}

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
            job_config: JPJobConfig,
            context: Optional[JPJobContext] = None,
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
                        "status": "queued",
                        "job_config_id": job_config.id_,
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
                # already existing; if not, an unknown problem occurred
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
                                    "process": job_config.json,
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
            info_db = self.config.db.get_row(
                "jobs", token, cols=["status", "report"]
            ).eval("fetching job info")
            if info_db is None:
                return

            # get info from orchestra (should be the most recent)
            try:
                info_registry = self.config.controller.get_info(token)
            except ValueError as exc_info:
                # fall back to db
                print(
                    "Error while aborting, could not fetch info from "
                    + f"registry: {exc_info}",
                    file=sys.stderr,
                )
                info_registry = info_db

            if info_registry.get("report") is None:
                info_registry["report"] = {}

            if info_registry.get("status") in [None, "queued", "running"]:
                # handle potential edge-cases
                if info_registry["report"].get("progress") is None:
                    info_registry["report"]["progress"] = {}
                info_registry["report"]["progress"]["numeric"] = 0
                info_registry["report"]["progress"]["verbose"] = (
                    f"aborted: {request.json.get('reason', '-')} "
                    + f"({request.json.get('origin', 'unknown')})"
                )
                info_registry["report"]["progress"]["status"] = "aborted"

                # TODO: release lock that ensures singular execution?
                self.config.db.update(
                    "jobs",
                    {
                        "token": token,
                        "status": "aborted",
                        "report": info_registry["report"],
                        "datetime_ended": info_db.get("datetime_ended")
                        or now(True).isoformat(),
                    },
                ).eval("finalizing job report")

        self._register_abort_job(
            bp, "/process", post_abort_hook=post_abort_hook
        )

    def initialize_service_adapters(self) -> None:
        """Initializes service-adapters."""
        self.adapters = {}
        for stage, Adapter, host in (
            (
                Stage.IMPORT_IES,
                ImportIEsAdapter,
                self.config.IMPORT_MODULE_HOST,
            ),
            (
                Stage.IMPORT_IPS,
                ImportIPsAdapter,
                self.config.IMPORT_MODULE_HOST,
            ),
            (
                Stage.BUILD_IP,
                BuildIPAdapter,
                self.config.IP_BUILDER_HOST,
            ),
            (
                Stage.VALIDATION_METADATA,
                ValidationMetadataAdapter,
                self.config.IP_BUILDER_HOST,
            ),
            (
                Stage.VALIDATION_PAYLOAD,
                ValidationPayloadAdapter,
                self.config.OBJECT_VALIDATOR_HOST,
            ),
            (
                Stage.PREPARE_IP,
                PrepareIPAdapter,
                self.config.PREPARATION_MODULE_HOST,
            ),
            (
                Stage.BUILD_SIP,
                BuildSIPAdapter,
                self.config.SIP_BUILDER_HOST,
            ),
            (
                Stage.TRANSFER,
                TransferAdapter,
                self.config.TRANSFER_MODULE_HOST,
            ),
            (Stage.INGEST, IngestAdapter, self.config.BACKEND_HOST),
        ):
            self.adapters[stage] = Adapter(
                host,
                interval=self.config.REQUEST_POLL_INTERVAL,
                timeout=self.config.PROCESS_TIMEOUT,
                request_timeout=self.config.REQUEST_TIMEOUT,
                max_retries=self.config.PROCESS_REQUEST_MAX_RETRIES,
                retry_interval=self.config.PROCESS_REQUEST_RETRY_INTERVAL,
            )

    def reinitialize_database_adapter(self) -> None:
        """
        Re-initializes the database adapter and initializes connection
        pool.
        """
        self.config.init_adapter()
        if not self.config.db.pool.is_open:
            self.config.db.pool.init_pool()

    def load_template_and_job_config(
        self,
        # pylint: disable=unused-argument
        context: JobContext,
        info: JobInfo,
        job_config: JPJobConfig,
    ) -> None:
        """
        Loads template and job-configuration from database and stores
        results in `job_config`.
        """
        # job-config
        jc_query = self.config.db.get_row(
            "job_configs",
            job_config.id_,
            cols=["template_id", "data_selection", "data_processing"],
        ).eval("loading job-configuration")

        if jc_query is None:
            raise ValueError(
                f"Job configuration with id '{job_config.id_}' does not "
                + "exist.",
            )

        job_config.data_selection = jc_query.get("data_selection", {}) or {}
        job_config.data_processing = jc_query.get("data_processing", {}) or {}

        # template
        job_config.template = self.config.db.get_row(
            "templates",
            jc_query.get("template_id"),
            cols=["type", "additional_information", "target_archive"],
        ).eval("loading template-configuration")

        if job_config.template is None:
            raise ValueError(
                f"Template with id '{jc_query.get('template_id')}' does not "
                + "exist.",
            )

        # patch in target_archive
        if job_config.template.get("target_archive") is None:
            job_config.template["target_archive"] = {}

    def get_threaded_job_context(
        self, context: JobContext
    ) -> tuple[Lock, JobContext]:
        """
        Returns a `JobContext` that can be used in a threaded
        environment.
        """
        context_lock = Lock()

        def threaded_push(db_update: bool = True):
            with context_lock:
                context.push(db_update)

        def threaded_add_child(child):
            with context_lock:
                context.add_child(child)

        def threaded_remove_child(id_: str):
            with context_lock:
                context.remove_child(id_)

        return context_lock, JobContext(
            threaded_push, threaded_add_child, threaded_remove_child
        )

    def collect_resumable_records(
        self,
        context: JobContext,
        info: JobInfo,
        job_config: JPJobConfig,
    ) -> list[Record]:
        """
        Returns a list of `Record`s that can be continued (not complete
        and artifacts are still available). Any `Record` that is not
        complete but has its artifacts expired is finalized as error.
        """
        # get list of relevant records
        records_query = self.config.db.custom_cmd(
            # pylint: disable=consider-using-f-string
            """
                SELECT id, job_token, ie_id, bitstream, skip_object_validation
                FROM records
                WHERE
                    job_config_id = {job_config_id}
                    AND status={status}
            """.format(
                job_config_id=self.config.db.decode(job_config.id_, "text"),
                status=self.config.db.decode(
                    RecordStatus.INPROCESS.value, "text"
                ),
            ),
            clear_schema_cache=False,
        ).eval("querying for resumable records")

        if len(records_query) > 0:
            info.report.log.log(
                LoggingContext.INFO,
                body=(
                    f"Found {len(records_query)} record(s) to potentially "
                    + "resume."
                ),
            )
        else:
            info.report.log.log(
                LoggingContext.INFO,
                body=("No records to be resumed in database."),
            )
        context.push()

        # build Record-objects
        records: list[Record] = [
            Record(
                id_=r[0],
                _resumable_token=r[1],
                ie_id=r[2],
                bitstream=self.config.db.encode(r[3], "boolean"),
                skip_object_validation=self.config.db.encode(r[4], "boolean"),
            )
            for r in records_query
        ]

        # extend artifact-life in database
        datetime_now = datetime.now().isoformat()
        if (
            job_config.execution_context is not None
            and job_config.execution_context.artifacts_ttl is not None
        ):
            datetime_expires = (
                datetime.now()
                + timedelta(seconds=job_config.execution_context.artifacts_ttl)
            ).isoformat()
            for r in records:
                # only extend if not already expired
                # update artifacts-table and jobs-table
                self.config.db.custom_cmd(
                    # pylint: disable=consider-using-f-string
                    """
                        UPDATE jobs
                        SET datetime_artifacts_expire = {datetime_expires}
                        WHERE
                            token = {token}
                            AND datetime_artifacts_expire > {now}
                    """.format(
                        datetime_expires=self.config.db.decode(
                            datetime_expires, "text"
                        ),
                        token=self.config.db.decode(r.resumable_token, "text"),
                        now=self.config.db.decode(datetime_now, "text"),
                    ),
                    clear_schema_cache=False,
                ).eval("extending lifetime for artifacts of resumable records")
                self.config.db.custom_cmd(
                    # pylint: disable=consider-using-f-string
                    """
                        UPDATE artifacts
                        SET datetime_expires = {expires}
                        WHERE
                            record_id = {record_id}
                            AND datetime_expires > {now}
                    """.format(
                        expires=self.config.db.decode(
                            datetime_expires, "text"
                        ),
                        record_id=self.config.db.decode(r.id_, "text"),
                        now=self.config.db.decode(datetime_now, "text"),
                    ),
                    clear_schema_cache=False,
                ).eval("extending lifetime for artifacts of resumable records")

        # filter for records with available artifacts
        jobs = {}
        failed_records = []
        resumable_records = []
        for r in records:
            # get job-infos for all records (skip duplicates)
            if r.resumable_token not in jobs:
                jobs[r.resumable_token] = self.config.db.get_row(
                    "jobs",
                    r.resumable_token,
                    cols=["datetime_artifacts_expire", "report"],
                ).eval("querying for resumable records")

            if (
                jobs[r.resumable_token] is None
                or jobs[r.resumable_token].get("datetime_artifacts_expire")
                is None
                or jobs[r.resumable_token]["datetime_artifacts_expire"]
                < datetime_now
            ):
                info.report.log.log(
                    LoggingContext.INFO,
                    body=f"Filtered record '{r.id_}' (artifacts expired).",
                )
                context.push()
                failed_records.append(r)
                continue

            resumable_records.append(r)

        # load stages and child-reports for records
        if info.report.children is None:
            info.report.children = {}
        for r in resumable_records:
            # init from report-data
            old_record = Record.from_json(
                (jobs[r.resumable_token].get("report", {}) or {})
                .get("data", {})
                .get("records", {})
                .get(r.id_, {"id": r.id_, "stages": {}})
            )
            # use only those that were successful
            for s, si in old_record.stages.items():
                if si.success:
                    r.stages[s] = si
                    info.report.children[r.stages[s].log_id] = (
                        jobs[r.resumable_token].get("report", {}) or {}
                    ).get("children", {}).get(r.stages[s].log_id)
        context.push()

        # records need to be at least beyond import-stage to be resumable
        # (just as precaution, should never happen..)
        resumable_and_validated_records = []
        for r in resumable_records:
            if (
                Stage.IMPORT_IES not in r.stages
                and Stage.IMPORT_IPS not in r.stages
            ):
                info.report.log.log(
                    LoggingContext.INFO,
                    body=(
                        f"Filtered record '{r.id_}' (missing valid "
                        + "checkpoint)."
                    ),
                )
                context.push()
                failed_records.append(r)
                continue
            resumable_and_validated_records.append(r)

        # update records that have failed
        for r in failed_records:
            self.config.db.update(
                "records",
                {
                    "id": r.id_,
                    "status": RecordStatus.PROCESS_ERROR.value,
                    "datetime_changed": now().isoformat(),
                },
            ).eval("updating record status")
            info.report.log.log(
                LoggingContext.INFO,
                body=f"Finalized failed record '{r.id_}'.",
            )
            context.push()

        # update records that will be resumed
        for r in resumable_and_validated_records:
            self.config.db.update(
                "records",
                {
                    "id": r.id_,
                    "job_token": info.token.value,
                    "datetime_changed": now().isoformat(),
                },
            ).eval("updating record status")

        return resumable_and_validated_records

    def import_new_records(
        self,
        context: JobContext,
        info: JobInfo,
        job_config: JPJobConfig,
    ) -> list[Record]:
        """
        Runs import and returns a list of `Record`s that have been
        imported. All records are written to the database.
        """
        import_record = Record("import")
        import_stage = self.get_next_stage(import_record, job_config)[0]
        self.run_stage(
            Lock(),
            context,
            info,
            import_stage,
            job_config,
            import_record,
            skip_eval=True,
            skip_post_stage=True,
        )
        # * log and exit on error
        if (
            not info.report.children[import_record.stages[import_stage].log_id]
            .get("data", {})
            .get("success", False)
        ):
            info.report.log.merge(
                Logger.from_json(
                    info.report.children[
                        import_record.stages[import_stage].log_id
                    ].get("log", {})
                ).pick(LoggingContext.ERROR)
            )
            info.report.log.log(
                LoggingContext.ERROR,
                body="Import of new records failed.",
            )
            context.push()
            return []
        # * generate record-objects
        records = []
        for record_json in (
            info.report.children[import_record.stages[import_stage].log_id]
            .get("data", {})
            .get("records", {})
            .values()
        ):
            record = Record(
                record_json["id"],
                started=True,
                import_type=record_json.get("importType"),
                oai_identifier=record_json.get("oaiIdentifier"),
                oai_datestamp=record_json.get("oaiDatestamp"),
                hotfolder_original_path=record_json.get(
                    "hotfolderOriginalPath"
                ),
                stages={
                    import_stage: RecordStageInfo(
                        completed=True,
                        success=record_json.get("success", False),
                        token=import_record.stages[import_stage].token,
                        log_id=import_record.stages[import_stage].log_id,
                        artifact=record_json.get(
                            "ie" if import_stage is Stage.IMPORT_IES else "ip",
                            {},
                        ).get("path"),
                    )
                },
            )

            # eval
            if not record.stages[import_stage].success:
                record.status = RecordStatus.IMPORT_ERROR
                record.completed = True
                info.report.log.log(
                    LoggingContext.ERROR,
                    # pylint: disable=consider-using-f-string
                    body="Failed to import record '{}' ({}).".format(
                        record.id_,
                        record.oai_identifier
                        or record.hotfolder_original_path
                        or "<unknown-source-id>",
                    ),
                )
                info.report.data.issues += 1
                context.push()

            # run updates in database
            self.execute_record_post_stage(
                Lock(),
                context,
                info,
                import_stage,
                job_config,
                record,
                record.stages[import_stage],
            )
            records.append(record)
        return records

    def get_next_stage(
        self, record: Record, job_config: JPJobConfig
    ) -> Optional[tuple[Stage, ...]]:
        """
        Returns a tuple of `Stages` that are eligible to be run next or
        `None` if record is done.
        """
        not_completed = RecordStageInfo()

        if record.stages.get(Stage.INGEST, not_completed).completed:
            return None
        if record.stages.get(Stage.TRANSFER, not_completed).completed:
            return (Stage.INGEST,)
        if record.stages.get(Stage.BUILD_SIP, not_completed).completed:
            if job_config.test_mode:
                return None
            return (Stage.TRANSFER,)
        # FIXME: skip preparation if no operations are defined
        if record.stages.get(Stage.PREPARE_IP, not_completed).completed:
            return (Stage.BUILD_SIP,)
        if record.stages.get(
            Stage.VALIDATION_METADATA, not_completed
        ).completed and (
            record.stages.get(
                Stage.VALIDATION_PAYLOAD, not_completed
            ).completed
            or record.bitstream
            or record.skip_object_validation
        ):
            return (Stage.PREPARE_IP,)

        if job_config.template.get("type") == "hotfolder":
            if record.stages.get(Stage.IMPORT_IPS, not_completed).completed:
                if record.bitstream or record.skip_object_validation:
                    return (Stage.VALIDATION_METADATA,)
                return (Stage.VALIDATION_METADATA, Stage.VALIDATION_PAYLOAD)
            return (Stage.IMPORT_IPS,)
        # type=oai,plugin
        if record.stages.get(Stage.BUILD_IP, not_completed).completed:
            if record.bitstream or record.skip_object_validation:
                return (Stage.VALIDATION_METADATA,)
            return (Stage.VALIDATION_METADATA, Stage.VALIDATION_PAYLOAD)
        if record.stages.get(Stage.IMPORT_IES, not_completed).completed:
            return (Stage.BUILD_IP,)
        return (Stage.IMPORT_IES,)

    def get_record_status(
        self,
        stage: Stage,
        record: Record,
    ) -> RecordStatus:
        """
        Returns new record status based on
        * current stage,
        * current status.
        """
        if record.status is not RecordStatus.INPROCESS:
            return record.status

        if not record.stages[stage].success:
            match stage:
                case Stage.IMPORT_IES | Stage.IMPORT_IPS:
                    return RecordStatus.IMPORT_ERROR
                case Stage.BUILD_IP:
                    return RecordStatus.BUILDIP_ERROR
                case Stage.VALIDATION_METADATA:
                    return RecordStatus.IPVAL_ERROR
                case Stage.VALIDATION_PAYLOAD:
                    return RecordStatus.OBJVAL_ERROR
                case Stage.PREPARE_IP:
                    return RecordStatus.PREPAREIP_ERROR
                case Stage.BUILD_SIP:
                    return RecordStatus.BUILDSIP_ERROR
                case Stage.TRANSFER:
                    return RecordStatus.TRANSFER_ERROR
                case Stage.INGEST:
                    return RecordStatus.INGEST_ERROR
                case _:
                    # this should not be possible because all stages are
                    # explicitly handled above..
                    raise ValueError(
                        "Unable to determine correct error code for record "
                        + f"'{record.id_}' in stage '{stage.value}' (stage "
                        + "not successful). Defaulting to generic process-"
                        + "error instead."
                    )

        return RecordStatus.INPROCESS

    def link_record_to_ie(
        self,
        lock: Lock,
        context: JobContext,
        info: JobInfo,
        job_config: JPJobConfig,
        record: Record,
    ) -> None:
        """
        Performs the following actions:
        * Creates link from Record to IE. If no matching IE exists,
          creates one beforehand.
        * Sets RecordStatus to an error-state if critical metadata is
          missing in Record.
        * Already existing IEs are updated if previously missing
          metadata has been collected in Record.
        """
        if job_config.test_mode:
            return

        # check prerequisites
        for p, name in [
            ("origin_system_id", "origin system ID"),
            ("external_id", "external ID"),
        ]:
            if getattr(record, p) is None:
                with lock:
                    info.report.log.log(
                        LoggingContext.ERROR,
                        body=(
                            f"Record '{record.id_}' is missing an "
                            + f"{name}. Unable to identify unique IE "
                            + "in database. Aborting record."
                        ),
                    )
                record.status = RecordStatus.IPVAL_ERROR
                context.push()
                return
        if (
            job_config.template.get("target_archive", {}).get(
                "id", job_config.default_target_archive_id
            )
            is None
        ):
            with lock:
                info.report.log.log(
                    LoggingContext.ERROR,
                    body=(
                        f"Template '{job_config.template.get('id')}' "
                        + f"({job_config.template.get('name')}) does not "
                        + "provide an archive ID."
                    ),
                )
            record.status = RecordStatus.PROCESS_ERROR
            context.push()
            return

        ie_query = self.config.db.custom_cmd(
            # pylint: disable=consider-using-f-string
            """
                SELECT id, source_organization FROM ies
                WHERE
                    job_config_id = {job_config_id}
                    AND origin_system_id={origin_system_id}
                    AND external_id={external_id}
                    AND archive_id={archive_id}
            """.format(
                job_config_id=self.config.db.decode(job_config.id_, "text"),
                origin_system_id=self.config.db.decode(
                    record.origin_system_id, "text"
                ),
                external_id=self.config.db.decode(record.external_id, "text"),
                archive_id=self.config.db.decode(
                    job_config.template["target_archive"].get(
                        "id", job_config.default_target_archive_id
                    ),
                    "text",
                ),
            ),
            clear_schema_cache=False,
        ).eval("querying for IE")
        # returns either one or no row (due to db-constraint)
        if len(ie_query) == 0:
            # create new IE
            record.ie_id = self.config.db.insert(
                "ies",
                {
                    "job_config_id": job_config.id_,
                    "source_organization": record.source_organization,
                    "origin_system_id": record.origin_system_id,
                    "external_id": record.external_id,
                    "archive_id": job_config.template["target_archive"].get(
                        "id", job_config.default_target_archive_id
                    ),
                },
            ).eval("creating new IE")
        else:
            record.ie_id = ie_query[0][0]
            # update IE if possible/required
            if (
                ie_query[0][1] is None
                and record.source_organization is not None
            ):
                self.config.db.update(
                    "ies",
                    {
                        "id": record.ie_id,
                        "source_organization": record.source_organization,
                    },
                ).eval("updating IE metadata")
        # link record to IE
        self.config.db.update(
            "records",
            {
                "id": record.id_,
                "ie_id": record.ie_id,
                "datetime_changed": now().isoformat(),
            },
        ).eval("linking record to IE")

    def execute_record_post_stage(
        self,
        lock: Lock,
        context: JobContext,
        info: JobInfo,
        stage: Stage,
        job_config: JPJobConfig,
        record: Record,
        stage_info: RecordStageInfo,
    ) -> None:
        """Runs post-stage actions."""
        # update records/ies
        match stage:
            case Stage.IMPORT_IES | Stage.IMPORT_IPS:
                self.config.db.insert(
                    "records",
                    {
                        "id": record.id_,
                        "job_config_id": job_config.id_,
                        "job_token": info.token.value,
                        "status": record.status.value,
                        "datetime_changed": now().isoformat(),
                        "import_type": record.import_type,
                        "oai_identifier": record.oai_identifier,
                        "oai_datestamp": record.oai_datestamp,
                        "hotfolder_original_path": record.hotfolder_original_path,
                    },
                ).eval("creating new record")
            case Stage.VALIDATION_METADATA:
                # create/update ie and link record to ie
                self.link_record_to_ie(lock, context, info, job_config, record)
            case Stage.INGEST:
                self.config.db.update(
                    "records",
                    {
                        "id": record.id_,
                        "archive_ie_id": record.archive_ie_id,
                        "archive_sip_id": record.archive_sip_id,
                    },
                ).eval("updating record metadata")

        # add artifact to database
        if (
            stage
            in (
                Stage.IMPORT_IES,
                Stage.IMPORT_IPS,
                Stage.BUILD_IP,
                Stage.PREPARE_IP,
                Stage.BUILD_SIP,
            )
            and stage_info.artifact is not None
        ):
            self.config.db.insert(
                "artifacts",
                {
                    "path": stage_info.artifact,
                    "record_id": record.id_,
                    "stage": stage.value,
                }
                | (  # conditionally set expiration-datetime
                    {}
                    if (
                        job_config.execution_context is None
                        or job_config.execution_context.artifacts_ttl is None
                    )
                    else {
                        "datetime_expires": (
                            datetime.now()
                            + timedelta(
                                seconds=job_config.execution_context.artifacts_ttl
                            )
                        ).isoformat()
                    }
                ),
            ).eval("updating artifact-table")

    def run_stage(
        self,
        lock: Lock,
        context: JobContext,
        info: JobInfo,
        stage: Stage,
        job_config: JPJobConfig,
        record: Record,
        *,
        skip_eval: bool = False,
        skip_post_stage: bool = False,
    ) -> None:
        """Runs stage."""
        try:
            # use explicit ref to avoid threading-related issues
            stage_info = RecordStageInfo()
            with lock:
                record.stages[stage] = stage_info
            adapter = self.adapters[stage]

            # * build request body
            stage_info.token = str(uuid4())
            request_body = adapter.build_request_body(job_config, record)

            # * link report to jp-report children
            stage_info.log_id = stage_info.token + "@" + stage.value
            record_info = services.APIResult(report={"args": request_body})
            with lock:
                info.report.children[stage_info.log_id] = record_info.report
            context.push()

            # * register child-job for abort
            if context.add_child is not None:
                context.add_child(
                    ChildJob(
                        stage_info.token,
                        stage_info.log_id,
                        adapter.get_picklable_abort_callback(
                            stage_info.token,
                            stage_info.log_id,
                            adapter.__class__,
                            adapter.url,
                            adapter.interval,
                            adapter.timeout,
                            adapter.request_timeout,
                            adapter.max_retries,
                            adapter.retry_interval,
                            adapter.retry_on,
                        ),
                    )
                )
                context.push()

            # * run
            adapter.run(
                request_body,
                None,
                info=record_info,
                update_hooks=(
                    # skip updating db for these to limit the amount of
                    # redundant write operations
                    lambda i: context.push(False),
                ),
            )

            # * un-register child
            if context.remove_child is not None:
                context.remove_child(stage_info.token)
                context.push()

            # * evaluate and apply to record
            if not skip_eval:
                with lock:
                    adapter.eval(record, record_info)

                    # copy errors
                    for entry in (
                        info.report.children[stage_info.log_id]
                        .get("log", {})
                        .get(LoggingContext.ERROR.name, [])
                    ):
                        info.report.log.log(
                            LoggingContext.ERROR,
                            body=(
                                f"Running stage '{stage.value}' for record "
                                + f"'{record.id_}' caused an error: "
                                + entry["body"]
                            ),
                            origin=entry["origin"],
                        )
            stage_info.completed = True
            context.push()

            # * run post-stage
            if not skip_post_stage and stage_info.success:
                self.execute_record_post_stage(
                    lock, context, info, stage, job_config, record, stage_info
                )
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            with lock:
                record.status = RecordStatus.PROCESS_ERROR
                info.report.log.log(
                    LoggingContext.ERROR,
                    body=(
                        f"Stage '{stage.value}' failed for record "
                        + f"'{record.id_}' ({type(exc_info).__name__}): "
                        + f"{exc_info}; "
                        + (
                            format_exc()
                            if self.config.PROCESS_LOG_ERROR_TRACEBACKS
                            else ""
                        )
                    ),
                )
                record.stages[stage].completed = True
                record.stages[stage].success = False
            context.push()

    def run_record(
        self,
        lock: Lock,
        context: JobContext,
        info: JobInfo,
        job_config: JPJobConfig,
        record: Record,
        *,
        skip_db_and_post_stage: bool = False,  # useful for tests
    ) -> None:
        """Processes given record."""
        try:
            threads = []
            record.started = True
            context.push()
            next_stages = None
            while True:
                # wait until all currently valid stages are completed
                if any(t.is_alive() for t in threads):
                    sleep(self.config.PROCESS_INTERVAL)
                    continue

                # update status
                if next_stages is not None:
                    for stage in next_stages:
                        record.status = self.get_record_status(stage, record)

                # write update to database
                if not skip_db_and_post_stage:
                    self.config.db.update(
                        "records",
                        {
                            "id": record.id_,
                            "status": record.status.value,
                            "datetime_changed": now().isoformat(),
                        },
                    ).eval("updating record status")

                # exit on error
                if record.status is not RecordStatus.INPROCESS:
                    break

                # get next set of stages
                next_stages = self.get_next_stage(record, job_config)

                # all done
                if next_stages is None:
                    break

                # run stages in threads
                for stage in next_stages:
                    threads.append(
                        Thread(
                            target=self.run_stage,
                            args=(
                                lock,
                                context,
                                info,
                                stage,
                                job_config,
                                record,
                            ),
                            kwargs={"skip_post_stage": skip_db_and_post_stage},
                        )
                    )
                    with lock:
                        record.stages[stage] = RecordStageInfo()
                    context.push()
                    threads[-1].start()

            # finalize record
            # if threads are still alive, wait a bit and log
            if any(t.is_alive() for t in threads):
                for t in threads:
                    t.join(self.config.PROCESS_INTERVAL)
                    if t.is_alive():
                        with lock:
                            info.report.log.log(
                                LoggingContext.INFO,
                                body=(
                                    "A thread did not stop (record "
                                    + f"'{record.id_}')."
                                ),
                            )
            record.completed = True
            if record.status is RecordStatus.INPROCESS:
                record.status = RecordStatus.COMPLETE
                with lock:
                    info.report.log.log(
                        LoggingContext.INFO,
                        body=f"Record '{record.id_}' completed.",
                    )
            else:
                with lock:
                    info.report.log.log(
                        LoggingContext.INFO,
                        body=(
                            f"Record '{record.id_}' stopped with a "
                            + f"'{record.status.value}'."
                        ),
                    )
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            record.completed = True
            record.status = RecordStatus.PROCESS_ERROR
            with lock:
                info.report.log.log(
                    LoggingContext.ERROR,
                    body=(
                        f"Processing record '{record.id_}' failed "
                        + f"({type(exc_info).__name__}): {exc_info};"
                        + (
                            format_exc()
                            if self.config.PROCESS_LOG_ERROR_TRACEBACKS
                            else ""
                        )
                    ),
                )
        finally:
            context.push()
            # write to database
            if not skip_db_and_post_stage:
                self.config.db.update(
                    "records",
                    {
                        "id": record.id_,
                        "status": record.status.value,
                        "datetime_changed": now().isoformat(),
                    },
                ).eval("updating record status")

    def run(
        self,
        lock: Lock,
        context: JobContext,
        info: JobInfo,
        job_config: JPJobConfig,
        job: Job,
    ) -> None:
        """Run loop to manage record-processing."""
        # remove broken records from queue (should only occur after import)
        for record in job.queued.copy():
            if not record.completed:
                continue
            job.completed.append(record)
            job.queued.remove(record)

        while len(job.queued) + len(job.processing) > 0:
            # detect finished records
            for record in job.processing.copy():
                if record.thread.is_alive():
                    continue
                if record.status is not RecordStatus.COMPLETE:
                    record.status = RecordStatus.PROCESS_ERROR
                    with lock:
                        info.report.log.log(
                            LoggingContext.ERROR,
                            body=(
                                f"Processing of record '{record.id_}' "
                                + "failed (terminated without "
                                + "finalization)."
                            ),
                        )
                if record.status is not RecordStatus.COMPLETE:
                    info.report.data.issues += 1
                context.push()
                job.processing.remove(record)
                job.completed.append(record)

            # start queued records
            for record in job.queued[
                0 : max(
                    0,
                    self.config.PROCESS_RECORD_CONCURRENCY
                    - len(job.processing),
                )
            ]:
                # set status here, that way it can be used to detect
                # whether record is finished
                record.status = RecordStatus.INPROCESS
                record.thread = Thread(
                    target=self.run_record,
                    args=(lock, context, info, job_config, record),
                    daemon=True,
                )
                job.queued.remove(record)
                job.processing.append(record)
                context.push()
                record.thread.start()

            sleep(self.config.PROCESS_INTERVAL)

        failed = len(
            [r for r in job.completed if r.status is not RecordStatus.COMPLETE]
        )
        info.report.log.log(
            LoggingContext.INFO,
            body=(
                f"Processed {len(job.completed)} record(s) "
                + f"({len(job.completed) - failed} successful, "
                + f"{failed} failed)."
            ),
        )
        context.push()

    def write_report_to_database(
        self,
        token: str,
        report: Report,
        additional_cols: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Writes report to database."""
        self.config.db.update(
            "jobs",
            {"token": token, "report": report.json} | (additional_cols or {}),
        ).eval("updating report")

    def process(
        self,
        context: JobContext,
        info: JobInfo,
    ) -> None:
        """Job instructions for the '/process' endpoint."""

        # run job
        self._process(context, info)

        # finalize db
        info.report.progress.complete()
        context.push()
        self.config.db.update(
            "jobs",
            {
                "token": info.token.value,
                "status": "completed",
                "datetime_ended": now(True).isoformat(),
            },
        ).eval("updating report")

        # make callback
        self._run_callback(
            context, info, info.config.request_body.get("callback_url")
        )

    def _process(
        self,
        context: JobContext,
        info: JobInfo,
    ) -> None:
        """Job instructions for the '/process' endpoint."""

        info.report.log.set_default_origin("Job Processor")

        # de-serialize request
        job_config = JPJobConfig.from_json(info.config.request_body["process"])
        job_config.archives = self.config.archives
        job_config.default_target_archive_id = (
            self.config.DEFAULT_TARGET_ARCHIVE_ID
        )
        job_config.execution_context = JPJobContext.from_json(
            info.config.request_body["context"]
        )

        # re-initialize database-adapter
        # since we are in a separate process, we need to use a new db-adapter
        # (new connections, to be specific) as sqlite/psycopg-connections must
        # not be shared across processes
        info.report.log.log(
            LoggingContext.EVENT,
            body="Setting up database connection.",
        )
        context.push()
        self.reinitialize_database_adapter()

        # update job info in database
        self.config.db.update(
            "jobs",
            {
                "token": info.token.value,
                "datetime_artifacts_expire": (
                    datetime.now()
                    + timedelta(
                        seconds=job_config.execution_context.artifacts_ttl or 0
                    )
                ).isoformat(),
                "status": "running",
                "datetime_started": now(True).isoformat(),
            },
        ).eval("updating report")

        # validate database connection and whether report exists in db
        info.report.log.log(
            LoggingContext.EVENT,
            body="Testing database connection.",
        )
        context.push()
        try:
            if (
                self.config.db.get_row(
                    "jobs", info.token.value, cols=["token"]
                ).eval("validation database-connection and report-entry")
                is None
            ):
                raise RuntimeError("Missing db-entry for job.")
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            print(
                f"Failed to run job '{job_config.id_}' (token: "
                + f"{info.token.value}): {exc_info}",
                file=sys.stderr,
            )
            return

        # patch context.push to include a database-update for report
        _original_context_push = context.push

        def push_with_db_update(db_update: bool = True):
            if db_update:
                self.write_report_to_database(info.token.value, info.report)
            _original_context_push()

        context.push = push_with_db_update

        # initialize service-adapters
        # service-adapter are based on urllib3 and the connection-pooling used
        # here is generally not compatible with multiprocessing (causes threads
        # to get deadlocked); these adapters therefore need to be initialized
        # in the same process they are used in
        info.report.log.log(
            LoggingContext.EVENT,
            body="Setting up adapters for dcm-services.",
        )
        context.push()
        self.initialize_service_adapters()

        # pull relevant template and job-config information from
        # database and store in job_config
        info.report.log.log(
            LoggingContext.EVENT,
            body=(
                "Loading information for job-configuration with id "
                + f"'{job_config.id_}'."
            ),
        )
        context.push()
        try:
            self.load_template_and_job_config(context, info, job_config)
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            info.report.log.log(
                LoggingContext.ERROR,
                body=(
                    "Failed to load configuration "
                    + f"({type(exc_info).__name__}): {exc_info}"
                ),
            )
            info.report.data.success = False
            context.push()
            return

        # patch context to work with threading
        info.report.log.log(
            LoggingContext.EVENT,
            body="Making preparations for parallel execution.",
        )
        context.push()
        context_lock, threaded_context = self.get_threaded_job_context(context)

        # collect records from database and import module
        info.report.children = {}
        info.report.log.log(
            LoggingContext.EVENT,
            body="Collecting records.",
        )
        context.push()
        job = Job()
        try:
            # find resumable records
            if not job_config.test_mode and job_config.resume:
                job.queued = self.collect_resumable_records(
                    context, info, job_config
                )
                resumed = len(job.queued)
            else:
                resumed = 0

            # import new records
            job.queued.extend(
                self.import_new_records(context, info, job_config)
            )

            # link all collected records to report
            for record in job.queued:
                info.report.data.records[record.id_] = record
            context.push()
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            info.report.log.log(
                LoggingContext.ERROR,
                body=(
                    "Critical error while collecting records "
                    + f"({type(exc_info).__name__}): {exc_info}; "
                    + (
                        format_exc()
                        if self.config.PROCESS_LOG_ERROR_TRACEBACKS
                        else ""
                    )
                ),
            )
            info.report.data.success = False
            context.push()
            return
        info.report.log.log(
            LoggingContext.EVENT,
            body="Collected records.",
        )
        info.report.log.log(
            LoggingContext.EVENT,
            body="Entering processing loop.",
        )
        context.push()

        # enter processing loop
        if len(job.queued) == 0:
            info.report.log.log(
                LoggingContext.INFO,
                body="No records collected.",
            )
            context.push()
            return

        info.report.log.log(
            LoggingContext.INFO,
            body=(
                f"Collected {len(job.queued)} record(s) to process "
                + f"({len(job.queued) - resumed} imported, {resumed} resumed)."
            ),
        )
        context.push()
        try:
            self.run(context_lock, threaded_context, info, job_config, job)
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            info.report.log.log(
                LoggingContext.ERROR,
                body=(
                    "Critical error in main processing-loop "
                    + f"({type(exc_info).__name__}): {exc_info}; "
                    + (
                        format_exc()
                        if self.config.PROCESS_LOG_ERROR_TRACEBACKS
                        else ""
                    )
                ),
            )
            info.report.data.success = False
            context.push()
            return

        info.report.log.log(
            LoggingContext.EVENT,
            body="Processing completed.",
        )
        info.report.data.success = True
        context.push()
