"""
This module defines the `INGEST-ServiceAdapter`.
"""

from dcm_common.services import APIResult
import dcm_backend_sdk

from dcm_job_processor.models import (
    Stage,
    Record,
    JobConfig,
    ArchiveAPI,
    RecordStageInfo,
)
from .interface import ServiceAdapter


class IngestAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `INGEST`-`Stage`."""

    _STAGE = Stage.INGEST
    _SERVICE_NAME = "Backend"
    _SDK = dcm_backend_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.IngestApi(client)

    def _get_api_endpoint(self):
        return self._api_client.ingest

    def _get_abort_endpoint(self):
        return self._api_client.abort_ingest

    def build_request_body(
        self, job_config: JobConfig, record: Record
    ) -> dict:
        """Returns request body."""
        ingest = {"ingest": {}}
        if record.stages.get(self.stage, RecordStageInfo()).token is not None:
            ingest["token"] = record.stages[self.stage].token

        archive_id = job_config.template.get("target_archive", {}).get(
            "id", job_config.default_target_archive_id
        )
        if archive_id is None:
            raise ValueError(
                "Missing id of target archive (neither set in template nor "
                + "as a default for the Job Processor)."
            )
        if archive_id not in job_config.archives:
            raise ValueError(f"Unknown archive id '{archive_id}'.")

        ingest["ingest"]["archiveId"] = archive_id

        if Stage.TRANSFER in record.stages:
            match (job_config.archives[archive_id].type_):
                case ArchiveAPI.ROSETTA_REST_V0:
                    ingest["ingest"]["target"] = {
                        "subdirectory": record.stages[Stage.TRANSFER].artifact
                    }
        else:
            raise ValueError(
                f"Missing target SIP to ingest for record '{record.id_}'."
            )

        return ingest

    def eval(self, record: Record, api_result: APIResult) -> None:
        record.stages[self.stage].success = self.success(api_result)
        record.archive_sip_id = (
            api_result.report.get("data", {})
            .get("details", {})
            .get("deposit", {})
            .get("sip_id")
        )
        record.archive_ie_id = (
            api_result.report.get("data", {})
            .get("details", {})
            .get("sip", {})
            .get("iePids")
        )
