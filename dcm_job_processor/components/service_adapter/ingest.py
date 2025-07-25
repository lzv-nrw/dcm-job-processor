"""
This module defines the `INGEST-ServiceAdapter`.
"""

from typing import Any

from dcm_common.services import APIResult
import dcm_backend_sdk

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record
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

    def _build_request_body(self, base_request_body: dict, target: Any):
        if target is not None:
            if "ingest" not in base_request_body:
                base_request_body["ingest"] = {}
            if "archiveId" not in base_request_body["ingest"]:
                base_request_body["ingest"]["archiveId"] = ""
            if "target" not in base_request_body["ingest"]:
                base_request_body["ingest"]["target"] = {}
            # TODO: the next step depends on the specific archive-system
            base_request_body["ingest"]["target"]["subdirectory"] = (
                target["path"]
            )
        return base_request_body

    def success(self, info: APIResult) -> bool:
        return info.report.get("data", {}).get("success", False)

    def export_target(self, info: APIResult) -> Any:
        return None

    def export_records(self, info: APIResult) -> dict[str, Record]:
        if info.report is None:
            return {}

        try:
            # TODO: this also needs to be updated to support different
            # archive systems
            sip_path = info.report["args"]["ingest"]["target"]["subdirectory"]
        except KeyError:
            return {}
        return {
            sip_path: Record(
                False, stages={
                    self._STAGE: APIResult(
                        True, self.success(info), info.report
                    )
                }
            )
        }

    def post_process_record(self, info: APIResult, record: Record) -> None:
        if info.report is None:
            return

        record.sip_id = (
            info.report.get("data", {}).get("details", {})
            .get("deposit", {})
            .get("sip_id")
        )
        record.ie_id = (
            info.report.get("data", {}).get("details", {})
            .get("sip", {})
            .get("iePids")
        )
