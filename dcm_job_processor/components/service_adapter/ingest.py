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
            if "rosetta" not in base_request_body["ingest"]:
                base_request_body["ingest"]["rosetta"] = {}
            base_request_body["ingest"]["rosetta"]["subdir"] = (
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
            sip_path = info.report["args"]["ingest"]["rosetta"]["subdir"]
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
