"""
This module defines the `IMPORT_IES-ServiceAdapter`.
"""

from typing import Any
from copy import deepcopy

from dcm_common.services import APIResult
import dcm_import_module_sdk

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record
from .interface import ServiceAdapter


class ImportIEsAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `IMPORT_IES`-`Stage`."""
    _STAGE = Stage.IMPORT_IES
    _SERVICE_NAME = "Import Module"
    _SDK = dcm_import_module_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ImportApi(client)

    def _get_api_endpoint(self):
        return self._api_client.import_external

    def _build_request_body(self, base_request_body: dict, target: Any):
        return base_request_body

    def success(self, info: APIResult) -> bool:
        return info.report.get("data", {}).get("success", False)

    def export_target(self, info: APIResult) -> Any:
        return next(
            (
                {"path": ie["path"]}
                for ie in info.report.get("data", {}).get("IEs", {}).values()
                if ie.get("fetchedPayload", False)
            ),
            None
        )

    def export_records(self, info: APIResult) -> dict[str, Record]:
        def _patch_report(report: dict, ie_id: str, ie: dict) -> dict:
            """Returns a report with replaced data-field."""
            _report = deepcopy(report)
            _report["data"]["IEs"] = {
                ie_id: ie
            }
            return _report
        return {
            ie.get("sourceIdentifier", ie_id): Record(
                False, stages={
                    self._STAGE: APIResult(
                        True, ie["fetchedPayload"], _patch_report(
                            info.report, ie.get("sourceIdentifier", ie_id), ie
                        )
                    )
                }
            )
            for ie_id, ie in info.report.get("data", {}).get("IEs", {}).items()
        }
