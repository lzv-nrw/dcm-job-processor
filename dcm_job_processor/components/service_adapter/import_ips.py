"""
This module defines the `IMPORT_IPS-ServiceAdapter`.
"""

from typing import Any
from copy import deepcopy

from dcm_common.services import APIResult
import dcm_import_module_sdk

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record
from .interface import ServiceAdapter


class ImportIPsAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `IMPORT_IPS`-`Stage`."""
    _STAGE = Stage.IMPORT_IPS
    _SERVICE_NAME = "Import Module"
    _SDK = dcm_import_module_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ImportApi(client)

    def _get_api_endpoint(self):
        return self._api_client.import_internal

    def _build_request_body(self, base_request_body: dict, target: Any):
        if target is not None:
            if "import" not in base_request_body:
                base_request_body["import"] = {}
            base_request_body["import"]["target"] = target
        return base_request_body

    def success(self, info: APIResult) -> bool:
        return info.report.get("data", {}).get("success", False)

    def export_target(self, info: APIResult) -> Any:
        return next(
            (
                {"path": ip["path"]}
                for ip in info.report.get("data", {}).get("IPs", {}).values()
            ),
            None
        )

    def export_records(self, info: APIResult) -> dict[str, Record]:
        def _patch_report(report: dict, ip_id: str, ip: dict) -> dict:
            """Returns a report with replaced data-field."""
            _report = deepcopy(report)
            _report["data"]["IPs"] = {
                ip_id: ip
            }
            return _report
        return {
            ip["path"]: Record(
                False, stages={
                    self._STAGE: APIResult(
                        True, True, _patch_report(
                            info.report, ip["path"], ip
                        )
                    )
                }
            )
            for ip in info.report.get("data", {}).get("IPs", {}).values()
        }
