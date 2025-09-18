"""
This module defines the `IMPORT_IPS-ServiceAdapter`.
"""

from typing import Any

from dcm_common.services import APIResult
import dcm_import_module_sdk

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record, RecordStageInfo
from .interface import ServiceAdapter


class ImportIPsAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `IMPORT_IPS`-`Stage`."""
    _STAGE = Stage.IMPORT_IPS
    _SERVICE_NAME = "Import Module"
    _SDK = dcm_import_module_sdk

    def __init__(self, *args, **kwargs) -> None:
        self._exported_targets = []
        super().__init__(*args, **kwargs)

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ImportApi(client)

    def _get_api_endpoint(self):
        return self._api_client.import_ips

    def _get_abort_endpoint(self):
        return self._api_client.abort

    def _build_request_body(self, base_request_body: dict, target: Any):
        if target is not None:
            if "import" not in base_request_body:
                base_request_body["import"] = {}
            base_request_body["import"]["target"] = target
        return base_request_body

    def success(self, info: APIResult) -> bool:
        return info.report.get("data", {}).get("success", False)

    def export_target(self, info: APIResult) -> Any:
        next_export = next(
            (
                ip["path"]
                for ip in info.report.get("data", {}).get("IPs", {}).values()
                if ip["path"] not in self._exported_targets
            ),
            None
        )
        if next_export is None:
            return None
        self._exported_targets.append(next_export)
        return {"path": next_export}

    def export_records(self, info: APIResult) -> dict[str, Record]:
        if info.report is None:
            return {}

        return {
            ip["path"]: Record(
                False, stages={
                    self._STAGE: RecordStageInfo(
                        True, True, None
                    )
                }
            )
            for ip in info.report.get("data", {}).get("IPs", {}).values()
        }
