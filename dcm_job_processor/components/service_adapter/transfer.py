"""
This module defines the `TRANSFER-ServiceAdapter`.
"""

from typing import Any
from pathlib import Path

from dcm_common.services import APIResult
import dcm_transfer_module_sdk

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record
from .interface import ServiceAdapter


class TransferAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `TRANSFER`-`Stage`."""
    _STAGE = Stage.TRANSFER
    _SERVICE_NAME = "Transfer Module"
    _SDK = dcm_transfer_module_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.TransferApi(client)

    def _get_api_endpoint(self):
        return self._api_client.transfer

    def _build_request_body(self, base_request_body: dict, target: Any):
        if target is not None:
            if "transfer" not in base_request_body:
                base_request_body["transfer"] = {}
            base_request_body["transfer"]["target"] = target
        return base_request_body

    def success(self, info: APIResult) -> bool:
        return info.report.get("data", {}).get("success", False)

    def export_target(self, info: APIResult) -> Any:
        try:
            if info.report["data"]["success"]:
                return {
                    "path": Path(
                        info.report["args"]["transfer"]["target"]["path"]
                    ).name
                }
        except KeyError:
            pass
        return None

    def export_records(self, info: APIResult) -> dict[str, Record]:
        if info.report is None:
            return {}

        try:
            sip_path = info.report["args"]["transfer"]["target"]["path"]
        except KeyError:
            return {}
        return {
            Path(sip_path).name: Record(
                False, stages={
                    self._STAGE: APIResult(
                        True, self.success(info), info.report
                    )
                }
            )
        }
