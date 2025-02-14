"""
This module defines the `BUILD_IP-ServiceAdapter`.
"""

from typing import Any
from pathlib import Path

from dcm_common.services import APIResult
import dcm_ip_builder_sdk

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record
from .interface import ServiceAdapter


class BuildIPAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `BUILD_IP`-`Stage`."""
    _STAGE = Stage.BUILD_IP
    _SERVICE_NAME = "IP Builder"
    _SDK = dcm_ip_builder_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.BuildApi(client)

    def _get_api_endpoint(self):
        return self._api_client.build

    def _build_request_body(self, base_request_body: dict, target: Any):
        if target is not None:
            if "build" not in base_request_body:
                base_request_body["build"] = {}
            base_request_body["build"]["target"] = target
        return base_request_body

    def success(self, info: APIResult) -> bool:
        return info.report.get("data", {}).get("success", False)

    def export_target(self, info: APIResult) -> Any:
        try:
            if info.report["data"]["success"]:
                return {"path": info.report["data"]["path"]}
        except KeyError:
            pass
        return None

    def export_records(self, info: APIResult) -> dict[str, Record]:
        try:
            ip_path = info.report["data"]["path"]
        except KeyError:
            return {}
        return {
            Path(ip_path).name: Record(
                False, stages={
                    self._STAGE: APIResult(
                        True, self.success(info), info.report
                    )
                }
            )
        }
