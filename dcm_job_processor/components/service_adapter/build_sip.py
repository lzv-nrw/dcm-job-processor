"""
This module defines the `BUILD_SIP-ServiceAdapter`.
"""

from typing import Any
from pathlib import Path

from dcm_common.services import APIResult
import dcm_sip_builder_sdk

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record
from .interface import ServiceAdapter


class BuildSIPAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `BUILD_SIP`-`Stage`."""
    _STAGE = Stage.BUILD_SIP
    _SERVICE_NAME = "SIP Builder"
    _SDK = dcm_sip_builder_sdk

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
            sip_path = info.report["data"]["path"]
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
