"""
This module defines the `VALIDATION-ServiceAdapter`.
"""

from typing import Any
from pathlib import Path

from dcm_common.services import APIResult
import dcm_ip_builder_sdk

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record
from .interface import ServiceAdapter


class _FakeAPI:
    """Dummy-API class"""
    get_report = None


class ValidationAdapter(ServiceAdapter):
    """
    `ServiceAdapter` for the `VALIDATION`-`Stage`.

    This adapter does not define any logic for making requests as it
    only represents an adapter for a meta-stage. Instead, it defines the
    common methods for the export of targets and records for all
    associated sub-stages.
    """
    _STAGE = Stage.VALIDATION
    _SERVICE_NAME = "Job Processor"
    _SDK = dcm_ip_builder_sdk

    def _get_api_clients(self):
        return None, _FakeAPI()

    def _get_api_endpoint(self):
        pass

    def _build_request_body(self, base_request_body: dict, target: Any):
        pass

    def success(self, info: APIResult) -> bool:
        return info.report.get("data", {}).get("valid", False)

    def export_target(self, info: APIResult) -> Any:
        try:
            if info.report["data"]["valid"]:
                return info.report["args"]["validation"]["target"]
        except KeyError:
            pass
        return None

    def export_records(self, info: APIResult) -> dict[str, Record]:
        try:
            ip_path = info.report["args"]["validation"]["target"]["path"]
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
