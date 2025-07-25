"""
This module defines the `VALIDATION_METADATA-ServiceAdapter`.
"""

from typing import Any

import dcm_ip_builder_sdk

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record, APIResult
from .validation import ValidationAdapter


class ValidationMetadataAdapter(ValidationAdapter):
    """`ServiceAdapter` for the `VALIDATION_METADATA`-`Stage`."""
    _STAGE = Stage.VALIDATION_METADATA
    _SERVICE_NAME = "IP Builder"
    _SDK = dcm_ip_builder_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ValidationApi(client)

    def _get_api_endpoint(self):
        return self._api_client.validate

    def _build_request_body(self, base_request_body: dict, target: Any):
        if target is not None:
            if "validation" not in base_request_body:
                base_request_body["validation"] = {}
            base_request_body["validation"]["target"] = target
        return base_request_body

    def post_process_record(self, info: APIResult, record: Record) -> None:
        if info.report is None:
            return

        record.origin_system_id = info.report.get("data", {}).get(
            "originSystemId"
        )
        record.external_id = info.report.get("data", {}).get("externalId")
