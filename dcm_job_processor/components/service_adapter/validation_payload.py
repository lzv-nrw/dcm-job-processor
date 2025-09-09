"""
This module defines the `VALIDATION_PAYLOAD-ServiceAdapter`.
"""

from typing import Any

import dcm_object_validator_sdk

from dcm_job_processor.models.job_config import Stage
from .validation import ValidationAdapter


class ValidationPayloadAdapter(ValidationAdapter):
    """`ServiceAdapter` for the `VALIDATION_PAYLOAD`-`Stage`."""
    _STAGE = Stage.VALIDATION_PAYLOAD
    _SERVICE_NAME = "Object Validator"
    _SDK = dcm_object_validator_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ValidationApi(client)

    def _get_api_endpoint(self):
        return self._api_client.validate

    def _get_abort_endpoint(self):
        return self._api_client.abort

    def _build_request_body(self, base_request_body: dict, target: Any):
        if "validation" not in base_request_body:
            base_request_body["validation"] = {}
        if target is not None:
            base_request_body["validation"]["target"] = target
        if "plugins" not in base_request_body["validation"]:
            base_request_body["validation"]["plugins"] = {}
        return base_request_body
