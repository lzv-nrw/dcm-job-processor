"""
This module defines the `VALIDATION_PAYLOAD-ServiceAdapter`.
"""

from dcm_common.services import APIResult
import dcm_object_validator_sdk

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
from .interface import ServiceAdapter


class ValidationPayloadAdapter(ServiceAdapter):
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

    def build_request_body(
        self, job_config: JobConfig, record: Record
    ) -> dict:
        """Returns request body."""
        validation = {"validation": {}}
        if record.stages.get(self.stage, RecordStageInfo()).token is not None:
            validation["token"] = record.stages[self.stage].token

        if Stage.BUILD_IP in record.stages:
            validation["validation"]["target"] = {
                "path": record.stages[Stage.BUILD_IP].artifact
            }
        elif Stage.IMPORT_IPS in record.stages:
            validation["validation"]["target"] = {
                "path": record.stages[Stage.IMPORT_IPS].artifact
            }
        else:
            raise ValueError(
                "Missing target IP to validate payload for record "
                + f"'{record.id_}'."
            )

        validation["validation"]["plugins"] = {
            "integrity": {"plugin": "integrity-bagit", "args": {}},
            "format": {
                "plugin": "jhove-fido-mimetype-bagit",
                "args": {},
            },
        }

        return validation

    def success(self, info: APIResult) -> bool:
        return info.report.get("data", {}).get("valid", False)

    def eval(self, record: Record, api_result: APIResult) -> None:
        record.stages[self.stage].success = self.success(api_result)
