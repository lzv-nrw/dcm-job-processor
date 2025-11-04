"""
This module defines the `PREPARE_IP-ServiceAdapter`.
"""

from dcm_common.services import APIResult
import dcm_preparation_module_sdk

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
from .interface import ServiceAdapter


class PrepareIPAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `PREPARE_IP`-`Stage`."""

    _STAGE = Stage.PREPARE_IP
    _SERVICE_NAME = "Preparation Module"
    _SDK = dcm_preparation_module_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.PreparationApi(client)

    def _get_api_endpoint(self):
        return self._api_client.prepare

    def _get_abort_endpoint(self):
        return self._api_client.abort

    def build_request_body(
        self, job_config: JobConfig, record: Record
    ) -> dict:
        """Returns request body."""
        prepare_ip = {"preparation": {}}
        if record.stages.get(self.stage, RecordStageInfo()).token is not None:
            prepare_ip["token"] = record.stages[self.stage].token

        if Stage.BUILD_IP in record.stages:
            prepare_ip["preparation"]["target"] = {
                "path": record.stages[Stage.BUILD_IP].artifact
            }
        elif Stage.IMPORT_IPS in record.stages:
            prepare_ip["preparation"]["target"] = {
                "path": record.stages[Stage.IMPORT_IPS].artifact
            }
        else:
            raise ValueError(
                f"Missing target IP to prepare for record '{record.id_}'."
            )

        rights_operations = job_config.data_processing.get(
            "preparation", {}
        ).get("rightsOperations")
        preservation_operations = job_config.data_processing.get(
            "preparation", {}
        ).get("preservationOperations")
        if (
            rights_operations is not None
            or preservation_operations is not None
        ):
            # Both 'rightsOperations' and 'preservationOperations' are
            # treated as 'bagInfoOperations' from the Preparation Module-API.
            # The two properties are separated in the backend-API to mirror
            # their separation in the client.
            prepare_ip["preparation"]["bagInfoOperations"] = (
                rights_operations or []
            ) + (preservation_operations or [])

        if record.bitstream:
            if "bagInfoOperations" not in prepare_ip["preparation"]:
                prepare_ip["preparation"]["bagInfoOperations"] = []
            prepare_ip["preparation"]["bagInfoOperations"].append(
                {
                    "type": "set",
                    "targetField": "Preservation-Level",
                    "value": "Bitstream",
                }
            )

        sig_prop_operations = job_config.data_processing.get(
            "preparation", {}
        ).get("sigPropOperations")
        if sig_prop_operations is not None:
            prepare_ip["preparation"][
                "sigPropOperations"
            ] = sig_prop_operations

        return prepare_ip

    def eval(self, record: Record, api_result: APIResult) -> None:
        record.stages[self.stage].success = self.success(api_result)
        record.stages[self.stage].artifact = api_result.report.get(
            "data", {}
        ).get("path")
