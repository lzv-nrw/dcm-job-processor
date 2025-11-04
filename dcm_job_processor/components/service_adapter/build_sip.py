"""
This module defines the `BUILD_SIP-ServiceAdapter`.
"""

from dcm_common.services import APIResult
import dcm_sip_builder_sdk

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
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

    def _get_abort_endpoint(self):
        return self._api_client.abort

    def build_request_body(
        self, job_config: JobConfig, record: Record
    ) -> dict:
        """Returns request body."""
        build_sip = {"build": {}}
        if record.stages.get(self.stage, RecordStageInfo()).token is not None:
            build_sip["token"] = record.stages[self.stage].token

        if Stage.PREPARE_IP in record.stages:
            build_sip["build"]["target"] = {
                "path": record.stages[Stage.PREPARE_IP].artifact
            }
        elif Stage.BUILD_IP in record.stages:  # after import_ies
            build_sip["build"]["target"] = {
                "path": record.stages[Stage.BUILD_IP].artifact
            }
        elif Stage.IMPORT_IPS in record.stages:
            build_sip["build"]["target"] = {
                "path": record.stages[Stage.IMPORT_IPS].artifact
            }
        else:
            raise ValueError(
                f"Missing target SIP to build for record '{record.id_}'."
            )

        return build_sip

    def eval(self, record: Record, api_result: APIResult) -> None:
        record.stages[self.stage].success = self.success(api_result)
        record.stages[self.stage].artifact = api_result.report.get(
            "data", {}
        ).get("path")
