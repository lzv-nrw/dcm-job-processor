"""
This module defines the `BUILD_IP-ServiceAdapter`.
"""

from dcm_common.services import APIResult
import dcm_ip_builder_sdk

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
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

    def _get_abort_endpoint(self):
        return self._api_client.abort_build

    def build_request_body(
        self, job_config: JobConfig, record: Record
    ) -> dict:
        """Returns request body."""
        build_ip = {"build": {}}
        if record.stages.get(self.stage, RecordStageInfo()).token is not None:
            build_ip["token"] = record.stages[self.stage].token

        if Stage.IMPORT_IES in record.stages:
            build_ip["build"]["target"] = {
                "path": record.stages[Stage.IMPORT_IES].artifact
            }
        else:
            raise ValueError(
                f"Missing target IP to build for record '{record.id_}'."
            )

        build_ip["build"]["validate"] = False

        type_ = job_config.data_processing.get("mapping", {}).get("type")

        if type_ == "plugin":
            build_ip["build"]["mappingPlugin"] = (
                job_config.data_processing.get("mapping", {}).get("data")
            )

        if type_ == "python":
            build_ip["build"]["mappingPlugin"] = {
                "plugin": "generic-mapper-plugin-string",
                "args": {
                    "mapper": {
                        "string": (
                            job_config.data_processing.get("mapping", {})
                            .get("data", {})
                            .get("contents")
                        ),
                        "args": {},
                    }
                },
            }

        if type_ == "xslt":
            build_ip["build"]["mappingPlugin"] = {
                "plugin": "xslt-plugin",
                "args": {
                    "xslt": (
                        job_config.data_processing.get("mapping", {})
                        .get("data", {})
                        .get("contents")
                    )
                },
            }

        return build_ip

    def eval(self, record: Record, api_result: APIResult) -> None:
        record.stages[self.stage].success = self.success(api_result)
        record.stages[self.stage].artifact = api_result.report.get(
            "data", {}
        ).get("path")
