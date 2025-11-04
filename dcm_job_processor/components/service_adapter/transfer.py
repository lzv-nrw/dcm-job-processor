"""
This module defines the `TRANSFER-ServiceAdapter`.
"""

from pathlib import Path

from dcm_common.services import APIResult
import dcm_transfer_module_sdk

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
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

    def _get_abort_endpoint(self):
        return self._api_client.abort

    def build_request_body(
        self, job_config: JobConfig, record: Record
    ) -> dict:
        """Returns request body."""
        transfer = {"transfer": {}}
        if record.stages.get(self.stage, RecordStageInfo()).token is not None:
            transfer["token"] = record.stages[self.stage].token

        if Stage.BUILD_SIP in record.stages:
            transfer["transfer"]["target"] = {
                "path": record.stages[Stage.BUILD_SIP].artifact
            }
        else:
            raise ValueError(
                f"Missing target SIP to transfer for record '{record.id_}'."
            )

        archive_id = job_config.template.get("target_archive", {}).get(
            "id", job_config.default_target_archive_id
        )
        if archive_id is None:
            raise ValueError(
                "Missing id of target archive (neither set in template nor "
                + "as a default for the Job Processor)."
            )
        if archive_id not in job_config.archives:
            raise ValueError(f"Unknown archive id '{archive_id}'.")
        transfer["transfer"]["destinationId"] = job_config.archives[
            archive_id
        ].transfer_destination_id

        return transfer

    def eval(self, record: Record, api_result: APIResult) -> None:
        record.stages[self.stage].success = self.success(api_result)
        if (
            Stage.BUILD_SIP in record.stages
            and record.stages[Stage.BUILD_SIP].artifact is not None
        ):
            record.stages[self.stage].artifact = Path(
                record.stages[Stage.BUILD_SIP].artifact
            ).name
