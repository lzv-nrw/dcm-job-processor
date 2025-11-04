"""
This module defines the `IMPORT_IPS-ServiceAdapter`.
"""

import dcm_import_module_sdk

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
from .interface import ServiceAdapter


class ImportIPsAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `IMPORT_IPS`-`Stage`."""

    _STAGE = Stage.IMPORT_IPS
    _SERVICE_NAME = "Import Module"
    _SDK = dcm_import_module_sdk

    def __init__(self, *args, **kwargs) -> None:
        self._exported_targets = []
        super().__init__(*args, **kwargs)

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ImportApi(client)

    def _get_api_endpoint(self):
        return self._api_client.import_ips

    def _get_abort_endpoint(self):
        return self._api_client.abort

    def build_request_body(
        self, job_config: JobConfig, record: Record
    ) -> dict:
        """Returns request body."""
        import_ips = {"import": {}}
        if record.stages.get(self.stage, RecordStageInfo()).token is not None:
            import_ips["token"] = record.stages[self.stage].token

        source_id = job_config.template.get("additional_information", {}).get(
            "source_id"
        )
        if source_id is None:
            raise ValueError(
                "Missing id of hotfolder."
            )
        path = job_config.data_selection.get("path")
        if path is None:
            raise ValueError("Missing target path in hotfolder.")
        import_ips["import"]["target"] = {
            "hotfolderId": source_id,
            "path": path,
        }

        import_ips["import"]["test"] = job_config.test_mode

        return import_ips

    def eval(self, record, api_result) -> None:
        raise RuntimeError("This method should not be called.")
