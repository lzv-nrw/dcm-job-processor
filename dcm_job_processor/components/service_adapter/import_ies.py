"""
This module defines the `IMPORT_IES-ServiceAdapter`.
"""

import dcm_import_module_sdk

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
from .interface import ServiceAdapter


class ImportIEsAdapter(ServiceAdapter):
    """`ServiceAdapter` for the `IMPORT_IES`-`Stage`."""

    _STAGE = Stage.IMPORT_IES
    _SERVICE_NAME = "Import Module"
    _SDK = dcm_import_module_sdk

    def __init__(self, *args, **kwargs) -> None:
        self._exported_targets = []
        super().__init__(*args, **kwargs)

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ImportApi(client)

    def _get_api_endpoint(self):
        return self._api_client.import_ies

    def _get_abort_endpoint(self):
        return self._api_client.abort

    def build_request_body(
        self, job_config: JobConfig, record: Record
    ) -> dict:
        """Returns request body."""
        import_ies = {"import": {}}
        if record.stages.get(self.stage, RecordStageInfo()).token is not None:
            import_ies["token"] = record.stages[self.stage].token

        if job_config.template.get("type") == "plugin":
            if "plugin" not in job_config.template.get(
                "additional_information", {}
            ):
                raise ValueError(
                    "Missing plugin-identifier while formatting job from "
                    + f"template '{job_config.template.get('id')}'."
                )
            import_ies["import"].update(
                {
                    "plugin": job_config.template["additional_information"][
                        "plugin"
                    ],
                    "args": (
                        job_config.template["additional_information"].get(
                            "args", {}
                        )
                    ),
                }
            )

        if job_config.template.get("type") == "oai":
            import_ies["import"].update(
                {
                    "plugin": "oai_pmh_v2",
                    "args": {
                        "base_url": job_config.template.get(
                            "additional_information", {}
                        ).get("url"),
                        "metadata_prefix": job_config.template.get(
                            "additional_information", {}
                        ).get("metadata_prefix"),
                    },
                    "jobConfigId": job_config.id_,
                }
            )

            transfer_url_info = job_config.template.get(
                "additional_information", {}
            ).get("transfer_url_filters")
            if transfer_url_info is not None:
                import_ies["import"]["args"][
                    "transfer_url_info"
                ] = transfer_url_info

            if job_config.data_selection is not None:
                sets = job_config.data_selection.get("sets")
                if sets is not None:
                    import_ies["import"]["args"]["set_spec"] = sets

                from_ = job_config.data_selection.get("from")
                if from_ is not None:
                    import_ies["import"]["args"]["from_"] = from_

                until = job_config.data_selection.get("until")
                if until is not None:
                    import_ies["import"]["args"]["until"] = until

                identifiers = job_config.data_selection.get("identifiers")
                if identifiers is not None:
                    import_ies["import"]["args"]["identifiers"] = identifiers

        if import_ies.get("import", {}).get("plugin") is None:
            raise ValueError(
                f"Unknown template-type '{job_config.template.get('type')}'."
            )

        import_ies["import"]["args"]["test"] = job_config.test_mode

        return import_ies

    def eval(self, record, api_result) -> None:
        raise RuntimeError("This method should not be called.")
