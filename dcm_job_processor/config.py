"""Configuration module for the 'Job Processor'-app."""

import os
from pathlib import Path
from importlib.metadata import version

import yaml
from dcm_common.services import OrchestratedAppConfig
import dcm_job_processor_api


class AppConfig(OrchestratedAppConfig):
    """
    Configuration for the 'Job Processor'-app.
    """

    # include this in config for compatible with common orchestration-
    # app extension
    FS_MOUNT_POINT = Path.cwd()

    # ------ PROCESS ------
    PROCESS_TIMEOUT = int(os.environ.get("PROCESS_TIMEOUT") or 30)

    IMPORT_MODULE_HOST = (
        os.environ.get("IMPORT_MODULE_HOST") or "http://localhost:8080"
    )
    IP_BUILDER_HOST = (
        os.environ.get("IP_BUILDER_HOST") or "http://localhost:8081"
    )
    OBJECT_VALIDATOR_HOST = (
        os.environ.get("OBJECT_VALIDATOR_HOST") or "http://localhost:8082"
    )
    SIP_BUILDER_HOST = (
        os.environ.get("SIP_BUILDER_HOST") or "http://localhost:8083"
    )
    TRANSFER_MODULE_HOST = (
        os.environ.get("TRANSFER_MODULE_HOST") or "http://localhost:8084"
    )
    BACKEND_HOST = (
        os.environ.get("BACKEND_HOST") or "http://localhost:8085"
    )

    # ------ IDENTIFY ------
    # generate self-description
    API_DOCUMENT = \
        Path(dcm_job_processor_api.__file__).parent / "openapi.yaml"
    API = yaml.load(
        API_DOCUMENT.read_text(encoding="utf-8"),
        Loader=yaml.SafeLoader
    )

    def set_identity(self) -> None:
        super().set_identity()
        self.CONTAINER_SELF_DESCRIPTION["description"] = (
            "This API provides job processor-related endpoints."
        )

        # version
        self.CONTAINER_SELF_DESCRIPTION["version"]["api"] = (
            self.API["info"]["version"]
        )
        self.CONTAINER_SELF_DESCRIPTION["version"]["app"] = version(
            "dcm-job-processor"
        )

        # configuration
        settings = self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
        settings["process"] = {
            "timeout": {
                "duration": self.PROCESS_TIMEOUT,
            },
        }

        self.CONTAINER_SELF_DESCRIPTION["configuration"]["services"] = {
            "import_module": self.IMPORT_MODULE_HOST,
            "ip_builder": self.IP_BUILDER_HOST,
            "object_validator": self.OBJECT_VALIDATOR_HOST,
            "sip_builder": self.SIP_BUILDER_HOST,
            "transfer_module": self.TRANSFER_MODULE_HOST,
            "backend": self.BACKEND_HOST
        }
