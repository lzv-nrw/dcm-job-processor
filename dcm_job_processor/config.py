"""Configuration module for the 'Job Processor'-app."""

import os
from pathlib import Path
from importlib.metadata import version

import yaml
from dcm_common.services import OrchestratedAppConfig, DBConfig
import dcm_database
import dcm_job_processor_api


class AppConfig(OrchestratedAppConfig, DBConfig):
    """
    Configuration for the 'Job Processor'-app.
    """

    # include this in config for compatible with common orchestration-
    # app extension
    FS_MOUNT_POINT = Path.cwd()

    # ------ PROCESS ------
    REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT") or 1)
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
    PREPARATION_MODULE_HOST = (
        os.environ.get("PREPARATION_MODULE_HOST") or "http://localhost:8083"
    )
    SIP_BUILDER_HOST = (
        os.environ.get("SIP_BUILDER_HOST") or "http://localhost:8084"
    )
    TRANSFER_MODULE_HOST = (
        os.environ.get("TRANSFER_MODULE_HOST") or "http://localhost:8085"
    )
    BACKEND_HOST = (
        os.environ.get("BACKEND_HOST") or "http://localhost:8086"
    )

    # ------ EXTENSIONS ------
    DB_INIT_STARTUP_INTERVAL = 1.0

    # ------ DATABASE ------
    DB_LOAD_SCHEMA = (int(os.environ.get("DB_LOAD_SCHEMA") or 0)) == 1
    DB_SCHEMA = Path(dcm_database.__file__).parent / "init.sql"
    DB_STRICT_SCHEMA_VERSION = (
        int(os.environ.get("DB_STRICT_SCHEMA_VERSION") or 0)
    ) == 1

    # ------ IDENTIFY ------
    # generate self-description
    API_DOCUMENT = \
        Path(dcm_job_processor_api.__file__).parent / "openapi.yaml"
    API = yaml.load(
        API_DOCUMENT.read_text(encoding="utf-8"),
        Loader=yaml.SafeLoader
    )

    def __init__(self) -> None:
        if self.DB_ADAPTER == "sqlite" and self.SQLITE_DB_FILE is None:
            # this limitation comes from the requirement to use the
            # database in the app as well as in jobs (separate processes)
            # generally, database connections cannot be inherited by
            # forking the process
            raise ValueError(
                "The Job Processor does not support an in-memory "
                + "SQLite-database."
            )
        super().__init__()

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
            "request_timeout": {
                "duration": self.REQUEST_TIMEOUT,
            },
            "process_timeout": {
                "duration": self.PROCESS_TIMEOUT,
            },
        }
        settings["database"]["schemaVersion"] = version("dcm-database")

        self.CONTAINER_SELF_DESCRIPTION["configuration"]["services"] = {
            "import_module": self.IMPORT_MODULE_HOST,
            "ip_builder": self.IP_BUILDER_HOST,
            "object_validator": self.OBJECT_VALIDATOR_HOST,
            "preparation_module": self.PREPARATION_MODULE_HOST,
            "sip_builder": self.SIP_BUILDER_HOST,
            "transfer_module": self.TRANSFER_MODULE_HOST,
            "backend": self.BACKEND_HOST
        }
