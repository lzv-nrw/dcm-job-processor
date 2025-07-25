from pathlib import Path

import pytest
from dcm_common.services.tests import (
    tmp_setup, tmp_cleanup, wait_for_report, external_service, run_service
)

from dcm_job_processor import app_factory
from dcm_job_processor.config import AppConfig


@pytest.fixture(scope="session", name="temp_folder")
def _temp_folder():
    return Path("test_dcm_job_processor/temp_folder/")


@pytest.fixture(scope="session", name="fixtures")
def _fixtures():
    return Path("test_dcm_job_processor/fixtures/")


@pytest.fixture(scope="session", autouse=True)
def disable_extension_logging():
    """
    Disables the stderr-logging via the helper method `print_status`
    of the `dcm_common.services.extensions`-subpackage.
    """
    # pylint: disable=import-outside-toplevel
    from dcm_common.services.extensions.common import PrintStatusSettings

    PrintStatusSettings.silent = True


@pytest.fixture(name="testing_config")
def _testing_config():
    """Returns test-config"""
    # setup config-class
    class TestingConfig(AppConfig):
        TESTING = True
        ORCHESTRATION_AT_STARTUP = False
        ORCHESTRATION_DAEMON_INTERVAL = 0.001
        ORCHESTRATION_ORCHESTRATOR_INTERVAL = 0.001
        ORCHESTRATION_ABORT_NOTIFICATIONS_STARTUP_INTERVAL = 0.01
        DB_ADAPTER_STARTUP_IMMEDIATELY = True
        DB_ADAPTER_STARTUP_INTERVAL = 0.01
        DB_INIT_STARTUP_INTERVAL = 0.01
        DB_LOAD_SCHEMA = True

    return TestingConfig


@pytest.fixture(name="client")
def _client(testing_config):
    """
    Returns test_client.
    """

    return app_factory(testing_config(), block=True).test_client()


@pytest.fixture(name="minimal_request_body")
def _minimal_request_body():
    return {
        "process": {
            "from": "import_ies",
            "to": "import_ies",
            "args": {
                "import_ies": {
                    "import": {
                        "plugin": "plugin-id", "args": {}
                    }
                },
                "build_ip": {
                    "build": {
                        "mappingPlugin": {
                            "plugin": "plugin-0",
                            "args": {}
                        },
                    }
                }
            }
        }
    }


@pytest.fixture(name="import_report")
def _import_report(testing_config, minimal_request_body):
    return {
        "host": testing_config.IMPORT_MODULE_HOST,
        "token": {
            "value": "8ab9b96b-313a-417c-af73-3c5f3f80606b",
            "expires": True,
            "expires_at": "2024-08-09T13:15:10+00:00"
        },
        "args": minimal_request_body.get("args", {}).get("import_ies", {}),
        "progress": {
            "status": "completed",
            "verbose": "Job terminated normally.",
            "numeric": 100
        },
        "log": {},
        "data": {
            "success": True,
            "IEs": {
                "ie0": {
                    "path": "ie/4a814fe6-b44e-4546-95ec-5aee27cc1d8c",
                    "sourceIdentifier": "test:oai_dc:f50036dd-b4ef",
                    "fetchedPayload": True,
                    "IPIdentifier": None
                }
            },
        }
    }


@pytest.fixture(name="ip_builder_report")
def _ip_builder_report(testing_config, run_service, minimal_request_body):
    return {
        "host": testing_config.IP_BUILDER_HOST,
        "token": {
            "value": "90bdd738-9358-465c-a04e-cce3e4b013ba",
            "expires": True,
            "expires_at": "2024-08-09T13:15:10+00:00"
        },
        "args": minimal_request_body.get("args", {}).get("build_ip", {}),
        "progress": {
            "status": "completed",
            "verbose": "Job terminated normally.",
            "numeric": 100
        },
        "log": {},
        "data": {
            "requestType": "build",
            "success": True,
            "path": "ip/389fb73c-25c0-40c6-8e17-3612729f6644",
            "valid": True,
            "originSystemId": "origin",
            "externalId": "external",
            "details": {}
        }
    }
