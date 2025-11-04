from pathlib import Path
from uuid import UUID, uuid3, uuid4

import pytest
from flask import jsonify, request
from dcm_common.services.tests import (
    tmp_setup, tmp_cleanup, wait_for_report, external_service, run_service
)

from dcm_job_processor.config import AppConfig
from dcm_job_processor.models import ArchiveAPI


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
def _testing_config(temp_folder):
    """Returns test-config"""
    # setup config-class
    class TestingConfig(AppConfig):
        TESTING = True
        ORCHESTRA_DAEMON_INTERVAL = 0.01
        ORCHESTRA_WORKER_INTERVAL = 0.01
        ORCHESTRA_WORKER_ARGS = {"messages_interval": 0.01}
        DB_ADAPTER_STARTUP_IMMEDIATELY = True
        DB_ADAPTER_STARTUP_INTERVAL = 0.01
        DB_INIT_STARTUP_INTERVAL = 0.01
        REQUEST_POLL_INTERVAL = 0.01
        DB_LOAD_SCHEMA = True
        SQLITE_DB_FILE = temp_folder / str(uuid4())

        TEST_ARCHIVE_ID = "test-archive"
        TEST_DESTINATION_ID = "destination-0"
        # pylint: disable=consider-using-f-string
        ARCHIVES_SRC = '[{{"id": "{}", "type": "{}", "transferDestinationId": "{}"}}]'.format(
            TEST_ARCHIVE_ID,
            ArchiveAPI.ROSETTA_REST_V0.value,
            TEST_DESTINATION_ID,
        )

        PROCESS_INTERVAL = 0.01
        REQUEST_POLL_INTERVAL = 0.01

    return TestingConfig


@pytest.fixture(name="demo_data")
def _demo_data():
    class DemoData:
        uuid_namespace = UUID("96ee5d00-d6fe-4993-9a2d-49670a65f2cf")

        template0 = str(uuid3(uuid_namespace, name="template0"))
        job_config0 = str(uuid3(uuid_namespace, name="job_config0"))

    return DemoData


@pytest.fixture(name="config_with_initialized_db")
def _config_with_initialized_db(testing_config, demo_data):
    """Generates config-object and prefills config.db with demo-data."""
    testing_config.DB_LOAD_SCHEMA = False
    config = testing_config()

    # load schema
    config.db.read_file(config.DB_SCHEMA).eval("db initialization")

    # create template
    config.db.insert(
        "templates",
        {
            "id": demo_data.template0,
            "name": "template 0",
            "type": "plugin",
            "additional_information": {"plugin": "test", "args": {}},
            "target_archive": {"id": testing_config.TEST_ARCHIVE_ID},
        },
    )

    # create job configuration
    config.db.insert(
        "job_configs",
        {
            "id": demo_data.job_config0,
            "template_id": demo_data.template0,
            "name": "job 0",
            "data_selection": {},
            "data_processing": {
                "mapping": {
                    "type": "plugin",
                    "data": {"plugin": "test", "args": {}},
                },
                "preparation": {
                    "rightsOperations": [
                        {
                            "type": "complement",
                            "targetField": "DC-Rights",
                            "value": "a",
                        }
                    ]
                },
            },
        },
    )

    return config


@pytest.fixture(name="token")
def _token():
    return {"value": "token", "expires": False}


@pytest.fixture(name="base_report")
def _base_report(token):
    return {
        "host": "",
        "token": token,
        "args": {},
        "progress": {
            "status": "completed",
            "verbose": "",
            "numeric": 100,
        },
    }


@pytest.fixture(name="dcm_services")
def _dcm_services(config_with_initialized_db, token, base_report, run_service):
    def run_dcm_service(host, endpoint, report_data):
        run_service(
            routes=[
                (
                    endpoint,
                    lambda: (jsonify(token), 201),
                    ["POST"],
                ),
                (
                    "/report",
                    lambda: (
                        jsonify(base_report | {"data": report_data}),
                        200,
                    ),
                    ["GET"],
                ),
            ],
            port=host.rsplit(":")[-1],
        )

    # run simple services
    run_dcm_service(
        config_with_initialized_db.IMPORT_MODULE_HOST,
        "/import/ies",
        {
            "success": True,
            "records": {
                (rid0 := str(uuid4())): {
                    "id": rid0,
                    "importType": "oai",
                    "oaiIdentifier": "a",
                    "oaiDatestamp": "9999",
                    "ie": {"path": "c"},
                    "completed": True,
                    "success": True,
                },
                (rid1 := str(uuid4())): {
                    "id": rid1,
                    "importType": "oai",
                    "completed": True,
                    "success": False,
                },
            },
        },
    )
    run_dcm_service(
        config_with_initialized_db.OBJECT_VALIDATOR_HOST,
        "/validate",
        {"success": True, "valid": True, "details": {}},
    )
    run_dcm_service(
        config_with_initialized_db.PREPARATION_MODULE_HOST,
        "/prepare",
        {"success": True, "path": "pip"},
    )
    run_dcm_service(
        config_with_initialized_db.SIP_BUILDER_HOST,
        "/build",
        {"success": True, "path": "sip"},
    )
    run_dcm_service(
        config_with_initialized_db.TRANSFER_MODULE_HOST,
        "/transfer",
        {"success": True},
    )
    run_dcm_service(
        config_with_initialized_db.BACKEND_HOST,
        "/ingest",
        {
            "success": True,
            "details": {
                "archiveApi": "rosetta-rest-api-v0",
                "deposit": {"sip_id": "sip-id"},
                "sip": {"iePids": "ie-pid"},
            },
        },
    )

    # run ip builder (special because of two different endpoints)
    build_token = str(uuid4())
    run_service(
        routes=[
            (
                "/build",
                lambda: (
                    jsonify({"value": build_token, "expires": False}),
                    201,
                ),
                ["POST"],
            ),
            (
                "/validate",
                lambda: (
                    jsonify(token),
                    201,
                ),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | (
                            {
                                "data": {
                                    "requestType": "build",
                                    "success": True,
                                    "path": "ip",
                                    "valid": True,
                                    "details": {},
                                },
                            }
                            if (request.args["token"] == build_token)
                            else {
                                "data": {
                                    "requestType": "validation",
                                    "success": True,
                                    "sourceOrganization": "x",
                                    "originSystemId": "y",
                                    "externalId": "z",
                                    "valid": True,
                                    "details": {},
                                }
                            }
                        )
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=config_with_initialized_db.IP_BUILDER_HOST.rsplit(":")[-1],
    )
