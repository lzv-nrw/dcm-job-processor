"""
Test module for the package `dcm-job-processor-sdk`.
"""

from time import sleep
from pathlib import Path
from uuid import uuid4

import pytest
import dcm_job_processor_sdk

from dcm_job_processor import app_factory


@pytest.fixture(name="app")
def _app(testing_config):
    testing_config.ORCHESTRATION_AT_STARTUP = True
    return app_factory(testing_config(), as_process=True)


@pytest.fixture(name="default_sdk", scope="module")
def _default_sdk():
    return dcm_job_processor_sdk.DefaultApi(
        dcm_job_processor_sdk.ApiClient(
            dcm_job_processor_sdk.Configuration(
                host="http://localhost:8087"
            )
        )
    )


@pytest.fixture(name="process_sdk", scope="module")
def _process_sdk():
    return dcm_job_processor_sdk.ProcessApi(
        dcm_job_processor_sdk.ApiClient(
            dcm_job_processor_sdk.Configuration(
                host="http://localhost:8087"
            )
        )
    )


def test_default_ping(
    default_sdk: dcm_job_processor_sdk.DefaultApi, app, run_service
):
    """Test default endpoint `/ping-GET`."""

    run_service(app, port=8087, probing_path="ready")

    response = default_sdk.ping()

    assert response == "pong"


def test_default_status(
    default_sdk: dcm_job_processor_sdk.DefaultApi, app, run_service
):
    """Test default endpoint `/identify-GET`."""

    run_service(app, port=8087, probing_path="ready")

    response = default_sdk.get_status()

    assert response.ready


def test_default_identify(
    default_sdk: dcm_job_processor_sdk.DefaultApi, app, run_service,
    testing_config
):
    """Test default endpoint `/identify-GET`."""

    run_service(app, port=8087, probing_path="ready")

    response = default_sdk.identify()

    assert response.to_dict() == testing_config().CONTAINER_SELF_DESCRIPTION


def test_process_report(
    process_sdk: dcm_job_processor_sdk.ProcessApi,
    testing_config,
    temp_folder,
    run_service,
    import_report,
    minimal_request_body,
):
    """Test endpoints `/process-POST` and `/report-GET`."""
    # prepare test-setup
    testing_config.ORCHESTRATION_AT_STARTUP = True
    testing_config.SQLITE_DB_FILE = Path(temp_folder / str(uuid4()))
    config = testing_config()
    app = app_factory(config, as_process=True)

    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/report", lambda: (import_report, 200), ["GET"]),
        ],
        port=8080
    )
    run_service(app, port=8087, probing_path="ready")

    submission = process_sdk.process(
        minimal_request_body | {"context": {"triggerType": "manual"}}
    )

    while True:
        try:
            report = process_sdk.get_report(token=submission.value)
            break
        except dcm_job_processor_sdk.exceptions.ApiException as e:
            assert e.status == 503
            sleep(0.1)

    report = process_sdk.get_report(token=submission.value)
    assert report.data.success
    assert len(report.children) == 1

    # validate database is being written
    jobs = config.db.get_rows("jobs").eval()
    assert len(jobs) == 1
    assert jobs[0]["token"] == submission.value

    records = config.db.get_rows("records").eval()
    assert len(records) == 1
    assert records[0]["job_token"] == submission.value


def test_process_report_404(
    process_sdk: dcm_job_processor_sdk.ProcessApi, app, run_service
):
    """Test process endpoint `/report-GET` without previous submission."""

    run_service(app, port=8087, probing_path="ready")

    with pytest.raises(dcm_job_processor_sdk.rest.ApiException) as exc_info:
        process_sdk.get_report(token="some-token")
    assert exc_info.value.status == 404
