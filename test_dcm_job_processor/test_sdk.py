"""
Test module for the package `dcm-job-processor-sdk`.
"""

from time import sleep
from uuid import uuid4

import pytest
import dcm_job_processor_sdk

from dcm_job_processor import app_factory


@pytest.fixture(name="sdk_testing_config")
def _sdk_testing_config(testing_config):
    class SDKTestingConfig(testing_config):
        DB_ADAPTER_STARTUP_IMMEDIATELY = True

    return SDKTestingConfig


@pytest.fixture(name="default_sdk", scope="module")
def _default_sdk():
    return dcm_job_processor_sdk.DefaultApi(
        dcm_job_processor_sdk.ApiClient(
            dcm_job_processor_sdk.Configuration(host="http://localhost:8087")
        )
    )


@pytest.fixture(name="process_sdk", scope="module")
def _process_sdk():
    return dcm_job_processor_sdk.ProcessApi(
        dcm_job_processor_sdk.ApiClient(
            dcm_job_processor_sdk.Configuration(host="http://localhost:8087")
        )
    )


def test_default_ping(
    default_sdk: dcm_job_processor_sdk.DefaultApi,
    sdk_testing_config,
    run_service,
):
    """Test default endpoint `/ping-GET`."""

    run_service(
        from_factory=lambda: app_factory(sdk_testing_config()), port=8087
    )

    response = default_sdk.ping()

    assert response == "pong"


def test_default_status(
    default_sdk: dcm_job_processor_sdk.DefaultApi,
    sdk_testing_config,
    run_service,
):
    """Test default endpoint `/identify-GET`."""

    run_service(
        from_factory=lambda: app_factory(sdk_testing_config()),
        port=8087,
        probing_path="ready",
    )

    response = default_sdk.get_status()

    assert response.ready


def test_default_identify(
    default_sdk: dcm_job_processor_sdk.DefaultApi,
    sdk_testing_config,
    run_service,
):
    """Test default endpoint `/identify-GET`."""

    run_service(
        from_factory=lambda: app_factory(sdk_testing_config()), port=8087
    )

    response = default_sdk.identify()

    assert (
        response.to_dict() == sdk_testing_config().CONTAINER_SELF_DESCRIPTION
    )


def test_process_report(
    process_sdk: dcm_job_processor_sdk.ProcessApi,
    config_with_initialized_db,
    sdk_testing_config,
    demo_data,
    dcm_services,
    run_service,
):
    """Test endpoints `/process-POST` and `/report-GET`."""

    run_service(
        from_factory=lambda: app_factory(sdk_testing_config()),
        port=8087,
        probing_path="ready",
    )

    submission = process_sdk.process(
        {
            "process": {
                "id": demo_data.job_config0,
            },
            "context": {
                "artifactsTTL": 1,
                "triggerType": "manual",
            },
        }
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
    assert report.data.issues == 1
    assert len(report.data.records) == 2
    assert len(report.children) == 8

    # validate database is being written
    jobs = config_with_initialized_db.db.get_rows("jobs").eval()
    assert len(jobs) == 1
    assert jobs[0]["token"] == submission.value

    records = config_with_initialized_db.db.get_rows("records").eval()
    assert len(records) == 2
    assert records[0]["job_token"] == submission.value


def test_process_report_404(
    process_sdk: dcm_job_processor_sdk.ProcessApi,
    sdk_testing_config,
    run_service,
):
    """Test process endpoint `/report-GET` without previous submission."""
    run_service(
        from_factory=lambda: app_factory(sdk_testing_config()),
        port=8087,
        probing_path="ready",
    )

    with pytest.raises(dcm_job_processor_sdk.rest.ApiException) as exc_info:
        process_sdk.get_report(token="some-token")
    assert exc_info.value.status == 404
