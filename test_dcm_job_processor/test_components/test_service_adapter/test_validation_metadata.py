"""
Test module for the `ServiceAdapter` associated with `Stage.VALIDATION_METADATA`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage, Record
from dcm_job_processor.components.service_adapter import (
    ValidationMetadataAdapter
)


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return ValidationMetadataAdapter(url)


@pytest.fixture(name="target")
def _target():
    return {
        "path": "ip/59438ebf-75e0-4345-8d6b-132a57e1e4f5"
    }


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "validation": {"modules": ["bagit_profile"]}
    }


@pytest.fixture(name="token")
def _token():
    return {
        "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
        "expires": True,
        "expires_at": "2024-08-09T13:15:10+00:00"
    }


@pytest.fixture(name="report")
def _report(url, token, request_body):
    return {
        "host": url,
        "token": token,
        "args": request_body,
        "progress": {
            "status": "completed",
            "verbose": "Job terminated normally.",
            "numeric": 100
        },
        "log": {
            "EVENT": [
                {
                    "datetime": "2024-08-09T12:15:10+00:00",
                    "origin": "IP Builder",
                    "body": "Some event"
                },
            ]
        },
        "data": {
            "requestType": "validation",
            "success": True,
            "valid": True,
            "originSystemId": "origin",
            "externalId": "external",
            "details": {}
        }
    }


@pytest.fixture(name="report_fail")
def _report_fail(report):
    report["data"]["valid"] = False
    return report


@pytest.fixture(name="ip_builder")
def _ip_builder(port, token, report, run_service):
    run_service(
        routes=[
            ("/validate", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port
    )


@pytest.fixture(name="ip_builder_fail")
def _ip_builder_fail(port, token, report_fail, run_service):
    run_service(
        routes=[
            ("/validate", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report_fail, 200), ["GET"]),
        ],
        port=port
    )


def fix_report_args(info: APIResult, target) -> None:
    """Fixes args in report (missing due to faked service)"""
    info.report["args"]["validation"]["target"] = target


def test_run(
    adapter: ValidationMetadataAdapter, request_body, target, report, ip_builder
):
    """Test method `run` of `ValidationMetadataAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert info.success
    assert info.report == report


def test_run_fail(
    adapter: ValidationMetadataAdapter, request_body, target, report_fail, ip_builder_fail
):
    """Test method `run` of `ValidationMetadataAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert not info.success
    assert info.report == report_fail


def test_success(
    adapter: ValidationMetadataAdapter, request_body, target, ip_builder
):
    """Test property `success` of `ValidationMetadataAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert adapter.success(info)


def test_success_fail(
    adapter: ValidationMetadataAdapter, request_body, target, ip_builder_fail
):
    """Test property `success` of `ValidationMetadataAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert not adapter.success(info)


def test_export_records(
    adapter: ValidationMetadataAdapter, request_body, target, ip_builder
):
    """Test method `export_records` of `ValidationMetadataAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.VALIDATION_METADATA in records[ip_id].stages
    assert (
        records[ip_id].stages[Stage.VALIDATION_METADATA].report == info.report
    )


def test_export_records_fail(
    adapter: ValidationMetadataAdapter, request_body, target, ip_builder_fail
):
    """Test method `export_records` of `ValidationMetadataAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.VALIDATION_METADATA in records[ip_id].stages
    assert (
        records[ip_id].stages[Stage.VALIDATION_METADATA].report == info.report
    )


def test_export_records_report_none(adapter: ValidationMetadataAdapter):
    """
    Test method `export_records` of `ValidationMetadataAdapter` for no report.
    """
    assert adapter.export_records(APIResult()) == {}


def test_export_target(
    adapter: ValidationMetadataAdapter, request_body, target, ip_builder
):
    """
    Test method `export_target` of `ValidationMetadataAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    target = adapter.export_target(info)
    assert target == info.report["args"]["validation"]["target"]


def test_export_target_fail(
    adapter: ValidationMetadataAdapter, request_body, target, ip_builder_fail
):
    """
    Test method `export_target` of `ValidationMetadataAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    target = adapter.export_target(info)
    assert target is None


def test_post_process_record(
    adapter: ValidationMetadataAdapter, request_body, report, target, ip_builder
):
    """
    Test method `post_process_record` of `ValidationMetadataAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    record = Record()
    adapter.post_process_record(info, record)
    assert record.external_id == report["data"]["externalId"]
    assert record.origin_system_id == report["data"]["originSystemId"]
