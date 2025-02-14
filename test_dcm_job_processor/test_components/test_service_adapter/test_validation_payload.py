"""
Test module for the `ServiceAdapter` associated with `Stage.VALIDATION_PAYLOAD`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage
from dcm_job_processor.components.service_adapter import (
    ValidationPayloadAdapter
)


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return ValidationPayloadAdapter(url)


@pytest.fixture(name="target")
def _target():
    return {
        "path": "ip/59438ebf-75e0-4345-8d6b-132a57e1e4f5"
    }


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "validation": {
            "plugins": {"request-0": {"plugin": "plugin-0", "args": {}}}
        }
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
                    "origin": "Object Validator",
                    "body": "Some event"
                },
            ]
        },
        "data": {
            "success": True,
            "valid": True,
            "details": {}
        }
    }


@pytest.fixture(name="report_fail")
def _report_fail(report):
    report["data"]["valid"] = False
    return report


@pytest.fixture(name="object_validator")
def _object_validator(port, token, report, run_service):
    run_service(
        routes=[
            ("/validate", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port
    )


@pytest.fixture(name="object_validator_fail")
def _object_validator_fail(port, token, report_fail, run_service):
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
    adapter: ValidationPayloadAdapter, request_body, target, report, object_validator
):
    """Test method `run` of `ValidationPayloadAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert info.success
    assert info.report == report


def test_run_fail(
    adapter: ValidationPayloadAdapter, request_body, target, report_fail, object_validator_fail
):
    """Test method `run` of `ValidationPayloadAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert not info.success
    assert info.report == report_fail


def test_success(
    adapter: ValidationPayloadAdapter, request_body, target, object_validator
):
    """Test property `success` of `ValidationPayloadAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert adapter.success(info)


def test_success_fail(
    adapter: ValidationPayloadAdapter, request_body, target, object_validator_fail
):
    """Test property `success` of `ValidationPayloadAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert not adapter.success(info)


def test_export_records(
    adapter: ValidationPayloadAdapter, request_body, target, object_validator
):
    """Test method `export_records` of `ValidationPayloadAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.VALIDATION_PAYLOAD in records[ip_id].stages
    assert (
        records[ip_id].stages[Stage.VALIDATION_PAYLOAD].report == info.report
    )


def test_export_records_fail(
    adapter: ValidationPayloadAdapter, request_body, target, object_validator_fail
):
    """Test method `export_records` of `ValidationPayloadAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.VALIDATION_PAYLOAD in records[ip_id].stages
    assert (
        records[ip_id].stages[Stage.VALIDATION_PAYLOAD].report == info.report
    )


def test_export_target(
    adapter: ValidationPayloadAdapter, request_body, target, object_validator
):
    """
    Test method `export_target` of `ValidationPayloadAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    target = adapter.export_target(info)
    assert target == info.report["args"]["validation"]["target"]


def test_export_target_fail(
    adapter: ValidationPayloadAdapter, request_body, target, object_validator_fail
):
    """
    Test method `export_target` of `ValidationPayloadAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    target = adapter.export_target(info)
    assert target is None
