"""
Test module for the `ServiceAdapter` associated with `Stage.PREPARE_IP`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage
from dcm_job_processor.components.service_adapter import PrepareIPAdapter


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return PrepareIPAdapter(url)


@pytest.fixture(name="target")
def _target():
    return {
        "path": "ip/59438ebf-75e0-4345-8d6b-132a57e1e4f5"
    }


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "preparation": {},
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
                    "origin": "Preparation Module",
                    "body": "Some event"
                },
            ]
        },
        "data": {
            "path": "pip/028c2879-0284-4d39-9f1c-db5eb174535e",
            "success": True,
        }
    }


@pytest.fixture(name="report_fail")
def _report_fail(report):
    report["data"]["success"] = False
    return report


@pytest.fixture(name="preparation_module")
def _preparation_module(port, token, report, run_service):
    run_service(
        routes=[
            ("/prepare", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port
    )


@pytest.fixture(name="preparation_module_fail")
def _preparation_module_fail(port, token, report_fail, run_service):
    run_service(
        routes=[
            ("/prepare", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report_fail, 200), ["GET"]),
        ],
        port=port
    )


def fix_report_args(info: APIResult, target) -> None:
    """Fixes args in report (missing due to faked service)"""
    info.report["args"]["preparation"]["target"] = target


def test_run(
    adapter: PrepareIPAdapter, request_body, target, report, preparation_module
):
    """Test method `run` of `PrepareIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert info.success
    assert info.report == report


def test_run_fail(
    adapter: PrepareIPAdapter,
    request_body,
    target,
    report_fail,
    preparation_module_fail,
):
    """Test method `run` of `PrepareIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert not info.success
    assert info.report == report_fail


def test_success(
    adapter: PrepareIPAdapter, request_body, target, preparation_module
):
    """Test property `success` of `PrepareIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert adapter.success(info)


def test_success_fail(
    adapter: PrepareIPAdapter, request_body, target, preparation_module_fail
):
    """Test property `success` of `PrepareIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert not adapter.success(info)


def test_export_records(
    adapter: PrepareIPAdapter, request_body, target, preparation_module
):
    """Test method `export_records` of `PrepareIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.PREPARE_IP in records[ip_id].stages
    assert records[ip_id].stages[Stage.PREPARE_IP].report == info.report


def test_export_records_fail(
    adapter: PrepareIPAdapter, request_body, target, preparation_module_fail
):
    """Test method `export_records` of `PrepareIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.PREPARE_IP in records[ip_id].stages
    assert records[ip_id].stages[Stage.PREPARE_IP].report == info.report


def test_export_records_report_none(adapter: PrepareIPAdapter):
    """
    Test method `export_records` of `PrepareIPAdapter` for no report.
    """
    assert adapter.export_records(APIResult()) == {}


def test_export_target(
    adapter: PrepareIPAdapter, request_body, target, preparation_module
):
    """
    Test method `export_target` of `PrepareIPAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    target = adapter.export_target(info)
    assert target["path"] == info.report["data"]["path"]


def test_export_target_fail(
    adapter: PrepareIPAdapter, request_body, target, preparation_module_fail
):
    """
    Test method `export_target` of `PrepareIPAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    target = adapter.export_target(info)
    assert target is None
