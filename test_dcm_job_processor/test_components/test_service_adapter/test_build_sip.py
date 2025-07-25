"""
Test module for the `ServiceAdapter` associated with `Stage.BUILD_SIP`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage
from dcm_job_processor.components.service_adapter import BuildSIPAdapter


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return BuildSIPAdapter(url)


@pytest.fixture(name="target")
def _target():
    return {
        "path": "pip/59438ebf-75e0-4345-8d6b-132a57e1e4f5"
    }


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "build": {},
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
                    "origin": "SIP Builder",
                    "body": "Some event"
                },
            ]
        },
        "data": {
            "path": "sip/028c2879-0284-4d39-9f1c-db5eb174535e",
            "success": True,
        }
    }


@pytest.fixture(name="report_fail")
def _report_fail(report):
    report["data"]["success"] = False
    return report


@pytest.fixture(name="sip_builder")
def _sip_builder(port, token, report, run_service):
    run_service(
        routes=[
            ("/build", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port
    )


@pytest.fixture(name="sip_builder_fail")
def _sip_builder_fail(port, token, report_fail, run_service):
    run_service(
        routes=[
            ("/build", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report_fail, 200), ["GET"]),
        ],
        port=port
    )


def fix_report_args(info: APIResult, target) -> None:
    """Fixes args in report (missing due to faked service)"""
    info.report["args"]["build"]["target"] = target


def test_run(
    adapter: BuildSIPAdapter, request_body, target, report, sip_builder
):
    """Test method `run` of `BuildSIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert info.success
    assert info.report == report


def test_run_fail(
    adapter: BuildSIPAdapter, request_body, target, report_fail, sip_builder_fail
):
    """Test method `run` of `BuildSIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert not info.success
    assert info.report == report_fail


def test_success(
    adapter: BuildSIPAdapter, request_body, target, sip_builder
):
    """Test property `success` of `BuildSIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert adapter.success(info)


def test_success_fail(
    adapter: BuildSIPAdapter, request_body, target, sip_builder_fail
):
    """Test property `success` of `BuildSIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert not adapter.success(info)


def test_export_records(
    adapter: BuildSIPAdapter, request_body, target, sip_builder
):
    """Test method `export_records` of `BuildSIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.BUILD_SIP in records[ip_id].stages
    assert records[ip_id].stages[Stage.BUILD_SIP].report == info.report


def test_export_records_fail(
    adapter: BuildSIPAdapter, request_body, target, sip_builder_fail
):
    """Test method `export_records` of `BuildSIPAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.BUILD_SIP in records[ip_id].stages
    assert records[ip_id].stages[Stage.BUILD_SIP].report == info.report


def test_export_records_report_none(adapter: BuildSIPAdapter):
    """
    Test method `export_records` of `BuildSIPAdapter` for no report.
    """
    assert adapter.export_records(APIResult()) == {}


def test_export_target(
    adapter: BuildSIPAdapter, request_body, target, sip_builder
):
    """
    Test method `export_target` of `BuildSIPAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    target = adapter.export_target(info)
    assert target["path"] == info.report["data"]["path"]


def test_export_target_fail(
    adapter: BuildSIPAdapter, request_body, target, sip_builder_fail
):
    """
    Test method `export_target` of `BuildSIPAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    target = adapter.export_target(info)
    assert target is None
