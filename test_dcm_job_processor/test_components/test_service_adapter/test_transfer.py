"""
Test module for the `ServiceAdapter` associated with `Stage.TRANSFER`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage
from dcm_job_processor.components.service_adapter import TransferAdapter


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return TransferAdapter(url)


@pytest.fixture(name="target")
def _target():
    return {
        "path": "sip/59438ebf-75e0-4345-8d6b-132a57e1e4f5"
    }


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "transfer": {}
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
                    "origin": "Transfer Module",
                    "body": "Some event"
                },
            ]
        },
        "data": {
            "success": True,
        }
    }


@pytest.fixture(name="report_fail")
def _report_fail(report):
    report["data"]["success"] = False
    return report


@pytest.fixture(name="transfer_module")
def _transfer_module(port, token, report, run_service):
    run_service(
        routes=[
            ("/transfer", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port
    )


@pytest.fixture(name="transfer_module_fail")
def _transfer_module_fail(port, token, report_fail, run_service):
    run_service(
        routes=[
            ("/transfer", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report_fail, 200), ["GET"]),
        ],
        port=port
    )


def fix_report_args(info: APIResult, target) -> None:
    """Fixes args in report (missing due to faked service)"""
    info.report["args"]["transfer"]["target"] = target


def test_run(
    adapter: TransferAdapter, request_body, target, report, transfer_module
):
    """Test method `run` of `TransferAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert info.success
    assert info.report == report


def test_run_fail(
    adapter: TransferAdapter, request_body, target, report_fail, transfer_module_fail
):
    """Test method `run` of `TransferAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert not info.success
    assert info.report == report_fail


def test_success(
    adapter: TransferAdapter, request_body, target, transfer_module
):
    """Test property `success` of `TransferAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert adapter.success(info)


def test_success_fail(
    adapter: TransferAdapter, request_body, target, transfer_module_fail
):
    """Test property `success` of `TransferAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert not adapter.success(info)


def test_export_records(
    adapter: TransferAdapter, request_body, target, transfer_module
):
    """Test method `export_records` of `TransferAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.TRANSFER in records[ip_id].stages


def test_export_records_fail(
    adapter: TransferAdapter, request_body, target, transfer_module_fail
):
    """Test method `export_records` of `TransferAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.TRANSFER in records[ip_id].stages


def test_export_records_report_none(adapter: TransferAdapter):
    """
    Test method `export_records` of `TransferAdapter` for no report.
    """
    assert adapter.export_records(APIResult()) == {}


def test_export_target(
    adapter: TransferAdapter, request_body, target, transfer_module
):
    """
    Test method `export_target` of `TransferAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    target = adapter.export_target(info)
    assert info.report["args"]["transfer"]["target"]["path"].endswith(
        target["path"]
    )


def test_export_target_fail(
    adapter: TransferAdapter, request_body, target, transfer_module_fail
):
    """
    Test method `export_target` of `TransferAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    target = adapter.export_target(info)
    assert target is None
