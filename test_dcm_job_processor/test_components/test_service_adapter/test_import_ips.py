"""
Test module for the `ServiceAdapter` associated with `Stage.IMPORT_IPS`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage
from dcm_job_processor.components.service_adapter import ImportIPsAdapter


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return ImportIPsAdapter(url)


@pytest.fixture(name="target")
def _target():
    return {
        "path": "ips"
    }


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "import": {}
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
                    "origin": "Import Module",
                    "body": "Some event"
                },
            ]
        },
        "data": {
            "success": True,
            "IPs": {
                "ips/a": {
                    "path": "ips/a",
                    "logId": "0@native",
                }
            },
        }
    }


@pytest.fixture(name="report_fail")
def _report_fail(report):
    report["data"]["success"] = False
    return report


@pytest.fixture(name="import_module")
def _import_module(port, token, report, run_service):
    run_service(
        routes=[
            ("/import/internal", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port
    )


@pytest.fixture(name="import_module_fail")
def _import_module_fail(port, token, report_fail, run_service):
    run_service(
        routes=[
            ("/import/internal", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report_fail, 200), ["GET"]),
        ],
        port=port
    )


def fix_report_args(info: APIResult, target) -> None:
    """Fixes args in report (missing due to faked service)"""
    info.report["args"]["import"]["target"] = target


def test_run(
    adapter: ImportIPsAdapter, request_body, target, report, import_module
):
    """Test method `run` of `ImportIPsAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert info.success
    assert info.report == report


def test_run_fail(
    adapter: ImportIPsAdapter, request_body, target, report_fail, import_module_fail
):
    """Test method `run` of `ImportIPsAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert not info.success
    assert info.report == report_fail


def test_success(
    adapter: ImportIPsAdapter, request_body, target, import_module
):
    """Test property `success` of `ImportIPsAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert adapter.success(info)


def test_success_fail(
    adapter: ImportIPsAdapter, request_body, target, import_module_fail
):
    """Test property `success` of `ImportIPsAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert not adapter.success(info)


def test_export_records(
    adapter: ImportIPsAdapter, request_body, target, import_module
):
    """Test method `export_records` of `ImportIPsAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.IMPORT_IPS in records[ip_id].stages
    assert records[ip_id].stages[Stage.IMPORT_IPS].report == info.report


def test_export_records_fail(
    adapter: ImportIPsAdapter, request_body, target, import_module_fail
):
    """Test method `export_records` of `ImportIPsAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    records = adapter.export_records(info)
    assert len(records) == 1
    ip_id = list(records)[0]
    assert Stage.IMPORT_IPS in records[ip_id].stages
    assert records[ip_id].stages[Stage.IMPORT_IPS].report == info.report


def test_export_target(
    adapter: ImportIPsAdapter, request_body, target, import_module
):
    """
    Test method `export_target` of `ImportIPsAdapter`.
    """
    adapter.run(request_body, target, info := APIResult())
    target = adapter.export_target(info)
    assert target["path"] == info.report["data"]["IPs"]["ips/a"]["path"]


def test_export_target_multiple(
    adapter: ImportIPsAdapter, request_body, target, import_module
):
    """
    Test method `export_target` of `ImportIPsAdapter` for two records.
    """
    adapter.run(request_body, target, info := APIResult())
    info.report["data"]["IPs"]["ips/b"] = (
        info.report["data"]["IPs"]["ips/a"].copy()
    )
    info.report["data"]["IPs"]["ips/b"]["path"] = "ips/b"
    assert (
        adapter.export_target(info)["path"] in [
            ip["path"] for ip in info.report["data"]["IPs"].values()
        ]
    )


def test_export_target_none(
    adapter: ImportIPsAdapter, request_body, target, import_module
):
    """
    Test method `export_target` of `ImportIPsAdapter` for no records.
    """
    adapter.run(request_body, target, info := APIResult())
    info.report["data"]["IPs"] = {}
    assert adapter.export_target(info) is None
