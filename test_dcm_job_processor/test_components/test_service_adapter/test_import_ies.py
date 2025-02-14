"""
Test module for the `ServiceAdapter` associated with `Stage.IMPORT_IES`.

Note that this module also contains the tests for the `ServiceAdapter`-
interface itself.
"""

from time import sleep

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage
from dcm_job_processor.components.service_adapter import ImportIEsAdapter


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return ImportIEsAdapter(url)


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "import": {"plugin": "demo", "args": {"number": 1}}
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


@pytest.fixture(name="import_module")
def _import_module(port, token, report, run_service):
    run_service(
        routes=[
            ("/import/external", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port
    )


@pytest.fixture(name="run_result")
def _run_result(
    adapter: ImportIEsAdapter, request_body, import_module
):
    """Returns result of `ImportIEsAdapter.run`."""
    adapter.run(request_body, None, info := APIResult())
    return info


def test_export_records_minimal(
    adapter: ImportIEsAdapter, run_result: APIResult
):
    """Test method `export_records` of `ImportIEsAdapter`."""
    records = adapter.export_records(run_result)
    assert len(records) == 1
    ie_id = run_result.report["data"]["IEs"]["ie0"]["sourceIdentifier"]
    assert ie_id in records
    assert Stage.IMPORT_IES in records[ie_id].stages
    assert (
        records[ie_id].stages[Stage.IMPORT_IES].report["data"]["IEs"][ie_id]
        == run_result.report["data"]["IEs"]["ie0"]
    )


def test_export_records_missing_source_identifier(
    adapter: ImportIEsAdapter, run_result: APIResult
):
    """
    Test behavior of method `export_records` of `ImportIEsAdapter` for a
    missing 'sourceIdentifier'.
    """
    del run_result.report["data"]["IEs"]["ie0"]["sourceIdentifier"]
    records = adapter.export_records(run_result)
    assert "ie0" in records


def test_export_records_two_records(
    adapter: ImportIEsAdapter, run_result: APIResult
):
    """
    Test behavior of method `export_records` of `ImportIEsAdapter` for
    two records.
    """
    ie_id0 = run_result.report["data"]["IEs"]["ie0"]["sourceIdentifier"]
    ie_id1 = "oai:123"
    run_result.report["data"]["IEs"]["ie1"] = (
        run_result.report["data"]["IEs"]["ie0"].copy()
    )
    run_result.report["data"]["IEs"]["ie1"]["sourceIdentifier"] = ie_id1
    records = adapter.export_records(run_result)
    assert len(records) == 2
    assert ie_id0 in records
    assert ie_id1 in records
    assert (
        records[ie_id0].stages[Stage.IMPORT_IES].report["data"]["IEs"]
        != records[ie_id1].stages[Stage.IMPORT_IES].report["data"]["IEs"]
    )


@pytest.mark.parametrize(
    "fetched_payload",
    (True, False),
    ids=["good", "bad"]
)
def test_export_records_bad(
    fetched_payload, adapter: ImportIEsAdapter, run_result: APIResult
):
    """
    Test behavior of method `export_records` of `ImportIEsAdapter` for
    a bad IE.
    """
    run_result.report["data"]["IEs"]["ie0"]["fetchedPayload"] = fetched_payload
    records = adapter.export_records(run_result)
    assert len(records) == 1
    assert not records[
        run_result.report["data"]["IEs"]["ie0"]["sourceIdentifier"]
    ].completed
    assert records[
        run_result.report["data"]["IEs"]["ie0"]["sourceIdentifier"]
    ].success is None
    assert records[
        run_result.report["data"]["IEs"]["ie0"]["sourceIdentifier"]
    ].stages[Stage.IMPORT_IES].completed
    assert records[
        run_result.report["data"]["IEs"]["ie0"]["sourceIdentifier"]
    ].stages[Stage.IMPORT_IES].success is fetched_payload


def test_export_target_single(
    adapter: ImportIEsAdapter, run_result: APIResult
):
    """
    Test method `export_target` of `ImportIEsAdapter` for single record.
    """
    assert (
        adapter.export_target(run_result)["path"]
        == run_result.report["data"]["IEs"]["ie0"]["path"]
    )


def test_export_target_multiple(
    adapter: ImportIEsAdapter, run_result: APIResult
):
    """
    Test method `export_target` of `ImportIEsAdapter` for two records.
    """
    run_result.report["data"]["IEs"]["ie1"] = (
        run_result.report["data"]["IEs"]["ie0"].copy()
    )
    run_result.report["data"]["IEs"]["ie1"]["sourceIdentifier"] = "oai:123"
    assert (
        adapter.export_target(run_result)["path"] in [
            ie["path"] for ie in run_result.report["data"]["IEs"].values()
        ]
    )


def test_export_target_none(
    adapter: ImportIEsAdapter, run_result: APIResult
):
    """
    Test method `export_target` of `ImportIEsAdapter` for no records.
    """
    del run_result.report["data"]["IEs"]["ie0"]
    assert adapter.export_target(run_result) is None
