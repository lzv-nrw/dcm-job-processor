"""
Test module for the `ServiceAdapter` associated with `Stage.IMPORT_IPS`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage, JobConfig, Record, RecordStageInfo
from dcm_job_processor.components.service_adapter import ImportIPsAdapter


@pytest.fixture(name="url")
def _url():
    return "http://localhost:8080"


@pytest.fixture(name="adapter")
def _adapter(url):
    return ImportIPsAdapter(url)


@pytest.fixture(name="report")
def _report(url):
    return {
        "host": url,
        "token": {
            "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
            "expires": True,
            "expires_at": "2024-08-09T13:15:10+00:00",
        },
        "args": {"import": {}},
        "progress": {
            "status": "completed",
            "verbose": "Job terminated normally.",
            "numeric": 100,
        },
        "log": {
            "EVENT": [
                {
                    "datetime": "2024-08-09T12:15:10+00:00",
                    "origin": "Import Module",
                    "body": "Some event",
                },
            ]
        },
        "data": {
            "success": True,
            "records": {
                "b9c5b0b2-2fde-429a-bf3a-743ca1a2f448": {
                    "id": "b9c5b0b2-2fde-429a-bf3a-743ca1a2f448",
                    "importType": "hotfolder",
                    "hotfolderOriginalPath": "ip-0",
                    "ip": {
                        "path": "ip/abcde-12345-fghijk-67890"
                    },
                    "completed": True,
                    "success": True,
                }
            },
        },
    }


@pytest.mark.parametrize(
    "success", [True, False], ids=["success", "no-success"]
)
def test_success(success, adapter: ImportIPsAdapter, report):
    """Test method `ImportIPsAdapter.success`."""
    report["data"]["success"] = success
    assert adapter.success(APIResult(report=report)) is success


@pytest.mark.parametrize(
    ("job_config", "record", "expected_request_body", "error"),
    [
        (
            JobConfig(
                "",
                _template={},
                _data_selection={"path": "a"},
            ),
            Record(""),
            None,
            True,
        ),
        (
            JobConfig(
                "",
                _template={
                    "additional_information": {"source_id": "hotfolder-0"}
                },
                _data_selection={},
            ),
            Record(""),
            None,
            True,
        ),
        (
            JobConfig(
                "",
                _template={
                    "additional_information": {"source_id": "hotfolder-0"}
                },
                _data_selection={"path": "a"},
            ),
            Record(""),
            {
                "import": {
                    "target": {"hotfolderId": "hotfolder-0", "path": "a"},
                    "test": False,
                }
            },
            False,
        ),
        (
            JobConfig(
                "",
                _template={
                    "additional_information": {"source_id": "hotfolder-0"}
                },
                _data_selection={"path": "a"},
                test_mode=True
            ),
            Record(""),
            {
                "import": {
                    "target": {"hotfolderId": "hotfolder-0", "path": "a"},
                    "test": True,
                },
            },
            False,
        ),
        (
            JobConfig(
                "",
                _template={
                    "additional_information": {"source_id": "hotfolder-0"}
                },
                _data_selection={"path": "a"},
            ),
            Record("", stages={Stage.IMPORT_IPS: RecordStageInfo(token="b")}),
            {
                "import": {
                    "target": {"hotfolderId": "hotfolder-0", "path": "a"},
                    "test": False,
                },
                "token": "b",
            },
            False,
        ),
    ],
    ids=[
        "missing-hotfolder-id",
        "missing-hotfolder-path",
        "minimal",
        "test-mode",
        "token",
    ],
)
def test_build_request_body_simple(
    job_config, record, expected_request_body, error, adapter: ImportIPsAdapter
):
    """Test method `ImportIPsAdapter.build_request_body`."""
    if error:
        with pytest.raises(ValueError) as exc_info:
            adapter.build_request_body(job_config, record)
        print(exc_info.value)
    else:
        assert (
            adapter.build_request_body(job_config, record)
            == expected_request_body
        )


def test_eval(adapter: ImportIPsAdapter, report):
    """Test method `ImportIPsAdapter.eval`."""
    with pytest.raises(RuntimeError):
        adapter.eval(Record(""), APIResult(report=report))
