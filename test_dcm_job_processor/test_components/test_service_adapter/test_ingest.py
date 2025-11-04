"""
Test module for the `ServiceAdapter` associated with `Stage.INGEST`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import (
    Stage,
    JobConfig,
    Record,
    RecordStageInfo,
    ArchiveConfiguration,
    ArchiveAPI,
)
from dcm_job_processor.components.service_adapter import IngestAdapter


@pytest.fixture(name="url")
def _url():
    return "http://localhost:8080"


@pytest.fixture(name="adapter")
def _adapter(url):
    return IngestAdapter(url)


@pytest.fixture(name="report")
def _report(url):
    return {
        "host": url,
        "token": {
            "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
            "expires": True,
            "expires_at": "2024-08-09T13:15:10+00:00",
        },
        "args": {
            "ingest": {
                "archiveId": "00873df3-150b-47f7-aceb-873de18c1cac",
                "target": {"subdirectory": "path/to/resource"},
            }
        },
        "progress": {
            "status": "completed",
            "verbose": "Job terminated normally.",
            "numeric": 100,
        },
        "log": {
            "EVENT": [
                {
                    "datetime": "2024-08-09T12:15:10+00:00",
                    "origin": "Backend",
                    "body": "Some event",
                },
            ]
        },
        "data": {
            "success": True,
            "details": {
                "archiveApi": "rosetta-rest-api-v0",
                "deposit": {"sip_id": "sip-id"},
                "sip": {"iePids": "ie-pid"},
            },
        },
    }


@pytest.mark.parametrize(
    "success", [True, False], ids=["success", "no-success"]
)
def test_success(success, adapter: IngestAdapter, report):
    """Test method `IngestAdapter.success`."""
    report["data"]["success"] = success
    assert adapter.success(APIResult(report=report)) is success


@pytest.mark.parametrize(
    ("job_config", "record", "expected_request_body", "error"),
    [
        (
            JobConfig("", _template={}),
            Record(""),
            None,
            True,
        ),
        (
            JobConfig(
                "",
                _template={"target_archive": {"id": "unknown"}},
                _archives={},
            ),
            Record(""),
            None,
            True,
        ),
        (
            JobConfig(
                "",
                _template={"target_archive": {"id": "archive-0"}},
                _archives={
                    "archive-0": ArchiveConfiguration(
                        "", ArchiveAPI.ROSETTA_REST_V0, "destination-0"
                    )
                },
            ),
            Record(
                "",
                stages={
                    Stage.TRANSFER: RecordStageInfo(artifact="a"),
                },
            ),
            {
                "ingest": {
                    "archiveId": "archive-0",
                    "target": {"subdirectory": "a"},
                },
            },
            False,
        ),
        (
            JobConfig(
                "",
                _template={},
                _default_target_archive_id="archive-0",
                _archives={
                    "archive-0": ArchiveConfiguration(
                        "", ArchiveAPI.ROSETTA_REST_V0, "destination-0"
                    )
                },
            ),
            Record(
                "",
                stages={
                    Stage.TRANSFER: RecordStageInfo(artifact="a"),
                },
            ),
            {
                "ingest": {
                    "archiveId": "archive-0",
                    "target": {"subdirectory": "a"},
                },
            },
            False,
        ),
        (
            JobConfig(
                "",
                _template={},
                _default_target_archive_id="archive-0",
                _archives={
                    "archive-0": ArchiveConfiguration(
                        "", ArchiveAPI.ROSETTA_REST_V0, "destination-0"
                    )
                },
            ),
            Record(
                "",
                stages={},
            ),
            None,
            True,
        ),
        (
            JobConfig(
                "",
                _template={},
                _default_target_archive_id="archive-0",
                _archives={
                    "archive-0": ArchiveConfiguration(
                        "", ArchiveAPI.ROSETTA_REST_V0, "destination-0"
                    )
                },
            ),
            Record(
                "",
                stages={
                    Stage.TRANSFER: RecordStageInfo(artifact="a"),
                    Stage.INGEST: RecordStageInfo(token="b"),
                },
            ),
            {
                "ingest": {
                    "archiveId": "archive-0",
                    "target": {"subdirectory": "a"},
                },
                "token": "b",
            },
            False,
        ),
    ],
    ids=[
        "missing-archive",
        "unkown-archive",
        "explicit-archive",
        "default-archive",
        "missing-target",
        "token",
    ],
)
def test_build_request_body_simple(
    job_config, record, expected_request_body, error, adapter: IngestAdapter
):
    """Test method `IngestAdapter.build_request_body`."""
    if error:
        with pytest.raises(ValueError) as exc_info:
            adapter.build_request_body(job_config, record)
        print(exc_info.value)
    else:
        assert (
            adapter.build_request_body(job_config, record)
            == expected_request_body
        )


def test_eval_ok(adapter: IngestAdapter, report):
    """Test method `IngestAdapter.eval`."""
    record = Record("", stages={Stage.INGEST: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.INGEST].success
    assert record.archive_sip_id == "sip-id"
    assert record.archive_ie_id == "ie-pid"


def test_eval_bad(adapter: IngestAdapter, report):
    """Test method `IngestAdapter.eval`."""
    report["data"] = {}
    record = Record("", stages={Stage.INGEST: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.INGEST].success is False
    assert record.archive_sip_id is None
    assert record.archive_ie_id is None
