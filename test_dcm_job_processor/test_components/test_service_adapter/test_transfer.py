"""
Test module for the `ServiceAdapter` associated with `Stage.TRANSFER`.
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
from dcm_job_processor.components.service_adapter import TransferAdapter


@pytest.fixture(name="url")
def _url():
    return "http://localhost:8080"


@pytest.fixture(name="adapter")
def _adapter(url):
    return TransferAdapter(url)


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
                    "origin": "Transfer Module",
                    "body": "Some event",
                },
            ]
        },
        "data": {
            "success": True,
        },
    }


@pytest.mark.parametrize(
    "success", [True, False], ids=["success", "no-success"]
)
def test_success(success, adapter: TransferAdapter, report):
    """Test method `TransferAdapter.success`."""
    report["data"]["success"] = success
    assert adapter.success(APIResult(report=report)) is success


@pytest.mark.parametrize(
    ("job_config", "record", "expected_request_body", "error"),
    [
        (
            JobConfig(""),
            Record(""),
            None,
            True,
        ),
        (
            JobConfig("", _template={}),
            Record(
                "", stages={Stage.BUILD_SIP: RecordStageInfo(artifact="a")}
            ),
            None,
            True,
        ),
        (
            JobConfig(
                "",
                _template={"target_archive": {"id": "unknown"}},
                _archives={},
            ),
            Record(
                "", stages={Stage.BUILD_SIP: RecordStageInfo(artifact="a")}
            ),
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
                "", stages={Stage.BUILD_SIP: RecordStageInfo(artifact="a")}
            ),
            {
                "transfer": {
                    "target": {"path": "a"},
                    "destinationId": "destination-0",
                }
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
                "", stages={Stage.BUILD_SIP: RecordStageInfo(artifact="a")}
            ),
            {
                "transfer": {
                    "target": {"path": "a"},
                    "destinationId": "destination-0",
                }
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
                    Stage.BUILD_SIP: RecordStageInfo(artifact="a"),
                    Stage.TRANSFER: RecordStageInfo(token="b"),
                },
            ),
            {
                "transfer": {
                    "target": {"path": "a"},
                    "destinationId": "destination-0",
                },
                "token": "b",
            },
            False,
        ),
    ],
    ids=[
        "missing-target",
        "missing-archive",
        "unkown-archive",
        "explicit-archive",
        "default-archive",
        "token",
    ],
)
def test_build_request_body_simple(
    job_config, record, expected_request_body, error, adapter: TransferAdapter
):
    """Test method `TransferAdapter.build_request_body`."""
    if error:
        with pytest.raises(ValueError) as exc_info:
            adapter.build_request_body(job_config, record)
        print(exc_info.value)
    else:
        assert (
            adapter.build_request_body(job_config, record)
            == expected_request_body
        )


def test_eval_ok(adapter: TransferAdapter, report):
    """Test method `TransferAdapter.eval`."""
    record = Record(
        "",
        stages={
            Stage.BUILD_SIP: RecordStageInfo(artifact="path/a"),
            Stage.TRANSFER: RecordStageInfo(),
        },
    )
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.TRANSFER].success
    assert record.stages[Stage.TRANSFER].artifact == "a"


def test_eval_bad(adapter: TransferAdapter, report):
    """Test method `TransferAdapter.eval`."""
    report["data"] = {}
    record = Record("", stages={Stage.TRANSFER: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.TRANSFER].success is False
    assert record.stages[Stage.TRANSFER].artifact is None
