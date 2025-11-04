"""
Test module for the `ServiceAdapter` associated with `Stage.BUILD_SIP`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
from dcm_job_processor.components.service_adapter import BuildSIPAdapter


@pytest.fixture(name="url")
def _url():
    return "http://localhost:8080"


@pytest.fixture(name="adapter")
def _adapter(url):
    return BuildSIPAdapter(url)


@pytest.fixture(name="artifact")
def _artifact():
    return "sip/028c2879-0284-4d39-9f1c-db5eb174535e"


@pytest.fixture(name="report")
def _report(url, artifact):
    return {
        "host": url,
        "token": {
            "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
            "expires": True,
            "expires_at": "2024-08-09T13:15:10+00:00",
        },
        "args": {
            "build": {
                "target": {"path": "ie/59438ebf-75e0-4345-8d6b-132a57e1e4f5"},
            },
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
                    "origin": "SIP Builder",
                    "body": "Some event",
                },
            ]
        },
        "data": {
            "success": True,
            "path": artifact,
        },
    }


@pytest.mark.parametrize(
    "success", [True, False], ids=["success", "no-success"]
)
def test_success(success, adapter: BuildSIPAdapter, report):
    """Test method `BuildSIPAdapter.success`."""
    report["data"]["success"] = success
    assert adapter.success(APIResult(report=report)) is success


@pytest.mark.parametrize(
    ("job_config", "record", "expected_request_body", "error"),
    [
        (JobConfig(""), Record(""), None, True),
        (
            JobConfig(""),
            Record(
                "", stages={Stage.PREPARE_IP: RecordStageInfo(artifact="b")}
            ),
            {"build": {"target": {"path": "b"}}},
            False,
        ),
        (
            JobConfig(""),
            Record("", stages={Stage.BUILD_IP: RecordStageInfo(artifact="b")}),
            {"build": {"target": {"path": "b"}}},
            False,
        ),
        (
            JobConfig(""),
            Record(
                "", stages={Stage.IMPORT_IPS: RecordStageInfo(artifact="b")}
            ),
            {"build": {"target": {"path": "b"}}},
            False,
        ),
        (
            JobConfig(""),
            Record(
                "",
                stages={
                    Stage.PREPARE_IP: RecordStageInfo(artifact="b"),
                    Stage.BUILD_SIP: RecordStageInfo(token="c"),
                },
            ),
            {"build": {"target": {"path": "b"}}, "token": "c"},
            False,
        ),
    ],
    ids=[
        "target-missing",
        "target-from-prepare-ip",
        "target-from-build-ip",
        "target-from-import-ip",
        "token",
    ],
)
def test_build_request_body_simple(
    job_config, record, expected_request_body, error, adapter: BuildSIPAdapter
):
    """Test method `BuildSIPAdapter.build_request_body`."""
    if error:
        with pytest.raises(ValueError) as exc_info:
            adapter.build_request_body(job_config, record)
        print(exc_info.value)
    else:
        assert (
            adapter.build_request_body(job_config, record)
            == expected_request_body
        )


def test_eval_ok(adapter: BuildSIPAdapter, report, artifact):
    """Test method `BuildSIPAdapter.eval`."""
    record = Record("", stages={Stage.BUILD_SIP: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.BUILD_SIP].success
    assert record.stages[Stage.BUILD_SIP].artifact == artifact


def test_eval_bad(adapter: BuildSIPAdapter, report):
    """Test method `BuildSIPAdapter.eval`."""
    report["data"] = {}
    record = Record("", stages={Stage.BUILD_SIP: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.BUILD_SIP].success is False
    assert record.stages[Stage.BUILD_SIP].artifact is None
