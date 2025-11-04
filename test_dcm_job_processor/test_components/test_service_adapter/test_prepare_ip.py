"""
Test module for the `ServiceAdapter` associated with `Stage.PREPARE_IP`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
from dcm_job_processor.components.service_adapter import PrepareIPAdapter


@pytest.fixture(name="url")
def _url():
    return "http://localhost:8080"


@pytest.fixture(name="adapter")
def _adapter(url):
    return PrepareIPAdapter(url)


@pytest.fixture(name="artifact")
def _artifact():
    return "pip/028c2879-0284-4d39-9f1c-db5eb174535e"


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
            "preparation": {
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
                    "origin": "Preparation Module",
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
def test_success(success, adapter: PrepareIPAdapter, report):
    """Test method `PrepareIPAdapter.success`."""
    report["data"]["success"] = success
    assert adapter.success(APIResult(report=report)) is success


@pytest.mark.parametrize(
    ("job_config", "record", "expected_request_body", "error"),
    [
        (JobConfig(""), Record(""), None, True),
        (
            JobConfig("", _data_processing={}),
            Record(
                "",
                stages={
                    Stage.BUILD_IP: RecordStageInfo(artifact="a"),
                },
            ),
            {"preparation": {"target": {"path": "a"}}},
            False,
        ),
        (
            JobConfig("", _data_processing={}),
            Record(
                "",
                stages={
                    Stage.IMPORT_IPS: RecordStageInfo(artifact="a"),
                },
            ),
            {"preparation": {"target": {"path": "a"}}},
            False,
        ),
        (
            JobConfig(
                "",
                _data_processing={
                    "preparation": {
                        "rightsOperations": ["b"],
                        "preservationOperations": ["c"],
                        "sigPropOperations": ["d"],
                    },
                },
            ),
            Record(
                "",
                stages={
                    Stage.IMPORT_IPS: RecordStageInfo(artifact="a"),
                },
            ),
            {
                "preparation": {
                    "target": {"path": "a"},
                    "bagInfoOperations": ["b", "c"],
                    "sigPropOperations": ["d"],
                }
            },
            False,
        ),
        (
            JobConfig(
                "",
                _data_processing={},
            ),
            Record(
                "",
                stages={
                    Stage.IMPORT_IPS: RecordStageInfo(artifact="a"),
                },
                bitstream=True,
            ),
            {
                "preparation": {
                    "target": {"path": "a"},
                    "bagInfoOperations": [
                        {
                            "type": "set",
                            "targetField": "Preservation-Level",
                            "value": "Bitstream",
                        }
                    ],
                }
            },
            False,
        ),
        (
            JobConfig("", _data_processing={}),
            Record(
                "",
                stages={
                    Stage.BUILD_IP: RecordStageInfo(artifact="a"),
                    Stage.PREPARE_IP: RecordStageInfo(token="c"),
                },
            ),
            {
                "preparation": {"target": {"path": "a"}},
                "token": "c",
            },
            False,
        ),
    ],
    ids=[
        "target-missing",
        "target-from-build-ip",
        "target-from-import-ips",
        "with-operations",
        "with-operations",
        "token",
    ],
)
def test_build_request_body_simple(
    job_config, record, expected_request_body, error, adapter: PrepareIPAdapter
):
    """Test method `PrepareIPAdapter.build_request_body`."""
    if error:
        with pytest.raises(ValueError) as exc_info:
            adapter.build_request_body(job_config, record)
        print(exc_info.value)
    else:
        assert (
            adapter.build_request_body(job_config, record)
            == expected_request_body
        )


def test_eval_ok(adapter: PrepareIPAdapter, report, artifact):
    """Test method `PrepareIPAdapter.eval`."""
    record = Record("", stages={Stage.PREPARE_IP: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.PREPARE_IP].success
    assert record.stages[Stage.PREPARE_IP].artifact == artifact


def test_eval_bad(adapter: PrepareIPAdapter, report):
    """Test method `PrepareIPAdapter.eval`."""
    report["data"] = {}
    record = Record("", stages={Stage.PREPARE_IP: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.PREPARE_IP].success is False
    assert record.stages[Stage.PREPARE_IP].artifact is None
