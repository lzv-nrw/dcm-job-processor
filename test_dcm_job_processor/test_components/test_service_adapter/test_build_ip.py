"""
Test module for the `ServiceAdapter` associated with `Stage.BUILD_IP`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
from dcm_job_processor.components.service_adapter import BuildIPAdapter


@pytest.fixture(name="url")
def _url():
    return "http://localhost:8080"


@pytest.fixture(name="adapter")
def _adapter(url):
    return BuildIPAdapter(url)


@pytest.fixture(name="artifact")
def _artifact():
    return "ip/028c2879-0284-4d39-9f1c-db5eb174535e"


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
                "mappingPlugin": {"plugin": "plugin-0", "args": {}},
            },
            "validation": {"modules": []},
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
                    "origin": "IP Builder",
                    "body": "Some event",
                },
            ]
        },
        "data": {
            "requestType": "build",
            "success": True,
            "path": artifact,
            "valid": True,
            "details": {},
        },
    }


@pytest.mark.parametrize(
    "success", [True, False], ids=["success", "no-success"]
)
def test_success(success, adapter: BuildIPAdapter, report):
    """Test method `BuildIPAdapter.success`."""
    report["data"]["success"] = success
    assert adapter.success(APIResult(report=report)) is success


@pytest.mark.parametrize(
    ("job_config", "record", "expected_request_body", "error"),
    [
        (JobConfig(""), Record(""), None, True),
        (
            JobConfig(
                "",
                _data_processing={"mapping": {"type": "plugin", "data": "a"}},
            ),
            Record(
                "", stages={Stage.IMPORT_IES: RecordStageInfo(artifact="b")}
            ),
            {
                "build": {
                    "mappingPlugin": "a",
                    "target": {"path": "b"},
                    "validate": False,
                }
            },
            False,
        ),
        (
            JobConfig(
                "",
                _data_processing={
                    "mapping": {"type": "python", "data": {"contents": "a"}}
                },
            ),
            Record(
                "", stages={Stage.IMPORT_IES: RecordStageInfo(artifact="b")}
            ),
            {
                "build": {
                    "mappingPlugin": {
                        "plugin": "generic-mapper-plugin-string",
                        "args": {"mapper": {"string": "a", "args": {}}},
                    },
                    "target": {"path": "b"},
                    "validate": False,
                }
            },
            False,
        ),
        (
            JobConfig(
                "",
                _data_processing={
                    "mapping": {"type": "xslt", "data": {"contents": "a"}}
                },
            ),
            Record(
                "", stages={Stage.IMPORT_IES: RecordStageInfo(artifact="b")}
            ),
            {
                "build": {
                    "mappingPlugin": {
                        "plugin": "xslt-plugin",
                        "args": {"xslt": "a"},
                    },
                    "target": {"path": "b"},
                    "validate": False,
                }
            },
            False,
        ),
        (
            JobConfig(
                "",
                _data_processing={"mapping": {"type": "plugin", "data": "a"}},
            ),
            Record(
                "",
                stages={
                    Stage.IMPORT_IES: RecordStageInfo(artifact="b"),
                    Stage.BUILD_IP: RecordStageInfo(token="c"),
                },
            ),
            {
                "build": {
                    "mappingPlugin": "a",
                    "target": {"path": "b"},
                    "validate": False,
                },
                "token": "c",
            },
            False,
        ),
    ],
    ids=[
        "target-missing",
        "plugin-minimal",
        "python-minimal",
        "xslt-minimal",
        "token",
    ],
)
def test_build_request_body_simple(
    job_config, record, expected_request_body, error, adapter: BuildIPAdapter
):
    """Test method `BuildIPAdapter.build_request_body`."""
    if error:
        with pytest.raises(ValueError) as exc_info:
            adapter.build_request_body(job_config, record)
        print(exc_info.value)
    else:
        assert (
            adapter.build_request_body(job_config, record)
            == expected_request_body
        )


def test_eval_ok(adapter: BuildIPAdapter, report, artifact):
    """Test method `BuildIPAdapter.eval`."""
    record = Record("", stages={Stage.BUILD_IP: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.BUILD_IP].success
    assert record.stages[Stage.BUILD_IP].artifact == artifact


def test_eval_bad(adapter: BuildIPAdapter, report):
    """Test method `BuildIPAdapter.eval`."""
    report["data"] = {}
    record = Record("", stages={Stage.BUILD_IP: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.BUILD_IP].success is False
    assert record.stages[Stage.BUILD_IP].artifact is None
