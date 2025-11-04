"""
Test module for the `ServiceAdapter` associated with `Stage.VALIDATION_PAYLOAD`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage, Record, JobConfig, RecordStageInfo
from dcm_job_processor.components.service_adapter import (
    ValidationPayloadAdapter,
)


@pytest.fixture(name="url")
def _url():
    return "http://localhost:8080"


@pytest.fixture(name="adapter")
def _adapter(url):
    return ValidationPayloadAdapter(url)


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
            "validation": {
                "target": {"path": "ip/59438ebf-75e0-4345-8d6b-132a57e1e4f5"},
            },
            "plugins": {"req-0": {"plugin": "some-plugin", "args": {}}},
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
                    "origin": "Object Validator",
                    "body": "Some event",
                },
            ]
        },
        "data": {
            "success": True,
            "valid": True,
            "details": {},
        },
    }


@pytest.mark.parametrize(
    "success", [True, False], ids=["success", "no-success"]
)
def test_success(success, adapter: ValidationPayloadAdapter, report):
    """Test method `ValidationPayloadAdapter.success`."""
    report["data"]["valid"] = success
    assert adapter.success(APIResult(report=report)) is success


@pytest.mark.parametrize(
    ("job_config", "record", "expected_request_body", "error"),
    [
        (JobConfig(""), Record(""), None, True),
        (
            JobConfig(""),
            Record("", stages={Stage.BUILD_IP: RecordStageInfo(artifact="a")}),
            {
                "validation": {
                    "target": {"path": "a"},
                    "plugins": {
                        "integrity": {"plugin": "integrity-bagit", "args": {}},
                        "format": {
                            "plugin": "jhove-fido-mimetype-bagit",
                            "args": {},
                        },
                    },
                }
            },
            False,
        ),
        (
            JobConfig(""),
            Record(
                "", stages={Stage.IMPORT_IPS: RecordStageInfo(artifact="a")}
            ),
            {
                "validation": {
                    "target": {"path": "a"},
                    "plugins": {
                        "integrity": {"plugin": "integrity-bagit", "args": {}},
                        "format": {
                            "plugin": "jhove-fido-mimetype-bagit",
                            "args": {},
                        },
                    },
                }
            },
            False,
        ),
        (
            JobConfig(""),
            Record(
                "",
                stages={
                    Stage.IMPORT_IPS: RecordStageInfo(artifact="a"),
                    Stage.VALIDATION_PAYLOAD: RecordStageInfo(token="b"),
                },
            ),
            {
                "validation": {
                    "target": {"path": "a"},
                    "plugins": {
                        "integrity": {"plugin": "integrity-bagit", "args": {}},
                        "format": {
                            "plugin": "jhove-fido-mimetype-bagit",
                            "args": {},
                        },
                    },
                },
                "token": "b",
            },
            False,
        ),
    ],
    ids=[
        "target-missing",
        "target-build-ip",
        "target-import-ips",
        "token",
    ],
)
def test_build_request_body_simple(
    job_config,
    record,
    expected_request_body,
    error,
    adapter: ValidationPayloadAdapter,
):
    """Test method `ValidationPayloadAdapter.build_request_body`."""
    if error:
        with pytest.raises(ValueError) as exc_info:
            adapter.build_request_body(job_config, record)
        print(exc_info.value)
    else:
        assert (
            adapter.build_request_body(job_config, record)
            == expected_request_body
        )


def test_eval_ok(adapter: ValidationPayloadAdapter, report):
    """Test method `ValidationPayloadAdapter.eval`."""
    record = Record("", stages={Stage.VALIDATION_PAYLOAD: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.VALIDATION_PAYLOAD].success


def test_eval_bad(adapter: ValidationPayloadAdapter, report):
    """Test method `ValidationPayloadAdapter.eval`."""
    report["data"] = {}
    record = Record("", stages={Stage.VALIDATION_PAYLOAD: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.VALIDATION_PAYLOAD].success is False


def test_eval_invalid(adapter: ValidationPayloadAdapter, report):
    """Test method `ValidationPayloadAdapter.eval`."""
    report["data"]["valid"] = False
    record = Record("", stages={Stage.VALIDATION_PAYLOAD: RecordStageInfo()})
    adapter.eval(record, APIResult(report=report))
    assert record.stages[Stage.VALIDATION_PAYLOAD].success is False
