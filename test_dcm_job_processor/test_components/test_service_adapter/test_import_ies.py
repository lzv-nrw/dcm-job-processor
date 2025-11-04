"""
Test module for the `ServiceAdapter` associated with `Stage.IMPORT_IES`.
"""

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage, JobConfig, Record, RecordStageInfo
from dcm_job_processor.components.service_adapter import ImportIEsAdapter


@pytest.fixture(name="url")
def _url():
    return "http://localhost:8080"


@pytest.fixture(name="adapter")
def _adapter(url):
    return ImportIEsAdapter(url)


@pytest.fixture(name="report")
def _report(url):
    return {
        "host": url,
        "token": {
            "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
            "expires": True,
            "expires_at": "2024-08-09T13:15:10+00:00",
        },
        "args": {"import": {"plugin": "demo", "args": {"number": 1}}},
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
                    "importType": "oai",
                    "oaiIdentifier": "oai:repository:12345",
                    "oaiDatestamp": "2025-01-01",
                    "ie": {
                        "path": "ie/abcde-12345-fghijk-67890"
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
def test_success(success, adapter: ImportIEsAdapter, report):
    """Test method `ImportIEsAdapter.success`."""
    report["data"]["success"] = success
    assert adapter.success(APIResult(report=report)) is success


@pytest.mark.parametrize(
    ("job_config", "record", "expected_request_body", "error"),
    [
        (
            JobConfig(
                "",
                _template={"type": "plugin"},
            ),
            Record(""),
            None,
            True,
        ),
        (
            JobConfig(
                "",
                _template={
                    "type": "plugin",
                    "additional_information": {"plugin": "test"},
                },
            ),
            Record(""),
            {"import": {"plugin": "test", "args": {"test": False}}},
            False,
        ),
        (
            JobConfig(
                "",
                _template={
                    "type": "plugin",
                    "additional_information": {
                        "plugin": "test",
                        "args": {"a": "b"},
                    },
                },
            ),
            Record(""),
            {"import": {"plugin": "test", "args": {"a": "b", "test": False}}},
            False,
        ),
        (
            JobConfig(
                "",
                test_mode=True,
                _template={
                    "type": "plugin",
                    "additional_information": {"plugin": "test"},
                },
            ),
            Record(""),
            {"import": {"plugin": "test", "args": {"test": True}}},
            False,
        ),
        (
            JobConfig(
                "job-0",
                _template={
                    "type": "oai",
                    "additional_information": {
                        "url": "url",
                        "metadata_prefix": "prefix",
                    },
                },
            ),
            Record(""),
            {
                "import": {
                    "plugin": "oai_pmh_v2",
                    "args": {
                        "base_url": "url",
                        "metadata_prefix": "prefix",
                        "test": False,
                    },
                    "jobConfigId": "job-0",
                }
            },
            False,
        ),
        (
            JobConfig(
                "job-0",
                _template={
                    "type": "oai",
                    "additional_information": {
                        "url": "url",
                        "metadata_prefix": "prefix",
                        "transfer_url_filters": "filter",
                    },
                },
                _data_selection={
                    "sets": "sets",
                    "from": "from",
                    "until": "until",
                    "identifiers": "identifiers",
                },
            ),
            Record(""),
            {
                "import": {
                    "plugin": "oai_pmh_v2",
                    "args": {
                        "base_url": "url",
                        "metadata_prefix": "prefix",
                        "transfer_url_info": "filter",
                        "set_spec": "sets",
                        "from_": "from",
                        "until": "until",
                        "identifiers": "identifiers",
                        "test": False,
                    },
                    "jobConfigId": "job-0",
                }
            },
            False,
        ),
        (
            JobConfig(
                "job-0",
                test_mode=True,
                _template={
                    "type": "oai",
                    "additional_information": {
                        "url": "url",
                        "metadata_prefix": "prefix",
                    },
                },
            ),
            Record(""),
            {
                "import": {
                    "plugin": "oai_pmh_v2",
                    "args": {
                        "base_url": "url",
                        "metadata_prefix": "prefix",
                        "test": True,
                    },
                    "jobConfigId": "job-0",
                }
            },
            False,
        ),
        (
            JobConfig(
                "",
                _template={
                    "type": "plugin",
                    "additional_information": {"plugin": "test"},
                },
            ),
            Record("", stages={Stage.IMPORT_IES: RecordStageInfo(token="a")}),
            {
                "import": {"plugin": "test", "args": {"test": False}},
                "token": "a",
            },
            False,
        ),
    ],
    ids=[
        "plugin-missing",
        "plugin-minimal",
        "plugin-with-args",
        "plugin-test-mode",
        "oai-minimal",
        "oai-extensive",
        "oai-test-mode",
        "token",
    ],
)
def test_build_request_body_simple(
    job_config, record, expected_request_body, error, adapter: ImportIEsAdapter
):
    """Test method `ImportIEsAdapter.build_request_body`."""
    if error:
        with pytest.raises(ValueError) as exc_info:
            adapter.build_request_body(job_config, record)
        print(exc_info.value)
    else:
        assert (
            adapter.build_request_body(job_config, record)
            == expected_request_body
        )


def test_eval(adapter: ImportIEsAdapter, report):
    """Test method `ImportIEsAdapter.eval`."""
    with pytest.raises(RuntimeError):
        adapter.eval(Record(""), APIResult(report=report))
