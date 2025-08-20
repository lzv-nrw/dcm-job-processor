"""
Test module for the `dcm_job_processor/handlers.py`.
"""

import pytest
from data_plumber_http.settings import Responses

from dcm_job_processor.models import Stage, JobConfig
from dcm_job_processor import handlers


@pytest.fixture(name="process_handler")
def _process_handler():
    return handlers.process_handler


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"no-process": None}, 400),
            ({"process": {"args": {}}}, 400),  # missing from
            ({"process": {"from": "import_ies"}}, 400),  # missing args
            (  # bad args type
                {"process": {"from": "import_ies", "args": None}},
                422,
            ),
            ({"process": {"from": None, "args": {}}}, 422),  # bad from type
            ({"process": {"from": "unknown", "args": {}}}, 422),  # bad from
            (  # bad to type
                {"process": {"from": "import_ies", "to": None, "args": {}}},
                422,
            ),
            (  # bad to
                {
                    "process": {
                        "from": "import_ies",
                        "to": "unknown",
                        "args": {},
                    }
                },
                422,
            ),
            (
                {"process": {"from": "import_ies", "args": {}}},
                Responses.GOOD.status,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "callbackUrl": None,
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "callbackUrl": "no.scheme/path",
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "callbackUrl": "https://lzv.nrw/callback",
                },
                Responses.GOOD.status,
            ),
            (  # context
                {
                    "process": {"from": "import_ies", "args": {}},
                    "context": None,
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "context": {},
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "context": {"unknown": None},
                },
                400,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "context": {"jobConfigId": None},
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "context": {"userTriggered": None},
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "context": {"datetimeTriggered": None},
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "context": {"datetimeTriggered": "0"},
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "context": {"triggerType": None},
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "context": {
                        "jobConfigId": "a",
                        "userTriggered": "b",
                        "datetimeTriggered": "2024-01-01T00:00:00+01:00",
                        "triggerType": "manual",
                    },
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "token": None
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "token": "non-uuid"
                },
                422,
            ),
            (
                {
                    "process": {"from": "import_ies", "args": {}},
                    "token": "37ee72d6-80ab-4dcd-a68d-f8d32766c80d"
                },
                Responses.GOOD.status,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_process_handler(process_handler, json, status):
    "Test `process_handler`."

    output = process_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["job_config"], JobConfig)
        assert output.data.value["job_config"].from_ in Stage
        if output.data.value["job_config"].to is not None:
            assert output.data.value["job_config"].to in Stage
