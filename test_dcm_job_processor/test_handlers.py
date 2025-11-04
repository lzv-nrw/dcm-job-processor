"""
Test module for the `dcm_job_processor/handlers.py`.
"""

import pytest
from data_plumber_http.settings import Responses

from dcm_job_processor.models import JobConfig
from dcm_job_processor import handlers


@pytest.fixture(name="process_handler")
def _process_handler():
    return handlers.process_handler


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"no-process": None}, 400),
            ({"process": {}}, 400),  # missing id
            ({"process": {"id": None}}, 422),  # bad id type
            ({"process": {"id": "some-id"}}, Responses.GOOD.status),  # ok
            ({"process": {"id": "some-id", "unknown": None}}, 400),  # unknown
            (
                {
                    "process": {"id": "some-id"},
                    "callbackUrl": None,
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "callbackUrl": "no.scheme/path",
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "callbackUrl": "https://lzv.nrw/callback",
                },
                Responses.GOOD.status,
            ),
            (  # context
                {
                    "process": {"id": "some-id"},
                    "context": None,
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "context": {},
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "context": {"unknown": None},
                },
                400,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "context": {"userTriggered": None},
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "context": {"datetimeTriggered": None},
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "context": {"datetimeTriggered": "0"},
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "context": {"triggerType": None},
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "context": {"artifactsTTL": None},
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "context": {"artifactsTTL": -1},
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "context": {
                        "userTriggered": "b",
                        "datetimeTriggered": "2024-01-01T00:00:00+01:00",
                        "triggerType": "manual",
                        "artifactsTTL": 1,
                    },
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "token": None
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
                    "token": "non-uuid"
                },
                422,
            ),
            (
                {
                    "process": {"id": "some-id"},
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
