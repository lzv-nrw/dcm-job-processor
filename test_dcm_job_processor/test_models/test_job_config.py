"""JobConfig-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_job_processor.models import JobConfig


test_job_config_json = get_model_serialization_test(
    JobConfig,
    (
        (("some-id",), {}),
        (("some-id", True, True), {}),
    ),
)


def test_job_config_getter_setter():
    """
    Test getter- and setter-functionality for extended information.
    """

    c = JobConfig("some-id")

    c.template = {"a": 0}
    c.data_selection = {"b": 1}
    c.data_processing = {"c": 2}
    c.archives = {"d": None}
    c.default_target_archive_id = "e"

    assert sorted(list(c.json.keys())) == ["id", "resume", "testMode"]
