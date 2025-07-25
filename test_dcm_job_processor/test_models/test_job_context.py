"""JobContext-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_job_processor.models import TriggerType, JobContext


test_job_config_json = get_model_serialization_test(
    JobContext,
    (
        ((), {}),
        (
            (),
            {
                "job_config_id": "a",
                "user_triggered": "b",
                "datetime_triggered": "0",
                "trigger_type": TriggerType.MANUAL,
            },
        ),
    ),
)
