"""Input handlers for the 'DCM Job Processor'-app."""

from data_plumber_http import Property, Object, String, Url
from dcm_common.services.handlers import UUID

from dcm_job_processor.models import TriggerType, JobContext, Stage, JobConfig


ISODateTime = String(
    pattern=r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}"
)


process_handler = Object(
    properties={
        Property("process", "job_config", required=True): Object(
            model=JobConfig,
            properties={
                Property("from", "from_", required=True): String(
                    enum=[s.name.lower() for s in Stage]
                ),
                Property("to"): String(enum=[s.name.lower() for s in Stage]),
                Property("args", required=True): Object(free_form=True),
            },
            accept_only=["from", "to", "args"],
        ),
        Property("context"): Object(
            model=JobContext,
            properties={
                Property("jobConfigId", "job_config_id"): String(),
                Property("userTriggered", "user_triggered"): String(),
                Property(
                    "datetimeTriggered", "datetime_triggered"
                ): ISODateTime,
                Property("triggerType", "trigger_type"): String(
                    enum=[t.value for t in TriggerType]
                ),
            },
            accept_only=[
                "jobConfigId",
                "userTriggered",
                "datetimeTriggered",
                "triggerType",
            ],
        ),
        Property("token"): UUID(),
        Property("callbackUrl", name="callback_url"): Url(
            schemes=["http", "https"]
        ),
    },
    accept_only=["process", "context", "token", "callbackUrl"],
).assemble()
