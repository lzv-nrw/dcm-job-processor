"""Input handlers for the 'DCM Job Processor'-app."""

from data_plumber_http import Property, Object, String, Boolean, Integer, Url
from dcm_common.services.handlers import UUID

from dcm_job_processor.models import TriggerType, JobContext, JobConfig


ISODateTime = String(
    pattern=r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}"
)


class HandlerTriggerType(String):
    def make(self, json, loc):
        r = super().make(json, loc)
        if r[0] is None:
            return r
        return TriggerType(r[0]), r[1], r[2]


process_handler = Object(
    properties={
        Property("process", "job_config", required=True): Object(
            model=JobConfig,
            properties={
                Property("id", "id_", required=True): String(),
                Property("testMode", "test_mode"): Boolean(),
                Property("resume"): Boolean(),
            },
            accept_only=["id", "testMode", "resume"],
        ),
        Property("context"): Object(
            model=JobContext,
            properties={
                Property("jobConfigId", "job_config_id"): String(),
                Property("userTriggered", "user_triggered"): String(),
                Property(
                    "datetimeTriggered", "datetime_triggered"
                ): ISODateTime,
                Property("triggerType", "trigger_type"): HandlerTriggerType(
                    enum=[t.value for t in TriggerType]
                ),
                Property("artifactsTTL", "artifacts_ttl"): Integer(
                    min_value=0
                ),
            },
            accept_only=[
                "jobConfigId",
                "userTriggered",
                "datetimeTriggered",
                "triggerType",
                "artifactsTTL",
            ],
        ),
        Property("token"): UUID(),
        Property("callbackUrl", name="callback_url"): Url(
            schemes=["http", "https"]
        ),
    },
    accept_only=["process", "context", "token", "callbackUrl"],
).assemble()
