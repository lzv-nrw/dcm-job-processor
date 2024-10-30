"""Input handlers for the 'DCM Job Processor'-app."""

from data_plumber_http import Property, Object, String, Url

from dcm_job_processor.models import Stage, JobConfig


process_handler = Object(
    properties={
        Property("process", "job_config", required=True): Object(
            model=JobConfig,
            properties={
                Property("from", "from_", required=True): String(
                    enum=[s.name.lower() for s in Stage]
                ),
                Property("to"): String(
                    enum=[s.name.lower() for s in Stage]
                ),
                Property("args", required=True): Object(free_form=True)
            },
            accept_only=["from", "to", "args"]
        ),
        Property("id", "id_"): String(),
        Property("callbackUrl", name="callback_url"):
            Url(schemes=["http", "https"])
    },
    accept_only=["process", "id", "callbackUrl"]
).assemble()
