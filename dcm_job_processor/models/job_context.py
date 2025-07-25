"""
JobContext data-model definition
"""

from typing import Optional
from enum import Enum

from dcm_common.models import DataModel


class TriggerType(Enum):
    """Job execution triggers"""

    MANUAL = "manual"
    SCHEDULED = "scheduled"
    ONETIME = "onetime"
    TEST = "test"


class JobContext(DataModel):
    """Job execution context"""

    job_config_id: Optional[str]
    user_triggered: Optional[str]
    datetime_triggered: Optional[str]
    trigger_type: Optional[TriggerType | str]

    def __init__(
        self,
        job_config_id: Optional[str] = None,
        user_triggered: Optional[str] = None,
        datetime_triggered: Optional[str] = None,
        trigger_type: Optional[TriggerType | str] = None,
    ) -> None:
        self.job_config_id = job_config_id
        self.user_triggered = user_triggered
        self.datetime_triggered = datetime_triggered
        self.trigger_type = (
            trigger_type if trigger_type is None else TriggerType(trigger_type)
        )

    @DataModel.serialization_handler("job_config_id", "jobConfigId")
    @classmethod
    def job_config_id_serialization(cls, value):
        """Performs `job_config_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("job_config_id", "jobConfigId")
    @classmethod
    def job_config_id_deserialization(cls, value):
        """Performs `job_config_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("user_triggered", "userTriggered")
    @classmethod
    def user_triggered_serialization(cls, value):
        """Performs `user_triggered`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("user_triggered", "userTriggered")
    @classmethod
    def user_triggered_deserialization(cls, value):
        """Performs `user_triggered`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("datetime_triggered", "datetimeTriggered")
    @classmethod
    def datetime_triggered_serialization(cls, value):
        """Performs `datetime_triggered`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler(
        "datetime_triggered", "datetimeTriggered"
    )
    @classmethod
    def datetime_triggered_deserialization(cls, value):
        """Performs `datetime_triggered`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("trigger_type", "triggerType")
    @classmethod
    def trigger_type_serialization(cls, value):
        """Performs `trigger_type`-serialization."""
        if value is None:
            DataModel.skip()
        return value.value

    @DataModel.deserialization_handler("trigger_type", "triggerType")
    @classmethod
    def trigger_type_deserialization(cls, value):
        """Performs `trigger_type`-deserialization."""
        if value is None:
            DataModel.skip()
        return TriggerType(value)
