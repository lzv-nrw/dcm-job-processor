"""
JobContext data-model definition
"""

from typing import Optional
from dataclasses import dataclass

from dcm_common.models import DataModel

from .enums import TriggerType


@dataclass
class JobContext(DataModel):
    """Job execution context"""

    user_triggered: Optional[str] = None
    datetime_triggered: Optional[str] = None
    trigger_type: Optional[TriggerType] = None
    artifacts_ttl: Optional[int] = None

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

    @DataModel.serialization_handler("artifacts_ttl", "artifactsTTL")
    @classmethod
    def artifacts_ttl_serialization(cls, value):
        """Performs `artifacts_ttl`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("artifacts_ttl", "artifactsTTL")
    @classmethod
    def artifacts_ttl_deserialization(cls, value):
        """Performs `artifacts_ttl`-deserialization."""
        if value is None:
            DataModel.skip()
        return value
