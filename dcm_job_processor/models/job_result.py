"""
JobResult data-model definition
"""

from typing import Optional
from dataclasses import dataclass, field

from dcm_common.models import JSONObject, DataModel, Report as BaseReport
from dcm_common.services import APIResult

from .job_config import Stage


@dataclass
class ServiceReport(BaseReport):
    """Generic `Report`-class for other DCM-services."""
    data: JSONObject = field(default_factory=lambda: {})
    children: Optional[JSONObject] = None


@dataclass
class Record(DataModel):
    """
    A `Record` corresponds to one object that is passed through the DCM-
    processing pipeline like a single intellectual entity or information
    package.
    """
    completed: bool = False
    success: Optional[bool] = None
    stages: dict[Stage | str, APIResult] = field(default_factory=lambda: {})

    @DataModel.serialization_handler("stages")
    @classmethod
    def stages_serialization(cls, value):
        """Performs `stages`-serialization."""
        return {
            (k.value.identifier if isinstance(k, Stage) else k): v.json
            for k, v in value.items()
        }

    @DataModel.deserialization_handler("stages")
    @classmethod
    def stages_deserialization(cls, value):
        """Performs `stages`-deserialization."""
        return {
            Stage.from_string(k): APIResult.from_json(v)
            for k, v in value.items()
        }

    def make_picklable(self) -> "Record":
        """
        Converts `Stage`-keys in `self.stages` to string using their
        identifiers (modification is made in-place).
        """
        self.stages = {
            (k.value.identifier if isinstance(k, Stage) else k): v
            for k, v in self.stages.items()
        }
        return self


@dataclass
class JobResult(DataModel):
    """
    A `JobResult` aggregates all information for all records in a
    single job.
    """
    success: Optional[bool] = None
    records: dict[str, Record] = field(default_factory=lambda: {})
