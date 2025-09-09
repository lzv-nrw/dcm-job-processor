"""
Report data-model definition
"""

from typing import Optional
from dataclasses import dataclass, field

from dcm_common.models import JSONObject
from dcm_common.orchestra import Report as BaseReport

from .job_result import JobResult


@dataclass
class Report(BaseReport):
    data: JobResult = field(default_factory=JobResult)
    children: Optional[dict[str, JSONObject | BaseReport]] = None
