"""
Report data-model definition
"""

from dataclasses import dataclass, field

from dcm_common.models import JSONObject, Report as BaseReport

from .job_result import JobResult


@dataclass
class Report(BaseReport):
    data: JobResult = field(default_factory=JobResult)

    @property
    def json(self) -> JSONObject:
        return self._link_children(super().json)

    @staticmethod
    def _link_children(json: JSONObject) -> None:
        """
        Modifies `json` by moving reports to individual stages of
        individual records into the root.children-field and adding
        links to those in place of their original location.
        """
        if "children" not in json:
            json["children"] = {}
        for record in json["data"].get("records", {}).values():
            for stage_id, stage in record.get("stages", {}).items():
                if "report" not in stage:
                    continue
                i = 0
                while (
                    log_id := (
                        f"{stage['report'].get('token', {}).get('value', '???')}-{i}@{stage_id}"
                    )
                ) in json["children"]:
                    i += 1
                json["children"][log_id] = stage["report"]
                del stage["report"]
                stage["logId"] = log_id
        return json
