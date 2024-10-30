"""Report-data class test-module."""

from dcm_common.services import APIResult
from dcm_common.models import Token
from dcm_common.models.data_model import get_model_serialization_test

from dcm_job_processor.models import (
    Report, JobResult, ServiceReport, Record, Stage
)


def test_report():
    """Test constructor of class `Report`."""
    data = JobResult()
    report = Report(host="", token=Token(), args={}, data=data)
    assert report.data == data


def test_report_missing_data():
    """Test constructor of class `Report`."""
    report = Report(host="", token=Token(), args={})
    assert isinstance(report.data, JobResult)


test_service_report_json = get_model_serialization_test(
    Report, (
        ((), {"host": ""}),
        (
            (), {
                "host": "", "token": Token(), "args": {},
                "data": JobResult(True),
            }
        ),
    )
)


def test_report_json__link_children():
    """
    Test property `json` of class `Report` with children that need
    to be linked.
    """

    stage = Stage.IMPORT_IES
    service_token = Token()
    service_report = ServiceReport(host="", token=service_token, args={}).json
    json = Report(
        host="", token=Token(), args={}, data=JobResult(records={
            "record0": Record(
                True, stages={
                    stage: APIResult(
                        True, report=service_report
                    )
                }
            )
        })
    ).json

    assert len(json["children"]) == 1
    assert "report" not in json["data"]["records"]["record0"]["stages"][stage.value.identifier]
    assert "logId" in json["data"]["records"]["record0"]["stages"][stage.value.identifier]
    log_id = json["data"]["records"]["record0"]["stages"][stage.value.identifier]["logId"]
    assert log_id == f"{service_token.value}-0@{stage.value.identifier}"
    assert log_id in json["children"]
    assert json["children"][log_id] == service_report


def test_report_json__link_children_multiple_same_token():
    """
    Test property `json` of class `Report` with children that need
    to be linked. Two records have the same token, and last one has
    another.
    """

    stage = Stage.IMPORT_IES
    service_token = Token()
    service_report0 = ServiceReport(host="", token=service_token, args={}).json
    service_report1 = ServiceReport(host="", token=service_token, args={}).json
    service_report2 = ServiceReport(host="", token=Token(), args={}).json
    json = Report(
        host="", token=Token(), args={}, data=JobResult(records={
            "record0": Record(
                True, stages={
                    stage: APIResult(
                        True, report=service_report0
                    )
                }
            ),
            "record1": Record(
                True, stages={
                    stage: APIResult(
                        True, report=service_report1
                    )
                }
            ),
            "record2": Record(
                True, stages={
                    stage: APIResult(
                        True, report=service_report2
                    )
                }
            )
        })
    ).json

    assert len(json["children"]) == 3
