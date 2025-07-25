"""JobResult and related data classes test-module."""

import threading
import pickle

import pytest
from dcm_common.services import APIResult
from dcm_common.models import Token
from dcm_common.models.data_model import get_model_serialization_test

from dcm_job_processor.models import (
    Stage, ServiceReport, Record, JobResult
)


def test_service_report():
    """Test constructor of class `ServiceReport`."""

    data = {"field1": "value1"}
    children = {"child": {"child-field1": "child-value1"}}
    report = ServiceReport(
        host="", token=Token(), args={}, data=data, children=children
    )

    assert report.data == data
    assert report.children == children


test_service_report_json = get_model_serialization_test(
    ServiceReport, (
        ((), {"host": ""}),
        (
            (), {
                "host": "", "token": Token(), "args": {},
                "data": {"field1": "value1"},
                "children": {"child": {"child-field1": "child-value1"}}
            }
        ),
    )
)


def test_service_report_empty_data():
    """
    Test property `json` of class `ServiceReport` with empty data.
    """

    report = ServiceReport(host="", token=Token(), args={})

    assert len(report.data) == 0

    assert "data" in report.json


def test_service_report_empty_children():
    """
    Test property `json` of class `ServiceReport` with empty children.
    """

    report = ServiceReport(host="", token=Token(), args={})

    assert report.children is None

    assert "children" not in report.json


def test_record():
    """Test constructor of class `Record`."""
    record = Record(success=True)
    assert record.success
    assert len(record.stages) == 0


def test_record_missing_success():
    """Test constructor of class `Record`."""
    record = Record()
    assert record.success is None


def test_record_with_reports():
    """Test constructor of class `Record`."""
    info = APIResult(
        report=ServiceReport(host="", token=Token(), args={}).json
    )
    record = Record(
        stages={
            Stage.IMPORT_IES: info
        }
    )
    assert Stage.IMPORT_IES in record.stages
    assert record.stages[Stage.IMPORT_IES] == info


def test_record_make_picklable(request):
    """Test method `make_picklable` of class `Record`."""
    # link un-picklable data to Stage.value.adapter
    def reset():
        Stage.IMPORT_IES.value.adapter = None
    request.addfinalizer(reset)
    Stage.IMPORT_IES.value.adapter = threading.Lock()

    record = Record(stages={Stage.IMPORT_IES: APIResult()})
    with pytest.raises(TypeError):
        pickle.dumps(record)
    pickle.dumps(record.make_picklable())
    pickle.dumps(record)
    assert list(record.stages.keys()) == [Stage.IMPORT_IES.value.identifier]


test_record_json = get_model_serialization_test(
    Record,
    (
        ((), {}),
        (
            (True,),
            {
                "stages": {Stage.IMPORT_IES: APIResult()},
                "external_id": "a",
                "origin_system_id": "b",
                "sip_id": "c",
                "ie_id": "d",
                "datetime_processed": "0",
            },
        ),
    ),
)


def test_job_result():
    """Test constructor of class `JobResult`."""
    result = JobResult(success=True)
    assert result.success
    assert len(result.records) == 0


def test_job_result_missing_success():
    """Test constructor of class `JobResult`."""
    result = JobResult()
    assert result.success is None


def test_job_result_with_records():
    """Test constructor of class `JobResult`."""
    record = Record()
    record_id = "id"
    result = JobResult(records={record_id: record})
    assert record_id in result.records
    assert result.records[record_id] == record


test_job_result_json = get_model_serialization_test(
    JobResult, (
        ((), {}),
        (
            (True,), {
                "records": {
                    "id": Record(
                        stages={Stage.IMPORT_IES: APIResult()}
                    )
                }
            }
        ),
    )
)


def test_job_result_json_missing_success():
    """Test property `json` of class `JobResult`."""
    assert "success" not in JobResult().json
