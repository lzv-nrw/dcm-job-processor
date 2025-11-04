"""JobResult and related data classes test-module."""

from dcm_common.orchestra import Token
from dcm_common.models.data_model import get_model_serialization_test

from dcm_job_processor.models import (
    Stage,
    ServiceReport,
    RecordStageInfo,
    RecordStatus,
    Record,
    JobResult,
)


def test_service_report():
    """Test constructor of class `ServiceReport`."""

    data = {"field1": "value1"}
    children = {"child": {"child-field1": "child-value1"}}
    report = ServiceReport(
        host="", token=Token("0"), args={}, data=data, children=children
    )

    assert report.data == data
    assert report.children == children


test_service_report_json = get_model_serialization_test(
    ServiceReport,
    (
        ((), {"host": ""}),
        (
            (),
            {
                "host": "",
                "token": Token("0"),
                "args": {},
                "data": {"field1": "value1"},
                "children": {"child": {"child-field1": "child-value1"}},
            },
        ),
    ),
)


def test_service_report_empty_data():
    """
    Test property `json` of class `ServiceReport` with empty data.
    """

    report = ServiceReport(host="", token=Token("0"), args={})

    assert len(report.data) == 0

    assert "data" in report.json


def test_service_report_empty_children():
    """
    Test property `json` of class `ServiceReport` with empty children.
    """

    report = ServiceReport(host="", token=Token("0"), args={})

    assert report.children is None

    assert "children" not in report.json


test_record_stage_info_json = get_model_serialization_test(
    RecordStageInfo,
    (
        ((), {}),
        ((True, True, "abc", "abc@xyz", "path/0"), {}),
    ),
)


test_record_json = get_model_serialization_test(
    Record,
    (
        (("some-id",), {}),
        (
            ("some-id",),
            {
                "started": "a",
                "completed": "b",
                "status": RecordStatus.COMPLETE,
                "datetime_changed": "0",
                "bitstream": True,
                "skip_object_validation": True,
                "external_id": "ext-id",
                "source_organization": "src-org",
                "origin_system_id": "orig-sys-id",
                "import_type": "oai",
                "oai_identifier": "oai-id",
                "oai_datestamp": "1",
                "hotfolder_original_path": "hotfolder-path",
                "archive_sip_id": "arch-sip-id",
                "archive_ie_id": "arch-ie-id",
                "ie_id": "ie-id",
                "stages": {Stage.IMPORT_IES: RecordStageInfo()},
            },
        ),
    ),
)


test_job_result_json = get_model_serialization_test(
    JobResult,
    (
        ((), {}),
        (
            (),
            {
                "success": False,
                "issues": 1,
                "records": {
                    "id": Record(
                        "id", stages={Stage.IMPORT_IES: RecordStageInfo()}
                    )
                },
            },
        ),
    ),
)
