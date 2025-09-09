"""Report-data class test-module."""

from dcm_common.orchestra import Token
from dcm_common.models.data_model import get_model_serialization_test

from dcm_job_processor.models import Report, JobResult


def test_report():
    """Test constructor of class `Report`."""
    data = JobResult()
    report = Report(host="", token=Token("0"), args={}, data=data)
    assert report.data == data


def test_report_missing_data():
    """Test constructor of class `Report`."""
    report = Report(host="", token=Token("0"), args={})
    assert isinstance(report.data, JobResult)


test_service_report_json = get_model_serialization_test(
    Report,
    (
        ((), {"host": ""}),
        (
            (),
            {
                "host": "",
                "token": Token("0"),
                "args": {},
                "data": JobResult(True),
            },
        ),
    ),
)
