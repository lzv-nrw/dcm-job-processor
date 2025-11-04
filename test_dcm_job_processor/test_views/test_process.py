"""Test-module for the ProcessView."""

from uuid import uuid4
import threading
from datetime import datetime

import pytest
from flask import jsonify, request
from dcm_common import LoggingContext, Logger
from dcm_common.orchestra import JobContext, JobConfig, JobInfo, Token

from dcm_job_processor import app_factory
from dcm_job_processor.views import ProcessView
from dcm_job_processor.models import (
    Stage,
    JobConfig as JPJobConfig,
    JobContext as JPJobContext,
    Record,
    RecordStageInfo,
    RecordStatus,
    Report,
)


def test_initialize_service_adapters(testing_config):
    """Test method `ProcessView.initialize_service_adapters`."""
    view = ProcessView(testing_config())

    assert len(view.adapters) == 0

    view.initialize_service_adapters()
    assert len(view.adapters) > 0
    for stage in Stage:
        assert stage in view.adapters


def test_reinitialize_database_adapter(testing_config):
    """Test method `ProcessView.reinitialize_database_adapter`."""
    view = ProcessView(testing_config())

    # adapter has been replaced
    original_id = id(view.config.db)
    view.reinitialize_database_adapter()
    assert id(view.config.db) != original_id

    # new adapter works
    assert view.config.db.get_table_names().eval() == []


def test_load_template_and_job_config(config_with_initialized_db, demo_data):
    """Test method `ProcessView.load_template_and_job_config`."""
    view = ProcessView(config_with_initialized_db)

    job_config = JPJobConfig(demo_data.job_config0)

    assert job_config.template is None
    assert job_config.data_selection is None
    assert job_config.data_processing is None

    view.load_template_and_job_config(
        JobContext(lambda: None), JobInfo(None), job_config
    )

    assert job_config.template is not None
    assert job_config.data_selection is not None
    assert job_config.data_processing is not None


def test_load_template_and_job_config_error(config_with_initialized_db):
    """Test method `ProcessView.load_template_and_job_config`."""
    view = ProcessView(config_with_initialized_db)

    job_config = JPJobConfig(str(uuid4()))

    with pytest.raises(ValueError) as exc_info:
        view.load_template_and_job_config(
            JobContext(lambda: None), JobInfo(None), job_config
        )

    print(exc_info.value)


def test_collect_resumable_records_no_records(
    config_with_initialized_db, demo_data
):
    """Test method `ProcessView.collect_resumable_records`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, report=Report())
    job_config = JPJobConfig(demo_data.job_config0)

    # run and eval
    assert (
        len(
            view.collect_resumable_records(
                JobContext(lambda: None),
                info,
                job_config,
            )
        )
        == 0
    )


@pytest.mark.parametrize(
    (
        "status",
        "bitstream",
        "skip_object_validation",
        "expire",
        "checkpoint",
        "success",
        "resumable_records",
    ),
    [
        (RecordStatus.INPROCESS, False, False, None, True, True, 1),
        (RecordStatus.COMPLETE, False, False, None, True, True, 0),
        (RecordStatus.PROCESS_ERROR, False, False, None, True, True, 0),
        (RecordStatus.INPROCESS, True, False, None, True, True, 1),
        (RecordStatus.INPROCESS, False, True, None, True, True, 1),
        (RecordStatus.INPROCESS, False, False, "0000", True, True, 0),
        (RecordStatus.INPROCESS, False, False, None, False, True, 0),
        (RecordStatus.INPROCESS, False, False, None, True, False, 0),
    ],
)
def test_collect_resumable_records_simple_record(
    status,
    bitstream,
    skip_object_validation,
    expire,
    checkpoint,
    success,
    resumable_records,
    config_with_initialized_db,
    demo_data,
):
    """Test method `ProcessView.collect_resumable_records`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, report=Report(), token=Token(str(uuid4())))
    job_config = JPJobConfig(
        demo_data.job_config0, _execution_context=JPJobContext(artifacts_ttl=1)
    )
    record_id = str(uuid4())
    token = str(uuid4())
    artifact = "test"

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs",
        {
            "token": token,
            "datetime_artifacts_expire": expire or "9999",
            "report": {
                "data": {
                    "records": (
                        {
                            record_id: {
                                "stages": {
                                    Stage.IMPORT_IES.value: {
                                        "completed": True,
                                        "success": success,
                                        "artifact": artifact,
                                        "log_id": "some-id",
                                    }
                                },
                            }
                        }
                        if checkpoint
                        else {}
                    )
                }
            },
        },
    ).eval()
    config_with_initialized_db.db.insert(
        # dummy that allows linking of resumed record
        "jobs",
        {"token": info.token.value, "report": {}},
    ).eval()
    config_with_initialized_db.db.insert(
        "records",
        {
            "id": record_id,
            "job_config_id": demo_data.job_config0,
            "job_token": token,
            "status": status.value,
            "bitstream": bitstream,
            "skip_object_validation": skip_object_validation,
        },
    ).eval()
    config_with_initialized_db.db.insert(
        "artifacts",
        {
            "path": artifact,
            "datetime_expires": expire or "9999",
            "record_id": record_id,
            "stage": Stage.IMPORT_IES.value,
        },
    ).eval()

    # run
    records = view.collect_resumable_records(
        JobContext(lambda: None),
        info,
        job_config,
    )

    # eval
    assert len(records) == resumable_records
    if len(records) > 0:
        assert records[0].id_ == record_id
        assert records[0].bitstream is bitstream
        assert records[0].skip_object_validation is skip_object_validation
        assert len(records[0].stages) == 1
        assert records[0].stages[Stage.IMPORT_IES].completed
        assert records[0].stages[Stage.IMPORT_IES].artifact == artifact
        assert records[0].stages[Stage.IMPORT_IES].log_id is None

        assert (
            config_with_initialized_db.db.get_row(
                "records",
                records[0].id_,
                cols=["job_token"],
            ).eval()["job_token"]
            == info.token.value
        )
    else:
        print(info.report.log.fancy(flatten=True))
        # record status unchanged if not in-process to begin with
        # record status set to error if filtered from resumable records
        assert (
            config_with_initialized_db.db.get_row(
                "records",
                record_id,
                cols=["status"],
            ).eval()["status"]
            == RecordStatus.COMPLETE.value
            if status is RecordStatus.COMPLETE
            else RecordStatus.PROCESS_ERROR.value
        )

    # check refresh of artifact lifetime
    # (refresh is done after initial SELECT but before additional checks
    # like the checkpoint are done)
    if len(records) > 0 or not checkpoint:
        assert (
            config_with_initialized_db.db.get_row(
                "jobs",
                token,
                cols=["datetime_artifacts_expire"],
            )
            .eval()["datetime_artifacts_expire"]
            .startswith(datetime.now().isoformat()[:10])
        )
        assert (
            config_with_initialized_db.db.get_rows(
                "artifacts",
                record_id,
                col="record_id",
                cols=["datetime_expires"],
            )
            .eval()[0]["datetime_expires"]
            .startswith(datetime.now().isoformat()[:10])
        )
    else:
        if success:
            assert config_with_initialized_db.db.get_row(
                "jobs",
                token,
                cols=["datetime_artifacts_expire"],
            ).eval()["datetime_artifacts_expire"] == (expire or "9999")
            assert config_with_initialized_db.db.get_rows(
                "artifacts",
                record_id,
                col="record_id",
                cols=["datetime_expires"],
            ).eval()[0]["datetime_expires"] == (expire or "9999")


def test_collect_resumable_records_multiple_records(
    config_with_initialized_db,
    demo_data,
):
    """Test method `ProcessView.collect_resumable_records`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, report=Report(), token=Token(str(uuid4())))
    job_config = JPJobConfig(
        demo_data.job_config0, _execution_context=JPJobContext(artifacts_ttl=1)
    )
    record_id_0 = str(uuid4())
    token_0 = str(uuid4())
    record_id_1 = str(uuid4())
    token_1 = str(uuid4())

    # pre-fill database
    # * two records in two separate jobs
    #   * one with a few completed stages
    #   * one without checkpoint
    config_with_initialized_db.db.insert(
        "jobs",
        {
            "token": token_0,
            "datetime_artifacts_expire": "9999",
            "report": {
                "data": {
                    "records": {
                        record_id_0: {
                            "stages": {
                                Stage.IMPORT_IES.value: {
                                    "success": True,
                                    "completed": True,
                                    "artifact": "test-0",
                                },
                                Stage.BUILD_IP.value: {
                                    "success": True,
                                    "completed": True,
                                    "artifact": "test-1",
                                },
                                Stage.VALIDATION_METADATA.value: {
                                    "success": True,
                                    "completed": True,
                                },
                                Stage.VALIDATION_PAYLOAD.value: {
                                    "completed": False,
                                },
                            },
                        }
                    }
                }
            },
        },
    ).eval()
    config_with_initialized_db.db.insert(
        "jobs",
        {
            "token": token_1,
            "datetime_artifacts_expire": "9999",
            "report": {
                "data": {
                    "records": {
                        record_id_0: {
                            "stages": {
                                Stage.IMPORT_IES.value: {
                                    "completed": False,
                                },
                            },
                        }
                    }
                }
            },
        },
    ).eval()
    config_with_initialized_db.db.insert(
        # dummy that allows linking of resumed record
        "jobs",
        {"token": info.token.value, "report": {}},
    ).eval()
    config_with_initialized_db.db.insert(
        "records",
        {
            "id": record_id_0,
            "job_config_id": demo_data.job_config0,
            "job_token": token_0,
            "status": RecordStatus.INPROCESS.value,
        },
    ).eval()
    config_with_initialized_db.db.insert(
        "records",
        {
            "id": record_id_1,
            "job_config_id": demo_data.job_config0,
            "job_token": token_1,
            "status": RecordStatus.INPROCESS.value,
        },
    ).eval()

    # run
    records = view.collect_resumable_records(
        JobContext(lambda: None),
        info,
        job_config,
    )

    print(info.report.log.fancy())

    # eval
    assert len(records) == 1
    assert records[0].id_ == record_id_0
    assert len(records[0].stages) == 3
    assert Stage.IMPORT_IES in records[0].stages
    assert Stage.BUILD_IP in records[0].stages
    assert Stage.VALIDATION_METADATA in records[0].stages


@pytest.mark.parametrize("import_type", ["oai", "hotfolder"])
def test_import_new_records_simple_import(
    import_type,
    config_with_initialized_db,
    token,
    base_report,
    run_service,
    demo_data,
):
    """Test method `ProcessView.import_new_records`."""
    stage = Stage.IMPORT_IES if import_type == "oai" else Stage.IMPORT_IPS
    view = ProcessView(config_with_initialized_db)
    view.initialize_service_adapters()
    info = JobInfo(None, token=Token(str(uuid4())), report=Report(children={}))
    job_config = JPJobConfig(
        demo_data.job_config0,
        _template={
            "type": import_type,
            "additional_information": {"source_id": "hotfolder-0"},
        },
        _data_selection={"path": "dir-0"},
    )
    record = Record(
        str(uuid4()),
        import_type=import_type,
        oai_identifier="a",
        oai_datestamp="9999",
        hotfolder_original_path="b",
    )
    artifact = "c"

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs",
        {
            "token": info.token.value,
        },
    ).eval()

    run_service(
        routes=[
            (
                "/import/ies" if import_type == "oai" else "/import/ips",
                lambda: (jsonify(token), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | {
                            "data": {
                                "success": True,
                                "records": {
                                    record.id_: {
                                        "id": record.id_,
                                        "importType": record.import_type,
                                        (
                                            "ie"
                                            if import_type == "oai"
                                            else "ip"
                                        ): {"path": artifact},
                                        "completed": True,
                                        "success": True,
                                    }
                                    | (
                                        {
                                            "oaiIdentifier": record.oai_identifier,
                                            "oaiDatestamp": record.oai_datestamp,
                                        }
                                        if import_type == "oai"
                                        else {
                                            "hotfolderOriginalPath": record.hotfolder_original_path,
                                        }
                                    )
                                },
                            }
                        }
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=config_with_initialized_db.IMPORT_MODULE_HOST.rsplit(":")[-1],
    )

    records = view.import_new_records(
        JobContext(lambda db_update=True: None), info, job_config
    )

    assert LoggingContext.ERROR not in info.report.log
    assert len(records) == 1

    assert records[0].started
    assert not records[0].completed
    assert records[0].status is RecordStatus.INPROCESS
    assert records[0].id_ == record.id_
    assert records[0].import_type == record.import_type
    if import_type == "oai":
        assert records[0].oai_identifier == record.oai_identifier
        assert records[0].oai_datestamp == record.oai_datestamp
    else:
        assert (
            records[0].hotfolder_original_path
            == record.hotfolder_original_path
        )
    assert stage in records[0].stages
    assert records[0].stages[stage].completed
    assert records[0].stages[stage].success
    assert records[0].stages[stage].artifact == artifact
    assert records[0].stages[stage].log_id in info.report.children

    assert config_with_initialized_db.db.get_row(
        "records",
        record.id_,
        cols=["status", "oai_identifier", "hotfolder_original_path"],
    ).eval() == {
        "status": RecordStatus.INPROCESS.value,
        "oai_identifier": (
            record.oai_identifier if import_type == "oai" else None
        ),
        "hotfolder_original_path": (
            None if import_type == "oai" else record.hotfolder_original_path
        ),
    }


def test_import_new_records_failed_record(
    config_with_initialized_db, token, base_report, run_service, demo_data
):
    """Test method `ProcessView.import_new_records`."""
    view = ProcessView(config_with_initialized_db)
    view.initialize_service_adapters()
    info = JobInfo(None, token=Token(str(uuid4())), report=Report(children={}))
    job_config = JPJobConfig(
        demo_data.job_config0,
        _template={"type": "oai"},
    )
    record_0 = Record(str(uuid4()), oai_identifier="a", oai_datestamp="9999")
    artifact = "c"
    record_1 = Record(str(uuid4()))

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs",
        {
            "token": info.token.value,
        },
    ).eval()

    run_service(
        routes=[
            ("/import/ies", lambda: (jsonify(token), 201), ["POST"]),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | {
                            "data": {
                                "success": True,
                                "records": {
                                    record_0.id_: {
                                        "id": record_0.id_,
                                        "importType": "oai",
                                        "oaiIdentifier": record_0.oai_identifier,
                                        "oaiDatestamp": record_0.oai_datestamp,
                                        "ie": {"path": artifact},
                                        "completed": True,
                                        "success": True,
                                    },
                                    record_1.id_: {
                                        "id": record_1.id_,
                                        "importType": "oai",
                                        "completed": True,
                                        "success": False,
                                    },
                                },
                            }
                        }
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=config_with_initialized_db.IMPORT_MODULE_HOST.rsplit(":")[-1],
    )

    records = view.import_new_records(
        JobContext(lambda db_update=True: None), info, job_config
    )

    print(info.report.log.fancy())

    assert LoggingContext.ERROR in info.report.log
    assert len(records) == 2

    if records[0].status is RecordStatus.INPROCESS:
        record_good = records[0]
        record_bad = records[1]
    else:
        record_good = records[1]
        record_bad = records[0]

    assert record_good.started
    assert record_bad.started
    assert not record_good.completed
    assert record_bad.completed
    assert record_bad.status is RecordStatus.IMPORT_ERROR
    assert record_good.oai_identifier == record_0.oai_identifier
    assert record_good.oai_datestamp == record_0.oai_datestamp
    assert record_bad.oai_identifier is None
    assert record_bad.oai_datestamp is None
    assert record_good.stages[Stage.IMPORT_IES].completed
    assert record_good.stages[Stage.IMPORT_IES].success
    assert record_good.stages[Stage.IMPORT_IES].artifact == artifact
    assert record_bad.stages[Stage.IMPORT_IES].completed
    assert record_bad.stages[Stage.IMPORT_IES].success is False
    assert record_bad.stages[Stage.IMPORT_IES].artifact is None

    assert (
        config_with_initialized_db.db.get_row(
            "records",
            record_0.id_,
            cols=["status"],
        ).eval()["status"]
        == RecordStatus.INPROCESS.value
    )
    assert (
        config_with_initialized_db.db.get_row(
            "records",
            record_1.id_,
            cols=["status"],
        ).eval()["status"]
        == RecordStatus.IMPORT_ERROR.value
    )


def test_import_new_records_api_error(
    config_with_initialized_db, token, demo_data, run_service
):
    """Test method `ProcessView.import_new_records`."""
    view = ProcessView(config_with_initialized_db)
    view.initialize_service_adapters()
    info = JobInfo(None, report=Report(children={}))
    job_config = JPJobConfig(demo_data.job_config0, _template={"type": "oai"})

    # run service that triggers an error
    run_service(
        routes=[
            ("/import/ies", lambda: (jsonify(token), 201), ["POST"]),
            (
                "/report",
                lambda: ("ERROR", 500),
                ["GET"],
            ),
        ],
        port=config_with_initialized_db.IMPORT_MODULE_HOST.rsplit(":")[-1],
    )

    records = view.import_new_records(
        JobContext(lambda db_update=True: None), info, job_config
    )

    print(info.report.log.fancy())
    assert LoggingContext.ERROR in info.report.log
    assert len(records) == 0


@pytest.mark.parametrize(
    ("record", "job_config", "next_stage"),
    [
        (
            Record("", stages={Stage.INGEST: RecordStageInfo(True)}),
            JPJobConfig(""),
            None,
        ),
        (
            Record("", stages={Stage.TRANSFER: RecordStageInfo(True)}),
            JPJobConfig(""),
            (Stage.INGEST,),
        ),
        (
            Record("", stages={Stage.BUILD_SIP: RecordStageInfo(True)}),
            JPJobConfig(""),
            (Stage.TRANSFER,),
        ),
        (
            Record("", stages={Stage.BUILD_SIP: RecordStageInfo(True)}),
            JPJobConfig("", test_mode=True),
            None,
        ),
        (
            Record("", stages={Stage.PREPARE_IP: RecordStageInfo(True)}),
            JPJobConfig(""),
            (Stage.BUILD_SIP,),
        ),
        (
            Record(
                "",
                stages={
                    Stage.VALIDATION_METADATA: RecordStageInfo(True),
                    Stage.VALIDATION_PAYLOAD: RecordStageInfo(True),
                },
            ),
            JPJobConfig(""),
            (Stage.PREPARE_IP,),
        ),
        (
            Record("", stages={Stage.IMPORT_IPS: RecordStageInfo(True)}),
            JPJobConfig("", _template={"type": "hotfolder"}),
            (Stage.VALIDATION_METADATA, Stage.VALIDATION_PAYLOAD),
        ),
        (
            Record(
                "",
                bitstream=True,
                stages={Stage.IMPORT_IPS: RecordStageInfo(True)},
            ),
            JPJobConfig("", _template={"type": "hotfolder"}),
            (Stage.VALIDATION_METADATA,),
        ),
        (
            Record(
                "",
                skip_object_validation=True,
                stages={Stage.IMPORT_IPS: RecordStageInfo(True)},
            ),
            JPJobConfig("", _template={"type": "hotfolder"}),
            (Stage.VALIDATION_METADATA,),
        ),
        (
            Record("", bitstream=True),
            JPJobConfig("", _template={"type": "hotfolder"}),
            (Stage.IMPORT_IPS,),
        ),
        (
            Record("", stages={Stage.BUILD_IP: RecordStageInfo(True)}),
            JPJobConfig("", _template={}),
            (Stage.VALIDATION_METADATA, Stage.VALIDATION_PAYLOAD),
        ),
        (
            Record(
                "",
                bitstream=True,
                stages={Stage.BUILD_IP: RecordStageInfo(True)},
            ),
            JPJobConfig("", _template={}),
            (Stage.VALIDATION_METADATA,),
        ),
        (
            Record(
                "",
                skip_object_validation=True,
                stages={Stage.BUILD_IP: RecordStageInfo(True)},
            ),
            JPJobConfig("", _template={}),
            (Stage.VALIDATION_METADATA,),
        ),
        (
            Record("", stages={Stage.IMPORT_IES: RecordStageInfo(True)}),
            JPJobConfig("", _template={}),
            (Stage.BUILD_IP,),
        ),
        (
            Record("", stages={}),
            JPJobConfig("", _template={}),
            (Stage.IMPORT_IES,),
        ),
    ],
)
def test_get_next_stage(record, job_config, next_stage, testing_config):
    """Test method `ProcessView.get_next_stage`."""
    view = ProcessView(testing_config())
    assert view.get_next_stage(record, job_config) == next_stage


@pytest.mark.parametrize(
    ("stage", "record", "expected_status"),
    [
        # not in process for any stage
        (
            None,
            Record("", status=RecordStatus.COMPLETE),
            RecordStatus.COMPLETE,
        ),
        (
            None,
            Record("", status=RecordStatus.PROCESS_ERROR),
            RecordStatus.PROCESS_ERROR,
        ),
        # errors
        (
            Stage.IMPORT_IES,
            Record(
                "",
                status=RecordStatus.INPROCESS,
                stages={Stage.IMPORT_IES: RecordStageInfo(success=False)},
            ),
            RecordStatus.IMPORT_ERROR,
        ),
        (
            Stage.IMPORT_IPS,
            Record(
                "",
                status=RecordStatus.INPROCESS,
                stages={Stage.IMPORT_IPS: RecordStageInfo(success=False)},
            ),
            RecordStatus.IMPORT_ERROR,
        ),
        (
            Stage.BUILD_IP,
            Record(
                "",
                status=RecordStatus.INPROCESS,
                stages={Stage.BUILD_IP: RecordStageInfo(success=False)},
            ),
            RecordStatus.BUILDIP_ERROR,
        ),
        (
            Stage.VALIDATION_METADATA,
            Record(
                "",
                status=RecordStatus.INPROCESS,
                stages={
                    Stage.VALIDATION_METADATA: RecordStageInfo(success=False)
                },
            ),
            RecordStatus.IPVAL_ERROR,
        ),
        (
            Stage.VALIDATION_PAYLOAD,
            Record(
                "",
                status=RecordStatus.INPROCESS,
                stages={
                    Stage.VALIDATION_PAYLOAD: RecordStageInfo(success=False)
                },
            ),
            RecordStatus.OBJVAL_ERROR,
        ),
        (
            Stage.PREPARE_IP,
            Record(
                "",
                status=RecordStatus.INPROCESS,
                stages={Stage.PREPARE_IP: RecordStageInfo(success=False)},
            ),
            RecordStatus.PREPAREIP_ERROR,
        ),
        (
            Stage.BUILD_SIP,
            Record(
                "",
                status=RecordStatus.INPROCESS,
                stages={Stage.BUILD_SIP: RecordStageInfo(success=False)},
            ),
            RecordStatus.BUILDSIP_ERROR,
        ),
        (
            Stage.TRANSFER,
            Record(
                "",
                status=RecordStatus.INPROCESS,
                stages={Stage.TRANSFER: RecordStageInfo(success=False)},
            ),
            RecordStatus.TRANSFER_ERROR,
        ),
        (
            Stage.INGEST,
            Record(
                "",
                status=RecordStatus.INPROCESS,
                stages={Stage.INGEST: RecordStageInfo(success=False)},
            ),
            RecordStatus.INGEST_ERROR,
        ),
    ],
)
def test_get_record_status(stage, record, expected_status, testing_config):
    """Test method `ProcessView.get_record_status`."""
    view = ProcessView(testing_config())
    assert view.get_record_status(stage, record) == expected_status


def test_link_record_to_ie_missing_record_ids(
    config_with_initialized_db, demo_data
):
    """Test method `ProcessView.link_record_to_ie`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, report=Report())
    record = Record(str(uuid4()))

    # no origin system id
    record.external_id = "a"
    view.link_record_to_ie(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        JPJobConfig(demo_data.job_config0),
        record,
    )
    assert record.ie_id is None
    assert LoggingContext.ERROR in info.report.log
    print(info.report.log[LoggingContext.ERROR][0].body)

    # no external id
    record.origin_system_id = "a"
    record.external_id = None
    info.report = Report()
    view.link_record_to_ie(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        JPJobConfig(demo_data.job_config0),
        record,
    )
    assert record.ie_id is None
    assert LoggingContext.ERROR in info.report.log
    print(info.report.log[LoggingContext.ERROR][0].body)


def test_link_record_to_ie_process_error(
    config_with_initialized_db, demo_data
):
    """Test method `ProcessView.link_record_to_ie`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, report=Report())
    record = Record(str(uuid4()), origin_system_id="a", external_id="b")

    view.link_record_to_ie(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        JPJobConfig(
            demo_data.job_config0,
            _template={"id": demo_data.template0, "name": "Test-Template"},
        ),
        record,
    )

    print(info.report.log.fancy())


def test_link_record_to_ie_new_ie(config_with_initialized_db, demo_data):
    """Test method `ProcessView.link_record_to_ie`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, report=Report(), token=Token(str(uuid4())))
    record = Record(
        str(uuid4()),
        source_organization="some organization",
        origin_system_id="a",
        external_id="b",
    )

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs", {"token": info.token.value}
    ).eval()
    config_with_initialized_db.db.insert(
        "records",
        {
            "id": record.id_,
            "job_config_id": demo_data.job_config0,
            "job_token": info.token.value,
            "status": RecordStatus.INPROCESS.value,
        },
    ).eval()

    # run
    view.link_record_to_ie(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        JPJobConfig(
            demo_data.job_config0,
            _template={
                "target_archive": {
                    "id": config_with_initialized_db.TEST_ARCHIVE_ID
                }
            },
        ),
        record,
    )

    # eval
    assert record.ie_id is not None
    ie_query = config_with_initialized_db.db.get_row(
        "ies", record.ie_id
    ).eval()
    assert ie_query["job_config_id"] == demo_data.job_config0
    assert ie_query["source_organization"] == record.source_organization
    assert ie_query["origin_system_id"] == record.origin_system_id
    assert ie_query["external_id"] == record.external_id
    assert ie_query["archive_id"] == config_with_initialized_db.TEST_ARCHIVE_ID

    record_query = config_with_initialized_db.db.get_row(
        "records", record.id_
    ).eval()
    assert record_query["ie_id"] == record.ie_id


def test_link_record_to_ie_update_ie(config_with_initialized_db, demo_data):
    """Test method `ProcessView.link_record_to_ie`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, report=Report(), token=Token(str(uuid4())))
    record = Record(
        str(uuid4()),
        source_organization="some organization",
        origin_system_id="a",
        external_id="b",
    )
    job_config = JPJobConfig(
        demo_data.job_config0,
        _template={
            "target_archive": {
                "id": config_with_initialized_db.TEST_ARCHIVE_ID
            }
        },
    )

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs", {"token": info.token.value}
    ).eval()
    config_with_initialized_db.db.insert(
        "records",
        {
            "id": record.id_,
            "job_config_id": job_config.id_,
            "job_token": info.token.value,
            "status": RecordStatus.INPROCESS.value,
        },
    ).eval()
    ie_id = str(uuid4())
    config_with_initialized_db.db.insert(
        "ies",
        {
            "id": ie_id,
            "job_config_id": job_config.id_,
            # missing source_organization
            "origin_system_id": record.origin_system_id,
            "external_id": record.external_id,
            "archive_id": config_with_initialized_db.TEST_ARCHIVE_ID,
        },
    ).eval()

    # run
    view.link_record_to_ie(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        job_config,
        record,
    )

    # eval
    assert record.ie_id == ie_id
    ie_query = config_with_initialized_db.db.get_row(
        "ies", record.ie_id
    ).eval()
    # source_organization has been updated
    assert ie_query["source_organization"] == record.source_organization

    record_query = config_with_initialized_db.db.get_row(
        "records", record.id_
    ).eval()
    assert record_query["ie_id"] == record.ie_id

    # run again with changed source_organization
    record.source_organization = "other organization"
    record.ie_id = None
    view.link_record_to_ie(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        job_config,
        record,
    )

    # did not change ie
    assert (
        config_with_initialized_db.db.get_row("ies", record.ie_id).eval()
        == ie_query
    )


@pytest.mark.parametrize("stage", [Stage.IMPORT_IES, Stage.IMPORT_IPS])
def test_execute_record_post_stage_import(
    stage, config_with_initialized_db, demo_data
):
    """Test method `ProcessView.execute_record_post_stage`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, report=Report(), token=Token(str(uuid4())))
    record = Record(
        str(uuid4()),
        oai_identifier="oai:0",
        oai_datestamp="2025-01-01",
        hotfolder_original_path="ip0",
    )

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs", {"token": info.token.value}
    ).eval()

    # run
    view.execute_record_post_stage(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        stage,
        JPJobConfig(demo_data.job_config0),
        record,
        RecordStageInfo(),
    )

    # eval
    record_query = config_with_initialized_db.db.get_row(
        "records", record.id_
    ).eval()
    assert record_query["job_config_id"] == demo_data.job_config0
    assert record_query["oai_identifier"] == record.oai_identifier
    assert record_query["oai_datestamp"] == record.oai_datestamp
    assert (
        record_query["hotfolder_original_path"]
        == record.hotfolder_original_path
    )


def test_execute_record_post_stage_metadata_validation(
    config_with_initialized_db,
    demo_data,
):
    """Test method `ProcessView.execute_record_post_stage`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None)
    record = Record(str(uuid4()))
    job_config = JPJobConfig(demo_data.job_config0)

    # mock link_record_to_ie
    link_record_to_ie_called = {"ok": False}
    view.link_record_to_ie = (
        lambda *args, **kwargs: link_record_to_ie_called.update({"ok": True})
    )

    # run
    view.execute_record_post_stage(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        Stage.VALIDATION_METADATA,
        job_config,
        record,
        RecordStageInfo(),
    )

    # eval
    assert link_record_to_ie_called["ok"]


def test_execute_record_post_stage_ingest(
    config_with_initialized_db, demo_data
):
    """Test method `ProcessView.execute_record_post_stage`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, token=Token(str(uuid4())))
    record = Record(str(uuid4()), archive_ie_id="ie0", archive_sip_id="sip0")
    job_config = JPJobConfig(demo_data.job_config0)

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs", {"token": info.token.value}
    ).eval()
    config_with_initialized_db.db.insert(
        "records",
        {
            "id": record.id_,
            "job_config_id": job_config.id_,
            "job_token": info.token.value,
            "status": RecordStatus.INPROCESS.value,
        },
    ).eval()

    # run
    view.execute_record_post_stage(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        Stage.INGEST,
        job_config,
        record,
        RecordStageInfo(),
    )

    # eval
    record_query = config_with_initialized_db.db.get_row(
        "records", record.id_
    ).eval()
    assert record_query["archive_ie_id"] == record.archive_ie_id
    assert record_query["archive_sip_id"] == record.archive_sip_id


def test_execute_record_post_stage_artifacts(
    config_with_initialized_db, demo_data
):
    """Test method `ProcessView.execute_record_post_stage`."""
    view = ProcessView(config_with_initialized_db)
    info = JobInfo(None, token=Token(str(uuid4())))
    record = Record(str(uuid4()))
    job_config = JPJobConfig(demo_data.job_config0)
    record_info = RecordStageInfo(artifact="a")

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs", {"token": info.token.value}
    ).eval()
    config_with_initialized_db.db.insert(
        "records",
        {
            "id": record.id_,
            "job_config_id": job_config.id_,
            "job_token": info.token.value,
            "status": RecordStatus.INPROCESS.value,
        },
    ).eval()

    # run
    view.execute_record_post_stage(
        threading.Lock(),
        JobContext(lambda: None),
        info,
        Stage.BUILD_IP,
        job_config,
        record,
        record_info,
    )

    # eval
    artifacts = config_with_initialized_db.db.get_rows("artifacts").eval()
    assert len(artifacts) == 1
    assert artifacts[0]["record_id"] == record.id_
    assert artifacts[0]["path"] == record_info.artifact
    assert artifacts[0]["stage"] == Stage.BUILD_IP.value


def test_run_stage_import_ies(token, base_report, testing_config, run_service):
    """Test method `ProcessView.run_stage`."""

    run_service(
        routes=[
            (
                "/import/ies",
                lambda: (jsonify(token), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | {
                            "data": {
                                "success": True,
                                "records": {
                                    "b9c5b0b2-2fde-429a-bf3a-743ca1a2f448": {
                                        "id": "b9c5b0b2-2fde-429a-bf3a-743ca1a2f448",
                                        "importType": "oai",
                                        "oaiIdentifier": "oai:repository:12345",
                                        "oaiDatestamp": "2025-01-01",
                                        "ie": {
                                            "path": "ie/abcde-12345-fghijk-67890"
                                        },
                                        "completed": True,
                                        "success": True,
                                    }
                                },
                            },
                        }
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=testing_config.IMPORT_MODULE_HOST.rsplit(":")[-1],
    )

    view = ProcessView(testing_config())
    view.initialize_service_adapters()

    info = JobInfo(None, report=Report(children={}))
    record = Record("")
    child_jobs = []
    removed_children = []
    view.run_stage(
        threading.Lock(),
        JobContext(
            lambda db_update=True: None,
            child_jobs.append,
            removed_children.append,
        ),
        info,
        Stage.IMPORT_IES,
        JPJobConfig(
            "",
            _template={
                "type": "plugin",
                "additional_information": {
                    "plugin": "test",
                    "args": {"a": "b"},
                },
            },
        ),
        record,
        skip_eval=True,
        skip_post_stage=True,
    )

    assert Stage.IMPORT_IES in record.stages
    assert record.stages[Stage.IMPORT_IES].completed
    assert record.stages[Stage.IMPORT_IES].success is None
    assert record.stages[Stage.IMPORT_IES].log_id in info.report.children
    assert len(child_jobs) == 1
    assert len(removed_children) == 1


def test_run_stage_import_ips(token, base_report, testing_config, run_service):
    """Test method `ProcessView.run_stage`."""

    run_service(
        routes=[
            (
                "/import/ips",
                lambda: (jsonify(token), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | {
                            "data": {
                                "success": True,
                                "IPs": {
                                    "ip0": {
                                        "path": "ip/a",
                                        "IEIdentifier": "ie0",
                                        "valid": True,
                                    }
                                },
                            },
                        }
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=testing_config.IMPORT_MODULE_HOST.rsplit(":")[-1],
    )

    view = ProcessView(testing_config())
    view.initialize_service_adapters()

    info = JobInfo(None, report=Report(children={}))
    record = Record("")
    child_jobs = []
    removed_children = []
    view.run_stage(
        threading.Lock(),
        JobContext(
            lambda db_update=True: None,
            child_jobs.append,
            removed_children.append,
        ),
        info,
        Stage.IMPORT_IPS,
        JPJobConfig(
            "",
            _template={"additional_information": {"source_id": "hotfolder-0"}},
            _data_selection={"path": "a"},
        ),
        record,
        skip_eval=True,
        skip_post_stage=True,
    )

    assert Stage.IMPORT_IPS in record.stages
    assert record.stages[Stage.IMPORT_IPS].completed
    assert record.stages[Stage.IMPORT_IPS].success is None
    assert record.stages[Stage.IMPORT_IPS].log_id in info.report.children
    assert len(child_jobs) == 1
    assert len(removed_children) == 1


def test_run_stage_build_ip(token, base_report, testing_config, run_service):
    """Test method `ProcessView.run_stage`."""

    run_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(token), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | {
                            "data": {
                                "requestType": "build",
                                "success": True,
                                "path": "ip/b",
                                "valid": True,
                                "details": {},
                            },
                        }
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=testing_config.IP_BUILDER_HOST.rsplit(":")[-1],
    )

    view = ProcessView(testing_config())
    view.initialize_service_adapters()

    info = JobInfo(None, report=Report(children={}))
    record = Record(
        "", stages={Stage.IMPORT_IES: RecordStageInfo(artifact="a")}
    )
    child_jobs = []
    removed_children = []
    view.run_stage(
        threading.Lock(),
        JobContext(
            lambda db_update=True: None,
            child_jobs.append,
            removed_children.append,
        ),
        info,
        Stage.BUILD_IP,
        JPJobConfig(
            "",
            _data_processing={
                "mapping": {
                    "type": "plugin",
                    "data": {"plugin": "test", "args": {}},
                }
            },
        ),
        record,
        skip_post_stage=True,
    )

    assert Stage.BUILD_IP in record.stages
    assert record.stages[Stage.BUILD_IP].completed
    assert record.stages[Stage.BUILD_IP].success
    assert record.stages[Stage.BUILD_IP].log_id in info.report.children
    assert len(child_jobs) == 1
    assert len(removed_children) == 1


def test_run_stage_process_error(testing_config):
    """Test method `ProcessView.run_stage`."""

    view = ProcessView(testing_config())
    view.initialize_service_adapters()

    info = JobInfo(None, report=Report(children={}))
    record = Record(
        "", stages={Stage.IMPORT_IES: RecordStageInfo(artifact="a")}
    )
    view.run_stage(
        threading.Lock(),
        JobContext(lambda db_update=True: None),
        info,
        Stage.BUILD_IP,
        # missing data_processing -> error
        JPJobConfig(""),
        record,
        skip_post_stage=True,
    )

    assert LoggingContext.ERROR in info.report.log
    print(info.report.log.fancy())

    assert Stage.BUILD_IP in record.stages
    assert record.stages[Stage.BUILD_IP].completed
    assert record.stages[Stage.BUILD_IP].success is False
    assert record.status is RecordStatus.PROCESS_ERROR


def test_run_stage_api_error(token, testing_config, run_service):
    """Test method `ProcessView.run_stage`."""

    run_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(token), 201),
                ["POST"],
            ),
            (
                "/report",
                # always return 500 -> error
                lambda: (
                    "ERROR",
                    500,
                ),
                ["GET"],
            ),
        ],
        port=testing_config.IP_BUILDER_HOST.rsplit(":")[-1],
    )

    view = ProcessView(testing_config())
    view.initialize_service_adapters()

    info = JobInfo(None, report=Report(children={}))
    record = Record(
        "", stages={Stage.IMPORT_IES: RecordStageInfo(artifact="a")}
    )
    view.run_stage(
        threading.Lock(),
        JobContext(lambda db_update=True: None),
        info,
        Stage.BUILD_IP,
        JPJobConfig(
            "",
            _data_processing={
                "mapping": {
                    "type": "plugin",
                    "data": {"plugin": "test", "args": {}},
                }
            },
        ),
        record,
        skip_post_stage=True,
    )

    assert (
        LoggingContext.ERROR.name
        in info.report.children[record.stages[Stage.BUILD_IP].log_id]["log"]
    )
    assert LoggingContext.ERROR in info.report.log
    print(info.report.log.fancy())

    assert Stage.BUILD_IP in record.stages
    assert record.stages[Stage.BUILD_IP].completed
    assert record.stages[Stage.BUILD_IP].success is False


# omitting other in-between stages in tests for `ProcessView.run_stage`:
# these are equivalent to the build_ip-stage; adapter-tests and other tests
# for this view cover the stage-specific processing


def test_run_record(token, base_report, testing_config, run_service):
    """Test method `ProcessView.run_record`."""

    # run services from build_ip up to sip-builder in test-mode (to limit
    # number of explicitly faked services)
    # * IP Builder
    run_service(
        routes=[
            (
                "/build",
                lambda: (
                    jsonify({"value": "build-token", "expires": False}),
                    201,
                ),
                ["POST"],
            ),
            (
                "/validate",
                lambda: (
                    jsonify({"value": "validation-token", "expires": False}),
                    201,
                ),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | (
                            {
                                "data": {
                                    "requestType": "build",
                                    "success": True,
                                    "path": "ip/b",
                                    "valid": True,
                                    "details": {},
                                },
                            }
                            if (request.args.get("token") == "build-token")
                            else {
                                "data": {
                                    "requestType": "validation",
                                    "success": True,
                                    "sourceOrganization": "x",
                                    "originSystemId": "y",
                                    "externalId": "z",
                                    "valid": True,
                                    "details": {},
                                }
                            }
                        )
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=testing_config.IP_BUILDER_HOST.rsplit(":")[-1],
    )
    # * Object Validator
    run_service(
        routes=[
            (
                "/validate",
                lambda: (jsonify(token), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | (
                            {
                                "data": {
                                    "success": True,
                                    "valid": True,
                                    "details": {},
                                }
                            }
                        )
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=testing_config.OBJECT_VALIDATOR_HOST.rsplit(":")[-1],
    )
    # * Preparation Module
    run_service(
        routes=[
            (
                "/prepare",
                lambda: (jsonify(token), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | (
                            {
                                "data": {
                                    "success": True,
                                    "path": "pip/c",
                                }
                            }
                        )
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=testing_config.PREPARATION_MODULE_HOST.rsplit(":")[-1],
    )
    # * SIP Builder
    run_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(token), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | (
                            {
                                "data": {
                                    "success": True,
                                    "path": "sip/d",
                                }
                            }
                        )
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=testing_config.SIP_BUILDER_HOST.rsplit(":")[-1],
    )

    view = ProcessView(testing_config())
    view.initialize_service_adapters()

    info = JobInfo(None, report=Report(children={}))
    record = Record(
        "",
        stages={Stage.IMPORT_IES: RecordStageInfo(True, True, artifact="a")},
    )

    view.run_record(
        threading.Lock(),
        JobContext(lambda db_update=True: None),
        info,
        JPJobConfig(
            "",
            test_mode=True,
            _template={
                "type": "plugin",
                "additional_information": {"plugin": "test", "args": {}},
            },
            _data_processing={
                "mapping": {
                    "type": "plugin",
                    "data": {"plugin": "test", "args": {}},
                },
                "preparation": {
                    "rightsOperations": [
                        {
                            "type": "complement",
                            "targetField": "DC-Rights",
                            "value": "a",
                        }
                    ]
                },
            },
        ),
        record,
        skip_db_and_post_stage=True,
    )

    print(info.report.log.fancy())

    assert record.started
    assert record.completed
    assert record.status is RecordStatus.COMPLETE
    assert record.source_organization == "x"
    assert record.origin_system_id == "y"
    assert record.external_id == "z"

    assert len(info.report.children) == 5

    assert Stage.BUILD_IP in record.stages
    assert record.stages[Stage.BUILD_IP].completed
    assert record.stages[Stage.BUILD_IP].success
    assert record.stages[Stage.BUILD_IP].artifact == "ip/b"
    assert record.stages[Stage.BUILD_IP].log_id in info.report.children

    assert Stage.VALIDATION_METADATA in record.stages
    assert record.stages[Stage.VALIDATION_METADATA].completed
    assert record.stages[Stage.VALIDATION_METADATA].success
    assert record.stages[Stage.VALIDATION_METADATA].artifact is None
    assert (
        record.stages[Stage.VALIDATION_METADATA].log_id in info.report.children
    )

    assert Stage.VALIDATION_PAYLOAD in record.stages
    assert record.stages[Stage.VALIDATION_PAYLOAD].completed
    assert record.stages[Stage.VALIDATION_PAYLOAD].success
    assert record.stages[Stage.VALIDATION_PAYLOAD].artifact is None
    assert (
        record.stages[Stage.VALIDATION_PAYLOAD].log_id in info.report.children
    )

    assert Stage.PREPARE_IP in record.stages
    assert record.stages[Stage.PREPARE_IP].completed
    assert record.stages[Stage.PREPARE_IP].success
    assert record.stages[Stage.PREPARE_IP].artifact == "pip/c"
    assert record.stages[Stage.PREPARE_IP].log_id in info.report.children

    assert Stage.BUILD_SIP in record.stages
    assert record.stages[Stage.BUILD_SIP].completed
    assert record.stages[Stage.BUILD_SIP].success
    assert record.stages[Stage.BUILD_SIP].artifact == "sip/d"
    assert record.stages[Stage.BUILD_SIP].log_id in info.report.children


def test_run_record_resume(token, base_report, testing_config, run_service):
    """Test method `ProcessView.run_record`."""

    # run sip-builder service
    run_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(token), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(
                        base_report
                        | (
                            {
                                "data": {
                                    "success": True,
                                    "path": "sip/d",
                                }
                            }
                        )
                    ),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=testing_config.SIP_BUILDER_HOST.rsplit(":")[-1],
    )

    view = ProcessView(testing_config())
    view.initialize_service_adapters()

    info = JobInfo(None, report=Report(children={}))
    record = Record(
        "",
        stages={
            Stage.IMPORT_IES: RecordStageInfo(True, True, artifact="a"),
            Stage.BUILD_IP: RecordStageInfo(True, True, artifact="b"),
            Stage.VALIDATION_METADATA: RecordStageInfo(True, True),
            Stage.VALIDATION_PAYLOAD: RecordStageInfo(True, True),
            Stage.PREPARE_IP: RecordStageInfo(True, True, artifact="c"),
        },
    )

    view.run_record(
        threading.Lock(),
        JobContext(lambda db_update=True: None),
        info,
        JPJobConfig(
            "",
            test_mode=True,
            _template={
                "type": "plugin",
            },
        ),
        record,
        skip_db_and_post_stage=True,
    )

    print(info.report.log.fancy())

    assert record.started
    assert record.completed
    assert record.status is RecordStatus.COMPLETE

    assert len(info.report.children) == 1

    assert Stage.BUILD_SIP in record.stages
    assert record.stages[Stage.BUILD_SIP].completed
    assert record.stages[Stage.BUILD_SIP].success
    assert record.stages[Stage.BUILD_SIP].artifact == "sip/d"
    assert record.stages[Stage.BUILD_SIP].log_id in info.report.children


def test_run_record_error(token, testing_config, run_service):
    """Test method `ProcessView.run_record`."""

    # run service that triggers an error
    run_service(
        routes=[
            (
                "/build",
                lambda: (
                    jsonify(token),
                    201,
                ),
                ["POST"],
            ),
            (
                "/report",
                lambda: ("ERROR", 500),
                ["GET"],
            ),
        ],
        port=testing_config.IP_BUILDER_HOST.rsplit(":")[-1],
    )

    view = ProcessView(testing_config())
    view.initialize_service_adapters()

    info = JobInfo(None, report=Report(children={}))
    record = Record(
        "",
        stages={Stage.IMPORT_IES: RecordStageInfo(True, True, artifact="a")},
    )

    view.run_record(
        threading.Lock(),
        JobContext(lambda db_update=True: None),
        info,
        JPJobConfig(
            "",
            test_mode=True,
            _template={
                "type": "plugin",
                "additional_information": {"plugin": "test", "args": {}},
            },
            _data_processing={
                "mapping": {
                    "type": "plugin",
                    "data": {"plugin": "test", "args": {}},
                },
            },
        ),
        record,
        skip_db_and_post_stage=True,
    )

    print(info.report.log.fancy())

    assert LoggingContext.INFO in info.report.log
    assert LoggingContext.ERROR in info.report.log

    assert record.started
    assert record.completed
    assert record.status is RecordStatus.BUILDIP_ERROR

    assert len(info.report.children) == 1

    assert Stage.BUILD_IP in record.stages
    assert record.stages[Stage.BUILD_IP].completed
    assert record.stages[Stage.BUILD_IP].success is False
    assert record.stages[Stage.BUILD_IP].log_id in info.report.children


def test_process_native(config_with_initialized_db, demo_data, dcm_services):
    """Test method `ProcessView.process`."""

    view = ProcessView(config_with_initialized_db)
    view.initialize_service_adapters()

    info = JobInfo(
        JobConfig(
            "process",
            original_body={},
            request_body={
                "process": {
                    "id": demo_data.job_config0,
                },
                "context": {
                    "artifactsTTL": 1,
                },
            },
        ),
        token=Token(str(uuid4())),
        report=Report(),
    )

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs", {"token": info.token.value}
    ).eval()

    child_jobs = []
    removed_child_jobs = []
    view.process(
        JobContext(
            lambda db_update=True: None,
            child_jobs.append,
            removed_child_jobs.append,
        ),
        info,
    )

    print(info.report.log.fancy())

    assert LoggingContext.ERROR in info.report.log
    assert info.report.data.success
    assert info.report.data.issues == 1
    assert len(info.report.data.records) == 2
    assert len(info.report.children) == 8
    assert len(child_jobs) == 8
    assert len(removed_child_jobs) == 8
    assert set(j.id for j in child_jobs) == set(removed_child_jobs)
    assert all(j.name in info.report.children for j in child_jobs)

    record_ids = list(info.report.data.records.keys())
    if (
        info.report.data.records[record_ids[0]].status
        is RecordStatus.IMPORT_ERROR
    ):
        record_bad = info.report.data.records[record_ids[0]]
        record_good = info.report.data.records[record_ids[1]]
    else:
        record_bad = info.report.data.records[record_ids[1]]
        record_good = info.report.data.records[record_ids[0]]

    assert record_bad.completed
    assert record_good.completed
    assert record_good.status is RecordStatus.COMPLETE
    assert record_good.oai_identifier is not None
    assert record_good.oai_datestamp is not None
    assert record_good.source_organization is not None
    assert record_good.origin_system_id is not None
    assert record_good.external_id is not None
    assert record_good.archive_ie_id is not None
    assert record_good.archive_sip_id is not None

    for stage_info in record_good.stages.values():
        assert stage_info.log_id in info.report.children

    assert (
        config_with_initialized_db.db.get_row(
            "records",
            record_good.id_,
            cols=["status"],
        ).eval()["status"]
        == RecordStatus.COMPLETE.value
    )
    assert (
        config_with_initialized_db.db.get_row(
            "records",
            record_bad.id_,
            cols=["status"],
        ).eval()["status"]
        == RecordStatus.IMPORT_ERROR.value
    )

    db_info = config_with_initialized_db.db.get_row(
        "jobs",
        info.token.value,
    ).eval()

    assert db_info["status"] == "completed"
    assert db_info["datetime_started"] is not None
    assert db_info["datetime_ended"] is not None


def test_process_flask(config_with_initialized_db, demo_data, dcm_services):
    """Test endpoint POST-`/process`."""

    app = app_factory(config_with_initialized_db)
    client = app.test_client()

    response = client.post(
        "/process",
        json={
            "process": {
                "id": demo_data.job_config0,
            },
            "context": {
                "artifactsTTL": 1,
            },
        },
    )

    assert response.status_code == 201
    token = response.json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={token}").json

    print(Logger.from_json(report["log"]).fancy())

    assert report["data"]["success"]
    assert report["data"]["issues"] == 1
    assert len(report["data"]["records"]) == 2

    assert len(config_with_initialized_db.db.get_rows("records").eval()) == 2


def test_abort_minimal(config_with_initialized_db):
    """Test endpoint DELETE-/process."""

    app = app_factory(config_with_initialized_db)
    client = app.test_client()
    app.extensions["orchestra"].stop(stop_on_idle=True)

    # pre-fill database
    token = str(uuid4())
    config_with_initialized_db.db.insert(
        "jobs",
        {
            "token": token,
        },
    ).eval()

    response = client.delete(
        f"/process?token={token}",
        json={"reason": "test", "origin": "from test"},
    )

    assert response.status_code == 200
    assert response.mimetype == "text/plain"

    db_info = config_with_initialized_db.db.get_row("jobs", token).eval()
    assert db_info["status"] == "aborted"
    assert db_info["datetime_ended"] is not None
    assert db_info["report"] == {
        "progress": {
            "status": "aborted",
            "verbose": "aborted: test (from test)",
            "numeric": 0,
        }
    }


def test_abort_stuck(config_with_initialized_db):
    """Test endpoint DELETE-/process."""

    app = app_factory(config_with_initialized_db)
    client = app.test_client()
    app.extensions["orchestra"].stop(stop_on_idle=True)

    # pre-fill database
    token = str(uuid4())
    config_with_initialized_db.db.insert(
        "jobs",
        {
            "token": token,
            "status": "running",
            "report": {
                "progress": {
                    "status": "running",
                }
            },
        },
    ).eval()

    client.delete(
        f"/process?token={token}",
        json={"reason": "test", "origin": "from test"},
    )

    db_info = config_with_initialized_db.db.get_row("jobs", token).eval()
    assert db_info["status"] == "aborted"
    assert db_info["datetime_ended"] is not None
    assert db_info["report"] == {
        "progress": {
            "status": "aborted",
            "verbose": "aborted: test (from test)",
            "numeric": 0,
        }
    }


def test_abort_completed(config_with_initialized_db):
    """Test endpoint DELETE-/process."""

    app = app_factory(config_with_initialized_db)
    client = app.test_client()
    app.extensions["orchestra"].stop(stop_on_idle=True)

    # pre-fill database
    token = str(uuid4())
    config_with_initialized_db.db.insert(
        "jobs",
        {
            "token": token,
            "status": "completed",
            "report": {
                "progress": {
                    "status": "running",
                }
            },
        },
    ).eval()

    client.delete(
        f"/process?token={token}",
        json={"reason": "test", "origin": "from test"},
    )

    db_info = config_with_initialized_db.db.get_row("jobs", token).eval()
    assert db_info["status"] == "completed"
    assert db_info["datetime_ended"] is None
    assert db_info["report"] == {
        "progress": {
            "status": "running",
        }
    }


def test_resume_and_abort_with_children(
    base_report,
    config_with_initialized_db,
    temp_folder,
    demo_data,
    run_service,
):
    """Test endpoint DELETE-/process."""
    record_id = str(uuid4())
    old_token = str(uuid4())

    # run services
    # * import module
    run_service(
        routes=[
            (
                "/import/ies",
                lambda: (
                    jsonify(
                        {"value": request.json.get("token"), "expires": False}
                    ),
                    201,
                ),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    jsonify(base_report | {"data": {"success": True}}),
                    200,
                ),
                ["GET"],
            ),
        ],
        port=config_with_initialized_db.IMPORT_MODULE_HOST.rsplit(":")[-1],
    )
    # * sip-builder
    child_deleted_file = temp_folder / str(uuid4())

    def abort_child():
        child_deleted_file.touch()
        return "ok", 200

    run_service(
        routes=[
            (
                "/build",
                lambda: (
                    jsonify(
                        {"value": request.json.get("token"), "expires": False}
                    ),
                    201,
                ),
                ["POST"],
            ),
            ("/build", abort_child, ["DELETE"]),
            ("/report", lambda: (jsonify(base_report), 503), ["GET"]),
        ],
        port=config_with_initialized_db.SIP_BUILDER_HOST.rsplit(":")[-1],
    )

    # pre-fill database
    config_with_initialized_db.db.insert(
        "jobs",
        {
            "token": old_token,
            "datetime_artifacts_expire": "9999",
            "report": base_report
            | {
                "data": {
                    "records": {
                        record_id: {
                            "completed": False,
                            "stages": {
                                Stage.IMPORT_IES.value: {
                                    "completed": True,
                                    "success": True,
                                    "artifact": "ie",
                                },
                                Stage.PREPARE_IP.value: {
                                    "completed": True,
                                    "success": True,
                                    "artifact": "pip",
                                },
                            },
                        }
                    }
                },
            },
        },
    ).eval()
    config_with_initialized_db.db.insert(
        "records",
        {
            "id": record_id,
            "job_config_id": demo_data.job_config0,
            "job_token": old_token,
            "status": RecordStatus.INPROCESS.value,
        },
    ).eval()

    # run job
    app = app_factory(config_with_initialized_db)
    client = app.test_client()
    token = client.post(
        "/process",
        json={
            "process": {
                "id": demo_data.job_config0,
            },
            "context": {
                "artifactsTTL": 1,
            },
        },
    ).json["value"]

    # wait until child-job is started
    report = {}
    while len(report.get("children", [])) < 2:  # expecting import+sip-builder
        report = client.get(f"/report?token={token}").json

    client.delete(
        f"/process?token={token}",
        json={"reason": "test", "origin": "from test"},
    )
    report = client.get(f"/report?token={token}").json

    print(Logger.from_json(report["log"]).fancy())

    app.extensions["orchestra"].stop(stop_on_idle=True)

    db_info = config_with_initialized_db.db.get_row("jobs", token).eval()

    assert db_info["status"] == "aborted"
    assert db_info["report"]["progress"]["status"] == "aborted"

    assert record_id in db_info["report"]["data"]["records"]
    assert (
        Stage.BUILD_SIP.value
        in db_info["report"]["data"]["records"][record_id]["stages"]
    )
    assert (
        db_info["report"]["data"]["records"][record_id]["stages"][
            Stage.BUILD_SIP.value
        ]["logId"]
        in db_info["report"]["children"]
    )

    db_record = config_with_initialized_db.db.get_row(
        "records", record_id
    ).eval()
    assert db_record["status"] == RecordStatus.INPROCESS.value

    assert child_deleted_file.is_file()
    assert LoggingContext.ERROR.name in db_info["report"]["log"]
