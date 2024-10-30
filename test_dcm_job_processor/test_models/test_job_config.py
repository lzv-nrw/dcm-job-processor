"""JobConfig-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_job_processor.models import Stage, JobConfig


def test_stage_from_string():
    """Test method `from_string` for class `Stage`."""
    assert Stage.from_string("import_ies") == Stage.IMPORT_IES
    assert Stage.from_string("unknown") is None


def test_stage_stages():
    """Test method `stages` for class `Stage`."""
    assert Stage.IMPORT_IES.stages() == (Stage.IMPORT_IES,)
    assert set(Stage.VALIDATION.stages()) == {
        Stage.VALIDATION_METADATA, Stage.VALIDATION_PAYLOAD
    }


def test_stage_self_and_children():
    """Test method `self_and_children` for class `Stage`."""
    assert Stage.IMPORT_IES.self_and_children() == (Stage.IMPORT_IES,)
    assert set(Stage.VALIDATION.self_and_children()) == {
        Stage.VALIDATION, Stage.VALIDATION_METADATA, Stage.VALIDATION_PAYLOAD
    }


def test_job_config_constructor_minimal():
    """Test constructor logic of class `JobConfig`."""
    config = JobConfig(Stage.IMPORT_IES)

    assert config.from_ == Stage.IMPORT_IES
    assert config.to is None
    assert config.args == {}


def test_job_config_constructor_to():
    """Test constructor logic of class `JobConfig`."""
    config = JobConfig(Stage.IMPORT_IES, Stage.INGEST)

    assert config.to == Stage.INGEST
    assert config.args == {}


def test_job_config_constructor_args():
    """Test constructor logic of class `JobConfig`."""
    import_args = {"plugin": "some-plugin"}
    config = JobConfig(Stage.IMPORT_IES, args={"import_ies": import_args})

    assert len(config.args) == 1
    assert config.args[Stage.IMPORT_IES] == import_args


test_job_config_json = get_model_serialization_test(
    JobConfig, (
        ((Stage.IMPORT_IES,), {}),
        (
            (Stage.IMPORT_IES, Stage.INGEST,),
            {"args": {"import_ies": {"plugin": "some-plugin"}}}
        ),
    )
)
