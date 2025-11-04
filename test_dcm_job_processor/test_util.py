"""Test module for `util.py`."""

import json
from uuid import uuid4

import pytest

from dcm_job_processor import util, models


@pytest.fixture(name="minimal_archive_configuration")
def _minimal_archive_configuration():
    return {
        "id": "0",
        "transferDestinationId": "0a",
        "type": models.ArchiveAPI.ROSETTA_REST_V0.value,
    }


def test_load_archive_configurations_from_string_basic():
    """Test function `load_archive_configurations_from_string`."""

    archive_0 = {
        "id": "0",
        "type": models.ArchiveAPI.ROSETTA_REST_V0.value,
        "transferDestinationId": "0a",
        "name": "a",
        "description": "b",
    }
    archive_1 = {
        "id": "1",
        "type": models.ArchiveAPI.ROSETTA_REST_V0.value,
        "transferDestinationId": "1a",
    }
    archives = util.load_archive_configurations_from_string(
        json.dumps([archive_0, archive_1])
    )

    assert len(archives) == 2
    assert "0" in archives
    assert "1" in archives
    assert archives["0"].json == {
        "id": archive_0["id"],
        "type": models.ArchiveAPI.ROSETTA_REST_V0.value,
        "transferDestinationId": archive_0["transferDestinationId"],
    }
    assert archives["1"].json == archive_1


def test_load_archive_configurations_from_string_bad_id(
    minimal_archive_configuration,
):
    """Test function `load_archive_configurations_from_string`."""
    with pytest.raises(ValueError) as exc_info:
        util.load_archive_configurations_from_string(
            json.dumps([minimal_archive_configuration | {"id": 0}])
        )
    print(exc_info.value)


def test_load_archive_configurations_from_string_duplicate_id(
    minimal_archive_configuration,
):
    """Test function `load_archive_configurations_from_string`."""
    with pytest.raises(ValueError) as exc_info:
        util.load_archive_configurations_from_string(
            json.dumps(
                [minimal_archive_configuration, minimal_archive_configuration]
            )
        )
    print(exc_info.value)


def test_load_archive_configurations_from_string_unknown_type(
    minimal_archive_configuration,
):
    """Test function `load_archive_configurations_from_string`."""
    with pytest.raises(ValueError) as exc_info:
        util.load_archive_configurations_from_string(
            json.dumps([minimal_archive_configuration | {"type": "unknown"}])
        )
    print(exc_info.value)


def test_load_archive_configurations_from_string_not_an_archive(
    minimal_archive_configuration,
):
    """Test function `load_archive_configurations_from_string`."""
    del minimal_archive_configuration["transferDestinationId"]
    with pytest.raises(ValueError) as exc_info:
        print(
            util.load_archive_configurations_from_string(
                json.dumps([minimal_archive_configuration])
            )
        )
    print(exc_info.value)


def test_load_archive_configurations_from_file_basic(
    temp_folder, minimal_archive_configuration
):
    """Test function `load_archive_configurations_from_file`."""

    file = temp_folder / str(uuid4())
    file.write_text(
        json.dumps([minimal_archive_configuration]), encoding="utf-8"
    )

    archives = util.load_archive_configurations_from_file(file)

    assert len(archives) == 1
    assert (
        archives[minimal_archive_configuration["id"]].json
        == minimal_archive_configuration
    )
