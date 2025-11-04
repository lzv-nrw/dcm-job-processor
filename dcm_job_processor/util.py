"""Utility definitions."""

from json import loads, JSONDecodeError
from pathlib import Path

from dcm_job_processor.models import ArchiveConfiguration


def load_archive_configurations_from_string(
    json: str,
) -> dict[str, ArchiveConfiguration]:
    """Loads archive configurations from the given JSON-string."""

    try:
        archives_json = loads(json)
    except JSONDecodeError as exc_info:
        raise ValueError(
            f"Invalid archive configuration: {exc_info}."
        ) from exc_info

    if not isinstance(archives_json, list):
        raise ValueError(
            "Invalid archive configuration: Expected list of archive "
            + f"configurations but got '{type(archives_json).__name__}'."
        )

    archives = {}
    for archive in archives_json:
        if not isinstance(archive.get("id"), str):
            raise ValueError(
                f"Bad archive id '{archive.get('id')}' (bad type)."
            )
        if archive["id"] in archives:
            raise ValueError(f"Non-unique archive id '{archive['id']}'.")
        try:
            archives[archive["id"]] = ArchiveConfiguration.from_json(archive)
        except (TypeError, ValueError, KeyError) as exc_info:
            raise ValueError(
                "Unable to deserialize archive configuration "
                + f"'{archive['id']}' ({type(exc_info).__name__}): "
                + f"{exc_info}"
            ) from exc_info

    return archives


def load_archive_configurations_from_file(
    path: Path,
) -> dict[str, ArchiveConfiguration]:
    """Loads archive configurations from the given `path` (JSON-file)."""
    return load_archive_configurations_from_string(
        path.read_text(encoding="utf-8")
    )
