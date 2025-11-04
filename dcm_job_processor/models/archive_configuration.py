"""
TargetArchive and related data-model definitions
"""

from dataclasses import dataclass

from dcm_common.models import DataModel

from .enums import ArchiveAPI


@dataclass
class ArchiveConfiguration(DataModel):
    """
    Data model for an archive configuration.

    Keyword arguments:
    id_ -- archive identifier
    type_ -- archive type (see also ArchiveAPI-enum)
    transfer_destination_id -- identifier for transfer destination
                               (passed on to Transfer Module)
    """

    id_: str
    type_: ArchiveAPI
    transfer_destination_id: str

    @DataModel.serialization_handler("id_", "id")
    @classmethod
    def id__serialization_handler(cls, value):
        """Handles `id_`-serialization."""
        return value

    @DataModel.deserialization_handler("id_", "id")
    @classmethod
    def id__deserialization_handler(cls, value):
        """Handles `id_`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("type_", "type")
    @classmethod
    def type__serialization_handler(cls, value):
        """Handles `type_`-serialization."""
        return value.value

    @DataModel.deserialization_handler("type_", "type")
    @classmethod
    def type__deserialization_handler(cls, value):
        """Handles `type_`-deserialization."""
        return ArchiveAPI(value)

    @DataModel.serialization_handler(
        "transfer_destination_id", "transferDestinationId"
    )
    @classmethod
    def transfer_destination_id_serialization_handler(cls, value):
        """Handles `transfer_destination_id`-serialization."""
        return value

    @DataModel.deserialization_handler(
        "transfer_destination_id", "transferDestinationId"
    )
    @classmethod
    def transfer_destination_id_deserialization_handler(cls, value):
        """Handles `transfer_destination_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value
