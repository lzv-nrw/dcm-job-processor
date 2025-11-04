"""
JobResult data-model definition
"""

from typing import Optional
from dataclasses import dataclass, field
import threading

from dcm_common.models import JSONObject, DataModel
from dcm_common.orchestra import Report as BaseReport, dillignore

from .enums import Stage, RecordStatus


@dataclass
class ServiceReport(BaseReport):
    """Generic `Report`-class for other DCM-services."""

    data: JSONObject = field(default_factory=dict)
    children: Optional[JSONObject] = None


@dataclass
class RecordStageInfo(DataModel):
    """
    A `RecordStageInfo` contains information for a single stage of a
    single record.
    """

    completed: bool = False
    success: Optional[bool] = None
    token: Optional[str] = None
    log_id: Optional[str] = None
    artifact: Optional[str] = None

    @DataModel.serialization_handler("log_id", "logId")
    @classmethod
    def log_id_serialization(cls, value):
        """Performs `log_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("log_id", "logId")
    @classmethod
    def log_id_deserialization(cls, value):
        """Performs `log_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value


@dillignore("_thread")
@dataclass
class Record(DataModel):
    """
    A `Record` corresponds to one object that is passed through the DCM-
    processing pipeline like a single intellectual entity or information
    package.
    """

    id_: str

    # processing
    started: bool = False
    completed: bool = False
    status: RecordStatus = field(
        default_factory=lambda: RecordStatus.INPROCESS
    )
    datetime_changed: Optional[str] = None

    # additional flags
    bitstream: bool = False
    skip_object_validation: bool = False

    # identifiers
    source_organization: Optional[str] = None
    external_id: Optional[str] = None
    origin_system_id: Optional[str] = None
    import_type: Optional[str] = None
    oai_identifier: Optional[str] = None
    oai_datestamp: Optional[str] = None
    hotfolder_original_path: Optional[str] = None
    archive_sip_id: Optional[str] = None
    archive_ie_id: Optional[str] = None
    ie_id: Optional[str] = None

    # details
    stages: dict[Stage | str, RecordStageInfo] = field(default_factory=dict)

    # internal state
    _thread: Optional[threading.Thread] = None
    _resumable_token: Optional[str] = None

    @DataModel.serialization_handler("id_", "id")
    @classmethod
    def id__serialization(cls, value):
        """Performs `id_`-serialization."""
        return value

    @DataModel.deserialization_handler("id_", "id")
    @classmethod
    def id__deserialization(cls, value):
        """Performs `id_`-deserialization."""
        return value

    @DataModel.serialization_handler("status")
    @classmethod
    def status_serialization(cls, value):
        """Performs `status`-serialization."""
        return value.value

    @DataModel.deserialization_handler("status")
    @classmethod
    def status_deserialization(cls, value):
        """Performs `status`-deserialization."""
        if value is None:
            DataModel.skip()
        return RecordStatus(value)

    @DataModel.serialization_handler(
        "source_organization", "sourceOrganization"
    )
    @classmethod
    def source_organization_serialization(cls, value):
        """Performs `source_organization`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler(
        "source_organization", "sourceOrganization"
    )
    @classmethod
    def source_organization_deserialization(cls, value):
        """Performs `source_organization`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("external_id", "externalId")
    @classmethod
    def external_id_serialization(cls, value):
        """Performs `external_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("external_id", "externalId")
    @classmethod
    def external_id_deserialization(cls, value):
        """Performs `external_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("origin_system_id", "originSystemId")
    @classmethod
    def origin_system_id_serialization(cls, value):
        """Performs `origin_system_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("origin_system_id", "originSystemId")
    @classmethod
    def origin_system_id_deserialization(cls, value):
        """Performs `origin_system_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("import_type", "importType")
    @classmethod
    def import_type_serialization(cls, value):
        """Performs `import_type`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("import_type", "importType")
    @classmethod
    def import_type_deserialization(cls, value):
        """Performs `import_type`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("skip_object_validation", "skipObjectValidation")
    @classmethod
    def skip_object_validation_serialization(cls, value):
        """Performs `skip_object_validation`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("skip_object_validation", "skipObjectValidation")
    @classmethod
    def skip_object_validation_deserialization(cls, value):
        """Performs `skip_object_validation`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("oai_identifier", "oaiIdentifier")
    @classmethod
    def oai_identifier_serialization(cls, value):
        """Performs `oai_identifier`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("oai_identifier", "oaiIdentifier")
    @classmethod
    def oai_identifier_deserialization(cls, value):
        """Performs `oai_identifier`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("oai_datestamp", "oaiDatestamp")
    @classmethod
    def oai_datestamp_serialization(cls, value):
        """Performs `oai_datestamp`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("oai_datestamp", "oaiDatestamp")
    @classmethod
    def oai_datestamp_deserialization(cls, value):
        """Performs `oai_datestamp`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler(
        "hotfolder_original_path", "hotfolderOriginalPath"
    )
    @classmethod
    def hotfolder_original_path_serialization(cls, value):
        """Performs `hotfolder_original_path`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler(
        "hotfolder_original_path", "hotfolderOriginalPath"
    )
    @classmethod
    def hotfolder_original_path_deserialization(cls, value):
        """Performs `hotfolder_original_path`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("archive_sip_id", "archiveSipId")
    @classmethod
    def archive_sip_id_serialization(cls, value):
        """Performs `archive_sip_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("archive_sip_id", "archiveSipId")
    @classmethod
    def archive_sip_id_deserialization(cls, value):
        """Performs `archive_sip_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("archive_ie_id", "archiveIeId")
    @classmethod
    def archive_ie_id_serialization(cls, value):
        """Performs `archive_ie_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("archive_ie_id", "archiveIeId")
    @classmethod
    def archive_ie_id_deserialization(cls, value):
        """Performs `archive_ie_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("ie_id", "ieId")
    @classmethod
    def ie_id_serialization(cls, value):
        """Performs `ie_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("ie_id", "ieId")
    @classmethod
    def ie_id_deserialization(cls, value):
        """Performs `ie_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("datetime_changed", "datetimeChanged")
    @classmethod
    def datetime_changed_serialization(cls, value):
        """Performs `datetime_changed`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("datetime_changed", "datetimeChanged")
    @classmethod
    def datetime_changed_deserialization(cls, value):
        """Performs `datetime_changed`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("stages")
    @classmethod
    def stages_serialization(cls, value):
        """Performs `stages`-serialization."""
        return {
            (k.value if isinstance(k, Stage) else k): v.json
            for k, v in value.items()
        }

    @DataModel.deserialization_handler("stages")
    @classmethod
    def stages_deserialization(cls, value):
        """Performs `stages`-deserialization."""
        return {
            Stage(k): RecordStageInfo.from_json(v) for k, v in value.items()
        }

    @property
    def thread(self) -> Optional[threading.Thread]:
        """Returns associated `Thread` if available."""
        return self._thread

    @thread.setter
    def thread(self, t) -> None:
        self._thread = t

    @property
    def resumable_token(self) -> Optional[str]:
        """Returns associated `resumable_token` if available."""
        return self._resumable_token

    @resumable_token.setter
    def resumable_token(self, resumable_token) -> None:
        self._resumable_token = resumable_token


@dataclass
class JobResult(DataModel):
    """
    A `JobResult` aggregates all information for all records in a
    single job.
    """

    success: Optional[bool] = None
    issues: int = 0
    records: dict[str, Record] = field(default_factory=dict)
