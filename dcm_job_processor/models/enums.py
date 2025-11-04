"""Enum definitions"""

from enum import Enum


class TriggerType(Enum):
    """Job execution triggers"""

    MANUAL = "manual"
    SCHEDULED = "scheduled"
    ONETIME = "onetime"
    TEST = "test"


class Stage(Enum):
    """Enum class for the stages in the DCM-processing pipeline."""

    IMPORT_IES = "import_ies"
    IMPORT_IPS = "import_ips"
    BUILD_IP = "build_ip"
    VALIDATION_METADATA = "validation_metadata"
    VALIDATION_PAYLOAD = "validation_payload"
    PREPARE_IP = "prepare_ip"
    BUILD_SIP = "build_sip"
    TRANSFER = "transfer"
    INGEST = "ingest"


class RecordStatus(Enum):
    """Record status"""

    INPROCESS = "in-process"
    COMPLETE = "complete"
    PROCESS_ERROR = "process-error"
    IMPORT_ERROR = "import-error"
    OBJVAL_ERROR = "obj-val-error"
    IPVAL_ERROR = "ip-val-error"
    BUILDIP_ERROR = "build-ip-error"
    PREPAREIP_ERROR = "prepare-ip-error"
    BUILDSIP_ERROR = "build-sip-error"
    TRANSFER_ERROR = "transfer-error"
    INGEST_ERROR = "ingest-error"


class ArchiveAPI(Enum):
    """Supported archive API types."""
    ROSETTA_REST_V0 = "rosetta-rest-api-v0"
