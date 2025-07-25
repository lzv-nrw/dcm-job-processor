"""
JobConfig data-model definition
"""

from typing import Optional
from dataclasses import dataclass, field
from enum import Enum

from dcm_common.models import JSONObject, DataModel


@dataclass
class _Stage:
    """Internal Record-class for data related to single stage."""

    identifier: str
    adapter: Optional["ServiceAdapter"] = None
    children: tuple["_Stage"] = field(default_factory=lambda: ())
    url: Optional[str] = None  # base url for making abort-call
    abort_path: Optional[str] = None


class Stage(Enum):
    """
    Enum class for the stages in the DCM-processing pipeline.

    Note that the field `adapter` in the enum-values are not
    initialized at import but have to be initialized manually.
    """

    IMPORT_IES = _Stage("import_ies")
    IMPORT_IPS = _Stage("import_ips")
    BUILD_IP = _Stage("build_ip")
    VALIDATION_METADATA = _Stage("validation_metadata")
    VALIDATION_PAYLOAD = _Stage("validation_payload")
    VALIDATION = _Stage(
        "validation", children=(VALIDATION_METADATA, VALIDATION_PAYLOAD)
    )
    PREPARE_IP = _Stage("prepare_ip")
    BUILD_SIP = _Stage("build_sip")
    TRANSFER = _Stage("transfer")
    INGEST = _Stage("ingest")

    @staticmethod
    def from_string(stage: str) -> Optional["Stage"]:
        """
        Returns `Stage` matching the given string value or `None` for no
        match.
        """
        return next(
            (s for s in Stage if s.value.identifier == stage),
            None
        )

    def stages(self) -> tuple["Stage"]:
        """Returns a tuple of child-`Stage`s or (if empty) self."""
        if not self.value.children:
            return (Stage.from_string(self.value.identifier),)
        return tuple(
            Stage.from_string(s.identifier)
            for s in self.value.children
        )

    def self_and_children(self) -> tuple["Stage"]:
        """Returns a tuple of self and child-`Stage`s."""
        return tuple(
            Stage.from_string(s.identifier)
            for s in (self.value,) + self.value.children
        )


class JobConfig(DataModel):
    """Job configuration"""

    from_: str | Stage
    to: str | Stage
    args: Optional[dict[str, JSONObject]]

    def __init__(
        self,
        from_: str | Stage,
        to: Optional[str | Stage] = None,
        args: Optional[dict[str, JSONObject]] = None,
    ):
        if isinstance(from_, str):
            self.from_ = Stage.from_string(from_)
        else:
            self.from_ = from_
        if isinstance(to, str):
            self.to = Stage.from_string(to)
        else:
            self.to = to
        if args:
            self.args = {
                Stage.from_string(k): v for k, v in args.items()
            }
        else:
            self.args = {}

    @DataModel.serialization_handler("from_", "from")
    @classmethod
    def from__serialization(cls, value):
        """Performs `from_`-serialization."""
        return value.value.identifier

    @DataModel.deserialization_handler("from_", "from")
    @classmethod
    def from__deserialization(cls, value):
        """Performs `from_`-deserialization."""
        return Stage.from_string(value)

    @DataModel.serialization_handler("to")
    @classmethod
    def to_serialization(cls, value):
        """Performs `to`-serialization."""
        return None if value is None else value.value.identifier

    @DataModel.deserialization_handler("to")
    @classmethod
    def to_deserialization(cls, value):
        """Performs `to`-deserialization."""
        if value is None:
            DataModel.skip()
        return Stage.from_string(value)

    @DataModel.serialization_handler("args")
    @classmethod
    def args_serialization(cls, value):
        """Performs `args`-serialization."""
        return {
            (None if k is None else k.value.identifier): v
            for k, v in value.items()
        }

    @DataModel.deserialization_handler("args")
    @classmethod
    def args_deserialization(cls, value):
        """Performs `args`-deserialization."""
        return value
