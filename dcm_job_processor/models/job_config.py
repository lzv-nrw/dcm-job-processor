"""
JobConfig data-model definition
"""

from typing import Optional, Mapping
from dataclasses import dataclass

from dcm_common.models import JSONObject, DataModel

from .job_context import JobContext
from .archive_configuration import ArchiveConfiguration


@dataclass
class JobConfig(DataModel):
    """Job configuration"""

    id_: str
    test_mode: bool = False
    resume: bool = True

    # additional properties which are only used during a running job
    # and thus are excluded from (de-)serialization
    _execution_context: Optional[JobContext] = None
    _template: Optional[JSONObject] = None
    _data_selection: Optional[JSONObject] = None
    _data_processing: Optional[JSONObject] = None
    _default_target_archive_id: Optional[str] = None
    _archives: Optional[Mapping[str, ArchiveConfiguration]] = None

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

    @DataModel.serialization_handler("test_mode", "testMode")
    @classmethod
    def test_mode_serialization(cls, value):
        """Performs `test_mode`-serialization."""
        return value

    @DataModel.deserialization_handler("test_mode", "testMode")
    @classmethod
    def test_mode_deserialization(cls, value):
        """Performs `test_mode`-deserialization."""
        return value

    @property
    def execution_context(self):
        """Returns execution context."""
        return self._execution_context

    @execution_context.setter
    def execution_context(self, execution_context):
        self._execution_context = execution_context

    @property
    def template(self):
        """Returns template-info."""
        return self._template

    @template.setter
    def template(self, template):
        self._template = template

    @property
    def data_selection(self):
        """Returns data-selection-info."""
        return self._data_selection

    @data_selection.setter
    def data_selection(self, data_selection):
        self._data_selection = data_selection

    @property
    def data_processing(self):
        """Returns data-processing-info."""
        return self._data_processing

    @data_processing.setter
    def data_processing(self, data_processing):
        self._data_processing = data_processing

    @property
    def archives(self):
        """Returns the available archive configurations."""
        return self._archives

    @archives.setter
    def archives(self, archives):
        self._archives = archives

    @property
    def default_target_archive_id(self):
        """Returns the default target-archive-id."""
        return self._default_target_archive_id

    @default_target_archive_id.setter
    def default_target_archive_id(self, default_target_archive_id):
        self._default_target_archive_id = default_target_archive_id
