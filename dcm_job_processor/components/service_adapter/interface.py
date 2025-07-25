"""
This module extends the `dcm-common`-`ServiceAdapter`-interface for the
Job Processor-app.
"""

from typing import Any
import abc

from dcm_common.services import APIResult, ServiceAdapter as ServiceAdapter_

from dcm_job_processor.models.job_config import Stage
from dcm_job_processor.models.job_result import Record


class ServiceAdapter(ServiceAdapter_, metaclass=abc.ABCMeta):
    """
    Extended `ServiceAdapter`-interface adding requirements for
    * `export_target` and
    * `export_records`,
    as well as the attribute `_STAGE`.
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "_get_api_clients")
            and hasattr(subclass, "_get_api_endpoint")
            and hasattr(subclass, "_build_request_body")
            and hasattr(subclass, "success")
            and hasattr(subclass, "export_target")
            and hasattr(subclass, "export_records")
            and callable(subclass._get_api_clients)
            and callable(subclass._get_api_endpoint)
            and callable(subclass._build_request_body)
            and callable(subclass.success)
            and callable(subclass.export_target)
            and callable(subclass.export_records)
            or NotImplemented
        )

    _STAGE = Stage.IMPORT_IES

    @property
    def stage(self) -> Stage:
        """Returns `Stage` this adapter is associated with."""
        return self._STAGE

    @abc.abstractmethod
    def _get_api_clients(self) -> tuple[Any, Any]:
        """
        Returns a tuple of default- and submission-related API clients.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} missing implementation of "
            + "`_get_api_clients`."
        )

    @abc.abstractmethod
    def export_target(self, info: APIResult) -> Any:
        """
        Returns the first valid target that arises from the given
        `APIResult` or `None`.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} missing implementation of "
            + "`export_target`."
        )

    @abc.abstractmethod
    def export_records(self, info: APIResult) -> dict[str, Record]:
        """
        Returns a mapping of identifiers and `Record`-objects that arise
        from the given `APIResult`. This is used to bootstrap the
        `JobData`-contents on the first `Stage`.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} missing implementation of "
            + "`export_records`."
        )

    def post_process_record(self, info: APIResult, record: Record) -> None:
        """
        Performs post-processing actions on given `record` in place.
        """
        return
