"""
This module extends the `dcm-common`-`ServiceAdapter`-interface for the
Job Processor-app.
"""

import abc

from dcm_common.services import APIResult, ServiceAdapter as ServiceAdapter_

from dcm_job_processor.models import Stage, Record, JobConfig


class ServiceAdapter(ServiceAdapter_, metaclass=abc.ABCMeta):
    """
    Extended `ServiceAdapter`-interface which adds methods
    * `build_request_body` and
    * `eval`,
    as well as the attribute `stage`.
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "_get_api_clients")
            and hasattr(subclass, "_get_api_endpoint")
            and hasattr(subclass, "_build_request_body")
            and hasattr(subclass, "success")
            and hasattr(subclass, "build_request_body")
            and hasattr(subclass, "continue_")
            and hasattr(subclass, "eval")
            and hasattr(subclass, "stage")
            and callable(subclass._get_api_clients)
            and callable(subclass._get_api_endpoint)
            and callable(subclass._build_request_body)
            and callable(subclass.success)
            and callable(subclass.build_request_body)
            and callable(subclass.continue_)
            and callable(subclass.eval)
            or NotImplemented
        )

    _STAGE = Stage.IMPORT_IES

    @property
    def stage(self) -> Stage:
        """Returns `Stage` this adapter is associated with."""
        return self._STAGE

    def success(self, info: APIResult) -> bool:
        return info.report.get("data", {}).get("success", False)

    def _build_request_body(self, base_request_body, target):
        return base_request_body

    @abc.abstractmethod
    def build_request_body(
        self, job_config: JobConfig, record: Record
    ) -> dict:
        """Returns request body."""
        raise NotImplementedError(
            f"{self.__class__.__name__} is missing its implementation of "
            + "`build_request_body`."
        )

    @abc.abstractmethod
    def eval(self, record: Record, api_result: APIResult) -> None:
        """
        Performs post-processing actions on given `record` in place.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} missing implementation of " + "`eval`."
        )
