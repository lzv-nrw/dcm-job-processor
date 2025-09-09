"""
This module defines the `Task` and `SubTask` components of the Job
Processor-app.
"""

from typing import Optional
from time import sleep
from threading import Thread
from dataclasses import dataclass, field
from uuid import uuid4

from dcm_common import LoggingContext
from dcm_common.services import APIResult
from dcm_common.orchestra.models import JobContext, ChildJob, JobInfo

from dcm_job_processor.models import Stage, RecordStageInfo


@dataclass
class SubTask:
    """
    A `SubTask` corresponds to a (sub-)`Stage`, i.e. the smallest unit
    of work in the sense that it cannot be broken down into parallel
    steps (usually a single request to a DCM-service).
    """

    base_request_body: dict = field(default_factory=lambda: {})
    target: dict = field(default_factory=lambda: {})
    info: APIResult = field(default_factory=APIResult)


class Task:
    """
    A `Task` is a collection of `SubTask`s that can be run in parallel.

    This collection is associated with a `Stage` in the stage sequence
    defined by the `ProcessManager`. Its `subtasks` are also associated
    with a `Stage` (which is usually more fine-grained, i.e. a sub-
    `Stage`).

    The `identifier` is used to locate child report output destinations
    in case of an abort.
    """

    def __init__(
        self,
        identifier: str,
        stage: Stage,
        subtasks: dict[Stage, SubTask],
    ) -> None:
        self.identifier = identifier
        self.stage = stage
        self.subtasks = subtasks
        self._thread: Optional[Thread] = None
        self._started = False

    def run(
        self,
        info: JobInfo,
        context: JobContext,
        interval: float = 0.1,
    ) -> Thread:
        """
        Start execution of a threaded task (non-blocking).
        """
        if self._started:
            raise RuntimeError("Task has already been started before.")

        def _run_task():
            subtask_threads: dict[Stage, Thread] = {}
            # initialize and run individual Tasks
            child_tokens = []
            for stage, subtask in self.subtasks.items():
                child_token = str(uuid4())
                child_tokens.append(child_token)
                log_id = f"{child_token}@{stage.value.identifier}"
                info.report.children[log_id] = {}
                if self.identifier != "<bootstrap>":
                    info.report.data.records[self.identifier].stages[
                        stage.value.identifier
                    ] = RecordStageInfo(log_id=log_id)

                # define custom callback for abort to avoid pickling issues
                # encountered in the ServiceAdapter-default (only in docker
                # for some reason)
                # (likely related to adapter-modules importing the Stage-enum)
                def get_abort_callback(
                    token,
                    child_name,
                    adapter_type,
                    url,
                    interval,
                    timeout,
                    request_timeout,
                    max_retries,
                    retry_interval,
                    retry_on,
                ):
                    def child_abort(info, context):
                        """
                        Helper function for abort via `ServiceAdapter`.
                        """
                        # sdk uses urllib3 which does not work well with
                        # dill (likely due to connection-pooling); create new
                        # instance of adapter instead to avoid pickling
                        adapter = adapter_type(
                            url,
                            interval,
                            timeout,
                            request_timeout,
                            max_retries,
                            retry_interval,
                            retry_on,
                        )
                        # abort
                        adapter.abort(
                            None,
                            args=(
                                token,
                                {
                                    "origin": context.origin,
                                    "reason": context.reason,
                                },
                            ),
                        )
                        # fetch latest report
                        if info.report.children is None:
                            info.report.children = {}
                        try:
                            info.report.children[child_name] = (
                                adapter.get_info(token).report
                            )
                        # pylint: disable=broad-exception-caught
                        except Exception as exc_info:
                            info.report.log.log(
                                LoggingContext.ERROR,
                                origin="Job Processor",
                                body=(
                                    "Failed to fetch latest results from child "
                                    + f"'{child_name}' at '{url}': {exc_info}"
                                ),
                            )

                    return child_abort

                context.add_child(
                    ChildJob(
                        child_token,
                        log_id,
                        get_abort_callback(
                            child_token,
                            log_id,
                            stage.value.adapter.__class__,
                            stage.value.adapter.url,
                            stage.value.adapter.interval,
                            stage.value.adapter.timeout,
                            stage.value.adapter.request_timeout,
                            stage.value.adapter.max_retries,
                            stage.value.adapter.retry_interval,
                            stage.value.adapter.retry_on,
                        ),
                    )
                )
                context.push()

                subtask.info.report = info.report.children[log_id]
                subtask_threads[stage] = Thread(
                    target=stage.value.adapter.run,
                    args=(
                        subtask.base_request_body | {"token": child_token},
                        subtask.target,
                        subtask.info,
                    ),
                )
                subtask_threads[stage].start()
            # wait until completion
            while any(
                subtask.is_alive() for subtask in subtask_threads.values()
            ):
                sleep(interval)
            for token in child_tokens:
                context.remove_child(token)
                context.push()
            # finalize APIResult-objects
            if self.identifier == "<bootstrap>":
                return
            for stage, subtask in self.subtasks.items():
                info.report.data.records[self.identifier].stages[
                    stage.value.identifier
                ].completed = True
                info.report.data.records[self.identifier].stages[
                    stage.value.identifier
                ].success = (
                    subtask.info.success is not None and subtask.info.success
                )
            context.push()

        self._thread = Thread(target=_run_task)
        self._thread.start()
        self._started = True
        return self._thread

    @property
    def completed(self) -> bool:
        """Returns `True` if the `Task` has terminated."""
        if self._thread is None:
            return False
        return self._started and not self._thread.is_alive()

    @property
    def success(self) -> bool:
        """
        Returns `True` if the `Task` has terminated and has been
        successful.
        """
        return self.completed and all(
            t.info.success for t in self.subtasks.values()
        )
