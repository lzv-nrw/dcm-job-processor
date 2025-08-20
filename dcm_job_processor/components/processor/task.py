"""
This module defines the `Task` and `SubTask` components of the Job
Processor-app.
"""

from typing import Optional
from time import sleep
from threading import Thread
from dataclasses import dataclass, field
from uuid import uuid4

from dcm_common.orchestration import Children
from dcm_common.services import APIResult

from dcm_job_processor.models.job_config import Stage


def task_report_target_destination(data, child):
    record_id, stage_id = child.id_
    if record_id not in data.data.records:
        # report does not exist yet - should only happend for bootstrap
        return {child.id_: {}}
    if stage_id not in data.data.records[record_id].stages:
        # report does not exist yet - should only happend for bootstrap
        return {child.id_: {}}
    if data.data.records[record_id].stages[stage_id].report is None:
        data.data.records[record_id].stages[stage_id].report = {}
    return {child.id_: data.data.records[record_id].stages[stage_id].report}


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
        interval: float = 0.1,
        push=None,
        children: Optional[Children] = None
    ) -> None:
        """
        Start execution of a threaded task (non-blocking).
        """
        if self._started:
            raise RuntimeError("Task has already been started before.")

        if push is None:
            def push():
                pass
        if children is None:
            children = Children()

        def _run_task():
            subtask_threads: dict[Stage, Thread] = {}
            submission_token = str(uuid4())
            # initialize and run individual Tasks
            for stage, subtask in self.subtasks.items():
                subtask_threads[stage] = Thread(
                    target=stage.value.adapter.run,
                    args=(
                        subtask.base_request_body
                        | {"token": submission_token},
                        subtask.target,
                        subtask.info,
                    ),
                    kwargs={
                        "post_submission_hooks": (
                            # link to children
                            children.link_ex(
                                url=stage.value.url,
                                abort_path=stage.value.abort_path,
                                tag=f"{self.identifier}:{stage.value.identifier}",
                                child_id=(
                                    self.identifier,
                                    stage.value.identifier,
                                ),
                                post_link_hook=push,
                                report_target_destination=task_report_target_destination,
                            ),
                        )
                    },
                )
                subtask_threads[stage].start()
            # wait until completion
            while any(
                subtask.is_alive() for subtask in subtask_threads.values()
            ):
                sleep(interval)
            for stage, subtask in self.subtasks.items():
                try:
                    children.remove(
                        f"{self.identifier}:{stage.value.identifier}"
                    )
                except KeyError:
                    # submission via adapter gave `None`; nothing to abort
                    pass
            push()
            # finalize APIResult-objects
            for stage, subtask in self.subtasks.items():
                subtask.info.completed = True
                subtask.info.success = (
                    subtask.info.success and subtask.info.success is not None
                )

        self._thread = Thread(target=_run_task)
        self._thread.start()
        self._started = True

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
