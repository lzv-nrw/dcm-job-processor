"""
This module defines the `ProcessManager` component of the Job Processor-
app.
"""

from typing import Optional
import sys
from dataclasses import dataclass

from dcm_common.services import APIResult
from dcm_common.orchestra import JobInfo

from dcm_job_processor.models.job_config import Stage, JobConfig
from dcm_job_processor.models.job_result import Record, JobResult
from .task import Task, SubTask


@dataclass
class _RecordInfo:
    """Stores information about a record that is being processed."""

    identifier: str
    record: Record
    info: APIResult
    task: Task
    stage: Stage


class ProcessManager:
    """
    A `ProcessManager`
    * tracks progress
    * manages execution order and report data
    * configures the individual `Task`s for a request

    Keyword arguments:
    config -- configuration information for a request
    result -- target where results are linked to
    """

    _SEQUENCE_COMMON = (
        Stage.VALIDATION,
        Stage.PREPARE_IP,
        Stage.BUILD_SIP,
        Stage.TRANSFER,
        Stage.INGEST,
    )
    _SEQUENCE_IES = (
        Stage.IMPORT_IES,
        Stage.BUILD_IP,
    ) + _SEQUENCE_COMMON
    _SEQUENCE_IPS = (Stage.IMPORT_IPS,) + _SEQUENCE_COMMON

    def __init__(self, config: JobConfig, info: JobInfo) -> None:
        self.result: JobResult = info.report.data
        self.child_reports = info.report.children
        self._config = config
        self._sequence = self._get_sequence()

        # configure and add initial task to queue and progress-tracking
        self._root = APIResult()
        self._queue = [
            self._get_simple_task(
                "<bootstrap>", self._sequence[0], info=self._root
            )
        ]
        # keys are given by 'id(record)' (or None for first task)
        self._state: dict[Optional[int], _RecordInfo] = {
            None: _RecordInfo(
                "<bootstrap>",
                None,
                self._root,
                self._queue[0],
                self._sequence[0],
            ),
        }

    def _get_sequence(self) -> tuple[Stage, ...]:
        """
        Determines and returns the `Stage`-sequence for the given
        `JobConfig`.
        """
        # select base on which to build actual sequence
        base_sequence = (
            self._SEQUENCE_IPS
            if self._config.from_ == Stage.IMPORT_IPS
            else self._SEQUENCE_IES
        )

        # construct sequence by searching base for first match then add
        # everything after that
        # first, find correct starting position in base
        from_index, from_stage = next(
            # this construct introduces support for
            # having a child-stage of a meta-stage in
            # config.from_
            (
                (i, s)
                for i, s in enumerate(base_sequence)
                if self._config.from_ in s.self_and_children()
            ),
            (-1, None),
        )
        if from_stage is None:
            return ()
        result = (self._config.from_,)

        # find correct stopping position in base
        to_stage = next(
            # this construct introduces support for
            # having a child-stage of a meta-stage in
            # config.to
            (
                s
                for s in base_sequence
                if self._config.to in s.self_and_children()
            ),
            base_sequence[-1],
        )

        # detect whether it is a single stage-sequence
        if self._config.from_ in to_stage.self_and_children():
            return result
        # detect whether it is the last item in base_sequence
        if len(base_sequence) <= from_index + 1:
            return result

        # build remainder
        for stage in base_sequence[from_index + 1 :]:
            # iterate remaining items
            result += (stage,)
            if stage in to_stage.self_and_children():
                break

        # fix meta-stage in config.to if necessary
        if self._config.to is not None and to_stage != self._config.to:
            return result[0:-1] + (self._config.to,)
        return result

    @property
    def sequence(self) -> tuple[Stage, ...]:
        """Returns the `Stage`-sequence for the given `JobConfig`."""
        return self._sequence

    def _get_next_stage(
        self, stage: Optional[Stage] = None
    ) -> Optional[Stage]:
        """
        Helper for determining the next `Stage` of a sequence. Returns
        either the next `Stage` or `None` indicating to stop execution
        of a branch.
        """
        if stage is None:
            return self._sequence[0]
        next_index = self._sequence.index(stage) + 1
        if next_index >= len(self._sequence):
            return None
        return self._sequence[next_index]

    def _get_simple_task(
        self,
        identifier: str,
        stage: Stage,
        target: Optional[dict] = None,
        info: Optional[APIResult] = None,
    ) -> Task:
        """
        Returns `Task` with `SubTask`s as listed by `Stage.stages`.
        `Task` is annotated with `stage` while `SubTask`s are labeled
        with items from `Stage.stages`. The given `target` and `info` is
        shared with all `SubTask`s. The field `identifier` is used to
        associate the generated `Task` with a record.
        """
        return Task(
            identifier,
            stage,
            {
                s: SubTask(
                    base_request_body=self._config.args.get(s, {}),
                    target=target,
                    info=info,
                )
                for s in stage.stages()
            },
        )

    @property
    def queue(self) -> list[Task]:
        """Returns currently queued `Task`s."""
        return self._queue.copy()

    def in_process(self) -> bool:
        """
        Returns `True` if there are unfinished `Record`s remaining in
        the internal tracking and otherwise `False`.
        """
        # either initial Task still exists or there are any uncompleted
        # records
        return None in self._state or any(
            not record_info.record.completed
            for record_info in self._state.values()
            if record_info.record is not None
        )

    def flush(self) -> None:
        """Empties current queue."""
        self._queue = []

    def update(self, flush: bool = False) -> None:
        """
        Updates current queue with `Task`s. If queue is not empty and
        `flush` has not been set, the queue is kept as is.
        """
        if flush:
            self.flush()
        elif self.queue:
            return

        # result bootstrap: check if initial Task is still running
        if None in self._state:
            # find new records and link to internal tracking
            for identifier, record in (
                self._sequence[0]
                .value.adapter.export_records(self._root)
                .items()
            ):
                # add new record to result
                if identifier not in self.result.records:
                    # link child-report id to record
                    # this assumes that the bootstrap-stage appears only once
                    # in the report-children
                    if record.stages.get(self._sequence[0]) is not None:
                        record.stages[self._sequence[0]].log_id = next(
                            (
                                c
                                for c in (self.child_reports or {}).keys()
                                if c.endswith(
                                    self._sequence[0].value.identifier
                                )
                            ),
                            None,
                        )
                    else:
                        print(
                            "ProcessManager was unable to link newly exported "
                            + f"record '{identifier}' for stage "
                            + f"'{self._sequence[0].value.identifier}' to a "
                            + "child-report: stage does not exist in record. "
                            + "This is probably an issue with a ServiceAdapter"
                            + "'s export_records-implementation.",
                            file=sys.stderr,
                        )

                    # link record to result
                    self.result.records[identifier] = record.make_picklable()
                    self._state[id(record)] = _RecordInfo(
                        identifier,
                        record,
                        self._root,
                        self._state[None].task,
                        self._sequence[0],
                    )
            # if completed, remove initial Task from tracking
            if self._root.completed:
                # if not successful, append initial record
                if not self._root.success and len(self.result.records) == 0:
                    self.result.records["<bootstrap>"] = Record(
                        True,
                        False,
                        {self._sequence[0].value.identifier: self._root},
                    )
                del self._state[None]

        # find new Tasks
        for record_info in self._state.values():
            if record_info.record is None or record_info.record.completed:
                continue
            if record_info.task.completed:
                if not record_info.task.success:
                    record_info.record.completed = True
                    record_info.record.success = False
                    continue
                next_stage = self._get_next_stage(record_info.stage)
                if next_stage is None:
                    record_info.record.completed = True
                    record_info.record.success = record_info.task.success
                    continue
                # meta-stages with multiple subtasks are required to not
                # generate independent targets (they need to be
                # symmetric so that here any of them can be used)
                first_child_or_self = record_info.stage.stages()[0]
                # create Task
                self._queue.append(
                    self._get_simple_task(
                        record_info.identifier,
                        next_stage,
                        first_child_or_self.value.adapter.export_target(
                            APIResult(
                                report=(
                                    None
                                    if self.child_reports is None
                                    else self.child_reports.get(
                                        record_info.record.stages[
                                            first_child_or_self.value.identifier
                                        ].log_id
                                    )
                                )
                            )
                            # record_info.info
                        ),
                    )
                )
                # link info objects to SubTasks in Task
                for stage in next_stage.stages():
                    record_info.record.stages[stage.value.identifier] = (
                        APIResult()
                    )
                    self._queue[-1].subtasks[stage].info = (
                        record_info.record.stages[stage.value.identifier]
                    )
                record_info.task = self._queue[-1]
                record_info.stage = next_stage
