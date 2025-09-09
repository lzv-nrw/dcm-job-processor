"""
Test module for the `Processor` component.
"""

from typing import Optional
from unittest.mock import patch
from functools import partial

import pytest
from dcm_common.orchestra import JobConfig, JobInfo, JobContext

from dcm_job_processor.models import (
    Stage,
    JobConfig as ProcessorJobConfig,
    JobResult,
    Report,
)
from dcm_job_processor.components import Processor


class FakeThread:
    def join(self):
        pass


class FakeTask:
    def __init__(self, identifier, cmd):
        self.identifier = identifier
        self.cmd = cmd

    def run(self, interval, push=None, children=None):
        self.cmd()
        return FakeThread()


@pytest.fixture(name="processor")
def _processor():
    return Processor()


@pytest.fixture(name="fake_process_manager")
def _fake_process_manager():
    """Returns a factory for faked ProcessManagers."""

    def _(
        queue: list[FakeTask], queues: Optional[list[list[FakeTask]]] = None
    ):
        queues = queues or []

        class FakeProcessManager:
            def __init__(self, config, result=None):
                self.queue = queue
                self._state = -1

            def in_process(self):
                return self._state < len(queues)

            def flush(self):
                self.queue = []

            def update(self, flush):
                if flush:
                    self.flush()
                self._state += 1
                try:
                    self.queue = queues[self._state]
                except IndexError:
                    self.queue = []

        return FakeProcessManager

    return _


@pytest.mark.parametrize("number", [0, 1, 2], ids=["single", "two", "three"])
def test_process_queue(number, processor: Processor, fake_process_manager):
    """Test method `process` of class `Processor` with single queue."""

    result = JobResult()
    queue = [
        FakeTask("task-0", partial(result.records.update, {f"ie{i}": {}}))
        for i in range(number)
    ]
    with patch(
        "dcm_job_processor.components.processor.processor.ProcessManager",
        fake_process_manager(queue),
    ):
        processor.process(
            JobInfo(JobConfig("", {}, {}), report=Report(children={})),
            JobContext(lambda: None),
            ProcessorJobConfig(Stage.IMPORT_IES),
            interval=0.0001,
        )
    assert len(result.records) == number


def test_process_multiple_queues(processor: Processor, fake_process_manager):
    """
    Test method `process` of class `Processor` with multiple queues.
    """

    result = JobResult()
    queue = [
        FakeTask("task-0", lambda: result.records.update({"ie0": {}})),
        FakeTask("task-0", lambda: result.records.update({"ie1": {}})),
    ]
    queues = [
        [
            FakeTask("task-0", lambda: result.records.update({"ie2": {}})),
            FakeTask("task-0", lambda: result.records.update({"ie3": {}})),
        ],
        [
            FakeTask("task-0", lambda: result.records.update({"ie4": {}})),
            FakeTask("task-0", lambda: result.records.update({"ie5": {}})),
        ],
    ]
    with patch(
        "dcm_job_processor.components.processor.processor.ProcessManager",
        fake_process_manager(queue, queues),
    ):
        processor.process(
            JobInfo(JobConfig("", {}, {}), report=Report(children={})),
            JobContext(lambda: None),
            ProcessorJobConfig(Stage.IMPORT_IES),
            interval=0.0001,
        )
    assert len(result.records) == 6


def test_process_wait(processor: Processor, fake_process_manager):
    """
    Test method `process` of class `Processor` with multiple queues
    but an intermediate empty queue.
    """

    result = JobResult()
    queue = [FakeTask("task-0", lambda: result.records.update({"ie0": {}}))]
    queues = [
        [],
        [FakeTask("task-0", lambda: result.records.update({"ie1": {}}))],
    ]
    with patch(
        "dcm_job_processor.components.processor.processor.ProcessManager",
        fake_process_manager(queue, queues),
    ):
        processor.process(
            JobInfo(JobConfig("", {}, {}), report=Report(children={})),
            JobContext(lambda: None),
            ProcessorJobConfig(Stage.IMPORT_IES),
            interval=0.0001,
        )
    assert len(result.records) == 2


def test_process_push(processor: Processor, fake_process_manager):
    """
    Test argument `push` for method `process` of class `Processor`.
    """

    result = JobResult()
    number = 4
    queue = []
    queues = [[] for _ in range(number - 1)]
    with patch(
        "dcm_job_processor.components.processor.processor.ProcessManager",
        fake_process_manager(queue, queues),
    ):
        data = {"counter": 0}
        processor.process(
            JobInfo(JobConfig("", {}, {}), report=Report(children={})),
            JobContext(lambda: data.update({"counter": data["counter"] + 1})),
            ProcessorJobConfig(Stage.IMPORT_IES),
            interval=0.0001,
        )
    assert data["counter"] == number


def test_process_on_update(processor: Processor, fake_process_manager):
    """
    Test method `process` of class `Processor` for the `on_update`
    argument.
    """

    result = JobResult(records={"ie": []})
    number = 5
    queue = []
    queues = [
        (
            # do nothing in first iterations, then change result.records
            # to trigger call to on_update
            [FakeTask("task-0", lambda: result.records["ie"].append(None))]
            if i >= 2
            else []
        )
        for i in range(number - 1)
    ]
    with patch(
        "dcm_job_processor.components.processor.processor.ProcessManager",
        fake_process_manager(queue, queues),
    ):
        data = {"counter": 0}
        processor.process(
            JobInfo(JobConfig("", {}, {}), report=Report(children={})),
            JobContext(lambda: None),
            ProcessorJobConfig(Stage.IMPORT_IES),
            interval=0.0001,
            on_update=lambda: data.update({"counter": data["counter"] + 1}),
        )
    assert data["counter"] < number
