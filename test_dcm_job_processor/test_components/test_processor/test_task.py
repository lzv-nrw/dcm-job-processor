"""
Test module for the sub-component `Task`.
"""

from time import sleep, time

import pytest
from dcm_common.services import APIResult
from dcm_common.orchestra import JobConfig, JobInfo, JobContext

from dcm_job_processor.models import Stage, Report, JobResult, Record
from dcm_job_processor.components.processor.task import Task, SubTask


@pytest.fixture(
    name="initialize_stage_adapter_link", autouse=True, scope="module"
)
def _initialize_stage_adapter_link(request):
    class FakeAdapter:
        url = None
        interval = None
        timeout = None
        request_timeout = None
        max_retries = None
        retry_interval = None
        retry_on = None

        def __init__(self, *args, **kwargs):
            pass

        def run(
            self,
            base_request_body: dict,
            target,
            info: APIResult,
            post_submission_hooks=None,
        ):
            info.report = {"start": time()}
            sleep(base_request_body["duration"])
            info.report["end"] = time()
            info.success = base_request_body["success"]
            info.completed = True

    Stage.IMPORT_IES.value.adapter = FakeAdapter()
    Stage.IMPORT_IPS.value.adapter = FakeAdapter()

    def reset():
        for stage in Stage:
            stage.value.adapter = None

    request.addfinalizer(reset)


def wait_for_completion(task: Task, interval: float = 0.0001) -> None:
    """Wait until `task` is flagged as `completed`."""
    while not task.completed:
        sleep(interval)


@pytest.mark.parametrize(
    "success", [True, False], ids=["success", "no-success"]
)
def test_run_minimal(success):
    """Test method `run` for `Task` for minimal setup."""
    info = APIResult()
    task = Task(
        "some-id",
        Stage.IMPORT_IES,
        subtasks={
            Stage.IMPORT_IES: SubTask(
                {"duration": 0, "success": success}, info=info
            )
        },
    )
    task.run(
        JobInfo(
            JobConfig("", {}, {}),
            report=Report(
                children={}, data=JobResult(records={"some-id": Record()})
            ),
        ),
        JobContext(lambda: None, lambda c: None, lambda t: None),
        0.001,
    )
    wait_for_completion(task)
    assert info.completed
    assert info.success is success
    assert task.success is success


@pytest.mark.parametrize(
    "success", [True, False], ids=["success", "no-success"]
)
def test_run_two_tasks(success):
    """Test method `run` for `Task` for a two-task setup."""
    info0 = APIResult()
    info1 = APIResult()
    task = Task(
        "some-id",
        Stage.IMPORT_IES,
        subtasks={
            Stage.IMPORT_IES: SubTask(
                {"duration": 0.05, "success": True}, info=info0
            ),
            Stage.IMPORT_IPS: SubTask(
                {"duration": 0.05, "success": success}, info=info1
            ),
        },
    )
    task.run(
        JobInfo(
            JobConfig("", {}, {}),
            report=Report(
                children={}, data=JobResult(records={"some-id": Record()})
            ),
        ),
        JobContext(lambda: None, lambda c: None, lambda t: None),
        0.001,
    )
    wait_for_completion(task)
    assert info0.completed and info1.completed
    assert info0.success and info1.success is success
    assert task.success is success
    # assert concurrency
    assert info0.report["start"] < info1.report["end"]
