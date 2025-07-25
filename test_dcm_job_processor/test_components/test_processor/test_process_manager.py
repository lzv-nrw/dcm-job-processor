"""
Test module for the sub-component `ProcessManager`.
"""

from time import sleep

import pytest
from dcm_common.services import APIResult

from dcm_job_processor.models import Stage, JobConfig, Record
from dcm_job_processor.components.processor.process_manager import (
    ProcessManager
)
from dcm_job_processor.components.processor.task import Task


@pytest.fixture(
    name="initialize_stage_adapter_link", autouse=True, scope="module"
)
def _initialize_stage_adapter_link(request):
    class FakeAdapter:
        """"""
        def __init__(self, stage: Stage, nrecords: int):
            self.stage = stage
            self.nrecords = nrecords
        def run(self, base_request_body, target, info, post_submission_hooks=None):
            info.success = base_request_body.get("success", True)
        def export_target(self, info: APIResult):
            return None
        def export_records(self, info: APIResult):
            if info.success:
                return {
                    f"ie{i}": Record(
                        False, stages={self.stage: None}
                    ) for i in range(self.nrecords)
                }
            return {}
    Stage.IMPORT_IES.value.adapter = FakeAdapter(Stage.IMPORT_IES, 1)
    Stage.BUILD_IP.value.adapter = FakeAdapter(Stage.BUILD_IP, 1)
    Stage.IMPORT_IPS.value.adapter = FakeAdapter(Stage.IMPORT_IPS, 2)

    def reset():
        for stage in Stage:
            stage.value.adapter = None

    request.addfinalizer(reset)


def run_sequentially(task: Task, interval: float = 0.0001) -> None:
    """Run `task` sequentially."""
    task.run(interval)
    while not task.completed:
        sleep(interval)


@pytest.fixture(name="config")
def _config():
    return JobConfig(
        from_=Stage.IMPORT_IES, to=Stage.VALIDATION, args={}
    )


@pytest.fixture(name="pm")
def _pm(config: JobConfig):
    return ProcessManager(config)


@pytest.mark.parametrize(
    ("from_", "to", "result"),
    [
        (Stage.BUILD_IP, Stage.BUILD_IP, (Stage.BUILD_IP,)),
        (Stage.BUILD_IP, Stage.VALIDATION, (Stage.BUILD_IP, Stage.VALIDATION)),
        (
            Stage.BUILD_IP, Stage.VALIDATION_METADATA, (
                Stage.BUILD_IP, Stage.VALIDATION_METADATA
            )
        ),
        (
            Stage.VALIDATION_METADATA, Stage.BUILD_SIP, (
                Stage.VALIDATION_METADATA, Stage.PREPARE_IP, Stage.BUILD_SIP
            )
        ),
        (
            Stage.VALIDATION_METADATA, Stage.VALIDATION_PAYLOAD, (
                Stage.VALIDATION_METADATA,  # expected as in, unintended usage
            )
        ),
        (
            Stage.BUILD_IP, Stage.BUILD_SIP, (
                Stage.BUILD_IP, Stage.VALIDATION, Stage.PREPARE_IP,
                Stage.BUILD_SIP
            )
        ),
        (
            Stage.BUILD_IP, None, (
                Stage.BUILD_IP, Stage.VALIDATION, Stage.PREPARE_IP,
                Stage.BUILD_SIP, Stage.TRANSFER, Stage.INGEST
            )
        ),
    ],
    ids=[
        "single-stage", "two-stage", "ending-on-substage",
        "starting-with-substage", "substage-to-substage", "three-stage",
        "full",
    ]
)
def test_sequence(from_, to, result):
    """
    Test constructor of class `ProcessManager`.

    Test the generation of processing-sequence in class `ProcessManager`
    (based on given JobConfig).
    """
    pm = ProcessManager(JobConfig(from_=from_, to=to))
    assert pm.sequence == result


def test_flush(pm: ProcessManager):
    """
    Test method `flush` of class `ProcessManager`.
    """
    assert len(pm.queue) == 1
    pm.flush()
    assert len(pm.queue) == 0


@pytest.mark.parametrize(
    "flush",
    [
        True, False
    ],
    ids=["flush", "no-flush"]
)
def test_update_flush(flush, pm: ProcessManager):
    """
    Test `flush`-argument of method `update` of class `ProcessManager`.
    """
    assert len(pm.queue) == 1
    pm.update(flush=flush)
    assert len(pm.queue) == int(not flush)


def test_update_minimal():
    """
    Test method `update` of class `ProcessManager` for minimal setup.
    """
    pm = ProcessManager(JobConfig(Stage.IMPORT_IES, Stage.IMPORT_IES, {}))
    run_sequentially(pm.queue[0])
    pm.update(flush=True)
    assert len(pm.queue) == 0


@pytest.mark.parametrize(
    "success",
    [True, False],
    ids=["success", "no-success"]
)
@pytest.mark.parametrize(
    "completed",
    [True, False],
    ids=["completed", "not-completed"]
)
def test_update_two_stages(completed, success):
    """
    Test method `update` of class `ProcessManager` for a two-`Stage`
    setup.
    """
    pm = ProcessManager(
        JobConfig(
            Stage.IMPORT_IES, Stage.BUILD_IP, {
                "import_ies": {"success": success}
            }
        )
    )
    if completed:
        run_sequentially(pm.queue[0])
    pm.update(flush=True)
    if completed and success:
        assert len(pm.queue) == 1
        assert pm.queue[0].stage == Stage.BUILD_IP
    else:
        assert len(pm.queue) == 0


def test_update_multiple_records():
    """
    Test method `update` of class `ProcessManager` for a two-`Stage`
    setup generating multiple records.
    """
    pm = ProcessManager(JobConfig(Stage.IMPORT_IPS, Stage.VALIDATION, {}))
    run_sequentially(pm.queue[0])
    pm.update(flush=True)
    assert len(pm.queue) == 2


def test_update_with_substages():
    """
    Test method `update` of class `ProcessManager` for a `Stage` with
    multiple substages.
    """
    pm = ProcessManager(JobConfig(Stage.BUILD_IP, Stage.VALIDATION, {}))
    run_sequentially(pm.queue[0])
    pm.update(flush=True)
    assert len(pm.queue) == 1
    assert len(pm.queue[0].subtasks) == 2
    # validate that substages do not share their info-object
    infos = tuple(task.info for task in pm.queue[0].subtasks.values())
    assert id(infos[0]) != id(infos[1])


def test_in_process():
    """Test method `in_process` of class `ProcessManager`."""
    pm = ProcessManager(JobConfig(Stage.IMPORT_IES, Stage.BUILD_IP, {}))
    assert pm.in_process()
    run_sequentially(pm.queue[0])
    pm.update(flush=True)
    assert pm.in_process()
    run_sequentially(pm.queue[0])
    pm.update(flush=True)
    assert not pm.in_process()


def test_in_process_failed_job():
    """
    Test method `in_process` of class `ProcessManager` for failed first
    `Stage`.
    """
    pm = ProcessManager(
        JobConfig(Stage.IMPORT_IES, Stage.BUILD_IP, {
            "import_ies": {"success": False}
        })
    )
    assert pm.in_process()
    run_sequentially(pm.queue[0])
    pm.update(flush=True)
    assert not pm.in_process()
