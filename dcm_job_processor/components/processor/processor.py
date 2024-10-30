"""
This module defines the `Processor` component of the Job Processor-app.
"""

from time import sleep

from dcm_common.orchestration import Children

from dcm_job_processor.models.job_config import JobConfig
from dcm_job_processor.models.job_result import JobResult
from .process_manager import ProcessManager


class Processor:
    """
    A `Processor` handles the top-level orchestration of the individual
    parts of a request.
    """

    def process(
        self,
        result: JobResult, push, children: Children,
        config: JobConfig,
        interval: float = 0.1
    ):
        """
        Execute a job as defined in `config` while continuously updating
        `result` with `interval`.
        """
        manager = ProcessManager(config, result)

        while manager.in_process():
            # start all pending Tasks
            for task in manager.queue:
                task.run(interval, push, children)

            # wait
            sleep(interval)

            # find new Tasks
            manager.update(flush=True)

            push()
