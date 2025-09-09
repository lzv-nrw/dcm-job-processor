"""
This module defines the `Processor` component of the Job Processor-app.
"""

from typing import Optional, Callable
from time import sleep

from dcm_common.orchestra import JobContext, JobInfo

from dcm_job_processor.models import JobConfig, Record
from .process_manager import ProcessManager


class Processor:
    """
    A `Processor` handles the top-level orchestration of the individual
    parts of a request.
    """

    def process(
        self,
        info: JobInfo,
        context: JobContext,
        config: JobConfig,
        interval: float = 0.1,
        on_update: Optional[Callable[[], None]] = None,
    ):
        """
        Execute a job as defined in `config` while continuously updating
        `result` with `interval`.
        """

        if info.report.children is None:
            info.report.children = {}

        manager = ProcessManager(config, info)

        current_result = info.report.json
        tasks = []
        while manager.in_process():
            # start all pending Tasks
            for task in manager.queue:
                if (
                    task.identifier != "<bootstrap>"
                    and task.identifier not in info.report.data.records
                ):
                    info.report.data.records[task.identifier] = Record()
                tasks.append(task.run(info, context, interval))

            # wait
            sleep(interval)

            # find new Tasks
            manager.update(flush=True)

            context.push()
            new_result = info.report.json
            if on_update and current_result != new_result:
                on_update()
                current_result = new_result

        # block until all tasks are completed
        for task in tasks:
            task.join()
