from __future__ import annotations

import logging
import pathlib
import sys
import time

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.executor.workflow import TaskFuture
from webs.executor.workflow import WorkflowExecutor
from webs.logging import WORK_LOG_LEVEL
from webs.wf.synthetic.config import SyntheticWorkflowConfig
from webs.wf.synthetic.utils import randbytes

logger = logging.getLogger(__name__)


def noop_task(data: bytes, output_size: int, sleep: float) -> bytes:
    """No-op sleep task.

    Args:
        data: Input byte string.
        output_size: Size in bytes of output byte-string.
        sleep: Minimum runtime of the task. Time required to generate the
            output data will be subtracted from this sleep time.

    Returns:
        Byte-string of length `output_size`.
    """
    start = time.perf_counter_ns()
    result = randbytes(output_size)
    elapsed = (time.perf_counter_ns() - start) / 1e9

    # Remove elapsed time for generating result from remaining
    # sleep time.
    time.sleep(max(0, sleep - elapsed))
    return result


class SyntheticWorkflow(ContextManagerAddIn):
    """Synthetic workflow.

    Args:
        config: Workflow configuration.
    """

    name = 'synthetic'
    config_type = SyntheticWorkflowConfig

    def __init__(self, config: SyntheticWorkflowConfig) -> None:
        self.config = config
        super().__init__()

    @classmethod
    def from_config(cls, config: SyntheticWorkflowConfig) -> Self:
        """Initialize a workflow from a config.

        Args:
            config: Workflow configuration.

        Returns:
            Workflow.
        """
        return cls(config)

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Run the workflow.

        Args:
            executor: Workflow task executor.
            run_dir: Run directory.
        """
        initial_data = randbytes(self.config.task_data_bytes)
        tasks: list[TaskFuture[bytes]] = []

        for i in range(self.config.task_count):
            input_data = initial_data if i == 0 else tasks[-1]
            task = executor.submit(
                noop_task,
                input_data,
                output_size=self.config.task_data_bytes,
                sleep=self.config.task_sleep,
            )
            tasks.append(task)
            logger.log(
                WORK_LOG_LEVEL,
                f'Submitted task {i+1}/{self.config.task_count} '
                f'(task_id={task.info.task_id})',
            )

        for i, task in enumerate(tasks):
            task.result()
            logger.log(
                WORK_LOG_LEVEL,
                f'Received task {i+1}/{self.config.task_count} '
                f'(task_id={task.info.task_id})',
            )
