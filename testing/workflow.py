from __future__ import annotations

import pathlib
import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


from webs.config import Config
from webs.context import ContextManagerAddIn
from webs.executor.workflow import WorkflowExecutor


def task() -> None:
    pass


class TestWorkflowConfig(Config):
    """Test workflow configuration."""

    tasks: int = 3


class TestWorkflow(ContextManagerAddIn):
    """Test workflow."""

    name = 'test-workflow'
    config_type = TestWorkflowConfig

    def __init__(self, tasks: int) -> None:
        self.tasks = tasks
        super().__init__()

    @classmethod
    def from_config(cls, config: TestWorkflowConfig) -> Self:
        return cls(tasks=config.tasks)

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        task_futures = [executor.submit(task) for _ in range(self.tasks)]

        for task_future in task_futures:
            task_future.result()
