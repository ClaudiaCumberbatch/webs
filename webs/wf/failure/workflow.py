from __future__ import annotations

import logging
import pathlib
import random
import sys
from pydantic_core import PydanticUndefined

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.executor.workflow import WorkflowExecutor
from webs.logging import WORK_LOG_LEVEL
from webs.wf.failure.config import FailureWorkflowConfig
from webs.workflow import register
from webs.wf.failure.failure_lib import *
from webs.workflow import get_registered
from webs.record import JSONRecordLogger

logger = logging.getLogger(__name__)

default_config_dic = {
    'mode': 'random', 
    'map_task_count': 2, 
}

@register()
class FailurerWorkflow(ContextManagerAddIn):
    """Failure injection workflow.

    Args:
        config: Workflow configuration.
    """

    name = 'failure-injection'
    config_type = FailureWorkflowConfig

    def __init__(self, config: FailureWorkflowConfig) -> None:
        self.config = config
        super().__init__()

    @classmethod
    def from_config(cls, config: FailureWorkflowConfig) -> Self:
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
        if random.random() < self.config.failure_rate:
            task = executor.submit(fails)
            logger.log(
                WORK_LOG_LEVEL,
                f'failure task finish with result {task.result()}',
            )
        else:
            # workflow = get_registered()[self.config.true_workflow].from_config(executor.config.workflow) # here config.workflow is a config instead of a workflow
            # record_logger = JSONRecordLogger(executor.config.run.task_record_file_name)

            # with workflow, record_logger, executor:
            #     workflow.run(executor=executor, run_dir=run_dir)

            config_type = get_registered()[self.config.true_workflow].config_type
            cfg = config_type(**default_config_dic)

            for key in cfg.__fields__:
                print(f"{key} @ {cfg.__fields__[key]}")
                if cfg.__fields__[key].default is PydanticUndefined:
                    setattr(cfg, key, default_config_dic[key])
            workflow = get_registered()[self.config.true_workflow].from_config(cfg)

            record_logger = JSONRecordLogger(executor.config.run.task_record_file_name)

            with workflow, record_logger, executor:
                workflow.run(executor=executor, run_dir=run_dir)
