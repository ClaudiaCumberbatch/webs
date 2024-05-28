from __future__ import annotations

from concurrent.futures import Executor
import logging
import pathlib
import random
import sys
from pydantic_core import PydanticUndefined

from webs.data.transform import TaskDataTransformer

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.executor.workflow import TaskFuture, WorkflowExecutor
from webs.logging import WORK_LOG_LEVEL
from webs.wf.failure.config import FailureWorkflowConfig
from webs.wf.failure.failure_lib import *
from webs.record import JSONRecordLogger, RecordLogger
from webs.workflow import get_registered_workflow

from types import TracebackType
from typing import Any
from typing import Callable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

P = ParamSpec('P')
T = TypeVar('T')

logger = logging.getLogger(__name__)

default_config_dic = {
    'mode': 'random', 
    'map_task_count': 5, 
}

def new_func(failure_rate, fail_task, success_task, *args, **kwargs):
    if random.random() < failure_rate:
        return fail_task()
    else:
        return success_task(*args, **kwargs)

class FlawExecutor(WorkflowExecutor):
    def __init__(
            self, 
            failure_rate: float,
            failure_type: str,
            compute_executor: Executor, 
            config, 
            *, 
            data_transformer: TaskDataTransformer[Any] | None = None, 
            record_logger: RecordLogger | None = None
        ) -> None:
        self.failure_rate = failure_rate
        self.failure_type = failure_type
        super().__init__(
            compute_executor, 
            config, 
            data_transformer=data_transformer, 
            record_logger=record_logger
        )

    def get_fail_task(self):
        if self.failure_type == 'random':
            return random.choice(FAILURE_LIB.values())
        elif self.failure_type in FAILURE_LIB.keys():
            return FAILURE_LIB[self.failure_type]
        else:
            return FAILURE_LIB['simple']
    
    def submit(
        self, 
        function: Callable[P, T], 
        /, 
        *args: Any, 
        **kwargs: Any
    ) -> TaskFuture[T]:
        fail_task = self.get_fail_task()

        logger.log(WORK_LOG_LEVEL, f"original function is {function}")
        logger.log(WORK_LOG_LEVEL, f"fail task is {fail_task}")

        logger.log(WORK_LOG_LEVEL, f"new func is {new_func}")
        return super().submit(new_func, self.failure_rate, fail_task, function, *args, **kwargs)


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
        random.seed(19491001)
        logger.log(WORK_LOG_LEVEL, f"Try to inject {self.config.failure_type} with rate {self.config.failure_rate}")
        
        # TODO: WorkflowExecutor init twice, should only be once
        # probably the cause of "Exception: attempt to clean up DFK when it has already been cleaned-up"
        flaw_executor = FlawExecutor(
            self.config.failure_rate,
            self.config.failure_type,
            executor.compute_executor,
            config=executor.config,
            data_transformer=executor.data_transformer,
            record_logger=executor.record_logger,
        )
        config_type = get_registered_workflow(self.config.true_workflow).config_type
        cfg = config_type(**default_config_dic)

        for key in cfg.__fields__:
            if cfg.__fields__[key].default is PydanticUndefined:
                setattr(cfg, key, default_config_dic[key])
        workflow = get_registered_workflow(self.config.true_workflow).from_config(cfg)

        record_logger = JSONRecordLogger(executor.config.run.task_record_file_name)

        with workflow, record_logger, flaw_executor:
            workflow.run(executor=flaw_executor, run_dir=run_dir)
        '''
        if random.random() < self.config.failure_rate:
            if self.config.failure_type == 'random':
                fail_task = random.choice(FAILURE_LIB.values())
            elif self.config.failure_type in FAILURE_LIB.keys():
                fail_task = FAILURE_LIB[self.config.failure_type]
            else:
                fail_task = FAILURE_LIB['simple']
            
            logger.log(WORK_LOG_LEVEL, f"Failure injected, function is {fail_task}")
            task = executor.submit(fail_task)
            logger.log(
                WORK_LOG_LEVEL,
                f'failure task finish with result {task.result()}',
            )
        else:
            config_type = get_registered_workflow(self.config.true_workflow).config_type
            cfg = config_type(**default_config_dic)

            for key in cfg.__fields__:
                if cfg.__fields__[key].default is PydanticUndefined:
                    setattr(cfg, key, default_config_dic[key])
            workflow = get_registered_workflow(self.config.true_workflow).from_config(cfg)

            record_logger = JSONRecordLogger(executor.config.run.task_record_file_name)

            with workflow, record_logger, executor:
                workflow.run(executor=executor, run_dir=run_dir)
        '''
