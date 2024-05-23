from __future__ import annotations

import argparse
import functools
import logging
import os
import pathlib
import sys
import time
from datetime import datetime
from typing import Callable
from typing import Sequence

# This import is necessary to ensure that all the workflow
# implementations get imported and therefore registered.
from webs import wf  # noqa: F401
from webs.executor.config import ExecutorChoicesConfig
from webs.executor.config import get_executor_config
from webs.executor.workflow import WorkflowExecutor
from webs.logging import init_logging
from webs.logging import RUN_LOG_LEVEL
from webs.record import JSONRecordLogger
from webs.run.config import BenchmarkConfig
from webs.run.config import RunConfig
from webs.workflow import get_registered_workflow
from webs.workflow import get_registered_workflow_names
from webs.wf.failure.config import FailureWorkflowConfig

logger = logging.getLogger('webs.run')


def parse_args_to_config(argv: Sequence[str]) -> BenchmarkConfig:
    """Parse sequence of string arguments into a config.

    Args:
        argv: Sequence of string arguments.

    Returns:
        Configuration.
    """
    parser = argparse.ArgumentParser(
        description='Workflow benchmark suite.',
        prog='python -m webs.run',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    subparsers = parser.add_subparsers(
        title='Workflows',
        dest='name',
        required=True,
        help='workflow to execute',
    )

    workflow_names = sorted(get_registered_workflow_names())
    for workflow_name in workflow_names:
        subparser = subparsers.add_parser(
            workflow_name,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )

        RunConfig.add_argument_group(subparser, argv=argv, required=True)
        ExecutorChoicesConfig.add_argument_group(
            subparser,
            argv=argv,
            required=True,
        )

        # Avoid importing the workflow and its dependencies unless the
        # user has already specified it.
        if workflow_name in argv:
            workflow = get_registered_workflow(workflow_name)
            workflow.config_type.add_argument_group(
                subparser,
                argv=argv,
                required=True,
            )

    args = parser.parse_args(argv)
    options = vars(args)

    workflow_name = options['name']
    executor_config = get_executor_config(**options)
    run_config = RunConfig(**options)
    workflow = get_registered_workflow(workflow_name)
    workflow_config = workflow.config_type(**options)

    return BenchmarkConfig(
        name=workflow_name,
        timestamp=datetime.now(),
        executor=executor_config,
        run=run_config,
        workflow=workflow_config,
    )


def _cwd_run_dir(
    function: Callable[[BenchmarkConfig], None],
) -> Callable[[BenchmarkConfig], None]:
    @functools.wraps(function)
    def _decorator(config: BenchmarkConfig) -> None:
        origin = pathlib.Path().absolute()
        try:
            os.chdir(config.get_run_dir())
            function(config)
        finally:
            os.chdir(origin)

    return _decorator


@_cwd_run_dir
def run(config: BenchmarkConfig) -> None:
    """Run a workflow using the configuration.

    This function changes the current working directory to
    `config.run.run_dir` so that all paths are relative to the current
    working directory.
    """
    start = time.perf_counter()

    cwd = pathlib.Path.cwd().resolve()

    logger.log(RUN_LOG_LEVEL, f'Starting workflow (name={config.name})')
    logger.log(RUN_LOG_LEVEL, config)
    logger.log(RUN_LOG_LEVEL, f'Runtime directory: {cwd}')

    config_json = config.model_dump_json(exclude={'timestamp'}, indent=4)
    with open('config.json', 'w') as f:
        f.write(config_json)

    workflow = get_registered_workflow(config.name).from_config(
        config.workflow,
    )

    compute_executor = config.executor.get_executor()
    record_logger = JSONRecordLogger(config.run.task_record_file_name)
    executor = WorkflowExecutor(compute_executor, config, record_logger=record_logger)

    with workflow, record_logger, executor:
        workflow.run(executor=executor, run_dir=cwd)

    runtime = time.perf_counter() - start
    logger.log(
        RUN_LOG_LEVEL,
        f'Finished workflow (name={config.name}, '
        f'runtime={runtime:.2f}s, tasks={executor.tasks_executed})',
    )


def main(argv: Sequence[str] | None = None) -> int:  # noqa: D103
    argv = argv if argv is not None else sys.argv[1:]
    config = parse_args_to_config(argv)

    log_file = (
        None
        if config.run.log_file_name is None
        else config.get_run_dir() / config.run.log_file_name
    )
    init_logging(
        log_file,
        config.run.log_level,
        config.run.log_file_level,
        force=True,
    )

    try:
        run(config)
    except BaseException:
        logger.exception('Caught unhandled exception')
        return 1
    else:
        return 0
