from __future__ import annotations

import multiprocessing
from typing import Optional
import sys

import globus_compute_sdk
from parsl.addresses import address_by_hostname
from parsl.channels import LocalChannel
from parsl.concurrent import ParslPoolExecutor
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors import ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.monitoring import MonitoringHub
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from pydantic import Field

from webs.executor.config import ExecutorConfig
from webs.executor.config import register

retry_path = '/home/szhou3/resilient_compute/resilience_test/expanse/retry'
sys.path.append(retry_path)
from retry_config import resilient_retry

@register(name='parsl')
class ParslConfig(ExecutorConfig):
    """Parsl configuration.

    Attributes:
        endpoint: Globus Compute endpoint UUID.
    """

    parsl_use_threads: bool = Field(
        False,
        description=(
            'use parsl ThreadPoolExecutor instead of HighThroughputExecutor'
        ),
    )
    parsl_workers: Optional[int] = Field(None, description='max parsl workers')  # noqa: UP007
    parsl_run_dir: str = Field(
        'parsl-runinfo',
        description='parsl run directory within the workflow run directory',
    )

    def get_executor_config(self) -> Config:
        """Create a Parsl config from this config."""
        workers = (
            self.parsl_workers
            if self.parsl_workers is not None
            else multiprocessing.cpu_count()
        )

        if self.parsl_use_threads:
            executor = ThreadPoolExecutor(max_threads=workers)
        else:
            executor = HighThroughputExecutor(
                label='htex-local',
                max_workers=workers,
                address=address_by_hostname(),
                cores_per_worker=1,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
                radio_mode="diaspora"
            )
            # worker_init='module load cpu/0.15.4; module load slurm; module load anaconda3/2020.11; source activate /home/szhou3/.conda/envs/parsl310'

            # executor = HighThroughputExecutor(
            #     max_workers=workers,
            #     label="htex-local",
            #     provider=SlurmProvider(
            #         'debug', # 'compute'
            #         account='cuw101',
            #         launcher=SrunLauncher(),
            #         # string to prepend to #SBATCH blocks in the submit
            #         # script to the scheduler
            #         scheduler_options='',
            #         # Command to be run before starting a worker, such as:
            #         # 'module load Anaconda; source activate parsl_env'.
            #         worker_init=worker_init,
            #         init_blocks=1,
            #         max_blocks=1,
            #         nodes_per_block=1,
            #     ),
            #     block_error_handler=False,
            #     radio_mode="diaspora"
            # )

        return Config(
            executors=[executor], 
            run_dir=self.parsl_run_dir,
            strategy='none',
            resilience_strategy='fail_type',
            app_cache=True, checkpoint_mode='task_exit',
            retries=1,
            monitoring=MonitoringHub(
                            hub_address="localhost",
                            monitoring_debug=False,
                            logging_endpoint='sqlite:///parsl-runinfo/monitoring.db',
                            resource_monitoring_interval=1,
            ),
            usage_tracking=True,
            retry_handler=resilient_retry
        )

    def get_executor(self) -> globus_compute_sdk.Executor:
        """Create an executor instance from the config."""
        return ParslPoolExecutor(self.get_executor_config())
