from __future__ import annotations

from pydantic import Field

from webs.config import Config


class FailureWorkflowConfig(Config):
    """Failure injection workflow configuration."""

    # Required arguments
    true_workflow: str = Field(description='"cholesky", "docking", "fedlearn", "mapreduce", "montage", or "synthetic" workflow')

    failure_rate: float = Field(1, description='failure rate')
    failure_type: str = Field('simple', description='"random", "dependency", "divide_zero", "environment", "memory", "simple", "ulimit", "walltime" and TODO')
