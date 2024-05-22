from __future__ import annotations

from pydantic import Field

from webs.config import Config


class FailureWorkflowConfig(Config):
    """Failure injection workflow configuration."""

    # Required arguments
    true_workflow: str = Field(description='"cholesky", "docking", "fedlearn", "mapreduce", "montage", or "synthetic" workflow')

    failure_rate: float = Field(1, description='failure rate')
    failure_type: str = Field('simple', description='"random", "simple", "memory", "ulimit", "dependency" and TODO')

    # @staticmethod
    # def add_argument_group(parser, argv, required, workflows=None):
    #     # Parse preliminary arguments to determine true_workflow
    #     preliminary_args = parser.parse_known_args(argv)[0]
    #     true_workflow = getattr(preliminary_args, 'true_workflow', None)
        
    #     if true_workflow and workflows and true_workflow in workflows:
    #         # Add arguments for the selected true_workflow
    #         workflows[true_workflow].add_argument_group(parser, argv, required=False)
