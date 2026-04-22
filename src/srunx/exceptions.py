class WorkflowError(Exception):
    """Base exception for workflow errors."""


class WorkflowValidationError(WorkflowError):
    """Exception raised when workflow validation fails."""


class WorkflowExecutionError(WorkflowError):
    """Exception raised when workflow execution fails."""


class SweepExecutionError(WorkflowError):
    """Exception raised when sweep materialize / execution fails at the orchestrator boundary."""
