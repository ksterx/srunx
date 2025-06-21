class WorkflowError(Exception):
    """Base exception for workflow errors."""


class WorkflowValidationError(WorkflowError):
    """Exception raised when workflow validation fails."""


class WorkflowExecutionError(WorkflowError):
    """Exception raised when workflow execution fails."""
