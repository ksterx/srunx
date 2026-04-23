class WorkflowError(Exception):
    """Base exception for workflow errors."""


class WorkflowValidationError(WorkflowError):
    """Exception raised when workflow validation fails."""


class WorkflowExecutionError(WorkflowError):
    """Exception raised when workflow execution fails."""


class SweepExecutionError(WorkflowError):
    """Exception raised when sweep materialize / execution fails at the orchestrator boundary."""


# ---------------------------------------------------------------------------
# Transport / job operation exceptions (introduced by CLI transport unification)
# ---------------------------------------------------------------------------


class TransportError(Exception):
    """Base class for transport-layer failures (local subprocess or SSH).

    Callers (CLI / poller) catch this to render a uniform user-facing
    error. Always attach a human-readable ``str(exc)`` summary.
    """


class TransportConnectionError(TransportError):
    """SSH connection failed or local sbatch binary unavailable."""


class TransportAuthError(TransportError):
    """SSH authentication failed (wrong key, refused, etc.)."""


class TransportTimeoutError(TransportError):
    """SSH or scontrol / sbatch call exceeded its timeout."""


class RemoteCommandError(TransportError):
    """A remote command (e.g. ``scontrol show job``) returned non-zero.

    Distinct from :class:`SubmissionError` — used for read-path commands
    that are not ``sbatch`` itself.
    """


class JobNotFoundError(Exception):
    """Job ID does not exist on the target SLURM cluster.

    Separate from :class:`TransportError` because 'missing job' is a
    user-level condition, not a transport failure.
    """


def __getattr__(name: str) -> type:
    """Surface a DeprecationWarning when the legacy ``JobNotFound`` alias is accessed.

    Pre-#169, the class was ``JobNotFound``; the rename kept a plain
    module-level binding as an alias, but that silently accepted the old
    name forever. Route it through ``__getattr__`` so importing
    ``JobNotFound`` now warns while still resolving to
    :class:`JobNotFoundError`.
    """
    if name == "JobNotFound":
        import warnings

        warnings.warn(
            "srunx.exceptions.JobNotFound is deprecated; use JobNotFoundError instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return JobNotFoundError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class SubmissionError(Exception):
    """sbatch invocation reached the cluster but returned non-zero.

    The sbatch process started (so transport succeeded) but sbatch
    itself rejected the script (syntax error, quota, etc.).
    """
