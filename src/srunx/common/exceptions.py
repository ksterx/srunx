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


class TransportSelectionError(ValueError):
    """Invalid transport selection (e.g. empty ``--profile`` or
    ``--profile`` + ``--local`` used together).

    Raised by the transport resolver (:func:`srunx.transport.registry`)
    so the selection logic stays free of any CLI framework dependency.
    The CLI option layer translates this into ``typer.BadParameter``
    (forwarding :attr:`param_hint`); MCP catches it and renders ``err()``.

    Subclasses :class:`ValueError` so callers that only catch the broad
    builtin still behave sensibly.

    Attributes:
        param_hint: Which CLI flag(s) the error concerns (e.g.
            ``"--profile / --local"``). Carried so the CLI translator can
            reproduce the exact ``typer.BadParameter`` UX; ``None`` when
            no specific flag applies.
    """

    def __init__(self, message: str, *, param_hint: str | None = None) -> None:
        super().__init__(message)
        self.param_hint = param_hint


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


class SubmissionError(Exception):
    """sbatch invocation reached the cluster but returned non-zero.

    The sbatch process started (so transport succeeded) but sbatch
    itself rejected the script (syntax error, quota, etc.).
    """
