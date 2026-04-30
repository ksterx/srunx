"""``srunx tail`` — display job logs (one-shot or --follow)."""

import sys
from typing import Annotated, Any

import typer

import srunx.slurm.local as _slurm_local  # noqa: E402,I001 — kept so ``patch("srunx.slurm.local.Slurm")`` intercepts all call sites
from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.common.exceptions import JobNotFoundError, TransportError
from srunx.transport import resolve_transport


def _run_tail_follow_ssh(
    job_ops: Any,
    *,
    job_id: int,
    last: int | None,
    interval: float,
) -> None:
    """Stream incremental log output from an SSH-hosted SLURM job.

    Mirrors the structural pattern of :func:`_run_squeue_live` — ``fetch``
    runs inside a ``try`` that renders transient failures (log file not
    yet created, SSH hiccup) without killing the loop, and
    :class:`KeyboardInterrupt` exits cleanly. Follows ``tail -f``
    convention of polling forever; the user is expected to stop via
    Ctrl+C.

    Log writes go straight to ``sys.stdout`` / ``sys.stderr`` (not
    through Rich) because this is an append-oriented stream, not a
    rewritten snapshot — using :class:`~rich.live.Live` would wipe and
    redraw every tick and destroy the scrollback the user is there to
    read.

    ``last`` applies to the **initial** chunk only; subsequent ticks
    print every new byte. Matches how ``tail -fn N`` works at the OS
    level.
    """
    import time as _time

    offset_out = 0
    offset_err = 0

    # First poll uses ``last_n=last`` so the remote runs ``tail -n N``
    # and only the tail ships over SSH — we never bring a multi-GB log
    # across the wire just to show its last 50 lines. The chunk
    # returns an offset at current EOF; subsequent ticks resume from
    # there.
    try:
        chunk = job_ops.tail_log_incremental(
            job_id, offset_out, offset_err, last_n=last
        )
    except Exception as exc:  # noqa: BLE001 — first poll can race the log file's creation
        typer.secho(
            f"(log not yet available: {exc})",
            err=True,
            fg=typer.colors.BRIGHT_BLACK,
        )
    else:
        if chunk.stdout:
            sys.stdout.write(chunk.stdout)
            sys.stdout.flush()
        if chunk.stderr and chunk.stderr != chunk.stdout:
            sys.stderr.write(chunk.stderr)
            sys.stderr.flush()
        offset_out = chunk.stdout_offset
        offset_err = chunk.stderr_offset

    try:
        while True:
            _time.sleep(interval)
            try:
                chunk = job_ops.tail_log_incremental(job_id, offset_out, offset_err)
            except Exception as exc:  # noqa: BLE001 — keep the tail alive through transient errors
                typer.secho(
                    f"(poll failed, retrying: {exc})",
                    err=True,
                    fg=typer.colors.BRIGHT_BLACK,
                )
                continue
            if chunk.stdout:
                sys.stdout.write(chunk.stdout)
                sys.stdout.flush()
            if chunk.stderr:
                sys.stderr.write(chunk.stderr)
                sys.stderr.flush()
            offset_out = chunk.stdout_offset
            offset_err = chunk.stderr_offset
    except KeyboardInterrupt:
        return


def tail(
    job_id: Annotated[int, typer.Argument(help="Job ID to show logs for")],
    follow: Annotated[
        bool,
        typer.Option("--follow", "-f", help="Stream logs in real-time (like tail -f)"),
    ] = False,
    interval: Annotated[
        float,
        typer.Option(
            "--interval",
            help=(
                "Seconds between polls when --follow is set over SSH "
                "(local follow inherits Slurm.tail_log's existing cadence). "
                "Default 2s."
            ),
        ),
    ] = 2.0,
    last: Annotated[
        int,
        typer.Option(
            "--last",
            "-n",
            help=(
                "Show only the last N lines (default 10, matches native "
                "``tail``). Use ``--all`` to dump the entire log."
            ),
        ),
    ] = 10,
    show_all: Annotated[
        bool,
        typer.Option(
            "--all",
            help=(
                "Dump the entire log instead of the last ``--last`` lines. "
                "Skips the ``tail -n`` optimization and ``cat``s the whole "
                "file — a multi-GB log will transfer across SSH as-is."
            ),
        ),
    ] = False,
    job_name: Annotated[
        str | None,
        typer.Option("--name", help="Job name for better log file detection"),
    ] = None,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Display job logs with optional real-time streaming.

    Defaults to the last 10 lines of the log (native ``tail``
    convention). Pass ``-n N`` for a different cap, or ``--all`` to
    dump everything. With ``--follow`` / ``-f``, the default also
    applies to the initial frame — subsequent ticks stream only the
    delta regardless.
    """
    if follow and interval <= 0:
        typer.secho(
            "--interval must be a positive number of seconds.",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)
    if last <= 0 and not show_all:
        typer.secho(
            "--last must be a positive integer (or use --all to dump everything).",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    # ``--all`` wins over ``--last``: passing ``last_n=None`` to the
    # adapter triggers the legacy ``cat`` full-read path.
    effective_last: int | None = None if show_all else last

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            if rt.transport_type == "local":
                # Local keeps the interactive ``Slurm.tail_log`` path for
                # follow + last_n + file-discovery behaviour the Protocol
                # does not yet expose.
                client = _slurm_local.Slurm()
                client.tail_log(
                    job_id=job_id,
                    job_name=job_name,
                    follow=follow,
                    last_n=effective_last,
                )
            elif follow:
                _run_tail_follow_ssh(
                    rt.job_ops,
                    job_id=job_id,
                    last=effective_last,
                    interval=interval,
                )
            else:
                # One-shot SSH read. ``last_n`` flows through so the
                # remote runs ``tail -n N`` — only the tail bytes cross
                # the SSH link. ``--all`` passes ``last_n=None`` which
                # falls back to ``cat`` for a full dump.
                chunk = rt.job_ops.tail_log_incremental(
                    job_id, 0, 0, last_n=effective_last
                )
                if chunk.stdout:
                    sys.stdout.write(chunk.stdout)
                if chunk.stderr and chunk.stderr != chunk.stdout:
                    sys.stderr.write(chunk.stderr)

    except JobNotFoundError:
        typer.secho(
            f"Job {job_id} not found",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1) from None
    except TransportError as exc:
        typer.secho(f"Transport error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from None
    except Exception as e:
        typer.secho(
            f"Error retrieving logs for job {job_id}: {e}",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1) from e
