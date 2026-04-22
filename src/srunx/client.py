"""SLURM client for job submission and management."""

import glob
import os
import re
import subprocess
import tempfile
import time
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING

from srunx.callbacks import Callback
from srunx.logging import get_logger
from srunx.models import (
    BaseJob,
    Job,
    JobStatus,
    JobType,
    RunnableJobType,
    ShellJob,
    render_job_script,
    render_shell_job_script,
)
from srunx.utils import GPU_TRES_RE, get_job_status, job_status_msg  # noqa: E402

if TYPE_CHECKING:
    from srunx.client_protocol import JobStatusInfo, LogChunk
    from srunx.rendering import SubmissionRenderContext

logger = get_logger(__name__)


class Slurm:
    """Client for interacting with SLURM workload manager."""

    def __init__(
        self,
        default_template: str | None = None,
        callbacks: Sequence[Callback] | None = None,
    ):
        """Initialize SLURM client.

        Args:
            default_template: Path to default job template.
            callbacks: List of callbacks.
        """
        self.default_template = default_template or self._get_default_template()
        self.callbacks = list(callbacks) if callbacks else []

    def submit(
        self,
        job: RunnableJobType,
        template_path: str | None = None,
        callbacks: Sequence[Callback] | None = None,
        verbose: bool = False,
        record_history: bool = True,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
    ) -> RunnableJobType:
        """Submit a job to SLURM.

        Args:
            job: Job configuration.
            template_path: Optional template path (uses default if not provided).
            callbacks: List of callbacks.
            verbose: Whether to print the rendered content.
            record_history: Whether to record job in history database.
            workflow_name: Name of the workflow if part of a workflow.
            workflow_run_id: ``workflow_runs.id`` when the job was
                submitted from a workflow; persisted on the ``jobs`` row
                so reports (``srunx report --workflow``, the Web history
                JOIN) actually pick up CLI-launched workflow jobs.

        Returns:
            Job instance with updated job_id and status.

        Raises:
            subprocess.CalledProcessError: If job submission fails.
        """
        result = None

        if isinstance(job, Job):
            template = template_path or self.default_template

            with tempfile.TemporaryDirectory() as temp_dir:
                script_path = render_job_script(
                    template,
                    job,
                    temp_dir,
                    verbose,
                )
                logger.debug(f"Generated SLURM script at: {script_path}")

                # Submit job with sbatch --parsable for reliable job ID extraction
                sbatch_cmd = ["sbatch", "--parsable", script_path]
                if job.environment.container:
                    logger.debug(f"Using container: {job.environment.container}")

                logger.debug(f"Executing command: {' '.join(sbatch_cmd)}")

                try:
                    result = subprocess.run(
                        sbatch_cmd,
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                except subprocess.CalledProcessError as e:
                    logger.error(f"Failed to submit job '{job.name}': {e}")
                    logger.error(f"Command: {' '.join(e.cmd)}")
                    logger.error(f"Return code: {e.returncode}")
                    logger.error(f"Stdout: {e.stdout}")
                    logger.error(f"Stderr: {e.stderr}")
                    raise

        elif isinstance(job, ShellJob):
            with tempfile.TemporaryDirectory() as temp_dir:
                script_path = render_shell_job_script(
                    job.script_path, job, temp_dir, verbose
                )
                try:
                    result = subprocess.run(
                        ["sbatch", "--parsable", script_path],
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                except subprocess.CalledProcessError as e:
                    logger.error(f"Failed to submit job '{job.script_path}': {e}")
                    logger.error(f"Command: {' '.join(e.cmd)}")
                    logger.error(f"Return code: {e.returncode}")
                    logger.error(f"Stdout: {e.stdout}")
                    logger.error(f"Stderr: {e.stderr}")
                    raise

        else:
            raise ValueError("Either 'command' or 'path' must be set")

        if result is None:
            render_job_script(template, job, output_dir=None, verbose=verbose)
            raise RuntimeError(
                f"Failed to submit job '{job.name}': No result from subprocess"
            )

        # --parsable outputs "job_id" or "job_id;cluster_name"
        job_id = int(result.stdout.strip().split(";")[0])
        job.job_id = job_id
        job.status = JobStatus.PENDING

        logger.debug(f"Successfully submitted job '{job.name}' with ID {job_id}")

        # Record in the state DB (best-effort; failures log at debug
        # and never surface to the caller — see cli_helpers.py).
        # Local ``Slurm`` only ever writes the local-transport triple;
        # SSH submissions take the ``SlurmSSHAdapter.submit`` path which
        # passes its own (transport_type='ssh', profile_name=...,
        # scheduler_key='ssh:<profile>') values.
        if record_history:
            from srunx.db.cli_helpers import record_submission_from_job

            record_submission_from_job(
                job,
                workflow_name=workflow_name,
                workflow_run_id=workflow_run_id,
                transport_type="local",
                profile_name=None,
                scheduler_key="local",
            )

        all_callbacks = self.callbacks[:]
        if callbacks:
            all_callbacks.extend(callbacks)
        for callback in all_callbacks:
            callback.on_job_submitted(job)

        return job

    @staticmethod
    def retrieve(job_id: int) -> BaseJob:
        """Retrieve job information from SLURM.

        Args:
            job_id: SLURM job ID.

        Returns:
            Job object with current status.
        """
        return get_job_status(job_id)

    def status(self, job_id: int) -> BaseJob:
        """Return a :class:`BaseJob` snapshot of ``job_id``'s state.

        Thin alias for :meth:`retrieve` that satisfies
        :class:`~srunx.client_protocol.JobOperationsProtocol`. Raises
        :class:`~srunx.exceptions.JobNotFound` when ``sacct`` has no row
        for ``job_id`` (the underlying ``get_job_status`` uses
        ``ValueError`` for that condition, which we rewrap here to match
        the Protocol contract).

        TODO (Phase 5a): Wrap ``subprocess.FileNotFoundError`` /
        ``CalledProcessError`` into ``TransportConnectionError`` /
        ``RemoteCommandError`` once CLI tests have caught up; leaving the
        raw ``subprocess.CalledProcessError`` as-is today preserves the
        existing test expectations.
        """
        from srunx.exceptions import JobNotFound

        try:
            return self.retrieve(job_id)
        except ValueError as exc:  # get_job_status raises ValueError on miss
            raise JobNotFound(f"Job {job_id} not found") from exc

    def tail_log_incremental(
        self,
        job_id: int,
        stdout_offset: int = 0,
        stderr_offset: int = 0,
    ) -> "LogChunk":
        """Return new log content since the given byte offsets.

        Reads local log files using ``open`` + ``seek`` instead of a
        subprocess ``tail``, so the Protocol contract (pure, no side
        effects, no stdout writes) is honoured. If the log file does not
        yet exist (e.g. the job is still PENDING), returns an empty chunk
        with the offsets unchanged — callers should treat a missing file
        as "no new data" rather than a hard error.
        """
        from srunx.client_protocol import LogChunk

        stdout_path, stderr_path = self._find_log_paths(job_id)
        stdout_content, new_stdout_offset = self._read_file_from_offset(
            stdout_path, stdout_offset
        )
        if stderr_path is not None and stderr_path != stdout_path:
            stderr_content, new_stderr_offset = self._read_file_from_offset(
                stderr_path, stderr_offset
            )
        else:
            stderr_content, new_stderr_offset = "", stderr_offset
        return LogChunk(
            stdout=stdout_content,
            stderr=stderr_content,
            stdout_offset=new_stdout_offset,
            stderr_offset=new_stderr_offset,
        )

    @staticmethod
    def _find_log_paths(
        job_id: int, job_name: str | None = None
    ) -> tuple[str | None, str | None]:
        """Locate (stdout_path, stderr_path) for *job_id*.

        Reuses :meth:`_find_log_files` discovery. When SLURM is configured
        with a single combined log (the srunx default), both returned
        paths will be the same — callers must deduplicate to avoid
        double-counting byte offsets. Returns ``(None, None)`` when no
        log file has appeared yet (common for PENDING jobs).
        """
        found_files, _ = Slurm._find_log_files(job_id, job_name)
        if not found_files:
            return None, None
        stdout_path: str | None = found_files[0]
        stderr_path: str | None = None
        for candidate in found_files:
            if "err" in Path(candidate).name.lower():
                stderr_path = candidate
                break
        if stderr_path is None:
            # srunx's default template routes stderr into the primary log.
            stderr_path = stdout_path
        return stdout_path, stderr_path

    @staticmethod
    def _read_file_from_offset(path: str | None, offset: int) -> tuple[str, int]:
        """Read from *offset* to EOF. Missing file → ``("", offset)``."""
        if not path:
            return "", offset
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                data = f.read()
        except FileNotFoundError:
            return "", offset
        except OSError as exc:
            logger.warning(f"Failed to read log file {path}: {exc}")
            return "", offset
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("utf-8", errors="replace")
        return text, offset + len(data)

    def cancel(self, job_id: int) -> None:
        """Cancel a SLURM job.

        Args:
            job_id: SLURM job ID to cancel.

        Raises:
            subprocess.CalledProcessError: If job cancellation fails.
        """
        logger.info(f"Cancelling job {job_id}")

        try:
            subprocess.run(
                ["scancel", str(job_id)],
                check=True,
            )
            logger.info(f"Successfully cancelled job {job_id}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            raise

    def queue(self, user: str | None = None) -> list[BaseJob]:
        """List jobs for a user.

        Args:
            user: Username (defaults to current user).

        Returns:
            List of Job objects.
        """
        # Format: JobID Partition Name User State Time TimeLimit Nodes Nodelist TRES
        cmd = [
            "squeue",
            "--format",
            "%.18i %.9P %.30j %.12u %.8T %.10M %.9l %.6D %R %b",
            "--noheader",
        ]
        if user:
            cmd.extend(["--user", user])

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        jobs = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue

            parts = line.split(maxsplit=9)  # Split into at most 10 parts
            if len(parts) >= 5:
                job_id = int(parts[0])
                partition = parts[1] if len(parts) > 1 else None
                job_name = parts[2]
                user_name = parts[3] if len(parts) > 3 else None
                status_str = parts[4]
                elapsed_time = parts[5] if len(parts) > 5 else None
                nodes_str = parts[7] if len(parts) > 7 else "1"
                tres = parts[9] if len(parts) > 9 else ""

                try:
                    status = JobStatus(status_str)
                except ValueError:
                    status = JobStatus.PENDING  # Default for unknown status

                # Parse number of nodes
                try:
                    nodes = int(nodes_str)
                except (ValueError, AttributeError):
                    nodes = 1

                # Parse GPU count from TRES (e.g., "gpu:8" or "billing=8,cpu=8,gres/gpu=8,mem=100G,node=1")
                gpus = 0
                if tres and "gpu" in tres.lower():
                    gpu_match = GPU_TRES_RE.search(tres)
                    if gpu_match:
                        gpus = int(gpu_match.group(1))

                job = BaseJob(
                    name=job_name,
                    job_id=job_id,
                    user=user_name,
                    partition=partition,
                    elapsed_time=elapsed_time,
                    nodes=nodes,
                    gpus=gpus,
                )
                job.status = status
                jobs.append(job)

        return jobs

    def queue_by_ids(self, job_ids: list[int]) -> dict[int, "JobStatusInfo"]:
        """Return a mapping of ``job_id`` -> :class:`JobStatusInfo` for active jobs.

        Active jobs are read from ``squeue``. Jobs that have already left
        the queue are looked up via ``sacct``. Jobs found in neither source
        are omitted from the returned dict.

        Args:
            job_ids: SLURM job IDs to query. An empty list yields ``{}``.

        Returns:
            Dict keyed by job_id. Missing jobs are absent from the dict.
        """
        from srunx.client_protocol import (
            JobStatusInfo,
            parse_slurm_datetime,
            parse_slurm_duration,
        )

        if not job_ids:
            return {}

        results: dict[int, JobStatusInfo] = {}
        id_arg = ",".join(str(i) for i in job_ids)

        # --- squeue: active (PENDING / RUNNING / etc.) ---
        squeue_cmd = [
            "squeue",
            "--jobs",
            id_arg,
            "--format",
            "%i|%T|%S|%M|%N",
            "--noheader",
        ]
        try:
            squeue_res = subprocess.run(
                squeue_cmd, capture_output=True, text=True, check=False
            )
        except FileNotFoundError:
            squeue_res = None

        if squeue_res and squeue_res.returncode == 0:
            for line in squeue_res.stdout.strip().splitlines():
                parts = line.split("|")
                if len(parts) < 5:
                    continue
                try:
                    jid = int(parts[0].strip())
                except ValueError:
                    continue
                results[jid] = JobStatusInfo(
                    status=parts[1].strip(),
                    started_at=parse_slurm_datetime(parts[2]),
                    duration_secs=parse_slurm_duration(parts[3]),
                    nodelist=(parts[4].strip() or None),
                )

        # --- sacct fallback: terminal jobs ---
        missing = [j for j in job_ids if j not in results]
        if missing:
            sacct_cmd = [
                "sacct",
                "--jobs",
                ",".join(str(i) for i in missing),
                "--format=JobID,State,Start,End,Elapsed,NodeList",
                "--noheader",
                "--parsable2",
            ]
            try:
                sacct_res = subprocess.run(
                    sacct_cmd, capture_output=True, text=True, check=False
                )
            except FileNotFoundError:
                sacct_res = None

            if sacct_res and sacct_res.returncode == 0:
                for line in sacct_res.stdout.strip().splitlines():
                    parts = line.split("|")
                    if len(parts) < 6:
                        continue
                    # Skip sub-steps like "12345.batch"
                    raw_id = parts[0].strip()
                    if "." in raw_id:
                        continue
                    try:
                        jid = int(raw_id)
                    except ValueError:
                        continue
                    if jid in results:
                        continue
                    raw_state = parts[1].strip()
                    status = raw_state.split()[0] if raw_state else "UNKNOWN"
                    results[jid] = JobStatusInfo(
                        status=status,
                        started_at=parse_slurm_datetime(parts[2]),
                        completed_at=parse_slurm_datetime(parts[3]),
                        duration_secs=parse_slurm_duration(parts[4]),
                        nodelist=(parts[5].strip() or None),
                    )

        return results

    def monitor(
        self,
        job_obj_or_id: JobType | int,
        poll_interval: int = 5,
        callbacks: Sequence[Callback] | None = None,
    ) -> JobType:
        """Wait for a job to complete.

        Args:
            job_obj_or_id: Job object or job ID.
            poll_interval: Polling interval in seconds.
            callbacks: List of callbacks.

        Returns:
            Completed job object.

        Raises:
            RuntimeError: If job fails.
        """
        if isinstance(job_obj_or_id, int):
            job = self.retrieve(job_obj_or_id)
        else:
            job = job_obj_or_id

        all_callbacks = self.callbacks[:]
        if callbacks:
            all_callbacks.extend(callbacks)

        msg = f"👀 {'MONITORING':<12} Job {job.name:<12} (ID: {job.job_id})"
        logger.info(msg)

        previous_status = None

        while True:
            job.refresh()

            # Log status changes
            if job.status != previous_status:
                status_str = job.status.value if job.status else "Unknown"
                logger.debug(f"Job(name={job.name}, id={job.job_id}) is {status_str}")
                previous_status = job.status

            match job.status:
                case JobStatus.COMPLETED:
                    logger.info(job_status_msg(job))
                    self._record_completion(job)
                    for callback in all_callbacks:
                        callback.on_job_completed(job)
                    return job
                case JobStatus.FAILED:
                    self._record_completion(job)
                    err_msg = self._build_error_msg(job)
                    for callback in all_callbacks:
                        callback.on_job_failed(job)
                    raise RuntimeError(err_msg)
                case JobStatus.CANCELLED | JobStatus.TIMEOUT:
                    self._record_completion(job)
                    err_msg = self._build_error_msg(job)
                    for callback in all_callbacks:
                        callback.on_job_cancelled(job)
                    raise RuntimeError(err_msg)
            time.sleep(poll_interval)

    @staticmethod
    def _record_completion(job: BaseJob) -> None:
        """Record job completion in the state DB."""
        if job.job_id is None:
            return
        from srunx.db.cli_helpers import record_completion

        record_completion(int(job.job_id), job.status)

    @staticmethod
    def _build_error_msg(job: BaseJob) -> str:
        """Build error message with log contents for a failed/cancelled job."""
        err_msg = job_status_msg(job) + "\n"
        if isinstance(job, Job):
            log_file = Path(job.log_dir) / f"{job.name}_{job.job_id}.log"
            if log_file.exists():
                with open(log_file) as f:
                    err_msg += f.read()
                    err_msg += f"\nLog file: {log_file}"
            else:
                err_msg += f"Log file not found: {log_file}"
        return err_msg

    def run(
        self,
        job: RunnableJobType,
        template_path: str | None = None,
        callbacks: Sequence[Callback] | None = None,
        poll_interval: int = 5,
        verbose: bool = False,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
        *,
        submission_context: "SubmissionRenderContext | None" = None,
    ) -> RunnableJobType:
        """Submit a job and wait for completion.

        ``submission_context`` is accepted for
        :class:`~srunx.client_protocol.WorkflowJobExecutorProtocol`
        conformance but intentionally ignored: local SLURM submission
        does not need mount-path translation, so the job is rendered
        verbatim from its own ``work_dir`` / ``log_dir`` fields. SSH-backed
        executors consume the context to rewrite local paths before render.
        """
        del submission_context  # unused — local path has no mount translation
        submitted_job = self.submit(
            job,
            template_path=template_path,
            callbacks=callbacks,
            verbose=verbose,
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
        )
        monitored_job = self.monitor(
            submitted_job, poll_interval=poll_interval, callbacks=callbacks
        )

        # Ensure the return type matches the expected type
        if isinstance(monitored_job, Job | ShellJob):
            return monitored_job
        else:
            # This should not happen in practice, but needed for type safety
            return submitted_job

    @staticmethod
    def _find_log_files(
        job_id: int | str, job_name: str | None = None
    ) -> tuple[list[str], list[str]]:
        """Find SLURM log files for a job.

        Returns:
            Tuple of (found_files, searched_dirs)
        """
        job_id_str = str(job_id)
        potential_log_patterns: list[str | None] = [
            f"{job_name}_{job_id_str}.log" if job_name else None,
            f"{job_name}_{job_id_str}.out" if job_name else None,
            f"*_{job_id_str}.log",
            f"*_{job_id_str}.out",
            f"slurm-{job_id_str}.out",
            f"slurm-{job_id_str}.err",
            f"job_{job_id_str}.log",
            f"{job_id_str}.log",
        ]
        patterns = [p for p in potential_log_patterns if p is not None]
        log_dirs = [
            os.environ.get("SLURM_LOG_DIR", ""),
            "./",
            "/tmp",
        ]

        found_files: list[str] = []
        for log_dir in log_dirs:
            if not log_dir:
                continue
            log_dir_path = Path(log_dir)
            if not log_dir_path.exists():
                continue
            for pattern in patterns:
                found_files.extend(glob.glob(str(log_dir_path / pattern)))

        return found_files, [d for d in log_dirs if d]

    @staticmethod
    def _read_log_contents(found_files: list[str]) -> tuple[str, str]:
        """Read output and error content from found log files.

        Returns:
            Tuple of (output_content, error_content)
        """
        output_content = ""
        error_content = ""
        if not found_files:
            return output_content, error_content

        primary_log = found_files[0]
        try:
            with open(primary_log, encoding="utf-8") as f:
                output_content = f.read()
        except (OSError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to read log file {primary_log}: {e}")
            output_content = f"Could not read log file {primary_log}: {e}"

        for log_file in found_files:
            if "err" in Path(log_file).name.lower():
                try:
                    with open(log_file, encoding="utf-8") as f:
                        error_content += f.read()
                except (OSError, UnicodeDecodeError) as e:
                    logger.warning(f"Failed to read error file {log_file}: {e}")

        return output_content, error_content

    def get_job_output(
        self, job_id: int | str, job_name: str | None = None
    ) -> tuple[str, str]:
        """Get job output from SLURM log files.

        Args:
            job_id: SLURM job ID
            job_name: Job name for better log file detection

        Returns:
            Tuple of (output_content, error_content)
        """
        found_files, _ = self._find_log_files(job_id, job_name)
        if not found_files:
            logger.warning(f"No log files found for job {job_id}")
        return self._read_log_contents(found_files)

    def get_job_output_detailed(
        self, job_id: int | str, job_name: str | None = None, skip_content: bool = False
    ) -> dict[str, str | list[str] | None]:
        """Get detailed job output information including found log files.

        Args:
            job_id: SLURM job ID
            job_name: Job name for better log file detection
            skip_content: If True, only find log files without reading content

        Returns:
            Dictionary with detailed log information
        """
        found_files, searched_dirs = self._find_log_files(job_id, job_name)
        primary_log = found_files[0] if found_files else None

        if skip_content:
            output_content, error_content = "", ""
        else:
            output_content, error_content = self._read_log_contents(found_files)

        return {
            "found_files": found_files,
            "primary_log": primary_log,
            "output": output_content,
            "error": error_content,
            "slurm_log_dir": os.environ.get("SLURM_LOG_DIR"),
            "searched_dirs": searched_dirs,
        }

    def tail_log(
        self,
        job_id: int | str,
        job_name: str | None = None,
        follow: bool = False,
        last_n: int | None = None,
        poll_interval: float = 1.0,
    ) -> None:
        """Display job logs with optional real-time streaming.

        Args:
            job_id: SLURM job ID
            job_name: Job name for better log file detection
            follow: If True, continuously stream new log lines (like tail -f)
            last_n: Show only the last N lines
            poll_interval: Polling interval in seconds for follow mode
        """
        from collections import deque

        from rich.console import Console

        console = Console()
        job_id_str = str(job_id)

        # Skip reading full content when we only need file paths
        skip_content = follow or (last_n is not None)
        log_info = self.get_job_output_detailed(
            job_id, job_name, skip_content=skip_content
        )
        primary_log = log_info.get("primary_log")
        found_files = log_info.get("found_files", [])

        if not found_files:
            console.print(f"[red]❌ No log files found for job {job_id_str}[/red]")
            searched_dirs = log_info.get("searched_dirs", [])
            if searched_dirs:
                console.print(
                    f"[yellow]📁 Searched in: {', '.join(searched_dirs)}[/yellow]"
                )
            slurm_log_dir = log_info.get("slurm_log_dir")
            if slurm_log_dir:
                console.print(f"[yellow]💡 SLURM_LOG_DIR: {slurm_log_dir}[/yellow]")
            return

        if not primary_log:
            console.print("[red]❌ Could not find primary log file[/red]")
            return

        log_file = Path(str(primary_log))
        console.print(f"[cyan]📄 Log file: {log_file}[/cyan]")

        try:
            if follow:
                # Real-time streaming mode (like tail -f)
                console.print(
                    "[yellow]📡 Streaming logs (Ctrl+C to stop)...[/yellow]\n"
                )

                # If last_n is specified, show last N lines first
                if last_n:
                    with open(log_file, encoding="utf-8") as f:
                        tail_lines = deque(f, maxlen=last_n)
                        for line in tail_lines:
                            console.print(line, end="")

                # Start streaming from current position
                with open(log_file, encoding="utf-8") as f:
                    # Always seek to end to avoid duplicating already-printed lines
                    f.seek(0, os.SEEK_END)

                    while True:
                        line = f.readline()
                        if line:
                            console.print(line, end="")
                        else:
                            # Check if job is still running
                            job = self.retrieve(int(job_id))
                            if job.status.value in [
                                "COMPLETED",
                                "FAILED",
                                "CANCELLED",
                                "TIMEOUT",
                            ]:
                                console.print(
                                    f"\n[yellow]Job {job_id} finished with status: {job.status.value}[/yellow]"
                                )
                                break
                            time.sleep(poll_interval)
            else:
                # Static display mode
                if last_n:
                    # Read only last N lines efficiently using deque
                    with open(log_file, encoding="utf-8") as f:
                        tail_lines = deque(f, maxlen=last_n)
                    output = "".join(tail_lines)
                else:
                    output = str(log_info.get("output", ""))

                if output:
                    console.print(output)
                else:
                    console.print("[yellow]Log file is empty[/yellow]")

        except FileNotFoundError:
            console.print(f"[red]❌ Log file not found: {log_file}[/red]")
        except PermissionError:
            console.print(f"[red]❌ Permission denied: {log_file}[/red]")
        except KeyboardInterrupt:
            console.print("\n[yellow]Streaming stopped by user[/yellow]")
        except Exception as e:
            console.print(f"[red]❌ Error reading log file: {e}[/red]")

    def _get_job_gpu_count(self, job_id: int) -> int | None:
        """
        Get GPU count for a job by parsing scontrol output.

        Tries TRES field first, then falls back to Gres field for compatibility.

        Args:
            job_id: SLURM job ID

        Returns:
            GPU count if found, None otherwise

        Raises:
            subprocess.CalledProcessError: If scontrol command fails
        """

        try:
            result = subprocess.run(
                ["scontrol", "show", "job", str(job_id)],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )

            # Try TRES field first (most reliable)
            # Pattern: TRES=...gres/gpu=N
            match = re.search(r"TRES=.*?gres/gpu=(\d+)", result.stdout)
            if match:
                return int(match.group(1))

            # Fallback to Gres or TresPerNode field
            # Pattern: Gres=gpu:N or TresPerNode=...gpu:N
            match = re.search(r"(?:TresPerNode|Gres)=.*?gpu[:/](\d+)", result.stdout)
            if match:
                return int(match.group(1))

            # No GPU information found
            return None

        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout querying GPU count for job {job_id}")
            return None
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to query GPU count for job {job_id}: {e}")
            return None

    def _get_default_template(self) -> str:
        """Get the default job template path."""
        return str(files("srunx.templates").joinpath("base.slurm.jinja"))


# Convenience functions for backward compatibility
def submit_job(
    job: RunnableJobType,
    template_path: str | None = None,
    callbacks: Sequence[Callback] | None = None,
    verbose: bool = False,
) -> RunnableJobType:
    """Submit a job to SLURM (convenience function).

    Args:
        job: Job configuration.
        template_path: Optional template path (uses default if not provided).
        callbacks: List of callbacks.
        verbose: Whether to print the rendered content.
    """
    client = Slurm()
    return client.submit(
        job, template_path=template_path, callbacks=callbacks, verbose=verbose
    )


def retrieve_job(job_id: int) -> BaseJob:
    """Get job status (convenience function).

    Args:
        job_id: SLURM job ID.
    """
    client = Slurm()
    return client.retrieve(job_id)


def cancel_job(job_id: int) -> None:
    """Cancel a job (convenience function).

    Args:
        job_id: SLURM job ID.
    """
    client = Slurm()
    client.cancel(job_id)
