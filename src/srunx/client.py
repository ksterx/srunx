"""SLURM client for job submission and management."""

import glob
import os
import subprocess
import tempfile
import time
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path

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
from srunx.utils import get_job_status, job_status_msg

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
    ) -> RunnableJobType:
        """Submit a job to SLURM.

        Args:
            job: Job configuration.
            template_path: Optional template path (uses default if not provided).
            callbacks: List of callbacks.
            verbose: Whether to print the rendered content.
            record_history: Whether to record job in history database.

        Returns:
            Job instance with updated job_id and status.

        Raises:
            subprocess.CalledProcessError: If job submission fails.
        """
        result = None

        if isinstance(job, Job):
            template = template_path or self.default_template

            with tempfile.TemporaryDirectory() as temp_dir:
                script_path = render_job_script(template, job, temp_dir, verbose)
                logger.debug(f"Generated SLURM script at: {script_path}")

                # Submit job with sbatch
                sbatch_cmd = ["sbatch", script_path]
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
                        ["sbatch", script_path],
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

        job_id = int(result.stdout.split()[-1])
        job.job_id = job_id
        job.status = JobStatus.PENDING

        logger.debug(f"Successfully submitted job '{job.name}' with ID {job_id}")

        # Record in history database
        if record_history:
            try:
                from srunx.history import get_history

                history = get_history()
                history.record_job(job)
            except Exception as e:
                logger.warning(f"Failed to record job in history: {e}")

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
        cmd = [
            "squeue",
            "--format",
            "%.18i %.9P %.15j %.8u %.8T %.10M %.9l %.6D %R",
            "--noheader",
        ]
        if user:
            cmd.extend(["--user", user])

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        jobs = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue

            parts = line.split()
            if len(parts) >= 5:
                job_id = int(parts[0])
                job_name = parts[2]
                status_str = parts[4]

                try:
                    status = JobStatus(status_str)
                except ValueError:
                    status = JobStatus.PENDING  # Default for unknown status

                job = BaseJob(
                    name=job_name,
                    job_id=job_id,
                )
                job.status = status
                jobs.append(job)

        return jobs

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
                    # Update history
                    try:
                        from srunx.history import get_history

                        history = get_history()
                        if job.job_id:
                            history.update_job_completion(job.job_id, JobStatus.COMPLETED)
                    except Exception as e:
                        logger.warning(f"Failed to update job history: {e}")

                    for callback in all_callbacks:
                        callback.on_job_completed(job)
                    return job
                case JobStatus.FAILED:
                    # Update history
                    try:
                        from srunx.history import get_history

                        history = get_history()
                        if job.job_id:
                            history.update_job_completion(job.job_id, JobStatus.FAILED)
                    except Exception as e:
                        logger.warning(f"Failed to update job history: {e}")

                    err_msg = job_status_msg(job) + "\n"
                    if isinstance(job, Job):
                        log_file = Path(job.log_dir) / f"{job.name}_{job.job_id}.log"
                        if log_file.exists():
                            with open(log_file) as f:
                                err_msg += f.read()
                                err_msg += f"\nLog file: {log_file}"
                        else:
                            err_msg += f"Log file not found: {log_file}"
                    for callback in all_callbacks:
                        callback.on_job_failed(job)
                    raise RuntimeError(err_msg)
                case JobStatus.CANCELLED | JobStatus.TIMEOUT:
                    # Update history
                    try:
                        from srunx.history import get_history

                        history = get_history()
                        if job.job_id:
                            history.update_job_completion(job.job_id, job.status)
                    except Exception as e:
                        logger.warning(f"Failed to update job history: {e}")

                    err_msg = job_status_msg(job) + "\n"
                    if isinstance(job, Job):
                        log_file = Path(job.log_dir) / f"{job.name}_{job.job_id}.log"
                        if log_file.exists():
                            with open(log_file) as f:
                                err_msg += f.read()
                                err_msg += f"\nLog file: {log_file}"
                        else:
                            err_msg += f"Log file not found: {log_file}"
                    for callback in all_callbacks:
                        callback.on_job_cancelled(job)
                    raise RuntimeError(err_msg)
            time.sleep(poll_interval)

    def run(
        self,
        job: RunnableJobType,
        template_path: str | None = None,
        callbacks: Sequence[Callback] | None = None,
        poll_interval: int = 5,
        verbose: bool = False,
    ) -> RunnableJobType:
        """Submit a job and wait for completion."""
        submitted_job = self.submit(
            job, template_path=template_path, callbacks=callbacks, verbose=verbose
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
        job_id_str = str(job_id)

        # Try multiple common SLURM log file patterns
        potential_log_patterns = [
            # Pattern from SBATCH directives: %x_%j.log (job_name_job_id.log)
            f"{job_name}_{job_id_str}.log" if job_name else None,
            f"{job_name}_{job_id_str}.out" if job_name else None,
            # Common SLURM_LOG_DIR patterns
            f"*_{job_id_str}.log",
            f"*_{job_id_str}.out",
            # Default SLURM patterns
            f"slurm-{job_id_str}.out",
            f"slurm-{job_id_str}.err",
            # Alternative patterns
            f"job_{job_id_str}.log",
            f"{job_id_str}.log",
        ]

        # Remove None values
        patterns = [p for p in potential_log_patterns if p is not None]

        # Common SLURM log directories to search
        log_dirs = [
            os.environ.get("SLURM_LOG_DIR", ""),
            "./",  # Current directory
            "/tmp",
        ]

        output_content = ""
        error_content = ""
        found_files = []

        for log_dir in log_dirs:
            if not log_dir:
                continue

            log_dir_path = Path(log_dir)
            if not log_dir_path.exists():
                continue

            for pattern in patterns:
                # Use glob to find matching files
                search_pattern = str(log_dir_path / pattern)
                matching_files = glob.glob(search_pattern)
                found_files.extend(matching_files)

        # Read content from found log files
        if found_files:
            # Use the first found file as primary output
            primary_log = found_files[0]
            try:
                with open(primary_log, encoding="utf-8") as f:
                    output_content = f.read()
            except (OSError, UnicodeDecodeError) as e:
                logger.warning(f"Failed to read log file {primary_log}: {e}")
                output_content = f"Could not read log file {primary_log}: {e}"

            # Look for separate error files
            for log_file in found_files:
                if "err" in Path(log_file).name.lower():
                    try:
                        with open(log_file, encoding="utf-8") as f:
                            error_content += f.read()
                    except (OSError, UnicodeDecodeError) as e:
                        logger.warning(f"Failed to read error file {log_file}: {e}")
        else:
            logger.warning(f"No log files found for job {job_id_str}")

        return output_content, error_content

    def get_job_output_detailed(
        self, job_id: int | str, job_name: str | None = None
    ) -> dict[str, str | list[str] | None]:
        """Get detailed job output information including found log files.

        Args:
            job_id: SLURM job ID
            job_name: Job name for better log file detection

        Returns:
            Dictionary with detailed log information
        """
        job_id_str = str(job_id)

        # Try multiple common SLURM log file patterns
        potential_log_patterns = [
            # Pattern from SBATCH directives: %x_%j.log (job_name_job_id.log)
            f"{job_name}_{job_id_str}.log" if job_name else None,
            f"{job_name}_{job_id_str}.out" if job_name else None,
            # Common SLURM_LOG_DIR patterns
            f"*_{job_id_str}.log",
            f"*_{job_id_str}.out",
            # Default SLURM patterns
            f"slurm-{job_id_str}.out",
            f"slurm-{job_id_str}.err",
            # Alternative patterns
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
        primary_log: str | None = None
        output_content = ""
        error_content = ""

        for log_dir in log_dirs:
            if not log_dir:
                continue

            log_dir_path = Path(log_dir)
            if not log_dir_path.exists():
                continue

            for pattern in patterns:
                search_pattern = str(log_dir_path / pattern)
                matching_files = glob.glob(search_pattern)
                found_files.extend(matching_files)

        if found_files:
            primary_log = found_files[0]
            try:
                with open(primary_log, encoding="utf-8") as f:
                    output_content = f.read()
            except (OSError, UnicodeDecodeError) as e:
                logger.warning(f"Failed to read log file {primary_log}: {e}")
                output_content = f"Could not read log file {primary_log}: {e}"

            # Look for separate error files
            for log_file in found_files:
                if "err" in Path(log_file).name.lower():
                    try:
                        with open(log_file, encoding="utf-8") as f:
                            error_content += f.read()
                    except (OSError, UnicodeDecodeError) as e:
                        logger.warning(f"Failed to read error file {log_file}: {e}")

        return {
            "found_files": found_files,
            "primary_log": primary_log,
            "output": output_content,
            "error": error_content,
            "slurm_log_dir": os.environ.get("SLURM_LOG_DIR"),
            "searched_dirs": [d for d in log_dirs if d],
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
        from rich.console import Console

        console = Console()
        job_id_str = str(job_id)

        # Find the log file
        log_info = self.get_job_output_detailed(job_id, job_name)
        primary_log = log_info.get("primary_log")
        found_files = log_info.get("found_files", [])

        if not found_files:
            console.print(f"[red]❌ No log files found for job {job_id_str}[/red]")
            searched_dirs = log_info.get("searched_dirs", [])
            if searched_dirs:
                console.print(f"[yellow]📁 Searched in: {', '.join(searched_dirs)}[/yellow]")
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
                console.print("[yellow]📡 Streaming logs (Ctrl+C to stop)...[/yellow]\n")

                # If last_n is specified, show last N lines first
                if last_n:
                    with open(log_file, encoding="utf-8") as f:
                        lines = f.readlines()
                        for line in lines[-last_n:]:
                            console.print(line, end="")

                # Start streaming from current position
                with open(log_file, encoding="utf-8") as f:
                    # Move to end if not showing last_n lines
                    if not last_n:
                        f.seek(0, os.SEEK_END)

                    while True:
                        line = f.readline()
                        if line:
                            console.print(line, end="")
                        else:
                            # Check if job is still running
                            job = self.retrieve(int(job_id))
                            if job.status.value in ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"]:
                                console.print(f"\n[yellow]Job {job_id} finished with status: {job.status.value}[/yellow]")
                                break
                            time.sleep(poll_interval)
            else:
                # Static display mode
                output = log_info.get("output", "")

                if last_n and isinstance(output, str):
                    lines = output.split("\n")
                    output = "\n".join(lines[-last_n:])

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

    def _get_default_template(self) -> str:
        """Get the default job template path."""
        return str(files("srunx.templates").joinpath("advanced.slurm.jinja"))


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
