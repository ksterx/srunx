"""SLURM client for job submission and management."""

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
    ShellJob,
    render_job_script,
)
from srunx.utils import get_job_status

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

    def run(
        self,
        job: Job | ShellJob,
        template_path: str | None = None,
        callbacks: Sequence[Callback] | None = None,
        verbose: bool = False,
    ) -> Job | ShellJob:
        """Submit a job to SLURM.

        Args:
            job: Job configuration.
            template_path: Optional template path (uses default if not provided).
            callbacks: List of callbacks.
            verbose: Whether to print the rendered content.

        Returns:
            Job instance with updated job_id and status.

        Raises:
            subprocess.CalledProcessError: If job submission fails.
        """

        if isinstance(job, Job):
            template = template_path or self.default_template

            with tempfile.TemporaryDirectory() as temp_dir:
                script_path = render_job_script(template, job, temp_dir, verbose)
                logger.debug(f"Generated SLURM script at: {script_path}")

                # Handle container execution
                sbatch_cmd = ["sbatch"]
                if job.environment.sqsh:
                    sbatch_cmd.extend(["--sqsh", job.environment.sqsh])
                    logger.debug(f"Using sqsh: {job.environment.sqsh}")

                sbatch_cmd.append(script_path)
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
            try:
                result = subprocess.run(
                    ["sbatch", job.path],
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

        else:
            raise ValueError("Either 'command' or 'file' must be set")

        time.sleep(3)
        job_id = int(result.stdout.split()[-1])
        job.job_id = job_id
        job.status = JobStatus.PENDING

        logger.debug(f"Successfully submitted job '{job.name}' with ID {job_id}")

        all_callbacks = self.callbacks[:]
        if callbacks:
            all_callbacks.extend(callbacks)
        for callback in all_callbacks:
            callback.on_job_submitted(job)

        return job

    @staticmethod
    def retrieve(job_id: int) -> BaseJob:
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
                    status=status,
                )
                jobs.append(job)

        return jobs

    def monitor(
        self,
        job_obj_or_id: BaseJob | Job | ShellJob | int,
        poll_interval: int = 30,
        callbacks: list[Callback] | None = None,
    ) -> BaseJob | Job | ShellJob:
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

        msg = f"⏳ Waiting for job {job.job_id} to complete (polling every {poll_interval}s)."
        if isinstance(job, Job):
            msg += f" Logging to {job.log_dir}/{job.name}_{job.job_id}.out"
        logger.info(msg)

        previous_status = None

        while True:
            job.refresh()

            # Log status changes
            if job.status != previous_status:
                status_str = job.status.value if job.status else "Unknown"
                logger.info(f"Job {job.job_id} status: {status_str}")
                previous_status = job.status

            match job.status:
                case JobStatus.COMPLETED:
                    logger.info(f"Job {job.job_id} completed successfully")
                    for callback in all_callbacks:
                        callback.on_job_completed(job)
                    return job
                case JobStatus.FAILED:
                    err_msg = f"SLURM job {job.job_id} failed.\n"
                    if isinstance(job, Job):
                        log_file = Path(job.log_dir) / f"{job.name}_{job.job_id}.out"
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
                    err_msg = (
                        f"SLURM job {job.job_id} was {job.status.value.lower()}.\n"
                    )
                    if isinstance(job, Job):
                        log_file = Path(job.log_dir) / f"{job.name}_{job.job_id}.out"
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

    def _get_default_template(self) -> str:
        """Get the default job template path."""
        return str(files("srunx.templates").joinpath("base.slurm.jinja"))


# Convenience functions for backward compatibility
def submit_job(
    job: Job | ShellJob,
    template_path: str | None = None,
    callbacks: Sequence[Callback] | None = None,
    verbose: bool = False,
) -> Job | ShellJob:
    """Submit a job to SLURM (convenience function).

    Args:
        job: Job configuration.
        template_path: Optional template path (uses default if not provided).
        callbacks: List of callbacks.
        verbose: Whether to print the rendered content.
    """
    client = Slurm()
    return client.run(
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
