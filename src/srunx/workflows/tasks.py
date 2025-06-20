from srunx.client import Slurm
from srunx.logging import get_logger
from srunx.models import BaseJob, Job, ShellJob

logger = get_logger(__name__)


def submit_and_monitor_job(
    job: Job | ShellJob, poll_interval: int = 5
) -> Job | ShellJob:
    """Submit a SLURM job and monitor until completion.

    This task handles the complete lifecycle of a SLURM job:
    submission, monitoring, and completion verification.

    Args:
        job: Job configuration.
        poll_interval: Status polling interval in seconds.

    Returns:
        Completed Job instance.

    Raises:
        RuntimeError: If the SLURM job fails.
        subprocess.CalledProcessError: If job operations fail.
    """
    logger.info(f"Starting SLURM job submission and monitoring for '{job.name}'")
    client = Slurm()

    # Submit the job
    submitted_job = client.run(job)
    logger.info(f"Job '{submitted_job.name}' submitted with ID {submitted_job.job_id}")

    # Wait for completion
    completed_job = client.monitor(submitted_job, poll_interval=poll_interval)
    logger.info(f"Job '{completed_job.name}' (ID: {completed_job.job_id}) completed")

    assert isinstance(completed_job, Job | ShellJob)

    return completed_job


def submit_job_async(job: Job | ShellJob) -> Job | ShellJob:
    """Submit a SLURM job without waiting for completion.

    Args:
        job: Job configuration.

    Returns:
        Submitted Job instance with job_id.
    """
    client = Slurm()
    submitted_job = client.run(job)
    logger.info(f"  - '{submitted_job.name}' (ID: {submitted_job.job_id})")
    assert isinstance(submitted_job, Job | ShellJob)
    return submitted_job


def wait_for_job(job_id: int, poll_interval: int = 5) -> BaseJob:
    """Wait for a job to complete.

    Args:
        job_id: SLURM job ID.
        poll_interval: Polling interval in seconds.

    Returns:
        Completed job object.
    """
    logger.info(f"Waiting for job {job_id} to complete")
    client = Slurm()
    completed_job = client.monitor(job_id, poll_interval)
    logger.info(f"Job {job_id} completed")
    return completed_job
