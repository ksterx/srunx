#!/usr/bin/env python3

import logging

from srunx.ssh.core.client import SSHSlurmClient

logging.basicConfig(level=logging.INFO)


def main():
    sample_sbatch_script = """#!/bin/bash
#SBATCH --job-name=test_job
#SBATCH --output=test_output.out
#SBATCH --error=test_error.err
#SBATCH --time=00:05:00
#SBATCH --ntasks=1

echo "Job started at $(date)"
echo "Running on node: $HOSTNAME"
sleep 60
echo "Job completed at $(date)"
"""

    hostname = "your-dgx-server.com"
    username = "your-username"

    with SSHSlurmClient(
        hostname=hostname,
        username=username,
        key_filename="~/.ssh/id_rsa",  # or use password="your-password"
    ) as client:
        job = client.submit_sbatch_job(sample_sbatch_script, job_name="example_job")

        if job:
            print(f"Job submitted: {job.job_id}")

            job = client.monitor_job(job, poll_interval=5)

            stdout, stderr = client.get_job_output(job.job_id, job.name)
            print(f"Job output:\n{stdout}")
            if stderr:
                print(f"Job errors:\n{stderr}")


if __name__ == "__main__":
    main()
