Job and Resource Monitoring
============================

srunx provides comprehensive monitoring capabilities for SLURM jobs and GPU resources.

Overview
--------

The monitoring system consists of three main components:

1. **Job Monitoring**: Track SLURM job state transitions
2. **Resource Monitoring**: Monitor GPU availability on partitions
3. **Scheduled Reporting**: Periodic cluster status reports via Slack

Monitor Commands
----------------

The ``srunx monitor`` command has three subcommands:

.. code-block:: bash

    srunx monitor jobs      # Monitor job state changes
    srunx monitor resources # Monitor GPU resource availability
    srunx monitor cluster   # Scheduled periodic reporting

Job Monitoring
--------------

Monitor single or multiple SLURM jobs until completion with automatic state transition detection.

Basic Usage
~~~~~~~~~~~

.. code-block:: bash

    # Monitor single job until completion
    srunx monitor jobs 12345

    # Monitor multiple jobs
    srunx monitor jobs 12345 12346 12347

    # Monitor all your jobs
    srunx monitor jobs --all

Continuous Monitoring
~~~~~~~~~~~~~~~~~~~~~

Monitor job state changes continuously with real-time notifications:

.. code-block:: bash

    # Continuous monitoring (Ctrl+C to stop)
    srunx monitor jobs 12345 --continuous

    # With custom poll interval (default: 60s)
    srunx monitor jobs 12345 --continuous --interval 30

    # With Slack notifications
    srunx monitor jobs 12345 --continuous --notify $WEBHOOK_URL

Monitoring Options
~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Option
     - Description
   * - ``--continuous``
     - Monitor continuously instead of until completion
   * - ``--interval SECONDS``
     - Polling interval (default: 60s, minimum: 1s)
   * - ``--timeout SECONDS``
     - Maximum monitoring duration (default: no timeout)
   * - ``--notify URL``
     - Slack webhook URL for notifications
   * - ``--all``
     - Monitor all user jobs

State Transitions
~~~~~~~~~~~~~~~~~

The job monitor detects and reports the following state transitions:

- **PENDING** → **RUNNING**: Job started execution
- **RUNNING** → **COMPLETED**: Job finished successfully
- **RUNNING** → **FAILED**: Job failed with error
- **RUNNING** → **CANCELLED**: Job was cancelled
- **RUNNING** → **TIMEOUT**: Job exceeded time limit

Resource Monitoring
-------------------

Monitor GPU resource availability on SLURM partitions with threshold-based waiting.

Display Current Resources
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Display current resource availability
    srunx monitor resources

    # Display resources for specific partition
    srunx monitor resources --partition gpu

    # Show output in JSON format
    srunx monitor resources --format json

Wait for Resources
~~~~~~~~~~~~~~~~~~

Block until sufficient GPU resources become available:

.. code-block:: bash

    # Wait for 4 GPUs to become available
    srunx monitor resources --min-gpus 4

    # Wait for resources on specific partition
    srunx monitor resources --min-gpus 2 --partition gpu

    # With timeout (default: no timeout)
    srunx monitor resources --min-gpus 4 --timeout 7200

Continuous Resource Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Monitor resource availability changes continuously:

.. code-block:: bash

    # Continuous monitoring with state change notifications
    srunx monitor resources --min-gpus 2 --continuous

    # With Slack notifications
    srunx monitor resources --min-gpus 4 --continuous --notify $WEBHOOK_URL

Resource Monitoring Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Option
     - Description
   * - ``--min-gpus N``
     - Minimum GPUs required (required for waiting/continuous)
   * - ``--partition NAME``
     - Monitor specific partition (default: all partitions)
   * - ``--continuous``
     - Monitor continuously with state change notifications
   * - ``--interval SECONDS``
     - Polling interval (default: 60s, minimum: 60s for resources)
   * - ``--timeout SECONDS``
     - Maximum monitoring duration
   * - ``--notify URL``
     - Slack webhook URL for notifications
   * - ``--format FORMAT``
     - Output format: table (default) or json

Resource Information
~~~~~~~~~~~~~~~~~~~~

The resource monitor provides:

- **Total GPUs**: Total GPU count across all available nodes
- **GPUs in Use**: GPUs currently allocated to running jobs
- **GPUs Available**: GPUs available for new jobs
- **GPU Utilization**: Percentage of GPUs in use
- **Nodes Total**: Total nodes in partition
- **Nodes Idle**: Idle nodes available for jobs
- **Nodes Down**: Unavailable nodes (DOWN/DRAIN/MAINT/RESERVED)

Node State Handling
~~~~~~~~~~~~~~~~~~~

The resource monitor automatically filters out unavailable nodes:

- **DOWN**: Node is down
- **DRAIN/DRAINING**: Node is being drained
- **MAINT**: Node is in maintenance mode
- **RESERVED**: Node is reserved

Only GPUs from available nodes are counted in the availability calculation.

Scheduled Reporting
-------------------

Send periodic SLURM cluster status reports via Slack webhooks.

Basic Usage
~~~~~~~~~~~

.. code-block:: bash

    # Send hourly reports
    srunx monitor cluster --schedule 1h --notify $WEBHOOK_URL

    # Send reports every 30 minutes
    srunx monitor cluster --schedule 30m --notify $WEBHOOK_URL

    # Send daily reports at 9 AM
    srunx monitor cluster --schedule "0 9 * * *" --notify $WEBHOOK_URL

Schedule Formats
~~~~~~~~~~~~~~~~

**Interval Format** (simple intervals):

- ``1h``, ``2h``, etc. - Hours
- ``30m``, ``45m``, etc. - Minutes (minimum: 1m)
- ``1d``, ``7d``, etc. - Days

**Cron Format** (precise scheduling):

- ``"0 9 * * *"`` - Every day at 9:00 AM
- ``"0 */6 * * *"`` - Every 6 hours
- ``"0 9 * * 1-5"`` - Weekdays at 9:00 AM
- ``"*/30 * * * *"`` - Every 30 minutes

Report Contents
~~~~~~~~~~~~~~~

The scheduled report includes:

**Job Statistics**:
  - Pending jobs count
  - Running jobs count
  - Completed jobs (if available)
  - Failed jobs (if available)

**GPU Resources**:
  - Total GPUs in partition
  - GPUs in use
  - GPUs available
  - Utilization percentage

**Node Statistics**:
  - Total nodes
  - Idle nodes
  - Down/unavailable nodes

**User Job Information** (optional):
  - Your pending jobs
  - Your running jobs
  - Your completed/failed jobs

**Running Jobs List** (optional):
  - Job ID and name
  - User
  - Status
  - Partition
  - Runtime (for running jobs)
  - GPU allocation

Report Options
~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Option
     - Description
   * - ``--schedule SPEC``
     - Schedule specification (interval or cron)
   * - ``--notify URL``
     - Slack webhook URL (required)
   * - ``--partition NAME``
     - Monitor specific partition
   * - ``--include SECTIONS``
     - Report sections: jobs, resources, user, running
   * - ``--max-jobs N``
     - Maximum jobs to list in running section (default: 10)
   * - ``--user USERNAME``
     - User for user-specific statistics (default: current user)

Include Sections
~~~~~~~~~~~~~~~~

Customize report contents with ``--include``:

.. code-block:: bash

    # Include all sections (default)
    srunx monitor cluster --schedule 1h --notify $WEBHOOK_URL \
        --include jobs,resources,user,running

    # Only job and resource statistics
    srunx monitor cluster --schedule 1h --notify $WEBHOOK_URL \
        --include jobs,resources

    # Only running jobs list
    srunx monitor cluster --schedule 30m --notify $WEBHOOK_URL \
        --include running --max-jobs 20

Available sections:

- ``jobs`` - Overall job queue statistics
- ``resources`` - GPU resource availability
- ``user`` - User-specific job statistics
- ``running`` - List of running jobs with details

Programmatic Usage
------------------

Job Monitoring
~~~~~~~~~~~~~~

.. code-block:: python

    from srunx import Slurm
    from srunx.monitor import JobMonitor
    from srunx.monitor.types import MonitorConfig
    from srunx.callbacks import SlackCallback

    client = Slurm()

    # Submit a job
    job = client.submit(job)

    # Monitor until completion
    monitor = JobMonitor(
        job_ids=[job.job_id],
        config=MonitorConfig(poll_interval=30, timeout=3600)
    )
    monitor.watch_until()  # Blocks until job completes or timeout

    # Continuous monitoring with callbacks
    slack_callback = SlackCallback(webhook_url="your_webhook_url")
    monitor = JobMonitor(
        job_ids=[job.job_id],
        config=MonitorConfig(
            poll_interval=10,
            mode=WatchMode.CONTINUOUS,
            notify_on_change=True
        ),
        callbacks=[slack_callback]
    )
    monitor.watch_continuous()  # Ctrl+C to stop

Resource Monitoring
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from srunx.monitor import ResourceMonitor
    from srunx.monitor.types import MonitorConfig, WatchMode
    from srunx.callbacks import SlackCallback

    # Wait for resources
    monitor = ResourceMonitor(
        min_gpus=4,
        partition="gpu",
        config=MonitorConfig(poll_interval=60, timeout=7200)
    )
    monitor.watch_until()  # Blocks until 4 GPUs available

    # Continuous monitoring with notifications
    callback = SlackCallback(webhook_url="your_webhook_url")
    monitor = ResourceMonitor(
        min_gpus=2,
        partition="gpu",
        config=MonitorConfig(
            poll_interval=60,
            mode=WatchMode.CONTINUOUS,
            notify_on_change=True
        ),
        callbacks=[callback]
    )
    monitor.watch_continuous()

Scheduled Reporting
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from srunx import Slurm
    from srunx.callbacks import SlackCallback
    from srunx.monitor.scheduler import ScheduledReporter
    from srunx.monitor.report_types import ReportConfig

    client = Slurm()
    callback = SlackCallback(webhook_url="your_webhook_url")

    # Configure report
    config = ReportConfig(
        schedule="1h",  # Every hour
        partition="gpu",
        include=["jobs", "resources", "user", "running"],
        max_jobs=10,
        user=None  # Use current user
    )

    # Start reporter
    reporter = ScheduledReporter(client, callback, config)
    reporter.run()  # Blocking execution, Ctrl+C to stop

Custom Callbacks
~~~~~~~~~~~~~~~~

Create custom callbacks by extending the ``Callback`` base class:

.. code-block:: python

    from srunx.callbacks import Callback
    from srunx.models import Job
    from srunx.monitor.types import ResourceSnapshot

    class CustomCallback(Callback):
        def on_job_completed(self, job: Job) -> None:
            print(f"Job {job.job_id} completed!")

        def on_job_failed(self, job: Job) -> None:
            print(f"Job {job.job_id} failed!")

        def on_resources_available(self, snapshot: ResourceSnapshot) -> None:
            print(f"{snapshot.gpus_available} GPUs now available!")

        def on_resources_exhausted(self, snapshot: ResourceSnapshot) -> None:
            print(f"Only {snapshot.gpus_available} GPUs remaining!")

    # Use custom callback
    monitor = JobMonitor(
        job_ids=[job_id],
        callbacks=[CustomCallback()]
    )
    monitor.watch_continuous()

Configuration
-------------

MonitorConfig
~~~~~~~~~~~~~

Configure monitoring behavior:

.. code-block:: python

    from srunx.monitor.types import MonitorConfig, WatchMode

    config = MonitorConfig(
        poll_interval=60,      # Poll every 60 seconds
        timeout=3600,          # Timeout after 1 hour
        mode=WatchMode.UNTIL_CONDITION,  # or WatchMode.CONTINUOUS
        notify_on_change=True  # Send notifications on state changes
    )

Configuration parameters:

- ``poll_interval`` - Seconds between polls (default: 60)
- ``timeout`` - Maximum monitoring duration in seconds (default: None)
- ``mode`` - ``UNTIL_CONDITION`` or ``CONTINUOUS``
- ``notify_on_change`` - Enable state change notifications (default: False)

.. note::
   For resource monitoring, the minimum poll interval is 60 seconds to avoid
   overloading SLURM. Intervals below 60s will trigger a warning.

Best Practices
--------------

Polling Intervals
~~~~~~~~~~~~~~~~~

- **Job Monitoring**: 10-60 seconds for active monitoring
- **Resource Monitoring**: 60+ seconds (minimum 60s enforced)
- **Scheduled Reporting**: 30 minutes to 1 hour for regular updates

Choose appropriate intervals based on:

- Cluster size and load
- Monitoring urgency
- SLURM system performance

Timeouts
~~~~~~~~

Always set timeouts for blocking operations:

.. code-block:: bash

    # Good: Set reasonable timeout
    srunx monitor jobs 12345 --timeout 3600

    # Bad: No timeout (could block indefinitely)
    srunx monitor jobs 12345

Error Handling
~~~~~~~~~~~~~~

The monitoring system includes automatic error recovery:

- SLURM command timeouts (30s per command)
- Graceful handling of node state variations
- Automatic retry on transient failures
- Safe handling of malformed SLURM output

Signal Handling
~~~~~~~~~~~~~~~

All monitoring operations support graceful shutdown:

- **Ctrl+C** (SIGINT) - Clean exit with status summary
- **SIGTERM** - Graceful shutdown for process management

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

**High CPU usage**:
  - Increase poll interval
  - Reduce number of monitored jobs
  - Use ``--timeout`` to limit duration

**Missing notifications**:
  - Verify Slack webhook URL
  - Check network connectivity
  - Review logs for callback errors

**Inaccurate GPU counts**:
  - Verify partition name
  - Check node states (DOWN/DRAIN nodes are excluded)
  - Ensure SLURM commands are accessible

Logging
~~~~~~~

Enable debug logging for troubleshooting:

.. code-block:: python

    from loguru import logger
    logger.add("monitor.log", level="DEBUG")

Or via environment variable:

.. code-block:: bash

    export LOGURU_LEVEL=DEBUG
    srunx monitor jobs 12345

Examples
--------

Complete Workflow Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from srunx import Slurm, Job
    from srunx.monitor import ResourceMonitor, JobMonitor
    from srunx.monitor.types import MonitorConfig
    from srunx.callbacks import SlackCallback

    client = Slurm()
    callback = SlackCallback(webhook_url="your_webhook_url")

    # Wait for resources
    print("Waiting for 4 GPUs...")
    resource_monitor = ResourceMonitor(
        min_gpus=4,
        config=MonitorConfig(poll_interval=60, timeout=3600)
    )
    resource_monitor.watch_until()

    # Submit job
    job = client.submit(job)
    print(f"Job {job.job_id} submitted")

    # Monitor job with notifications
    job_monitor = JobMonitor(
        job_ids=[job.job_id],
        config=MonitorConfig(poll_interval=30),
        callbacks=[callback]
    )
    job_monitor.watch_until()
    print("Job completed!")

Automated Job Queue Management
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    #!/bin/bash
    # automated_queue.sh

    # Wait for resources
    srunx monitor resources --min-gpus 8 --timeout 7200

    # Submit batch of jobs
    for experiment in exp1 exp2 exp3; do
        srunx submit python train.py --name $experiment
    done

    # Monitor all submitted jobs
    srunx monitor jobs --all --notify $SLACK_WEBHOOK

Cluster Status Dashboard
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Start scheduled reporting for cluster dashboard
    srunx monitor cluster \
        --schedule "*/15 * * * *" \
        --notify $SLACK_WEBHOOK \
        --include jobs,resources,running \
        --max-jobs 20
