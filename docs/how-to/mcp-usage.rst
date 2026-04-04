Using srunx MCP Tools
=====================

This guide shows how to accomplish specific tasks through Claude Code using the
srunx MCP tools. Each section is a self-contained recipe.

For setup instructions, see :doc:`/tutorials/mcp-setup`.
For the full tool reference, see :doc:`/reference/mcp-tools`.

Submit a Job
------------

Ask Claude Code to submit a job with the resources you need:

.. code-block:: text

   > Submit a job named "training" that runs "python train.py --epochs 50"
   > with 4 GPUs, 64GB memory, on the gpu partition, using conda env pytorch

Claude Code calls ``submit_job`` and returns the job ID. You can then
reference that ID in follow-up prompts:

.. code-block:: text

   > What's the status of that job?

Create a Workflow from Natural Language
---------------------------------------

Describe your pipeline and let Claude Code generate the workflow YAML:

.. code-block:: text

   > Create a workflow called "ml_pipeline" with three jobs:
   > 1. "preprocess" runs "python preprocess.py" on 1 node
   > 2. "train" runs "python train.py" with 2 GPUs, depends on preprocess,
   >    uses conda env ml_env, time limit 8 hours
   > 3. "evaluate" runs "python evaluate.py", depends on train
   > Save it to workflows/ml_pipeline.yaml

Claude Code calls ``create_workflow`` with the job definitions and writes
the YAML file. You can then validate and run it:

.. code-block:: text

   > Validate the workflow at workflows/ml_pipeline.yaml
   > Run it

Monitor Resources and Make Decisions
-------------------------------------

Use resource checks to decide when and where to submit jobs:

.. code-block:: text

   > Check GPU availability on all partitions. If there are at least 4 GPUs
   > free on any partition, submit my training job there.

Claude Code calls ``get_resources``, inspects the result, and conditionally
calls ``submit_job`` with the partition that has capacity.

Check Job Logs
--------------

Retrieve stdout and stderr from completed or running jobs:

.. code-block:: text

   > Show me the logs for job 12345

Claude Code calls ``get_job_logs`` and displays the output. For SSH jobs,
it fetches logs from the remote cluster.

Sync Files Before Job Submission
---------------------------------

Ensure your latest code is on the remote cluster before submitting:

.. code-block:: text

   > Sync the ml-project mount and then submit "python train.py" with
   > 2 GPUs via SSH

Claude Code calls ``sync_files`` with the mount name, then calls
``submit_job`` with ``use_ssh=True``. The sync uses your configured mount
points from the SSH profile.

For explicit paths instead of named mounts:

.. code-block:: text

   > Sync ./src to ~/workspace/src on the remote cluster (dry run first)

Claude Code calls ``sync_files`` with ``local_path`` and ``remote_path``,
first with ``dry_run=True`` to preview, then again to execute.

Use SSH Mode for Remote Clusters
---------------------------------

Most tools accept a ``use_ssh`` flag. You can tell Claude Code to operate
remotely:

.. code-block:: text

   > List jobs on the remote cluster

.. code-block:: text

   > Cancel job 12345 on the remote cluster

.. code-block:: text

   > Check resources on the gpu partition via SSH

Claude Code routes these through the active SSH profile. To see which
profiles are configured:

.. code-block:: text

   > List my SSH profiles

Run Partial Workflows
---------------------

Execute specific portions of a workflow:

.. code-block:: text

   > Run only the "train" job from workflows/ml_pipeline.yaml

.. code-block:: text

   > Run the workflow from "train" to "evaluate", skipping preprocess

.. code-block:: text

   > Do a dry run of workflows/ml_pipeline.yaml to see what would execute

Claude Code uses the ``single_job``, ``from_job``, ``to_job``, and
``dry_run`` parameters of ``run_workflow``.

Combine Multiple Operations
----------------------------

Claude Code can chain tools in a single conversation turn:

.. code-block:: text

   > Check if there are at least 2 GPUs available. If yes, sync my
   > ml-project mount and submit "python train.py --lr 0.001" with
   > 2 GPUs via SSH. Show me the job ID when done.

This triggers a sequence: ``get_resources`` -> ``sync_files`` -> ``submit_job``.

Another multi-step example:

.. code-block:: text

   > Find all workflows in this project, validate each one, and tell me
   > which ones have issues.

This calls ``list_workflows`` then ``validate_workflow`` for each file found.

Inspect Configuration
---------------------

Review your srunx setup:

.. code-block:: text

   > Show my srunx configuration

.. code-block:: text

   > What SSH profiles do I have configured? Show their mount points.

These call ``get_config`` and ``list_ssh_profiles`` respectively.

Tips
----

- Claude Code picks the right tool based on your intent. You do not need
  to name tools explicitly.
- Include "via SSH" or "on the remote cluster" to trigger SSH mode.
- Use "dry run" to preview any destructive operation before executing.
- Reference job IDs from earlier in the conversation -- Claude Code tracks
  context across turns.
