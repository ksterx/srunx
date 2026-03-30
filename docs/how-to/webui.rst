Web UI How-to Guide
===================

Practical recipes for common Web UI tasks.

Connect to a Different Cluster
-------------------------------

**Using an SSH profile:**

.. code-block:: bash

   SRUNX_SSH_PROFILE=other-cluster srunx-web

**Using direct connection parameters:**

.. code-block:: bash

   SRUNX_SSH_HOSTNAME=slurm.example.com SRUNX_SSH_USERNAME=user srunx-web

**With a custom SSH key:**

.. code-block:: bash

   SRUNX_SSH_HOSTNAME=slurm.example.com \
   SRUNX_SSH_USERNAME=user \
   SRUNX_SSH_KEY=~/.ssh/id_ed25519 \
   srunx-web

Change the Server Port
-----------------------

By default, the server runs on port 8000. Set the ``SRUNX_WEB_PORT`` environment variable or edit the config:

.. code-block:: bash

   # Start on port 3001
   SRUNX_WEB_PORT=3001 srunx-web

Monitor GPU Resources
----------------------

1. Navigate to **Resources** in the sidebar
2. Each partition shows:

   * GPU utilization bar with color coding (green < 70%, orange < 90%, red > 90%)
   * Total/idle/down node counts
   * Overall utilization percentage

3. Data refreshes every 10 seconds via polling

Cancel a Running Job
---------------------

1. Navigate to **Jobs**
2. Find the job in the table (use search or status filter)
3. Click the red **cancel button** (X icon) in the Actions column
4. The job will be cancelled via ``scancel`` on the remote cluster

View Job Logs
--------------

1. Navigate to **Jobs**
2. Click the **log icon** next to a job
3. Switch between **stdout** and **stderr** tabs
4. For running jobs, logs are polled every 3 seconds

Upload and Visualize a Workflow
--------------------------------

1. Navigate to **Workflows**
2. Click **Upload YAML**
3. Select a ``.yaml`` or ``.yml`` file
4. After upload, click **View DAG** on the workflow card
5. The DAG view shows:

   * Job nodes with status badges
   * Dependency edges between jobs
   * Resource indicators (GPU count, node count)
   * Click a node to see detailed job information

6. Toggle between **DAG** and **List** views using the toolbar buttons

.. warning::

   Workflow YAML files containing ``python:`` args are rejected for security reasons.
   Use ``python:`` args only via the CLI (``srunx flow run``).

Build a Workflow with the DAG Builder
---------------------------------------

1. Navigate to **Workflows** and click **New Workflow**
2. Enter a workflow name in the toolbar input (alphanumeric, hyphens, and underscores only)
3. Click **Add Job** to add job nodes to the canvas
4. Click each node to open the property panel and configure name, command, and resources
5. Drag from a node's bottom handle to another node's top handle to create a dependency
6. Click **Save Workflow** to validate and persist the workflow as YAML

The builder performs client-side validation before submitting:

* Every job must have a non-empty name and command
* Job names must be unique
* The dependency graph must be acyclic (no circular dependencies)

Edit Job Properties
--------------------

1. Click a job node on the DAG builder canvas
2. The property panel slides in from the right with these sections:

   * **Basic** — Name, command, work directory, log directory
   * **Resources** — Nodes, GPUs per node, tasks per node, CPUs per task, memory, time limit, partition, node list
   * **Environment** — Conda environment, virtualenv path, environment variables
   * **Container** — Enable/disable container mode with runtime (Pyxis, Apptainer, Singularity), image, mounts, workdir
   * **Retry** — Retry count and delay in seconds

3. Changes are applied immediately to the node on the canvas
4. Click the **X** button or click empty canvas space to close the panel
5. Click the **trash icon** to delete the selected job

Use the File Browser for Remote Paths
---------------------------------------

The file browser lets you pick files and directories from your local project tree, automatically translating them to remote paths.

1. Ensure you have at least one mount configured (see :doc:`/tutorials/webui` Step 8)
2. In the property panel, click the **folder icon** next to Command, Work Dir, or Log Dir
3. Select a mount from the **Project** dropdown
4. Browse the directory tree by clicking folders to expand them
5. Click a file or directory to select it (the footer shows the remote path)
6. Click **Select** to insert the remote path into the field

.. note::

   When selecting a file for the Command field, the path is made relative to the work directory if one is set. This keeps your commands portable.

Manage Mount Points
--------------------

Mount points define local-to-remote directory mappings for the file browser.

**Add a mount:**

.. code-block:: bash

   srunx ssh profile mount add myserver ml-project \
       --local ~/projects/ml-project \
       --remote /home/researcher/ml-project

**List mounts:**

.. code-block:: bash

   srunx ssh profile mount list myserver

**Remove a mount:**

.. code-block:: bash

   srunx ssh profile mount remove myserver ml-project

Mounts are stored in the SSH profile configuration. The ``--local`` path is the directory on your local machine; ``--remote`` is the corresponding path on the SLURM cluster.

Sync Files Before Running
---------------------------

The file browser shows local files, but workflows execute on the remote cluster. Ensure files are synchronized before running.

**From the file browser:**

1. Open the file browser from any path field in the property panel
2. Click **Sync Now** in the yellow banner at the top of the file tree
3. Wait for the sync to complete (the button changes to "Synced")

**From the command line:**

.. code-block:: bash

   rsync -avz --delete \
       -e "ssh -i ~/.ssh/id_ed25519" \
       ~/projects/ml-project/ \
       researcher@dgx.example.com:/home/researcher/ml-project/

.. warning::

   If you modify local files after syncing, you must sync again before running the workflow on the cluster.

Change Dependency Types
------------------------

By default, edges use ``afterok`` (run only if the upstream job completes successfully). To change the dependency type:

1. Click an edge on the DAG builder canvas
2. A popover appears with four options:

   * ``afterok`` — Run after successful completion (default)
   * ``after`` — Run after the job finishes regardless of exit code
   * ``afterany`` — Run after the job reaches any terminal state
   * ``afternotok`` — Run only if the upstream job fails

3. Click the desired type. The edge updates immediately

Run Without SSH (Frontend Only)
--------------------------------

For frontend development or demos, the server can start without an SSH connection:

.. code-block:: bash

   srunx-web

If no SSH profile is configured, the server starts with a warning:

.. code-block:: text

   No SSH configuration provided. Set SRUNX_SSH_PROFILE or
   SRUNX_SSH_HOSTNAME + SRUNX_SSH_USERNAME to connect to a SLURM cluster.

SLURM endpoints will return ``503 Service Unavailable``, but the frontend loads normally.

Develop the Frontend
---------------------

For frontend development with hot-reload:

.. code-block:: bash

   # Terminal 1: Start the backend
   srunx-web

   # Terminal 2: Start the Vite dev server
   cd src/srunx/web/frontend
   npm install
   npm run dev

The Vite dev server runs on ``http://localhost:3000`` and proxies API requests to the backend on port 8000.

Run Tests
----------

**Backend tests:**

.. code-block:: bash

   uv run pytest tests/web/ -v

**Frontend E2E tests:**

.. code-block:: bash

   cd src/srunx/web/frontend
   npm run test:e2e

**Frontend type check:**

.. code-block:: bash

   cd src/srunx/web/frontend
   npx tsc --noEmit
