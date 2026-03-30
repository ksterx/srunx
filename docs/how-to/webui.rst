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
