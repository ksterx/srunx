Web UI Setup
============

This tutorial walks you through installing and starting the srunx Web UI to manage SLURM jobs from your browser.

Prerequisites
-------------

* srunx installed (see :doc:`installation`)
* An SSH profile configured for your SLURM cluster (see :doc:`/how-to/user_guide`)
* Python 3.12+
* Node.js 18+ (for frontend development only; not needed for production use)

Step 1: Install Web Dependencies
---------------------------------

The Web UI is an optional feature. Install it with the ``web`` extra:

.. code-block:: bash

   uv sync --extra web

This installs FastAPI, uvicorn, and other backend dependencies.

Step 2: Configure SSH Connection
---------------------------------

The Web UI connects to your SLURM cluster via SSH. If you already have an srunx SSH profile configured, the Web UI will use it automatically.

Check your current profile:

.. code-block:: bash

   srunx ssh profile list

If you don't have a profile, create one:

.. code-block:: bash

   srunx ssh profile add myserver --hostname dgx.example.com --username researcher

Set it as the current profile:

.. code-block:: bash

   # The Web UI auto-detects the current profile
   srunx ssh profile add myserver
   # myserver is now the default

Step 3: Start the Server
-------------------------

.. code-block:: bash

   srunx-web

You should see:

.. code-block:: text

   INFO:     Using current SSH profile: myserver
   INFO:     Connecting to SLURM server via SSH...
   INFO:     SSH connection established
   INFO:     Uvicorn running on http://127.0.0.1:8000

Open http://127.0.0.1:8000 in your browser.

Step 4: Explore the Dashboard
------------------------------

The Dashboard shows:

* **Active Jobs** — Number of running and pending SLURM jobs
* **Failed** — Jobs that failed
* **GPU Availability** — Available GPUs across partitions
* **Active Jobs Table** — Clickable links to job logs
* **Resource Gauges** — Per-partition GPU utilization

.. note::

   Data is fetched via SSH polling. The dashboard refreshes automatically every 10 seconds.

Step 5: View Jobs
------------------

Navigate to **Jobs** to see all SLURM queue entries:

* Use the **search bar** to filter by job name
* Use the **status dropdown** to filter by state (RUNNING, PENDING, FAILED, etc.)
* Click the **log icon** to view job output
* Click the **cancel button** to stop a running job

Step 6: Upload a Workflow
--------------------------

Navigate to **Workflows** and click **Upload YAML**:

1. Select a workflow YAML file from your local machine
2. The file is validated and stored on the server
3. View the DAG visualization showing job dependencies

Example workflow YAML:

.. code-block:: yaml

   name: ml-pipeline
   jobs:
     - name: preprocess
       command: ["python", "preprocess.py"]
       nodes: 1

     - name: train
       command: ["python", "train.py"]
       depends_on: [preprocess]
       gpus_per_node: 4

     - name: evaluate
       command: ["python", "evaluate.py"]
       depends_on: [train]

Step 7: Build a Workflow Visually
----------------------------------

The DAG builder lets you create workflows interactively instead of writing YAML by hand.

1. Navigate to **Workflows** and click **New Workflow**
2. Enter a workflow name in the toolbar (e.g., ``ml-pipeline``)
3. Click **Add Job** to create your first node. It appears on the canvas as ``job_1``
4. Click the node to open the property panel on the right. Set:

   * **Name**: ``preprocess``
   * **Command**: ``python preprocess.py``

5. Click **Add Job** again. In the property panel, set:

   * **Name**: ``train``
   * **Command**: ``python train.py --epochs 100``
   * **GPUs per Node**: ``4``
   * **Time Limit**: ``4:00:00``

6. Drag from the **bottom handle** of ``preprocess`` to the **top handle** of ``train`` to create a dependency edge
7. (Optional) Click the edge to open the dependency type selector and choose between ``afterok``, ``after``, ``afterany``, or ``afternotok``
8. Click **Save Workflow** in the toolbar. The workflow is validated, saved as YAML, and you are redirected to the DAG view

.. note::

   The builder validates your workflow before saving: every job must have a name and command, job names must be unique, and the dependency graph must be acyclic.

Step 8: Set Up Mount Points
-----------------------------

Mount points let the file browser in the DAG builder map between local directories and remote paths on the SLURM cluster.

1. Add a mount to your SSH profile:

   .. code-block:: bash

      srunx ssh profile mount add myserver ml-project \
          --local ~/projects/ml-project \
          --remote /home/researcher/ml-project

2. Verify the mount was created:

   .. code-block:: bash

      srunx ssh profile mount list myserver

3. In the DAG builder, click a job node to open the property panel
4. Click the **folder icon** next to the Command, Work Dir, or Log Dir field
5. The file browser opens showing your configured mounts. Select a mount, browse the project tree, and click **Select**
6. The selected local path is translated to the corresponding remote path and inserted into the field

.. note::

   Click **Sync Now** in the file browser to push local files to the remote server via rsync before running a workflow.

Next Steps
----------

* :doc:`/how-to/webui` — Common Web UI tasks
* :doc:`/reference/webui-api` — REST API reference
