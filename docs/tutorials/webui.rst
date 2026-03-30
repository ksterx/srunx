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

Next Steps
----------

* :doc:`/how-to/webui` — Common Web UI tasks
* :doc:`/reference/webui-api` — REST API reference
