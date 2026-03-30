Web UI REST API Reference
=========================

The srunx Web UI exposes a REST API at ``http://127.0.0.1:8000/api/``. All responses are JSON.

Jobs
----

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Endpoint
     - Description
   * - GET
     - ``/api/jobs``
     - List all SLURM jobs in the queue
   * - GET
     - ``/api/jobs/{job_id}``
     - Get detailed status for a specific job
   * - POST
     - ``/api/jobs``
     - Submit a new job
   * - DELETE
     - ``/api/jobs/{job_id}``
     - Cancel a running or pending job
   * - GET
     - ``/api/jobs/{job_id}/logs``
     - Get stdout/stderr log contents

GET /api/jobs
^^^^^^^^^^^^^

Returns a list of all jobs from ``squeue``.

**Response:**

.. code-block:: json

   [
     {
       "name": "train-resnet",
       "job_id": 18431,
       "status": "RUNNING",
       "depends_on": [],
       "command": [],
       "resources": {
         "nodes": 1,
         "gpus_per_node": 8,
         "partition": "gpu",
         "time_limit": "8:00:00"
       },
       "partition": "gpu",
       "nodes": 1,
       "gpus": 8,
       "elapsed_time": "1:30:00"
     }
   ]

POST /api/jobs
^^^^^^^^^^^^^^

Submit a new job with a SLURM script.

**Request body:**

.. code-block:: json

   {
     "name": "my-job",
     "script_content": "#!/bin/bash\n#SBATCH --gpus=1\npython train.py",
     "job_name": "training-run"
   }

**Response (201):**

.. code-block:: json

   {
     "name": "my-job",
     "job_id": 18500,
     "status": "PENDING",
     "depends_on": [],
     "command": [],
     "resources": {}
   }

DELETE /api/jobs/{job_id}
^^^^^^^^^^^^^^^^^^^^^^^^^

Cancel a job. Returns ``204 No Content`` on success.

GET /api/jobs/{job_id}/logs
^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Response:**

.. code-block:: json

   {
     "stdout": "Epoch 1/10: loss=0.85...",
     "stderr": "WARNING: GPU memory high"
   }

Resources
---------

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Endpoint
     - Description
   * - GET
     - ``/api/resources``
     - Get GPU and node availability per partition

GET /api/resources
^^^^^^^^^^^^^^^^^^

**Query parameters:**

* ``partition`` (optional) — Filter to a specific partition

**Response:**

.. code-block:: json

   [
     {
       "timestamp": "2026-03-30T09:00:00+00:00",
       "partition": "gpu",
       "total_gpus": 32,
       "gpus_in_use": 24,
       "gpus_available": 8,
       "jobs_running": 3,
       "nodes_total": 4,
       "nodes_idle": 1,
       "nodes_down": 0,
       "gpu_utilization": 0.75,
       "has_available_gpus": true
     }
   ]

.. note::

   Multi-node jobs are correctly accounted for: ``gpus_in_use = gpus_per_node * num_nodes``.

Workflows
---------

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Endpoint
     - Description
   * - GET
     - ``/api/workflows``
     - List all workflow definitions
   * - GET
     - ``/api/workflows/{name}``
     - Get a specific workflow with its jobs
   * - POST
     - ``/api/workflows/validate``
     - Validate YAML content
   * - POST
     - ``/api/workflows/upload``
     - Upload and save a workflow YAML
   * - POST
     - ``/api/workflows/{name}/run``
     - Start a workflow run (creates tracking record)
   * - GET
     - ``/api/workflows/runs``
     - List workflow run records

POST /api/workflows/upload
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Request body:**

.. code-block:: json

   {
     "yaml": "name: my-pipeline\njobs:\n  - name: step1\n    command: ['echo', 'hello']",
     "filename": "my-pipeline.yaml"
   }

**Validation rules:**

* Filename must be alphanumeric with hyphens/underscores only
* File extension must be ``.yaml`` or ``.yml``
* Content size limit: 1MB
* ``python:`` args are rejected (security)

POST /api/workflows/validate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Request body:**

.. code-block:: json

   {"yaml": "name: test\njobs: []"}

**Response:**

.. code-block:: json

   {"valid": true}

   // or
   {"valid": false, "errors": ["Duplicate job name: step1"]}

History
-------

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Endpoint
     - Description
   * - GET
     - ``/api/history``
     - Get recent job execution history
   * - GET
     - ``/api/history/stats``
     - Get aggregate job statistics

GET /api/history/stats
^^^^^^^^^^^^^^^^^^^^^^

**Query parameters:**

* ``from`` (optional) — Start date (ISO format)
* ``to`` (optional) — End date (ISO format)

**Response:**

.. code-block:: json

   {
     "total": 42,
     "completed": 35,
     "failed": 4,
     "cancelled": 3,
     "avg_runtime_seconds": 3600.0
   }

Error Responses
---------------

All errors follow this format:

.. code-block:: json

   {"detail": "Error message description"}

**Status codes:**

* ``400`` — Invalid input (e.g., negative job ID)
* ``404`` — Resource not found (job, workflow)
* ``413`` — YAML content too large
* ``422`` — Validation error
* ``502`` — SLURM command failed
* ``503`` — SSH connection not configured

Configuration
-------------

The Web UI is configured via environment variables:

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Variable
     - Default
     - Description
   * - ``SRUNX_SSH_PROFILE``
     - (current profile)
     - srunx SSH profile name
   * - ``SRUNX_SSH_HOSTNAME``
     - —
     - Direct SSH hostname
   * - ``SRUNX_SSH_USERNAME``
     - —
     - Direct SSH username
   * - ``SRUNX_SSH_KEY``
     - —
     - Path to SSH private key
   * - ``SRUNX_SSH_PORT``
     - 22
     - SSH port
   * - ``SRUNX_WORKFLOW_DIR``
     - ``workflows/``
     - Directory for workflow YAML files
