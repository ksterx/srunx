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
     - ``/api/workflows/create``
     - Create a workflow from structured JSON (DAG builder)
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

POST /api/workflows/create
^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a workflow from a structured JSON payload. Used by the DAG builder.

**Request body:**

.. code-block:: json

   {
     "name": "ml-pipeline",
     "jobs": [
       {
         "name": "preprocess",
         "command": ["python", "preprocess.py"],
         "depends_on": [],
         "resources": {"nodes": 1},
         "work_dir": "/home/researcher/ml-project"
       },
       {
         "name": "train",
         "command": ["python", "train.py", "--epochs", "100"],
         "depends_on": ["preprocess"],
         "resources": {"nodes": 1, "gpus_per_node": 4, "time_limit": "4:00:00"},
         "environment": {"conda": "ml_env"}
       }
     ]
   }

**Job fields:**

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Required
     - Description
   * - ``name``
     - Yes
     - Job name (alphanumeric, hyphens, underscores)
   * - ``command``
     - Yes
     - Command as a list of strings
   * - ``depends_on``
     - No
     - List of upstream job names, optionally prefixed with dependency type (e.g., ``afternotok:preprocess``)
   * - ``resources``
     - No
     - Object with ``nodes``, ``gpus_per_node``, ``ntasks_per_node``, ``cpus_per_task``, ``memory_per_node``, ``time_limit``, ``partition``, ``nodelist``
   * - ``environment``
     - No
     - Object with ``conda``, ``venv``, ``container``, ``env_vars``
   * - ``work_dir``
     - No
     - Working directory on the remote cluster
   * - ``log_dir``
     - No
     - Log output directory
   * - ``retry``
     - No
     - Number of retry attempts
   * - ``retry_delay``
     - No
     - Delay between retries in seconds

**Response (200):**

.. code-block:: json

   {
     "name": "ml-pipeline",
     "jobs": [
       {
         "name": "preprocess",
         "job_id": null,
         "status": "UNKNOWN",
         "depends_on": [],
         "command": ["python", "preprocess.py"],
         "resources": {"nodes": 1, "gpus_per_node": null, "partition": null, "time_limit": null}
       }
     ]
   }

**Error responses:**

* ``409`` — Workflow with the same name already exists
* ``422`` — Validation error (invalid name, duplicate job names, dependency cycle, Pydantic validation failure)

Files
-----

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Endpoint
     - Description
   * - GET
     - ``/api/files/mounts``
     - List configured mount points for the current SSH profile
   * - GET
     - ``/api/files/browse``
     - Browse local filesystem under a mount's local root
   * - POST
     - ``/api/files/sync``
     - Sync a mount's local directory to the remote via rsync

GET /api/files/mounts
^^^^^^^^^^^^^^^^^^^^^

Returns the list of mount points from the current SSH profile. Only mount names and remote prefixes are returned; local paths are never exposed.

**Response:**

.. code-block:: json

   [
     {
       "name": "ml-project",
       "remote": "/home/researcher/ml-project"
     }
   ]

Returns an empty list if no SSH profile is configured or the profile has no mounts.

GET /api/files/browse
^^^^^^^^^^^^^^^^^^^^^

Browse directory contents under a mount's local root, returning entries with their corresponding remote paths.

**Query parameters:**

* ``mount`` (required) — Mount name (must match a configured mount)
* ``path`` (optional) — Relative path within the mount root (default: root directory)

**Response:**

.. code-block:: json

   {
     "entries": [
       {"name": "train.py", "type": "file", "size": 2048},
       {"name": "data", "type": "directory", "size": null},
       {"name": "latest", "type": "symlink", "size": null, "accessible": true}
     ],
     "remote_prefix": "/home/researcher/ml-project/src",
     "mount_name": "ml-project"
   }

**Entry types:** ``file``, ``directory``, ``symlink``. Symlinks include an ``accessible`` field indicating whether the link target is within the mount boundary.

.. warning::

   **Security:** The resolved path must stay within the mount's local root. Path traversal attempts (e.g., ``../../etc/passwd``) return ``403 Forbidden``. Symlinks pointing outside the mount boundary are marked ``accessible: false`` and cannot be followed. Local filesystem paths are never included in the response.

**Error responses:**

* ``400`` — Path is not a directory
* ``403`` — Path outside mount boundary or permission denied
* ``404`` — Mount not found, directory not found, or no SSH profile configured

POST /api/files/sync
^^^^^^^^^^^^^^^^^^^^

Sync a mount's local directory to the remote server via rsync.

**Request body:**

.. code-block:: json

   {"mount": "ml-project"}

**Response (200):**

.. code-block:: json

   {"status": "synced", "mount": "ml-project"}

**Error responses:**

* ``404`` — Mount not found
* ``502`` — rsync command failed
* ``503`` — No SSH profile configured or rsync not installed

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

* ``400`` — Invalid input (e.g., negative job ID, path is not a directory)
* ``403`` — Path outside mount boundary or permission denied
* ``404`` — Resource not found (job, workflow, mount, directory)
* ``409`` — Resource already exists (e.g., workflow with duplicate name)
* ``413`` — YAML content too large
* ``422`` — Validation error
* ``502`` — SLURM command or rsync command failed
* ``503`` — SSH connection not configured or rsync not installed

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
