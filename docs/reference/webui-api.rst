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
   * - DELETE
     - ``/api/workflows/{name}``
     - Delete a workflow YAML file
   * - POST
     - ``/api/workflows/{name}/run``
     - Run a workflow (sync mounts, submit jobs, monitor)
   * - GET
     - ``/api/workflows/runs``
     - List workflow run records
   * - GET
     - ``/api/workflows/runs/{run_id}``
     - Get run status with live job statuses
   * - POST
     - ``/api/workflows/runs/{run_id}/cancel``
     - Cancel all jobs in a run

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

DELETE /api/workflows/{name}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Delete a workflow YAML file from the workflow directory.

**Path parameters:**

* ``name`` — Workflow name (alphanumeric, hyphens, underscores)

**Response (200):**

.. code-block:: json

   {"status": "deleted", "name": "ml-pipeline"}

**Error responses:**

* ``404`` — Workflow not found
* ``422`` — Invalid workflow name

POST /api/workflows/{name}/run
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Run a workflow end-to-end: identify and sync referenced mounts, render SLURM scripts, submit jobs in topological order with ``--dependency`` flags, and start a background monitor that polls ``sacct`` every 10 seconds.

**Path parameters:**

* ``name`` — Workflow name

**Response (202):**

.. code-block:: json

   {
     "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
     "workflow_name": "ml-pipeline",
     "started_at": "2026-03-30T12:00:00+00:00",
     "completed_at": null,
     "status": "running",
     "job_ids": {
       "preprocess": "18500",
       "train": "18501",
       "evaluate": "18502"
     },
     "job_statuses": {
       "preprocess": "PENDING",
       "train": "PENDING",
       "evaluate": "PENDING"
     },
     "error": null
   }

The ``status`` field transitions through: ``syncing``, ``submitting``, ``running``, then a terminal state (``completed``, ``failed``, or ``cancelled``).

**Error responses:**

* ``404`` — Workflow not found
* ``422`` — Invalid workflow name
* ``500`` — Script rendering failed
* ``502`` — Mount sync failed or sbatch submission failed

GET /api/workflows/runs/{run_id}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Get the current status and job-level details for a single workflow run. Job statuses are updated by the background monitor every 10 seconds.

**Path parameters:**

* ``run_id`` — UUID of the run (returned by the ``POST /{name}/run`` endpoint)

**Response (200):**

.. code-block:: json

   {
     "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
     "workflow_name": "ml-pipeline",
     "started_at": "2026-03-30T12:00:00+00:00",
     "completed_at": null,
     "status": "running",
     "job_ids": {"preprocess": "18500", "train": "18501"},
     "job_statuses": {"preprocess": "COMPLETED", "train": "RUNNING"},
     "error": null
   }

**Error responses:**

* ``404`` — Run not found

POST /api/workflows/runs/{run_id}/cancel
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cancel all SLURM jobs associated with a workflow run. Each submitted job is cancelled via ``scancel``. The run status is set to ``cancelled``.

**Path parameters:**

* ``run_id`` — UUID of the run

**Response (200):**

.. code-block:: json

   {"status": "cancelled", "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}

If some jobs fail to cancel (e.g., already completed), the response includes a ``warnings`` array:

.. code-block:: json

   {
     "status": "cancelled",
     "run_id": "a1b2c3d4-...",
     "warnings": ["evaluate: Job 18502 not found"]
   }

**Error responses:**

* ``404`` — Run not found
* ``422`` — Run is already in a terminal state (completed, failed, or cancelled)

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
     - List configured mount points (name and remote only)
   * - GET
     - ``/api/files/mounts/config``
     - List mounts with full details (name, local, remote)
   * - POST
     - ``/api/files/mounts``
     - Add a mount to the current SSH profile
   * - DELETE
     - ``/api/files/mounts/{mount_name}``
     - Remove a mount from the current SSH profile
   * - GET
     - ``/api/files/browse``
     - Browse local filesystem under a mount's local root
   * - GET
     - ``/api/files/read``
     - Read text file contents from a mount
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

GET /api/files/mounts/config
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Returns all mounts with full details including local paths. Used by the mount management UI.

**Response:**

.. code-block:: json

   [
     {
       "name": "ml-project",
       "local": "/home/user/projects/ml-project",
       "remote": "/home/researcher/ml-project"
     }
   ]

Returns an empty list if no SSH profile is configured or the profile has no mounts.

POST /api/files/mounts
^^^^^^^^^^^^^^^^^^^^^^^

Add a new mount to the current SSH profile. The mount is persisted to the profile configuration file.

**Request body:**

.. code-block:: json

   {
     "name": "ml-project",
     "local": "/home/user/projects/ml-project",
     "remote": "/home/researcher/ml-project"
   }

**Response (200):**

.. code-block:: json

   {
     "name": "ml-project",
     "local": "/home/user/projects/ml-project",
     "remote": "/home/researcher/ml-project"
   }

**Error responses:**

* ``409`` — A mount with the same name already exists
* ``422`` — Validation error (missing required fields or invalid values)
* ``503`` — No SSH profile configured

DELETE /api/files/mounts/{mount_name}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Remove a mount from the current SSH profile.

**Path parameters:**

* ``mount_name`` — Mount name to remove

**Response (200):**

.. code-block:: json

   {"status": "deleted", "name": "ml-project"}

**Error responses:**

* ``404`` — Mount not found
* ``503`` — No SSH profile configured

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
       {"name": "latest", "type": "symlink", "size": null, "accessible": true, "target_kind": "directory"}
     ],
     "remote_prefix": "/home/researcher/ml-project/src",
     "mount_name": "ml-project"
   }

**Entry types:** ``file``, ``directory``, ``symlink``. Symlinks include an ``accessible`` field indicating whether the link target is within the mount boundary, and a ``target_kind`` field (``"file"`` or ``"directory"``) indicating the type of the link target. Accessible symlinks to directories can be expanded in the file explorer.

.. warning::

   **Security:** The resolved path must stay within the mount's local root. Path traversal attempts (e.g., ``../../etc/passwd``) return ``403 Forbidden``. Symlinks pointing outside the mount boundary are marked ``accessible: false`` and cannot be followed. Local filesystem paths are never included in the response.

**Error responses:**

* ``400`` — Path is not a directory
* ``403`` — Path outside mount boundary or permission denied
* ``404`` — Mount not found, directory not found, or no SSH profile configured

GET /api/files/read
^^^^^^^^^^^^^^^^^^^^

Read text file contents from a mount's local root. Used by the file explorer to preview scripts before submission.

**Query parameters:**

* ``mount`` (required) — Mount name
* ``path`` (required) — Relative path within the mount root

**Response:**

.. code-block:: json

   {
     "content": "#!/bin/bash\n#SBATCH --gpus=1\npython train.py",
     "path": "scripts/train.sh",
     "mount": "ml-project"
   }

**Error responses:**

* ``400`` — ``path`` parameter missing or path is not a file
* ``403`` — Path outside mount boundary
* ``404`` — File not found or no SSH profile configured
* ``413`` — File exceeds 1 MB size limit

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
* ``502`` — rsync command failed (includes missing rsync binary)
* ``503`` — No SSH profile configured

Config
------

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Endpoint
     - Description
   * - GET
     - ``/api/config``
     - Get current merged configuration
   * - PUT
     - ``/api/config``
     - Update user configuration
   * - GET
     - ``/api/config/paths``
     - List config file paths with existence status
   * - POST
     - ``/api/config/reset``
     - Reset user config to defaults
   * - GET
     - ``/api/config/ssh/profiles``
     - List SSH profiles and current active profile
   * - POST
     - ``/api/config/ssh/profiles``
     - Add a new SSH profile
   * - PUT
     - ``/api/config/ssh/profiles/{name}``
     - Update an existing SSH profile
   * - DELETE
     - ``/api/config/ssh/profiles/{name}``
     - Delete an SSH profile
   * - POST
     - ``/api/config/ssh/profiles/{name}/activate``
     - Set profile as current active
   * - POST
     - ``/api/config/ssh/profiles/{name}/mounts``
     - Add a mount to a profile
   * - DELETE
     - ``/api/config/ssh/profiles/{name}/mounts/{mount_name}``
     - Remove a mount from a profile
   * - GET
     - ``/api/config/env``
     - List active SRUNX_* environment variables
   * - GET
     - ``/api/config/projects``
     - List projects from current profile's mounts
   * - GET
     - ``/api/config/projects/{mount_name}``
     - Read project config (.srunx.json)
   * - PUT
     - ``/api/config/projects/{mount_name}``
     - Update project config
   * - POST
     - ``/api/config/projects/{mount_name}/init``
     - Initialize project config with example values

GET /api/config
^^^^^^^^^^^^^^^^

Returns the current merged configuration (system + user + project).

**Response:**

.. code-block:: json

   {
     "resources": {
       "nodes": 1,
       "gpus_per_node": 0,
       "ntasks_per_node": 1,
       "cpus_per_task": 1,
       "memory_per_node": null,
       "time_limit": null,
       "partition": null,
       "nodelist": null
     },
     "environment": {
       "conda": null,
       "venv": null,
       "container": null,
       "env_vars": {}
     },
     "notifications": {
       "slack_webhook_url": null
     },
     "log_dir": "logs",
     "work_dir": null
   }

PUT /api/config
^^^^^^^^^^^^^^^^

Validate and save configuration to the user config file (``~/.config/srunx/config.json``).

**Request body:** A full or partial ``SrunxConfig`` object (same shape as GET response).

**Response:** The updated merged configuration.

**Error responses:**

* ``422`` — Validation error
* ``500`` — Failed to write config file

GET /api/config/paths
^^^^^^^^^^^^^^^^^^^^^^

Returns all config file paths with their existence status and source label.

**Response:**

.. code-block:: json

   [
     {"path": "/etc/srunx/config.json", "exists": false, "source": "system"},
     {"path": "/home/user/.config/srunx/config.json", "exists": true, "source": "user"},
     {"path": ".srunx.json", "exists": false, "source": "project (.srunx.json)"},
     {"path": "srunx.json", "exists": false, "source": "project (srunx.json)"}
   ]

POST /api/config/reset
^^^^^^^^^^^^^^^^^^^^^^^

Reset user config to defaults. Overwrites ``~/.config/srunx/config.json`` with a fresh ``SrunxConfig``.

**Response:** The default configuration.

GET /api/config/ssh/profiles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Returns all SSH profiles and identifies the current active profile.

**Response:**

.. code-block:: json

   {
     "current": "dgx-server",
     "profiles": {
       "dgx-server": {
         "hostname": "dgx.example.com",
         "username": "researcher",
         "key_filename": "~/.ssh/id_ed25519",
         "port": 22,
         "description": "Main DGX cluster",
         "ssh_host": null,
         "proxy_jump": null,
         "mounts": [],
         "env_vars": {}
       }
     }
   }

POST /api/config/ssh/profiles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add a new SSH profile.

**Request body:**

.. code-block:: json

   {
     "name": "dgx-server",
     "hostname": "dgx.example.com",
     "username": "researcher",
     "key_filename": "~/.ssh/id_ed25519",
     "port": 22,
     "description": "Main DGX cluster",
     "ssh_host": null,
     "proxy_jump": null
   }

**Response:** The created profile object.

**Error responses:**

* ``409`` — Profile with the same name already exists

PUT /api/config/ssh/profiles/{name}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Update an existing SSH profile. Only valid ``ServerProfile`` fields are applied: ``hostname``, ``username``, ``key_filename``, ``port``, ``description``, ``ssh_host``, ``proxy_jump``, ``env_vars``.

**Request body:** Object with fields to update (partial update).

**Response:** The updated profile object.

**Error responses:**

* ``404`` — Profile not found

DELETE /api/config/ssh/profiles/{name}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Delete an SSH profile and all its mounts.

**Response:**

.. code-block:: json

   {"ok": true}

**Error responses:**

* ``404`` — Profile not found

POST /api/config/ssh/profiles/{name}/activate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set a profile as the current active profile.

**Response:**

.. code-block:: json

   {"ok": true}

**Error responses:**

* ``404`` — Profile not found

POST /api/config/ssh/profiles/{name}/mounts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add a mount point to a profile.

**Request body:**

.. code-block:: json

   {
     "name": "ml-project",
     "local": "/home/user/projects/ml-project",
     "remote": "/home/researcher/ml-project"
   }

**Response:** The created mount object.

**Error responses:**

* ``404`` — Profile not found
* ``422`` — Validation error

DELETE /api/config/ssh/profiles/{name}/mounts/{mount_name}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Remove a mount from a profile.

**Response:**

.. code-block:: json

   {"ok": true}

**Error responses:**

* ``404`` — Profile or mount not found

GET /api/config/env
^^^^^^^^^^^^^^^^^^^^

Returns all ``SRUNX_*`` and ``SLACK_WEBHOOK_URL`` environment variables currently set in the server process.

**Response:**

.. code-block:: json

   [
     {
       "name": "SRUNX_DEFAULT_PARTITION",
       "value": "gpu",
       "description": "Default SLURM partition"
     },
     {
       "name": "SRUNX_SSH_PROFILE",
       "value": "dgx-server",
       "description": "SSH profile for web server"
     }
   ]

Returns an empty list if no ``SRUNX_*`` variables are set.

GET /api/config/projects
^^^^^^^^^^^^^^^^^^^^^^^^^

List projects derived from the current SSH profile's mounts.

**Response:**

.. code-block:: json

   [
     {
       "mount_name": "ml-project",
       "local_path": "/home/user/projects/ml-project",
       "remote_path": "/home/researcher/ml-project",
       "config_exists": true,
       "config_path": "/home/user/projects/ml-project/.srunx.json"
     }
   ]

Returns an empty list if no SSH profile is active.

GET /api/config/projects/{mount_name}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Read ``.srunx.json`` from a mount's local directory.

**Response:**

.. code-block:: json

   {
     "mount_name": "ml-project",
     "local_path": "/home/user/projects/ml-project",
     "config_path": "/home/user/projects/ml-project/.srunx.json",
     "exists": true,
     "config": {
       "resources": {"gpus_per_node": 4, "time_limit": "8:00:00"},
       "environment": {"conda": "ml_env"}
     }
   }

**Error responses:**

* ``400`` — No active SSH profile
* ``404`` — Mount not found in active profile

PUT /api/config/projects/{mount_name}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Save ``.srunx.json`` to a mount's local directory.

**Request body:** A ``SrunxConfig`` object.

**Response:** The saved project config response.

**Error responses:**

* ``400`` — No active SSH profile
* ``404`` — Mount not found
* ``422`` — Validation error
* ``500`` — Failed to write file

POST /api/config/projects/{mount_name}/init
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Initialize ``.srunx.json`` with example values in a mount's local directory.

**Response:** The created project config response with example values.

**Error responses:**

* ``400`` — No active SSH profile
* ``404`` — Mount not found
* ``409`` — ``.srunx.json`` already exists

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
* ``404`` — Resource not found (job, workflow, mount, directory, run)
* ``409`` — Resource already exists (e.g., workflow or mount with duplicate name)
* ``413`` — YAML content too large
* ``422`` — Validation error or invalid state transition (e.g., cancelling an already-terminal run)
* ``500`` — Internal error (e.g., script rendering failure)
* ``502`` — SLURM command, rsync command, or sbatch submission failed
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
