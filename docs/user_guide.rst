User Guide
==========

This comprehensive guide covers all aspects of using srunx for SLURM job management.

Core Concepts
-------------

Jobs
~~~~

A job in srunx represents a computational task that will be executed on a SLURM cluster. Jobs are defined with:

- **Command**: The command to execute
- **Resources**: CPU, memory, GPU, and time requirements
- **Environment**: Conda, virtual environment, or container setup
- **Dependencies**: Job dependencies for workflow orchestration

Resources
~~~~~~~~~

srunx provides fine-grained control over resource allocation:

- ``--nodes``: Number of compute nodes
- ``--tasks-per-node``: Tasks per node
- ``--cpus-per-task``: CPUs per task
- ``--gpus-per-node``: GPUs per node
- ``--memory-per-node``: Memory per node
- ``--time-limit``: Maximum execution time

Environment Management
~~~~~~~~~~~~~~~~~~~~~~

srunx supports three environment types:

1. **Conda**: ``--conda env_name``
2. **Virtual Environment**: ``--venv /path/to/venv``
3. **Singularity (SQSH)**: ``--sqsh /path/to/container.sqsh``

Command Line Interface
----------------------

Job Submission
~~~~~~~~~~~~~~

Basic submission:

.. code-block:: bash

   srunx submit <command>

With resource specification:

.. code-block:: bash

   srunx submit python train.py \
     --name "training_job" \
     --nodes 2 \
     --gpus-per-node 2 \
     --memory-per-node "64GB" \
     --time-limit "8:00:00" \
     --conda ml_env

Job Monitoring
~~~~~~~~~~~~~~

Check status:

.. code-block:: bash

   srunx status 12345

List all jobs:

.. code-block:: bash

   srunx list

List with filters:

.. code-block:: bash

   srunx list --state RUNNING
   srunx list --name training

Job Control
~~~~~~~~~~~

Cancel a job:

.. code-block:: bash

   srunx cancel 12345

Monitor job until completion:

.. code-block:: bash

   srunx submit python script.py --wait

Workflows
---------

Workflow Definition
~~~~~~~~~~~~~~~~~~~

Workflows are defined in YAML format with jobs and dependencies:

.. code-block:: yaml

   name: data_pipeline
   description: "Complete data processing pipeline"

   jobs:
     - name: download_data
       command: ["python", "download.py"]
       nodes: 1
       memory_per_node: "8GB"

     - name: preprocess
       command: ["python", "preprocess.py", "--input", "data/raw"]
       depends_on: [download_data]
       nodes: 1
       cpus_per_task: 4

     - name: train_model
       command: ["python", "train.py"]
       depends_on: [preprocess]
       nodes: 2
       gpus_per_node: 1
       conda: pytorch_env
       time_limit: "12:00:00"

     - name: evaluate
       command: ["python", "evaluate.py"]
       depends_on: [train_model]
       nodes: 1
       async: true

Workflow Execution
~~~~~~~~~~~~~~~~~~

Run a workflow:

.. code-block:: bash

   srunx flow run pipeline.yaml

Validate workflow syntax:

.. code-block:: bash

   srunx flow validate pipeline.yaml

Run with custom parameters:

.. code-block:: bash

   srunx flow run pipeline.yaml --dry-run

Advanced Features
-----------------

Callbacks and Notifications
~~~~~~~~~~~~~~~~~~~~~~~~~~~

srunx supports job completion callbacks, including Slack notifications:

.. code-block:: python

   from srunx.callbacks import SlackCallback
   from srunx.client import Slurm

   callback = SlackCallback(webhook_url="https://hooks.slack.com/...")
   client = Slurm()

   job = client.submit(
       command=["python", "train.py"],
       name="training_job",
       callback=callback
   )

Template Customization
~~~~~~~~~~~~~~~~~~~~~~

srunx uses Jinja2 templates for SLURM script generation. You can customize templates by:

1. Copying default templates from ``srunx/templates/``
2. Modifying them for your needs
3. Specifying custom template path

Programmatic Usage
~~~~~~~~~~~~~~~~~~

Use srunx from Python code:

.. code-block:: python

   from srunx.client import Slurm
   from srunx.models import Job, JobResource, JobEnvironment

   # Create client
   client = Slurm()

   # Define job
   job = Job(
       name="my_job",
       command=["python", "script.py"],
       resources=JobResource(
           nodes=2,
           gpus_per_node=1,
           memory_per_node="32GB",
           time_limit="4:00:00"
       ),
       environment=JobEnvironment(conda="ml_env")
   )

   # Submit job
   job_id = client.submit(job)

   # Monitor job
   status = client.retrieve(job_id)
   print(f"Job {job_id} status: {status.state}")

Best Practices
--------------

Resource Planning
~~~~~~~~~~~~~~~~~

1. **Right-size your jobs**: Don't over-allocate resources
2. **Use time limits**: Prevent runaway jobs
3. **Monitor resource usage**: Optimize for future jobs

Environment Management
~~~~~~~~~~~~~~~~~~~~~~

1. **Use environment isolation**: Conda, venv, or containers
2. **Pin dependencies**: Ensure reproducibility
3. **Test environments**: Validate before large runs

Workflow Design
~~~~~~~~~~~~~~~

1. **Break down jobs**: Smaller, focused jobs are easier to debug
2. **Use dependencies wisely**: Minimize blocking dependencies
3. **Handle failures**: Design for partial workflow recovery

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

**Job fails to start**
  - Check resource availability
  - Verify environment exists
  - Review SLURM script syntax

**Workflow hangs**
  - Check for circular dependencies
  - Verify all dependencies are satisfiable
  - Review job logs

**Environment errors**
  - Ensure conda/venv paths are correct
  - Check environment activation
  - Verify package availability

Debug Mode
~~~~~~~~~~

Enable debug logging:

.. code-block:: bash

   export SRUNX_LOG_LEVEL=DEBUG
   srunx submit python script.py

View generated SLURM scripts:

.. code-block:: bash

   srunx submit --dry-run python script.py
