Quick Start
===========

This guide will help you get started with srunx quickly.

Basic Job Submission
---------------------

Submit a simple Python script:

.. code-block:: bash

   srunx submit python my_script.py

Submit with specific resources:

.. code-block:: bash

   srunx submit python train.py --name ml_job --gpus-per-node 1 --nodes 2

Submit with conda environment:

.. code-block:: bash

   srunx submit python process.py --conda ml_env --memory-per-node 32GB

Job Management
--------------

Check job status:

.. code-block:: bash

   srunx status <job_id>

List your jobs:

.. code-block:: bash

   srunx list

Cancel a job:

.. code-block:: bash

   srunx cancel <job_id>

Workflow Example
----------------

Create a workflow YAML file (``workflow.yaml``):

.. code-block:: yaml

   name: ml_pipeline
   jobs:
     - name: preprocess
       command: ["python", "preprocess.py"]
       resources:
         nodes: 1

     - name: train
       command: ["python", "train.py"]
       depends_on: [preprocess]
       resources:
         gpus_per_node: 1
         conda: ml_env
         memory_per_node: "32GB"
         time_limit: "4:00:00"

     - name: evaluate
       command: ["python", "evaluate.py"]
       depends_on: [train]

Run the workflow:

.. code-block:: bash

   srunx flow run workflow.yaml

Validate a workflow:

.. code-block:: bash

   srunx flow validate workflow.yaml

Environment Setup
-----------------

srunx supports multiple environment types:

Conda Environment
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   srunx submit python script.py --conda my_env

Python Virtual Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   srunx submit python script.py --venv /path/to/venv

Singularity Container (SQSH)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   srunx submit python script.py --sqsh /path/to/container.sqsh

Next Steps
----------

- Read the :doc:`user_guide` for detailed usage instructions
- Check the :doc:`api` for programmatic usage
- Explore :doc:`workflows` for complex job orchestration
