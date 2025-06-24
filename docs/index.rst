srunx documentation
===================

srunx is a powerful Python library for managing SLURM jobs and workflows. It provides a simple command-line interface and Python API for submitting, monitoring, and orchestrating computational jobs on HPC clusters.

Features
--------

* **Simple Job Submission**: Submit jobs with intuitive command-line interface
* **Resource Management**: Fine-grained control over compute resources
* **Environment Support**: Conda, virtual environments, and Singularity containers
* **Workflow Orchestration**: YAML-based workflow definition with dependency management
* **Monitoring and Callbacks**: Real-time job monitoring with notification support
* **Template System**: Flexible SLURM script generation with Jinja2 templates

Quick Example
-------------

Submit a simple job:

.. code-block:: bash

   srunx submit python train.py --gpus-per-node 2 --conda ml_env

Define a workflow:

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
         time_limit: "8:00:00"

.. toctree::
   :maxdepth: 2
   :caption: User Guide:

   installation
   quickstart
   user_guide
   workflows

.. toctree::
   :maxdepth: 2
   :caption: Reference:

   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
