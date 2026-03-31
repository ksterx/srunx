srunx documentation
===================

srunx is a powerful Python library for managing SLURM jobs and workflows. It provides a simple command-line interface and Python API for submitting, monitoring, and orchestrating computational jobs on HPC clusters.

Features
--------

* **Simple Job Submission**: Submit jobs with intuitive command-line interface
* **Resource Management**: Fine-grained control over compute resources
* **Environment Support**: Conda, virtual environments, and containers (Apptainer/Singularity, Pyxis)
* **Workflow Orchestration**: YAML-based workflow definition with dependency management
* **Monitoring and Callbacks**: Real-time job monitoring with notification support
* **Project Sync**: rsync-based project directory synchronization to remote SLURM servers
* **Template System**: Flexible SLURM script generation with Jinja2 templates
* **Web UI**: Browser-based dashboard for job management, resource monitoring, and workflow DAG visualization

Quick Example
-------------

Submit a simple job:

.. code-block:: bash

   srunx submit python train.py --gpus-per-node 2 --conda ml_env

Submit with an Apptainer container:

.. code-block:: bash

   srunx submit python train.py --container "runtime=apptainer,image=pytorch.sif,nv=true"

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
         memory_per_node: "32GB"
         time_limit: "8:00:00"
       environment:
         conda: ml_env

.. toctree::
   :maxdepth: 2
   :caption: Tutorials

   tutorials/installation
   tutorials/quickstart
   tutorials/webui

.. toctree::
   :maxdepth: 2
   :caption: How-to Guides

   how-to/user_guide
   how-to/workflows
   how-to/monitoring
   how-to/sync
   how-to/webui
   how-to/settings
   how-to/explorer

.. toctree::
   :maxdepth: 2
   :caption: Reference

   reference/api
   reference/webui-api

.. toctree::
   :maxdepth: 2
   :caption: Explanation

   explanation/architecture

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
