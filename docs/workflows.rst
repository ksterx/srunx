Workflows
=========

srunx provides a powerful workflow system for orchestrating complex multi-step computational pipelines on SLURM clusters.

Overview
--------

Workflows in srunx are defined using YAML files that specify:

- **Jobs**: Individual computational steps
- **Dependencies**: Execution order and prerequisites
- **Resources**: Computational requirements for each task
- **Environments**: Software environments for execution

Workflow Definition
-------------------

Basic Structure
~~~~~~~~~~~~~~~

.. code-block:: yaml

   name: workflow_name
   description: "Optional workflow description"

   jobs:
     - name: task1
       command: ["python", "script1.py"]
       # ... task configuration

     - name: task2
       command: ["python", "script2.py"]
       depends_on: [task1]
       # ... task configuration

Task Configuration
~~~~~~~~~~~~~~~~~~

Each task supports the following configuration options:

**Command and Environment**

.. code-block:: yaml

   - name: my_task
     command: ["python", "train.py", "--epochs", "100"]
     conda: ml_environment
     # OR
     venv: /path/to/virtualenv
     # OR
     sqsh: /path/to/container.sqsh

**Resource Allocation**

.. code-block:: yaml

   - name: gpu_task
     command: ["python", "gpu_training.py"]
     nodes: 2
     tasks_per_node: 1
     cpus_per_task: 8
     gpus_per_node: 2
     memory_per_node: "64GB"
     time_limit: "12:00:00"

**Dependencies**

.. code-block:: yaml

   - name: dependent_task
     command: ["python", "process.py"]
     depends_on: [preprocess, download]
     async: false  # Wait for dependencies (default)

**Job Naming and Organization**

.. code-block:: yaml

   - name: organized_task
     job_name: "custom_slurm_job_name"
     partition: gpu
     account: my_account

Dependencies
------------

Linear Dependencies
~~~~~~~~~~~~~~~~~~~

Simple sequential execution:

.. code-block:: yaml

   name: linear_pipeline
   jobs:
     - name: step1
       command: ["python", "step1.py"]

     - name: step2
       command: ["python", "step2.py"]
       depends_on: [step1]

     - name: step3
       command: ["python", "step3.py"]
       depends_on: [step2]

Parallel Dependencies
~~~~~~~~~~~~~~~~~~~~~

Multiple jobsdepending on the same prerequisite:

.. code-block:: yaml

   name: parallel_pipeline
   jobs:
     - name: preprocess
       command: ["python", "preprocess.py"]

     - name: train_model_a
       command: ["python", "train_a.py"]
       depends_on: [preprocess]

     - name: train_model_b
       command: ["python", "train_b.py"]
       depends_on: [preprocess]

     - name: ensemble
       command: ["python", "ensemble.py"]
       depends_on: [train_model_a, train_model_b]

Complex Dependencies
~~~~~~~~~~~~~~~~~~~~

Advanced dependency patterns:

.. code-block:: yaml

   name: complex_pipeline
   jobs:
     - name: data_download
       command: ["python", "download.py"]

     - name: data_validation
       command: ["python", "validate.py"]
       depends_on: [data_download]

     - name: feature_engineering
       command: ["python", "features.py"]
       depends_on: [data_validation]

     - name: model_training
       command: ["python", "train.py"]
       depends_on: [feature_engineering]

     - name: model_evaluation
       command: ["python", "evaluate.py"]
       depends_on: [model_training]

     - name: report_generation
       command: ["python", "report.py"]
       depends_on: [model_evaluation, data_validation]

Workflow Examples
-----------------

Machine Learning Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   name: ml_pipeline
   description: "Complete machine learning training pipeline"

   jobs:
     - name: data_preprocessing
       command: ["python", "preprocess.py", "--input", "raw_data/"]
       nodes: 1
       cpus_per_task: 4
       memory_per_node: "16GB"
       time_limit: "2:00:00"

     - name: feature_selection
       command: ["python", "feature_selection.py"]
       depends_on: [data_preprocessing]
       nodes: 1
       cpus_per_task: 8
       memory_per_node: "32GB"

     - name: hyperparameter_tuning
       command: ["python", "hyperopt.py", "--trials", "100"]
       depends_on: [feature_selection]
       nodes: 4
       gpus_per_node: 1
       conda: pytorch_env
       time_limit: "8:00:00"

     - name: final_training
       command: ["python", "train_final.py"]
       depends_on: [hyperparameter_tuning]
       nodes: 2
       gpus_per_node: 2
       conda: pytorch_env
       time_limit: "12:00:00"

     - name: model_validation
       command: ["python", "validate.py"]
       depends_on: [final_training]
       nodes: 1
       gpus_per_node: 1
       conda: pytorch_env

     - name: deployment_prep
       command: ["python", "prepare_deployment.py"]
       depends_on: [model_validation]
       nodes: 1
       async: true

Bioinformatics Pipeline
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   name: genomics_pipeline
   description: "RNA-seq analysis pipeline"

   jobs:
     - name: quality_control
       command: ["fastqc", "*.fastq.gz"]
       nodes: 1
       cpus_per_task: 16

     - name: trimming
       command: ["trim_galore", "--paired", "sample_R1.fastq.gz", "sample_R2.fastq.gz"]
       depends_on: [quality_control]
       nodes: 1
       cpus_per_task: 8

     - name: alignment
       command: ["STAR", "--runThreadN", "32", "--genomeDir", "genome_index"]
       depends_on: [trimming]
       nodes: 1
       cpus_per_task: 32
       memory_per_node: "64GB"
       time_limit: "4:00:00"

     - name: quantification
       command: ["featureCounts", "-T", "16", "-a", "annotation.gtf"]
       depends_on: [alignment]
       nodes: 1
       cpus_per_task: 16

     - name: differential_expression
       command: ["Rscript", "deseq2_analysis.R"]
       depends_on: [quantification]
       nodes: 1
       cpus_per_task: 4
       conda: r_env

Workflow Execution
------------------

Running Workflows
~~~~~~~~~~~~~~~~~

Execute a workflow:

.. code-block:: bash

   srunx flow run pipeline.yaml

Validate workflow before execution:

.. code-block:: bash

   srunx flow validate pipeline.yaml

Dry run (show what would be executed):

.. code-block:: bash

   srunx flow run pipeline.yaml --dry-run

Monitoring Workflows
~~~~~~~~~~~~~~~~~~~~

srunx provides built-in workflow monitoring:

- **Progress tracking**: See which jobs are running/completed
- **Dependency resolution**: Automatic job scheduling based on dependencies
- **Error handling**: Failed jobs don't block independent jobs
- **Logging**: Comprehensive logging of workflow execution

Workflow Management
-------------------

Error Handling
~~~~~~~~~~~~~~

When a job fails:

1. **Dependent jobs are blocked**: Jobs depending on failed job won't run
2. **Independent jobs continue**: Other jobs in the workflow continue
3. **Detailed logging**: Error information is captured and logged
4. **Manual intervention**: You can fix issues and restart failed jobs

Restart and Recovery
~~~~~~~~~~~~~~~~~~~~

srunx supports workflow restart capabilities:

.. code-block:: bash

   # Resume from a specific job
   srunx flow run pipeline.yaml --start-from job_name

   # Skip completed jobs
   srunx flow run pipeline.yaml --resume

Best Practices
--------------

Workflow Design
~~~~~~~~~~~~~~~

1. **Modular jobs**: Keep jobs focused and independent when possible
2. **Resource optimization**: Right-size resources for each job
3. **Checkpointing**: Save intermediate results for recovery
4. **Testing**: Test individual jobs before full workflow execution

Dependency Management
~~~~~~~~~~~~~~~~~~~~~

1. **Minimize dependencies**: Reduce blocking relationships
2. **Parallel execution**: Design for maximum parallelism
3. **Data dependencies**: Ensure data flow matches job dependencies
4. **Avoid cycles**: srunx will detect and reject circular dependencies

Resource Planning
~~~~~~~~~~~~~~~~~

1. **Job profiling**: Understand resource needs for each job
2. **Queue management**: Consider cluster queue policies
3. **Time limits**: Set appropriate time limits for each job
4. **Resource sharing**: Balance resource allocation across jobs

Advanced Features
-----------------

Conditional Execution
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   - name: conditional_job
     command: ["python", "conditional.py"]
     depends_on: [prerequisite]
     condition: "file_exists('trigger.txt')"

Parameter Substitution
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   name: parameterized_workflow
   parameters:
     dataset: "experiment_1"
     epochs: 100

   jobs:
     - name: training
       command: ["python", "train.py", "--dataset", "{{dataset}}", "--epochs", "{{epochs}}"]

Workflow Templates
~~~~~~~~~~~~~~~~~~

Create reusable workflow templates:

.. code-block:: yaml

   name: ml_template
   template: true

   jobs:
     - name: preprocess
       command: ["python", "preprocess.py", "--input", "{{input_path}}"]

     - name: train
       command: ["python", "train.py", "--model", "{{model_type}}"]
       depends_on: [preprocess]
