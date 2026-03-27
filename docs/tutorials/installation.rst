Installation
============

Requirements
------------

- Python 3.12 or higher
- SLURM workload manager
- Access to a SLURM cluster

Installing from PyPI
---------------------

.. code-block:: bash

   pip install srunx

Installing from Source
----------------------

.. code-block:: bash

   git clone https://github.com/ksterx/srunx.git
   cd srunx
   uv sync
   uv run pip install -e .

Development Installation
------------------------

For development, you'll need to install the development dependencies:

.. code-block:: bash

   git clone https://github.com/ksterx/srunx.git
   cd srunx
   uv sync --group dev

Verification
------------

To verify the installation:

.. code-block:: bash

   srunx --help

This should display the help message for the srunx command-line interface.