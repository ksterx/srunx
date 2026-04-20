Installation
============

Requirements
------------

- Python 3.12 or higher
- `uv <https://docs.astral.sh/uv/>`_ (recommended) or pip
- Access to a SLURM cluster (local or via SSH)

Installing with uv (recommended)
----------------------------------

.. code-block:: bash

   uv add srunx

The Web UI is included in the base install — no extras required.

Installing with pip
--------------------

.. code-block:: bash

   pip install srunx

Installing from Source
----------------------

.. code-block:: bash

   git clone https://github.com/ksterx/srunx.git
   cd srunx
   uv sync

Development Installation
------------------------

For development, install with the dev dependency group:

.. code-block:: bash

   git clone https://github.com/ksterx/srunx.git
   cd srunx
   uv sync --group dev --all-extras

Verification
------------

To verify the installation:

.. code-block:: bash

   srunx --help

This should display the help message for the srunx command-line interface.