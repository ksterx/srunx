"""Job-oriented CLI commands.

Each subcommand lives in its own submodule (``sbatch``, ``squeue``,
``scancel``, ``sinfo``, ``gpus``, ``tail``). They are wired into the
Typer app in :mod:`srunx.cli.main`.

This ``__init__`` deliberately does **not** re-export the command
functions. If it did, ``from .sbatch import sbatch`` would rebind the
package attribute ``sbatch`` from the submodule to the function,
breaking ``import srunx.cli.commands.jobs.sbatch as jobs_module``
(used by tests that monkeypatch module-level attributes like
``get_config``).
"""
