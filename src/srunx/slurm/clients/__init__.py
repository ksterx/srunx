"""Concrete SLURM client implementations.

Each module here is a transport-specific implementation of the
:class:`~srunx.slurm.protocols.Client` Protocol:

- :mod:`srunx.slurm.clients.local` — :class:`LocalClient`, the
  in-process ``sbatch`` / ``squeue`` / ``scancel`` driver.
- :mod:`srunx.slurm.clients.ssh` — :class:`SlurmSSHClient`, the
  SSH-transport sibling.

Public consumers should generally import :class:`Slurm` from
:mod:`srunx.slurm.local` (the legacy wrapper that wires DB recording +
callbacks) rather than reaching into this subpackage directly.
"""
