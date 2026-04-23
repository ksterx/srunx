"""Runtime layer — submission path: rendering, templates, workflow, sweep.

See tracking issue #156 for the target architecture. This package is the
canonical home for everything that turns a declarative Job/Workflow into
an sbatch submission; it may reach into domain, slurm, integrations, and
support but never the interfaces or observability layers.
"""
