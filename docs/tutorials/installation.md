---
description: Install srunx with uv or pip. Requires Python 3.12+ and access to a SLURM cluster, either locally or via SSH.
---

# Installation

## Requirements

- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Access to a SLURM cluster (local or via SSH)

## Installing with uv (recommended)

``` bash
uv add srunx
```

The Web UI is included in the base install — no extras required.

## Installing with pip

``` bash
pip install srunx
```

## Installing from Source

``` bash
git clone https://github.com/ksterx/srunx.git
cd srunx
uv sync
```

## Development Installation

For development, install with the dev dependency group:

``` bash
git clone https://github.com/ksterx/srunx.git
cd srunx
uv sync --group dev --all-extras
```

## Verification

To verify the installation:

``` bash
srunx --help
```

This should display the help message for the srunx command-line interface.
