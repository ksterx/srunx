"""A ``RemoteShell`` whose "remote" is ``sh -s`` on this machine (snapshot tests)."""

from __future__ import annotations

import os
import subprocess

# tests/conftest.py replaces ``subprocess.run`` for every test; keep the real one.
REAL_RUN = subprocess.run


class LocalShell:
    """Runs scripts through a local ``/bin/sh -s``; paths are local paths."""

    def __init__(self, path_env: str) -> None:
        self.env = {**os.environ, "PATH": path_env}

    def run_script(
        self, script: str, *, timeout: float | None = None
    ) -> subprocess.CompletedProcess[str]:
        return REAL_RUN(
            ["/bin/sh", "-s"],
            input=script,
            capture_output=True,
            text=True,
            timeout=timeout or 60,
            env=self.env,
            check=False,
        )

    def ssh_command(self) -> list[str]:
        return ["ssh"]

    def remote_spec(self, path: str) -> str:
        return path
