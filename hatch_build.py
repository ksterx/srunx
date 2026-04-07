"""Custom Hatch build hook to build the frontend before packaging."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict) -> None:
        root = Path(self.root)
        frontend_dir = root / "src" / "srunx" / "web" / "frontend"
        dist_dir = frontend_dir / "dist"

        if not dist_dir.exists():
            if shutil.which("npm") is None:
                self.app.display_warning(
                    "npm not found — skipping frontend build. "
                    "The wheel will not include the Web UI."
                )
                return

            self.app.display("Building frontend...")
            subprocess.check_call(["npm", "ci"], cwd=str(frontend_dir))
            subprocess.check_call(["npm", "run", "build"], cwd=str(frontend_dir))

        if dist_dir.exists():
            build_data["force_include"][str(dist_dir)] = "srunx/web/frontend/dist"
