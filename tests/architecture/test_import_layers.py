"""AST-based import-layer guard (Phase 1 of #156, issue #157).

Scans every ``src/srunx/**/*.py`` file statically and asserts that cross-layer
imports respect the target 6-layer architecture:

    interfaces -> runtime / slurm / observability / integrations -> domain -> common

Files have not moved yet (Phases 2-8 do that). Each top-level module is mapped
to the layer it will belong to post-migration via ``MODULE_LAYERS``. Imports
that already violate the target rule today are listed in ``KNOWN_VIOLATIONS``
with a pointer to the phase that resolves them. The test fails when:

* a *new* violation appears (import crosses a forbidden boundary, not whitelisted)
* a whitelist entry is *stale* (the import no longer exists — phase landed, remove it)

This forces the whitelist to shrink as phases complete.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "srunx"

# Map each top-level package/module under ``srunx/`` to its TARGET layer.
# Source files move in later phases; the layer name is stable.
MODULE_LAYERS: dict[str, str] = {
    # common — canonical package landed in Phase 8e (#164); the
    # top-level shims (config / exceptions / logging / _version) stay
    # mapped for back-compat and route into srunx.common.* via sys.modules
    # aliasing.
    "_version": "common",
    "common": "common",
    "config": "common",
    "exceptions": "common",
    "logging": "common",
    # domain (Phase 3 / #159 splits models.py into domain/)
    "domain": "domain",
    "models": "domain",
    # runtime (Phase 2 / #158 + Phase 7 / #163)
    "runtime": "runtime",
    "rendering": "runtime",  # shim — canonical home: srunx.runtime.rendering
    "runner": "runtime",  # shim — canonical home: srunx.runtime.workflow.runner
    "template": "runtime",  # shim — canonical home: srunx.runtime.templates
    "sweep": "runtime",
    "security": "runtime",
    # slurm (Phase 4 / #160 + Phase 6 / #162)
    "client": "slurm",
    "client_protocol": "slurm",
    "slurm": "slurm",
    "transport": "slurm",
    "utils": "slurm",
    # observability (Phase 5 / #161 + Phase 8 / #164)
    "observability": "observability",
    "callbacks": "observability",
    "db": "observability",
    "formatters": "observability",
    "monitor": "observability",
    "notifications": "observability",
    "pollers": "observability",
    # integrations
    "containers": "integrations",
    "ssh": "integrations",
    "sync": "integrations",
    # interfaces
    "cli": "interfaces",
    "mcp": "interfaces",
    "web": "interfaces",
}

# Allowed cross-layer edges per target architecture (#156).
ALLOWED: dict[str, set[str]] = {
    "common": {"domain"},
    "domain": {"common"},
    "integrations": {"common", "domain"},
    "runtime": {"common", "domain", "integrations", "slurm"},
    "slurm": {"common", "domain", "integrations", "runtime"},
    "observability": {"common", "domain", "integrations", "slurm"},
    "interfaces": {
        "common",
        "domain",
        "integrations",
        "slurm",
        "runtime",
        "observability",
    },
}

# Cross-layer imports that exist today and are resolved in a later phase.
# Key: (source_top_level, imported_top_level). Value: human-readable resolution pointer.
# Each entry must reference the phase / issue that removes it. When the phase
# lands, the corresponding import disappears and the test flags the entry as
# stale — forcing this whitelist to shrink.
KNOWN_VIOLATIONS: dict[tuple[str, str], str] = {
    # observability -> interfaces (callbacks reaches back into CLI-side helper).
    ("callbacks", "cli"): (
        "Phase 5 (#161) / Phase 8 (#164): callbacks absorbed into observability; "
        "CLI-side notification helper inverted so interfaces -> observability."
    ),
    # slurm -> observability (legacy ``Slurm`` shim wires the default sink chain).
    ("client", "callbacks"): (
        "Shim-only: ``srunx.client.Slurm`` imports ``Callback`` for its legacy "
        "``callbacks=`` type hint. Removed when the shim is deleted."
    ),
    ("client", "observability"): (
        "Shim-only: ``srunx.client.Slurm`` composes ``CallbackSink`` + "
        "``DBRecorderSink`` into the default sink chain so legacy "
        "``Slurm(callbacks=[...])`` keeps working. Removed when the shim "
        "is deleted."
    ),
    # domain -> runtime (models.py is a backward-compat shim re-exporting renderers).
    ("models", "runtime"): (
        "Shim re-exports from :mod:`srunx.runtime.rendering`. Remove when "
        "external callers migrate and models.py is deleted (post #156)."
    ),
    # observability -> runtime (poller reaches into sweep state service).
    ("pollers", "sweep"): (
        "Phase 8 (#164): sweep state_service split — pure state ops move under observability."
    ),
    # runtime -> observability (runner mixes DAG execution with DB + callbacks).
    ("runtime", "callbacks"): (
        "``runtime.workflow.runner`` still invokes ``Callback`` methods directly. "
        "Resolved by extending Phase 5's sink pattern into the workflow runner "
        "(follow-up)."
    ),
    ("runtime", "db"): (
        "``runtime.workflow.runner`` / ``runtime.workflow.transitions`` write "
        "``workflow_runs`` rows directly. Resolved by extending Phase 5's sink "
        "pattern into the workflow runner (follow-up)."
    ),
    # runtime -> observability (sweep orchestration writes DB + notifications directly).
    (
        "sweep",
        "callbacks",
    ): "Phase 7/8 (#163/#164): sweep orchestration goes through sink.",
    ("sweep", "db"): (
        "Phase 7/8 (#163/#164): sweep orchestrator/reconciler/state_service "
        "use storage facade."
    ),
    ("sweep", "notifications"): "Phase 8 (#164): notifications accessed via sink.",
    # slurm -> observability (transport registry still imports callbacks).
    ("transport", "callbacks"): "Phase 5 (#161): transport registry uses sink.",
    # slurm -> observability (SSH adapter still writes DB + invokes callbacks
    # directly; sink extraction comes in a Phase 5 follow-up for SSH).
    ("slurm", "callbacks"): (
        "SSH adapter / executor still import legacy ``Callback``. "
        "Phase 5 sink pattern extended to SSH in a follow-up."
    ),
    ("slurm", "db"): (
        "SSH adapter writes ``srunx.db`` rows directly. "
        "Phase 5 sink pattern extended to SSH in a follow-up."
    ),
    # slurm -> interfaces (SSH adapter consumes cli-layer submission plan).
    ("slurm", "cli"): (
        "Phase 8 (#164): ``cli/submission_plan.py`` moves to ``runtime/`` "
        "(interfaces-agnostic submission plan)."
    ),
}


@dataclass(frozen=True)
class ImportEdge:
    source_file: Path
    source_module: str
    imported_module: str


def _module_name(path: Path) -> str:
    rel = path.relative_to(SRC_ROOT.parent)
    parts = list(rel.with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _top_level(dotted: str) -> str:
    parts = dotted.split(".")
    if len(parts) < 2 or parts[0] != "srunx":
        return ""
    return parts[1]


def _layer_of(module: str) -> str | None:
    return MODULE_LAYERS.get(_top_level(module))


def _resolve_relative(
    source_module: str, is_package: bool, level: int, module: str | None
) -> str:
    """Resolve a PEP 328 relative import to its absolute dotted path.

    - ``is_package`` is True when the source file is ``__init__.py`` (so the
      source module is itself a package); otherwise the current package is the
      module's parent.
    - Level 1 refers to the current package; level 2 to its parent; etc.
    """
    parts = source_module.split(".")
    if not is_package:
        parts = parts[:-1]
    if level > 1:
        drop = level - 1
        parts = parts[:-drop] if drop <= len(parts) else []
    if module:
        parts.append(module)
    return ".".join(parts)


def _collect_edges() -> list[ImportEdge]:
    edges: list[ImportEdge] = []
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        source_module = _module_name(path)
        if not source_module.startswith("srunx"):
            continue
        is_package = path.name == "__init__.py"
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.level > 0:
                    target = _resolve_relative(
                        source_module, is_package, node.level, node.module
                    )
                else:
                    target = node.module or ""
                if target.startswith("srunx"):
                    edges.append(ImportEdge(path, source_module, target))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("srunx"):
                        edges.append(ImportEdge(path, source_module, alias.name))
    return edges


def _find_unmapped_top_levels(edges: list[ImportEdge]) -> set[str]:
    seen: set[str] = set()
    for edge in edges:
        for mod in (edge.source_module, edge.imported_module):
            top = _top_level(mod)
            if top and top not in MODULE_LAYERS:
                seen.add(top)
    return seen


def test_every_top_level_module_is_mapped() -> None:
    edges = _collect_edges()
    missing = _find_unmapped_top_levels(edges)
    assert not missing, (
        f"New top-level srunx.* module(s) not assigned to a layer: {sorted(missing)}. "
        "Add them to MODULE_LAYERS."
    )


def test_import_layer_boundaries() -> None:
    edges = _collect_edges()
    violations: list[tuple[ImportEdge, str, str]] = []
    for edge in edges:
        src_layer = _layer_of(edge.source_module)
        dst_layer = _layer_of(edge.imported_module)
        if src_layer is None or dst_layer is None:
            continue
        if src_layer == dst_layer:
            continue
        if dst_layer not in ALLOWED.get(src_layer, set()):
            violations.append((edge, src_layer, dst_layer))

    unresolved: list[tuple[ImportEdge, str, str]] = []
    fired_keys: set[tuple[str, str]] = set()
    for edge, sl, dl in violations:
        src_top = _top_level(edge.source_module)
        dst_top = _top_level(edge.imported_module)
        key = (src_top, dst_top)
        if key in KNOWN_VIOLATIONS:
            fired_keys.add(key)
        else:
            unresolved.append((edge, sl, dl))

    stale = set(KNOWN_VIOLATIONS) - fired_keys

    messages: list[str] = []
    if unresolved:
        messages.append(
            "NEW cross-layer violations (fix the import, or add to KNOWN_VIOLATIONS "
            "with a resolution pointer):"
        )
        repo_root = SRC_ROOT.parent.parent
        for edge, sl, dl in unresolved:
            rel = edge.source_file.relative_to(repo_root)
            messages.append(
                f"  {rel}: srunx.{_top_level(edge.source_module)} ({sl}) "
                f"-> {edge.imported_module} ({dl})"
            )
    if stale:
        messages.append(
            "STALE whitelist entries (phase landed, remove from KNOWN_VIOLATIONS):"
        )
        for key in sorted(stale):
            messages.append(f"  {key[0]} -> {key[1]}: {KNOWN_VIOLATIONS[key]}")

    assert not messages, "\n" + "\n".join(messages)
