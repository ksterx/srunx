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
MODULE_LAYERS: dict[str, str] = {
    # common
    "common": "common",
    # domain
    "domain": "domain",
    # runtime
    "runtime": "runtime",
    # slurm
    "slurm": "slurm",
    "transport": "slurm",
    "utils": "slurm",
    # observability
    "observability": "observability",
    "callbacks": "observability",
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
        "``NotificationWatchCallback`` calls into CLI-side ``attach_notification_watch``. "
        "Resolved by moving that helper into observability."
    ),
    # observability -> runtime (active-watch poller imports sweep state service).
    ("observability", "runtime"): (
        "Poller imports ``runtime.sweep.state_service`` directly; resolved when "
        "state_service's pure state ops move under observability."
    ),
    # runtime -> observability (runner / workflow / sweep still invoke Callback directly).
    ("runtime", "callbacks"): (
        "``runtime.workflow.runner`` + ``runtime.sweep.orchestrator`` still call "
        "``Callback`` methods directly. Resolved by extending the sink pattern."
    ),
    # runtime -> observability (runner / workflow / sweep write DB + invoke notifications).
    ("runtime", "observability"): (
        "``runtime.workflow.{runner,transitions}`` write ``workflow_runs`` rows and "
        "``runtime.sweep.{aggregator,orchestrator,reconciler,state_service}`` invoke "
        "``NotificationService`` + storage repositories directly. Resolved by "
        "extending the sink pattern into the workflow runner + sweep orchestrator."
    ),
    # slurm -> observability (Slurm wrapper wires default sink chain; SSH adapter writes DB).
    ("slurm", "observability"): (
        "``slurm.local.Slurm`` wrapper composes ``CallbackSink`` + ``DBRecorderSink`` "
        "into the default sink chain; ``slurm.ssh`` writes ``observability.storage`` "
        "rows directly. Sink pattern extension to SSH is the follow-up."
    ),
    ("slurm", "callbacks"): (
        "SSH adapter / executor still import legacy ``Callback``. "
        "Sink pattern extended to SSH in a follow-up."
    ),
    (
        "transport",
        "callbacks",
    ): "Transport registry still consumes Callback; sink follow-up.",
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
