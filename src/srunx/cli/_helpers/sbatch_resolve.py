"""Merge config defaults with a script's ``#SBATCH`` directives (design §6).

``srunx sbatch script.sh`` must not override what the script itself
declares. Values from config layers 2–6 (system / user / cluster /
project defaults) are forwarded to ``sbatch`` only for options the script
does not set and the user did not type; built-in defaults (layer 1) are
never forwarded — Slurm already has its own.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from srunx.cli._helpers.sbatch_helpers import last_option_value, script_sbatch_tokens

# The #SBATCH options srunx understands: canonical long name → short form.
DIRECTIVES: dict[str, str | None] = {
    "--nodes": "-N",
    "--ntasks": "-n",
    "--ntasks-per-node": None,
    "--cpus-per-task": "-c",
    "--gpus-per-node": None,
    "--gres": None,
    "--mem": None,
    "--time": "-t",
    "--partition": "-p",
    "--account": "-A",
    "--qos": "-q",
    "--job-name": "-J",
    "--output": "-o",
    "--error": "-e",
    "--chdir": "-D",
    "--nodelist": "-w",
    "--clusters": "-M",
    # Alternative spellings of resources srunx configures; recognised so an
    # inherited default never contradicts them.
    "--mem-per-cpu": None,
    "--mem-per-gpu": None,
    "--cpus-per-gpu": None,
    "--gpus": "-G",
    "--gpus-per-task": None,
    "--gpus-per-socket": None,
}

# Config key (dotted, as in SrunxConfig.sources) → sbatch option.
CONFIG_OPTIONS: dict[str, str] = {
    "resources.nodes": "--nodes",
    "resources.gpus_per_node": "--gpus-per-node",
    "resources.ntasks_per_node": "--ntasks-per-node",
    "resources.cpus_per_task": "--cpus-per-task",
    "resources.memory_per_node": "--mem",
    "resources.time_limit": "--time",
    "resources.nodelist": "--nodelist",
    "resources.partition": "--partition",
    "work_dir": "--chdir",
}

# A script option that already covers a config key even though the option
# differs (``--gres=gpu:4`` decides GPUs just like ``--gpus-per-node``).
_COVERED_BY: dict[str, tuple[str, ...]] = {
    "--gpus-per-node": (
        "--gpus-per-node",
        "--gres",
        "--gpus",
        "--gpus-per-task",
        "--gpus-per-socket",
    ),
    "--mem": ("--mem", "--mem-per-cpu", "--mem-per-gpu"),
    "--cpus-per-task": ("--cpus-per-task", "--cpus-per-gpu"),
}


def _known_long_options() -> frozenset[str]:
    from srunx.cli._helpers.sbatch_passthrough import (
        REJECTED_SBATCH_OPTIONS,
        SBATCH_OPTIONS,
    )

    return (
        frozenset(DIRECTIVES)
        | frozenset(SBATCH_OPTIONS)
        | frozenset(REJECTED_SBATCH_OPTIONS)
    )


def canonical_option(name: str) -> str:
    """Expand an unambiguous sbatch long-option abbreviation (``--part`` →
    ``--partition``), like sbatch's ``getopt_long``. Others are returned as-is.
    """
    if not name.startswith("--"):
        return name
    known = _known_long_options()
    if name in known:
        return name
    matches = [o for o in known if o.startswith(name)]
    return matches[0] if len(matches) == 1 else name


def _expand_short_bundle(tok: str) -> list[str]:
    """``-vp gpu`` → ``-v -p`` (+ value token kept by the caller);
    ``-vpgpu`` → ``-v -p gpu``. Mirrors getopt's short-option bundling."""
    from srunx.cli._helpers.sbatch_passthrough import _ALL_SHORT_TAKES_VALUE

    out: list[str] = []
    body = tok[1:]
    for i, letter in enumerate(body):
        out.append(f"-{letter}")
        if _ALL_SHORT_TAKES_VALUE.get(letter, False):
            rest = body[i + 1 :]
            if rest:
                out.append(rest)
            break
    return out


def canonicalize_tokens(tokens: Sequence[str]) -> list[str]:
    """Expand long abbreviations and short-option bundles."""
    out: list[str] = []
    for tok in tokens:
        if tok.startswith("--"):
            head, sep, rest = tok.partition("=")
            out.append(canonical_option(head) + sep + rest)
        elif tok.startswith("-") and len(tok) > 2:
            out.extend(_expand_short_bundle(tok))
        else:
            out.append(tok)
    return out


def translate_local_paths(args: Sequence[str], mounts: Sequence[Any]) -> list[str]:
    """Rewrite absolute local paths under a mount to their remote location.

    Applied to config-derived ``--chdir`` / ``--output`` / ``--error``
    values before they are sent to a remote ``sbatch``.
    """
    from pathlib import Path

    out: list[str] = []
    for arg in args:
        head, sep, value = arg.partition("=")
        if sep and head in ("--chdir", "--output", "--error") and value.startswith("/"):
            local = Path(value)
            best = None
            for m in mounts:
                root = Path(m.local)
                if local == root or root in local.parents:
                    if best is None or len(str(root)) > len(str(best[0])):
                        best = (root, m.remote)
            if best is not None:
                rel = local.relative_to(best[0])
                remote = best[1].rstrip("/")
                value = remote if str(rel) == "." else f"{remote}/{rel}"
        out.append(head + sep + value)
    return out


def parse_directives(script_text: str) -> dict[str, str]:
    """Return ``{canonical option: value}`` for the understood ``#SBATCH`` options.

    The last occurrence wins, like Slurm. Unambiguous long-option
    abbreviations are expanded first. Unknown options are ignored here and
    left for sbatch to interpret.
    """
    tokens = canonicalize_tokens(
        [t for line in script_sbatch_tokens(script_text) for t in line]
    )
    found: dict[str, str] = {}
    for long, short in DIRECTIVES.items():
        value = last_option_value(tokens, short or "\0", long)
        if value is not None:
            found[long] = value
    return found


def has_clusters_option(tokens: Sequence[str]) -> bool:
    """True when *tokens* carry sbatch ``-M`` / ``--clusters`` in any spelling.

    Deliberately broad: any long prefix of ``--clusters`` from ``--cl`` on
    (even if sbatch might call it ambiguous), and ``M`` anywhere in a short
    option bundle before the first value-taking letter (``-vMother``).
    """
    from srunx.cli._helpers.sbatch_passthrough import _ALL_SHORT_TAKES_VALUE

    for tok in tokens:
        if tok.startswith("--"):
            name = tok.split("=", 1)[0]
            if len(name) >= 4 and "--clusters".startswith(name):
                return True
            continue
        if tok.startswith("-") and len(tok) > 1:
            for letter in tok[1:]:
                if letter == "M":
                    return True
                if _ALL_SHORT_TAKES_VALUE.get(letter, False):
                    break
    return False


@dataclass
class ScriptMerge:
    """Result of combining config defaults with a script's directives."""

    args: list[str] = field(default_factory=list)
    forwarded: dict[str, tuple[str, str]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def _config_value(config: object, dotted: str) -> object:
    node = config
    for part in dotted.split("."):
        node = getattr(node, part)
    return node


def merge_script_defaults(
    config: object,
    directives: dict[str, str],
    *,
    explicit_options: set[str],
) -> ScriptMerge:
    """Forward config values the script and the command line leave unset.

    Args:
        config: The effective :class:`~srunx.common.config.SrunxConfig`
            (its ``sources`` tell built-in values apart from configured ones).
        directives: Output of :func:`parse_directives`.
        explicit_options: sbatch options the user typed (they always win).
    """
    sources: dict[str, str] = getattr(config, "sources", {})
    explicit_options = {canonical_option(o) for o in explicit_options}
    result = ScriptMerge()
    for key, option in CONFIG_OPTIONS.items():
        source = sources.get(key)
        if source is None:
            continue  # built-in default: never forwarded
        value = _config_value(config, key)
        if value is None or value == "":
            continue
        covering = _COVERED_BY.get(option, (option,))
        script_value = next((directives[o] for o in covering if o in directives), None)
        if script_value is not None:
            if "[clusters." in source and str(value) != script_value:
                result.warnings.append(
                    f"script sets {option}={script_value} but {source} "
                    f"sets {key} = {value!r}; the script wins"
                )
            continue
        if any(o in explicit_options for o in covering):
            continue
        result.args.append(f"{option}={value}")
        result.forwarded[option] = (str(value), source)

    log_source = sources.get("log_dir")
    if (
        log_source is not None
        and "--output" not in directives
        and "--error" not in directives
        and "--output" not in explicit_options
    ):
        log_dir = getattr(config, "log_dir", None)
        if log_dir:
            pattern = f"{log_dir}/%x_%j.log"
            result.args += [f"--output={pattern}", f"--error={pattern}"]
            result.forwarded["--output"] = (pattern, log_source)
    return result
