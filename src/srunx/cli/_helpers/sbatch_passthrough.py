"""sbatch(1) option table + argv rewrite for ``srunx sbatch``.

Reference: SLURM 24.11 ``sbatch(1)``.

Known limitation (not fixed here): array job IDs (``123_4``) are parsed
with ``int()`` in ``srunx.slurm.clients._ssh_queries`` — ``int("123_4")``
is 1234 (Python's numeric-literal digit separator), so array jobs are
mis-tracked by ``squeue``/``history`` once ``--array`` is submitted
through this passthrough. Accepting ``--array`` here does not fix that
downstream bug; it is tracked separately.

Design (see plan.md for the full rationale):

* :data:`SBATCH_OPTIONS` — sbatch options srunx does not model itself,
  accepted verbatim. ``srunx`` options always win (see
  :func:`rewrite_sbatch_argv`), so this table is disjoint from srunx's own
  spellings by construction (tested in
  ``tests/cli/_helpers/test_sbatch_passthrough.py``).
* :data:`REJECTED_SBATCH_OPTIONS` — sbatch options rejected outright
  because they would corrupt job-ID parsing (``--test-only``), silently
  swallow output (``--quiet``/``-Q``), or bypass srunx's own script/wrap
  handling (``--wrap``). See module-level rationale table below.
* :func:`rewrite_sbatch_argv` — pure ``argv -> argv`` rewrite run *before*
  Click parses ``sbatch``'s arguments (:class:`SbatchCommand`). Recognized
  sbatch spellings are normalized into ``--sbatch-arg=<token>``; srunx's
  own spellings and anything unrecognized pass through untouched.
* :func:`validate_passthrough_args` — the single point (called from
  ``sbatch()`` after Click parsing) that enforces the token-format rule
  (R2.6) and the reject list (R2.5) against the final ``--sbatch-arg``
  list, whether those tokens came from the rewrite above or were typed by
  hand.

Long-option abbreviations (``getopt_long`` style, e.g. ``--arr=1-10`` for
``--array=1-10``) are intentionally **not** accepted by
:func:`rewrite_sbatch_argv` — an abbreviated spelling that doesn't match
any entry in :data:`SBATCH_OPTIONS` / :data:`REJECTED_SBATCH_OPTIONS`
falls through untouched and Click reports "No such option" (simple and
safe; use ``--sbatch-arg=--arr=1-10`` to opt in to an abbreviation
explicitly). :func:`validate_passthrough_args` *does* honor abbreviations,
but only for the reject list (real sbatch would resolve a rejected
option's abbreviation the same way, so refusing to recognize it here would
let a rejected option slip through under a shortened spelling).

Table scope (R3.1b) — explicitly excluded:

* ``--overlap`` — ``srun``-only; ``sbatch`` has no such option.
* ``--container`` — srunx already uses this spelling for its own container
  configuration (``--container=<image or key=value list>``); accepting
  the sbatch spelling too would collide.
* ``-W`` / ``--wait`` — srunx's own ``--wait`` blocks the CLI on job
  completion; sbatch's ``-W``/``--wait`` has the scheduler itself block
  until the job completes. Different meanings, so the sbatch spelling is
  not exposed (a bare ``-W`` on the srunx CLI is just unknown -> exit 2).
* ``--wait-all-nodes`` is a *different* sbatch option (delays job start
  until all allocated nodes are booted) and is included.
* ``-v`` / ``--verbose`` is srunx's own verbose flag (own always wins —
  see :func:`rewrite_sbatch_argv`), so ``srunx sbatch -v`` controls
  srunx's own output, not sbatch's ``-v``/``--verbose``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import click
import typer

SBATCH_ARG_OPT = "--sbatch-arg"

# long -> (short | None, takes_value)
#
# ``takes_value`` is True only for options sbatch requires a mandatory
# argument for. Options with an *optional* argument (``--exclusive[=...]``,
# ``--nice[=...]``, ``--propagate[=...]``, ``--get-user-env[=...]``,
# ``--no-kill[=...]``) are recorded as False: their value can only be
# supplied via the ``--opt=value`` form, matching getopt_long's
# optional-argument behaviour and preventing e.g. ``--exclusive script.sh``
# from swallowing the script path as its value.
SBATCH_OPTIONS: dict[str, tuple[str | None, bool]] = {
    "--array": ("-a", True),
    "--account": ("-A", True),
    "--dependency": ("-d", True),
    "--qos": ("-q", True),
    "--output": ("-o", True),
    "--error": ("-e", True),
    "--exclusive": (None, False),
    "--begin": ("-b", True),
    "--deadline": (None, True),
    "--mail-type": (None, True),
    "--mail-user": (None, True),
    "--ntasks": ("-n", True),
    "--mem-per-cpu": (None, True),
    "--mem-per-gpu": (None, True),
    "--constraint": ("-C", True),
    "--exclude": ("-x", True),
    "--reservation": (None, True),
    "--comment": (None, True),
    "--requeue": (None, False),
    "--no-requeue": (None, False),
    "--hold": ("-H", False),
    "--nice": (None, False),
    "--priority": (None, True),
    "--signal": (None, True),
    "--switches": (None, True),
    "--distribution": ("-m", True),
    "--overcommit": ("-O", False),
    "--oversubscribe": ("-s", False),
    "--contiguous": (None, False),
    "--threads-per-core": (None, True),
    "--sockets-per-node": (None, True),
    "--cores-per-socket": (None, True),
    "--ntasks-per-core": (None, True),
    "--ntasks-per-socket": (None, True),
    "--gpus": ("-G", True),
    "--gpus-per-task": (None, True),
    "--gpu-bind": (None, True),
    "--gpu-freq": (None, True),
    "--cpus-per-gpu": (None, True),
    "--mincpus": (None, True),
    "--tmp": (None, True),
    "--clusters": ("-M", True),
    "--licenses": ("-L", True),
    "--network": (None, True),
    "--propagate": (None, False),
    "--kill-on-invalid-dep": (None, True),
    "--open-mode": (None, True),
    "--wckey": (None, True),
    "--uid": (None, True),
    "--gid": (None, True),
    "--acctg-freq": (None, True),
    "--batch": (None, True),
    "--delay-boot": (None, True),
    "--extra-node-info": ("-B", True),
    "--get-user-env": (None, False),
    "--ignore-pbs": (None, False),
    "--no-kill": ("-k", False),
    "--power": (None, True),
    "--spread-job": (None, False),
    "--thread-spec": (None, True),
    "--use-min-nodes": (None, False),
    "--wait-all-nodes": (None, True),
    "--input": ("-i", True),
    "--core-spec": ("-S", True),
    "--nodefile": ("-F", True),
    "--time-min": (None, True),
    "--hint": (None, True),
    "--mem-bind": (None, True),
    "--cpu-freq": (None, True),
    "--export-file": (None, True),
    "--prefer": (None, True),
    "--reboot": (None, False),
    "--tres-per-task": (None, True),
    "--tres-bind": (None, True),
}

# long -> short | None. Rejected outright by validate_passthrough_args
# (R2.5) — see the module docstring for why.
REJECTED_SBATCH_OPTIONS: dict[str, str | None] = {
    "--parsable": None,
    "--test-only": None,
    "--quiet": "-Q",
    "--wrap": None,
    "--help": "-h",
    "--usage": None,
    "--version": "-V",
}

# Every real sbatch(1) short option that takes a mandatory argument, used
# only to decompose a raw short-option cluster (e.g. ``-JQ``) into its
# individual letters for the reject check in
# ``_short_cluster_contains_rejected``. This intentionally includes short
# spellings srunx models as "own" (``-J``/``--job-name``,
# ``-N``/``--nodes``, ...) as well as passthrough-table shorts, because a
# raw cluster passed manually via ``--sbatch-arg=-JQ`` must be decomposed
# using real sbatch semantics regardless of which side of the own/table
# split a given letter falls on.
_OWN_SHORT_TAKES_VALUE: dict[str, bool] = {
    "D": True,
    "J": True,
    "N": True,
    "c": True,
    "p": True,
    "t": True,
    "w": True,
    "Q": False,
    "h": False,
    "v": False,
}


def _all_short_takes_value() -> dict[str, bool]:
    merged = dict(_OWN_SHORT_TAKES_VALUE)
    for _long, (short, takes_value) in SBATCH_OPTIONS.items():
        if short:
            merged[short.lstrip("-")] = takes_value
    return merged


_ALL_SHORT_TAKES_VALUE: dict[str, bool] = _all_short_takes_value()
_REJECTED_SHORT_LETTERS: frozenset[str] = frozenset(
    short.lstrip("-") for short in REJECTED_SBATCH_OPTIONS.values() if short
)


@dataclass(frozen=True)
class OwnOptionSpec:
    """A srunx-native option's spellings + whether it consumes a value."""

    spellings: frozenset[str]
    takes_value: bool


def own_option_specs(params: Iterable[click.Parameter]) -> tuple[OwnOptionSpec, ...]:
    """Derive :class:`OwnOptionSpec` entries from a command's Click params.

    Only :class:`click.Option` instances contribute (the positional
    ``script`` :class:`click.Argument` is excluded — it's not a flag
    spelling to protect). ``takes_value`` mirrors Click's own notion of
    "does this option consume the next token": true unless the option is
    a flag or a counter.
    """
    specs: list[OwnOptionSpec] = []
    for param in params:
        if not isinstance(param, click.Option):
            continue
        spellings = frozenset((*param.opts, *param.secondary_opts))
        takes_value = not param.is_flag and not getattr(param, "count", False)
        specs.append(OwnOptionSpec(spellings=spellings, takes_value=takes_value))
    return tuple(specs)


def _short_to_long_table() -> dict[str, tuple[str, bool]]:
    """``-x`` -> (long name, takes_value) for both tables (no collisions)."""
    mapping: dict[str, tuple[str, bool]] = {}
    for long, (short, takes_value) in SBATCH_OPTIONS.items():
        if short:
            mapping[short] = (long, takes_value)
    for long, short in REJECTED_SBATCH_OPTIONS.items():
        if short:
            mapping[short] = (long, False)
    return mapping


def rewrite_sbatch_argv(args: Sequence[str], own: Sequence[OwnOptionSpec]) -> list[str]:
    """Rewrite ``sbatch`` argv so recognized native options reach sbatch.

    Pure function: no side effects, no exceptions raised for unrecognized
    tokens (those are left untouched for Click to reject as usual). See
    the module docstring for the design and R3.2 for the token
    classification rules this implements.
    """
    own_lookup: dict[str, bool] = {}
    for spec in own:
        for spelling in spec.spellings:
            own_lookup[spelling] = spec.takes_value

    short_to_long = _short_to_long_table()

    out: list[str] = []
    i = 0
    n = len(args)
    seen_dashdash = False

    while i < n:
        tok = args[i]

        if seen_dashdash:
            out.append(tok)
            i += 1
            continue

        if tok == "--":
            seen_dashdash = True
            out.append(tok)
            i += 1
            continue

        if tok.startswith("--"):
            head, _eq, inline_value = tok.partition("=")
            has_inline = "=" in tok

            if head in own_lookup:
                out.append(tok)
                i += 1
                if not has_inline and own_lookup[head] and i < n:
                    out.append(args[i])
                    i += 1
                continue

            in_table = head in SBATCH_OPTIONS
            in_rejected = head in REJECTED_SBATCH_OPTIONS
            if in_table or in_rejected:
                takes_value = SBATCH_OPTIONS[head][1] if in_table else False
                if has_inline:
                    out.append(f"{SBATCH_ARG_OPT}={head}={inline_value}")
                    i += 1
                elif takes_value:
                    if i + 1 < n:
                        out.append(f"{SBATCH_ARG_OPT}={head}={args[i + 1]}")
                        i += 2
                    else:
                        # Trailing value-less occurrence: leave it bare so
                        # downstream validation / sbatch itself reports the
                        # missing-value error (I4-2).
                        out.append(f"{SBATCH_ARG_OPT}={head}")
                        i += 1
                else:
                    out.append(f"{SBATCH_ARG_OPT}={head}")
                    i += 1
                continue

            out.append(tok)
            i += 1
            continue

        if tok.startswith("-") and tok not in ("-", "--"):
            body = tok[1:]
            pos = 0
            body_len = len(body)
            consumed_next = False
            while pos < body_len:
                c = body[pos]
                short = f"-{c}"
                if short in own_lookup:
                    takes_value = own_lookup[short]
                    out.append(short)
                    pos += 1
                    if takes_value:
                        if pos < body_len:
                            out.append(body[pos:])
                            pos = body_len
                        elif i + 1 < n:
                            out.append(args[i + 1])
                            consumed_next = True
                        break
                    continue

                long_name, takes_value = short_to_long.get(short, (None, False))
                if long_name is None:
                    # Unknown short char: leave the remainder untouched
                    # for Click to reject.
                    out.append(f"-{body[pos:]}")
                    pos = body_len
                    break
                if pos + 1 < body_len:
                    out.append(f"{SBATCH_ARG_OPT}={long_name}={body[pos + 1 :]}")
                    pos = body_len
                elif takes_value:
                    if i + 1 < n:
                        out.append(f"{SBATCH_ARG_OPT}={long_name}={args[i + 1]}")
                        consumed_next = True
                    else:
                        out.append(f"{SBATCH_ARG_OPT}={long_name}")
                    pos = body_len
                else:
                    out.append(f"{SBATCH_ARG_OPT}={long_name}")
                    pos = body_len
                break

            i += 2 if consumed_next else 1
            continue

        out.append(tok)
        i += 1

    return out


def _rejected_reason(head: str) -> str | None:
    """Return the rejected long name ``head`` matches (exact or true
    getopt_long-style prefix), or ``None``."""
    for rejected_long in REJECTED_SBATCH_OPTIONS:
        if rejected_long.startswith(head):
            return rejected_long
    return None


def _short_cluster_rejected_letter(body: str) -> str | None:
    """Scan a raw short-option cluster body (no leading ``-``) letter by
    letter, stopping at the first value-taking option (the remainder is
    that option's attached value, not further option letters). Returns the
    first rejected letter found, or ``None``."""
    for c in body:
        if c in _REJECTED_SHORT_LETTERS:
            return c
        if _ALL_SHORT_TAKES_VALUE.get(c, False):
            break
    return None


def validate_passthrough_args(tokens: Sequence[str]) -> list[str]:
    """Validate ``--sbatch-arg`` values: format (R2.6) then reject list (R2.5).

    Called once, from ``sbatch()`` after Click parsing, against the final
    ``--sbatch-arg`` list — whichever combination of rewrite-generated and
    hand-typed tokens it contains. Raises :class:`typer.BadParameter` on
    the first violation found.
    """
    for tok in tokens:
        if tok in ("-", "--") or not tok.startswith("-"):
            raise typer.BadParameter(
                f"--sbatch-arg value must be a single sbatch option token "
                f"starting with '-' (got {tok!r}).",
                param_hint=SBATCH_ARG_OPT,
            )

        head = tok.split("=", 1)[0]
        if head.startswith("--"):
            rejected = _rejected_reason(head)
            if rejected is not None:
                raise typer.BadParameter(
                    f"sbatch option {head!r} is not allowed via --sbatch-arg "
                    f"(resolves to rejected option {rejected!r}).",
                    param_hint=SBATCH_ARG_OPT,
                )
        else:
            rejected_letter = _short_cluster_rejected_letter(head[1:])
            if rejected_letter is not None:
                raise typer.BadParameter(
                    f"sbatch option {tok!r} is not allowed via --sbatch-arg "
                    f"(contains rejected short option '-{rejected_letter}').",
                    param_hint=SBATCH_ARG_OPT,
                )

    return list(tokens)


class SbatchCommand(typer.core.TyperCommand):
    """``sbatch`` Click command with the native-option-passthrough rewrite.

    Overrides :meth:`parse_args` — the earliest Click hook that sees the
    raw argv — to rewrite recognized sbatch spellings into
    ``--sbatch-arg=<token>`` *before* Click's own parser runs. This keeps
    ``main()`` and ``CliRunner.invoke`` on the exact same code path (R3.4).
    """

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        own = own_option_specs(self.get_params(ctx))
        return super().parse_args(ctx, rewrite_sbatch_argv(args, own))
