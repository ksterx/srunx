"""Tests for ``srunx.cli._helpers.sbatch_passthrough``.

Covers the pure ``argv -> argv`` rewrite (:func:`rewrite_sbatch_argv`),
the reject-list + format validator (:func:`validate_passthrough_args`),
and the two table/own-set invariants (R3.3 / AC9).
"""

from __future__ import annotations

import pytest
import typer

from srunx.cli._helpers.sbatch_passthrough import (
    REJECTED_SBATCH_OPTIONS,
    SBATCH_OPTIONS,
    own_option_specs,
    rewrite_sbatch_argv,
    validate_passthrough_args,
)
from srunx.cli.main import app


def _own():
    """Real own-option specs for the live ``sbatch`` command, including the
    ``-h``/``--help`` option Click auto-adds (only visible once the
    context is built the way real CLI dispatch builds it: sub-command
    context parented under the root, so ``help_option_names`` from the
    root's ``context_settings`` is inherited)."""
    root = typer.main.get_command(app)
    root_ctx = root.make_context("srunx", ["sbatch"], resilient_parsing=True)
    cmd = root.commands["sbatch"]
    sub_ctx = cmd.make_context("sbatch", [], parent=root_ctx, resilient_parsing=True)
    return own_option_specs(cmd.get_params(sub_ctx))


class TestRewriteFiveFormsEquivalent:
    @pytest.mark.parametrize(
        "argv",
        [
            ["--array=1-10", "s.sh"],
            ["--array", "1-10", "s.sh"],
            ["s.sh", "--array", "1-10"],
            ["-a", "1-10", "s.sh"],
            ["-a1-10", "s.sh"],
        ],
    )
    def test_rewrite_five_forms_equivalent(self, argv):
        result = rewrite_sbatch_argv(argv, _own())
        passthrough = [t for t in result if t.startswith("--sbatch-arg=")]
        assert passthrough == ["--sbatch-arg=--array=1-10"]
        assert "s.sh" in result


def test_rewrite_mixed_with_own_flags():
    result = rewrite_sbatch_argv(["-t", "5:00", "--array", "1-10", "s.sh"], _own())
    assert result == ["-t", "5:00", "--sbatch-arg=--array=1-10", "s.sh"]


class TestRewriteOwnOptionWins:
    def test_wrap_then_array_own_wraps_first(self):
        # own's --wrap takes a value; the following --array must be
        # consumed AS --wrap's value, not reinterpreted as a table option.
        result = rewrite_sbatch_argv(["--wrap", "--array"], _own())
        assert result == ["--wrap", "--array"]

    def test_dash_n4_not_rewritten(self):
        result = rewrite_sbatch_argv(["-N4", "s.sh"], _own())
        assert result == ["-N", "4", "s.sh"]

    def test_sync_no_sync_not_rewritten(self):
        assert rewrite_sbatch_argv(["--sync"], _own()) == ["--sync"]
        assert rewrite_sbatch_argv(["--no-sync"], _own()) == ["--no-sync"]

    def test_vj_cluster_own_wins_entirely(self):
        # -v (own flag) + -J (own, takes value) clustered: Click would
        # read "-v"=True, "-J"'s value = the *next* token. Must not let
        # the table steal --array here.
        result = rewrite_sbatch_argv(["-vJ", "--array", "script.sh"], _own())
        assert result == ["-v", "-J", "--array", "script.sh"]

    def test_vn_space_form_own_wins(self):
        result = rewrite_sbatch_argv(["-vN", "4", "script.sh"], _own())
        assert result == ["-v", "-N", "4", "script.sh"]


def test_rewrite_sbatch_arg_value_not_reinterpreted():
    # --sbatch-arg is own and takes a value; its value must pass through
    # untouched even though it looks like a table spelling.
    result = rewrite_sbatch_argv(["--sbatch-arg", "--array=1-10"], _own())
    assert result == ["--sbatch-arg", "--array=1-10"]


def test_rewrite_double_dash_passthrough():
    result = rewrite_sbatch_argv(["--", "--array", "1-10"], _own())
    assert result == ["--", "--array", "1-10"]


def test_rewrite_unknown_token_untouched():
    result = rewrite_sbatch_argv(["--profil", "dgx", "s.sh"], _own())
    assert result == ["--profil", "dgx", "s.sh"]


def test_rewrite_optional_arg_option_takes_equals_only():
    # --exclusive has an optional argument; space form must NOT swallow
    # the script path as its value.
    result = rewrite_sbatch_argv(["--exclusive", "s.sh"], _own())
    assert result == ["--sbatch-arg=--exclusive", "s.sh"]


def test_rewrite_required_value_may_start_with_dash():
    result = rewrite_sbatch_argv(["--begin", "-5", "s.sh"], _own())
    assert result == ["--sbatch-arg=--begin=-5", "s.sh"]


def test_rewrite_trailing_value_option_left_bare():
    result = rewrite_sbatch_argv(["s.sh", "--array"], _own())
    assert result == ["s.sh", "--sbatch-arg=--array"]


class TestRejectExactAndPrefix:
    @pytest.mark.parametrize(
        "token",
        [
            "--parsable",
            "--pars",
            "--test-only",
            "--test-onl",
            "--te",
            "--quiet",
            "--qu",
            "-Q",
            "--wrap=x",
            "--wr",
            "--help",
            "-h",
            "--usage",
            "--version",
            "-V",
        ],
    )
    def test_reject_exact_and_prefix(self, token):
        with pytest.raises(typer.BadParameter):
            validate_passthrough_args([token])


class TestRejectNonOptionToken:
    @pytest.mark.parametrize("token", ["foo", "-", "--"])
    def test_reject_non_option_token(self, token):
        with pytest.raises(typer.BadParameter):
            validate_passthrough_args([token])


def test_validate_accepts_normal_tokens():
    tokens = ["--array=1-10", "--exclusive", "--qos=normal"]
    assert validate_passthrough_args(tokens) == tokens


def test_short_cluster_rejects_hq_and_qh_but_not_jq():
    with pytest.raises(typer.BadParameter):
        validate_passthrough_args(["-HQ"])
    with pytest.raises(typer.BadParameter):
        validate_passthrough_args(["-QH"])
    # -J takes a value; "Q" here is -J's value, not a real -Q flag.
    assert validate_passthrough_args(["-JQ"]) == ["-JQ"]


class TestTableInvariants:
    def test_table_disjoint_from_own_options(self):
        own_spellings: set[str] = set()
        for spec in _own():
            own_spellings.update(spec.spellings)

        table_spellings: set[str] = set()
        for long, (short, _tv) in SBATCH_OPTIONS.items():
            table_spellings.add(long)
            if short:
                table_spellings.add(short)

        assert table_spellings & own_spellings == set()

    def test_rejected_own_intersection_is_exact(self):
        own_spellings: set[str] = set()
        for spec in _own():
            own_spellings.update(spec.spellings)

        rejected_spellings: set[str] = set()
        for long, short in REJECTED_SBATCH_OPTIONS.items():
            rejected_spellings.add(long)
            if short:
                rejected_spellings.add(short)

        assert rejected_spellings & own_spellings == {
            "--quiet",
            "-Q",
            "--wrap",
            "--help",
            "-h",
        }
