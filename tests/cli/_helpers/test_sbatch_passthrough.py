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
    """The rewrite leaves a bare value-taking option alone; rejecting it is
    :func:`validate_passthrough_args`' job (see
    ``TestRejectBareValueTakingOption``)."""
    result = rewrite_sbatch_argv(["s.sh", "--array"], _own())
    assert result == ["s.sh", "--sbatch-arg=--array"]


class TestRejectBareValueTakingOption:
    """A value-taking option left bare must be rejected before submission.

    Letting it through is not a harmless "sbatch will complain" case:
    sbatch consumes the following script path as the option's value, is
    left with no batch script, and falls back to reading one from stdin —
    hanging an interactive shell or submitting whatever is piped in.
    """

    @pytest.mark.parametrize("tok", ["--array", "--comment", "--account"])
    def test_bare_value_option_rejected(self, tok):
        with pytest.raises(typer.BadParameter, match="requires a value"):
            validate_passthrough_args([tok], _own())

    @pytest.mark.parametrize("tok", ["-a", "-t", "-d", "-o"])
    def test_bare_short_value_option_rejected(self, tok):
        """Same hazard via the short spelling."""
        with pytest.raises(typer.BadParameter, match="requires a value"):
            validate_passthrough_args([tok], _own())

    @pytest.mark.parametrize(
        "tok", ["--job-name", "--chdir", "--nodes", "--time", "--partition", "--mem"]
    )
    def test_bare_modeled_long_option_rejected(self, tok):
        """Options srunx models itself are absent from SBATCH_OPTIONS, but a
        raw ``--sbatch-arg=--job-name`` still reaches sbatch, where it takes
        a mandatory value — so the same stdin-fallback hazard applies."""
        with pytest.raises(typer.BadParameter, match="requires a value"):
            validate_passthrough_args([tok], _own())

    @pytest.mark.parametrize("tok", ["--job-name=x", "--chdir=/tmp"])
    def test_modeled_long_option_with_value_accepted(self, tok):
        assert validate_passthrough_args([tok], _own()) == [tok]

    def test_every_own_value_taking_long_is_guarded(self):
        """Mechanical completeness check.

        The set is derived from Click at call time rather than listed by
        hand precisely so it cannot drift; a hand-written list was missing
        16 of these. This test fails if the derivation regresses to a
        partial source.
        """
        guarded = []
        for spec in _own():
            if not spec.takes_value:
                continue
            for spelling in spec.spellings:
                if not spelling.startswith("--"):
                    continue
                try:
                    validate_passthrough_args([spelling], _own())
                except typer.BadParameter:
                    guarded.append(spelling)
        # Every value-taking long srunx defines must be caught when bare.
        expected = {
            s
            for spec in _own()
            if spec.takes_value
            for s in spec.spellings
            if s.startswith("--")
        }
        assert set(guarded) == expected

    @pytest.mark.parametrize("tok", ["--arr", "--depend", "--comm"])
    def test_bare_abbreviated_value_option_rejected(self, tok):
        """The module docstring advertises ``--sbatch-arg=--arr=1-10`` as the
        way to opt into an abbreviation, so a bare abbreviation is an
        expected input and must be caught the same way."""
        with pytest.raises(typer.BadParameter, match="requires a value"):
            validate_passthrough_args([tok], _own())

    @pytest.mark.parametrize("tok", ["-aH", "-a1-10", "--arr=1-10"])
    def test_short_and_abbreviated_with_value_accepted(self, tok):
        assert validate_passthrough_args([tok], _own()) == [tok]

    @pytest.mark.parametrize("tok", ["-Ateam", "-Jtrain", "-olog.out", "-C gpu"])
    def test_attached_value_ending_in_option_letter_accepted(self, tok):
        """The scan must stop at the first value-taking letter.

        Everything after it is that option's attached value — even when the
        value happens to end in a letter that names another value-taking
        option (``-Ateam`` ends in ``m`` = ``--distribution``, ``-Jtrain``
        in ``n`` = ``--ntasks``, ``-olog.out`` in ``t`` = ``--time``).
        Checking the last character instead rejects all three.
        """
        assert validate_passthrough_args([tok], _own()) == [tok]

    def test_value_taking_letter_mid_cluster_without_value_rejected(self):
        # -v takes no value, -t does and ends the cluster.
        with pytest.raises(typer.BadParameter, match="requires a value"):
            validate_passthrough_args(["-vt"], _own())

    @pytest.mark.parametrize("tok", ["--array=1-10", "--comment=hi"])
    def test_with_value_accepted(self, tok):
        assert validate_passthrough_args([tok], _own()) == [tok]

    @pytest.mark.parametrize("tok", ["--exclusive", "--hold", "--requeue"])
    def test_valueless_options_still_accepted(self, tok):
        assert validate_passthrough_args([tok], _own()) == [tok]


class TestShortClusterRewrite:
    """A valueless short flag does not swallow the rest of its cluster.

    getopt_long keeps scanning after a flag that takes no argument, so
    ``-HO`` is ``--hold --overcommit``, never ``--hold=O``.
    """

    def test_valueless_flags_decompose(self):
        assert rewrite_sbatch_argv(["-HO", "s.sh"], _own()) == [
            "--sbatch-arg=--hold",
            "--sbatch-arg=--overcommit",
            "s.sh",
        ]

    def test_valueless_flags_then_value_taking_tail(self):
        assert rewrite_sbatch_argv(["-HOa", "1-5", "s.sh"], _own()) == [
            "--sbatch-arg=--hold",
            "--sbatch-arg=--overcommit",
            "--sbatch-arg=--array=1-5",
            "s.sh",
        ]

    def test_value_taking_short_still_consumes_remainder(self):
        # -a takes a value, so the rest of the cluster IS its value.
        assert rewrite_sbatch_argv(["-aH", "s.sh"], _own()) == [
            "--sbatch-arg=--array=H",
            "s.sh",
        ]


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
            validate_passthrough_args([token], _own())


class TestRejectNonOptionToken:
    @pytest.mark.parametrize("token", ["foo", "-", "--"])
    def test_reject_non_option_token(self, token):
        with pytest.raises(typer.BadParameter):
            validate_passthrough_args([token], _own())


def test_validate_accepts_normal_tokens():
    tokens = ["--array=1-10", "--exclusive", "--qos=normal"]
    assert validate_passthrough_args(tokens, _own()) == tokens


def test_short_cluster_rejects_hq_and_qh_but_not_jq():
    with pytest.raises(typer.BadParameter):
        validate_passthrough_args(["-HQ"], _own())
    with pytest.raises(typer.BadParameter):
        validate_passthrough_args(["-QH"], _own())
    # -J takes a value; "Q" here is -J's value, not a real -Q flag.
    assert validate_passthrough_args(["-JQ"], _own()) == ["-JQ"]


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
