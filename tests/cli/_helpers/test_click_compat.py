"""Tests for :mod:`srunx.cli._helpers.click_compat`.

Both helpers exist because typer >= 0.26 builds commands from a vendored
copy of Click, so the classes and enums reaching srunx are no longer the
ones ``import click`` provides. Each test below pairs the real Click
object with a stand-in for the vendored one.
"""

from __future__ import annotations

import enum

import click
import typer.core

from srunx.cli._helpers.click_compat import OPTION_CLASSES, is_commandline


class TestOptionClasses:
    def test_click_option_matches(self):
        assert isinstance(click.Option(["--x"]), OPTION_CLASSES)

    def test_typer_option_matches(self):
        assert isinstance(typer.core.TyperOption(param_decls=["--x"]), OPTION_CLASSES)

    def test_arguments_do_not_match(self):
        assert not isinstance(click.Argument(["script"]), OPTION_CLASSES)
        assert not isinstance(
            typer.core.TyperArgument(param_decls=["script"]), OPTION_CLASSES
        )


class _VendoredParameterSource(enum.Enum):
    """Stand-in for typer's vendored enum: same member names, other class."""

    COMMANDLINE = enum.auto()
    DEFAULT = enum.auto()


class TestIsCommandline:
    def test_accepts_clicks_own_enum(self):
        assert is_commandline(click.core.ParameterSource.COMMANDLINE)

    def test_accepts_a_foreign_enum_with_the_same_member(self):
        # An identity comparison against click's enum would report "not
        # typed by the user" for every parameter under typer >= 0.26,
        # silently dropping explicit -N / -c / -t / -J from the sbatch
        # command line.
        assert is_commandline(_VendoredParameterSource.COMMANDLINE)

    def test_rejects_defaults_and_none(self):
        assert not is_commandline(click.core.ParameterSource.DEFAULT)
        assert not is_commandline(_VendoredParameterSource.DEFAULT)
        assert not is_commandline(None)
