"""Tests for :mod:`srunx.cli._helpers.click_compat`.

Both helpers exist because typer >= 0.26 builds commands from a vendored
copy of Click, so the classes and enums reaching srunx are no longer the
ones ``import click`` provides. Each test below pairs the real Click
object with a stand-in for the vendored one.
"""

from __future__ import annotations

import enum

import click
import typer

from srunx.cli._helpers.click_compat import is_commandline, is_option


class _VendoredOption:
    """Stand-in for a vendored-Click option: no shared base with click's."""

    param_type_name = "option"


def _typer_params() -> dict[str, object]:
    """Parameters exactly as the installed typer builds them.

    Built through a real command rather than by calling ``TyperOption``
    directly: that constructor is internal and its signature changes
    between typer releases (0.29 added a required ``type_descriptor``).
    """
    app = typer.Typer()

    @app.command()
    def cmd(script: str, x: int = typer.Option(0, "--x")) -> None: ...

    return {str(p.name): p for p in typer.main.get_command(app).params}


class TestIsOption:
    def test_click_option_matches(self):
        assert is_option(click.Option(["--x"]))

    def test_typer_option_matches(self):
        assert is_option(_typer_params()["x"])

    def test_unrelated_class_with_option_type_name_matches(self):
        # What typer >= 0.26 hands over: an option that is not a
        # click.Option subclass. An isinstance test would miss it.
        assert is_option(_VendoredOption())

    def test_arguments_do_not_match(self):
        assert not is_option(click.Argument(["script"]))
        assert not is_option(_typer_params()["script"])
        assert not is_option(object())


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
