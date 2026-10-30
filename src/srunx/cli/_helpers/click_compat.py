"""Click-type lookups that survive typer's vendored-Click split.

typer >= 0.26 ships its own copy of Click under ``typer._click`` and
builds commands out of *those* classes. Two consequences reach the
``sbatch`` CLI, and both fail silently rather than loudly:

* :class:`typer.core.TyperOption` is no longer a :class:`click.Option`
  subclass, so ``isinstance(param, click.Option)`` matches nothing. That
  emptied the own-option set in
  :func:`srunx.cli._helpers.sbatch_passthrough.own_option_specs`, and
  with no own spellings to protect, every srunx-native flag lost to the
  native-sbatch argv rewrite: ``srunx sbatch --wrap "cmd"`` failed with
  "sbatch option '--wrap' is not allowed via --sbatch-arg" and
  ``srunx sbatch --help`` with "Missing job source".
* ``ctx.get_parameter_source()`` returns a member of typer's vendored
  ``ParameterSource`` enum, which compares unequal to every member of
  ``click.core.ParameterSource``. A ``source == ParameterSource.COMMANDLINE``
  check therefore never fired, dropping explicitly typed resource flags
  (``-N`` / ``-c`` / ``-t`` / ``-p`` / ``-w`` / ``-D``, plus ``-J``) from
  the submission.

Both lookups live here so the rest of the CLI never has to know which
Click the installed typer is built on.
"""

from __future__ import annotations

from typing import Protocol

import click
import typer.core

#: ``isinstance`` target matching an option parameter under either layout.
#: Under typer < 0.26 ``TyperOption`` *is* a ``click.Option``, so naming
#: both is redundant there and load-bearing from 0.26 on.
OPTION_CLASSES: tuple[type, ...] = (click.Option, typer.core.TyperOption)


class OptionLike(Protocol):
    """The attributes srunx reads off an option parameter.

    A structural type, because the nominal one differs per typer version
    (see :data:`OPTION_CLASSES`): narrowing on that tuple leaves a plain
    ``object``, so callers ``cast`` to this after the isinstance check.
    """

    opts: list[str]
    secondary_opts: list[str]
    is_flag: bool


_COMMANDLINE = click.core.ParameterSource.COMMANDLINE.name


def is_commandline(source: object | None) -> bool:
    """Did the user actually type this parameter on the command line?

    Takes the return of ``ctx.get_parameter_source(name)`` (``None`` is
    accepted, for callers defending against signature drift). Compares by
    enum *member name* rather than identity, because the enum class the
    context yields depends on the installed typer — matching on the name
    keeps this correct however typer vendors Click next.
    """
    return getattr(source, "name", None) == _COMMANDLINE
