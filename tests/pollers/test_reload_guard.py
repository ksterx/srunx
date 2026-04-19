"""Unit tests for :mod:`srunx.pollers.reload_guard`."""

from __future__ import annotations

from srunx.pollers.reload_guard import is_reload_mode, should_start_pollers


class TestIsReloadMode:
    """Coverage for :func:`is_reload_mode`."""

    def test_unset_env_and_clean_argv_is_not_reload(self) -> None:
        assert is_reload_mode(env={}, argv=["uvicorn", "srunx.web.app:app"]) is False

    def test_uvicorn_reload_env_var_set_is_reload(self) -> None:
        assert is_reload_mode(env={"UVICORN_RELOAD": "1"}, argv=["uvicorn"]) is True

    def test_uvicorn_reload_env_empty_string_is_not_reload(self) -> None:
        # Empty string is falsy, matching typical "unset" semantics.
        assert is_reload_mode(env={"UVICORN_RELOAD": ""}, argv=["uvicorn"]) is False

    def test_reload_flag_in_argv_is_reload(self) -> None:
        assert (
            is_reload_mode(env={}, argv=["uvicorn", "srunx.web.app:app", "--reload"])
            is True
        )

    def test_both_signals_is_reload(self) -> None:
        assert (
            is_reload_mode(
                env={"UVICORN_RELOAD": "true"},
                argv=["uvicorn", "--reload"],
            )
            is True
        )

    def test_arbitrary_truthy_env_value_is_reload(self) -> None:
        assert is_reload_mode(env={"UVICORN_RELOAD": "yes"}, argv=[]) is True

    def test_disable_poller_env_alone_is_not_reload(self) -> None:
        # SRUNX_DISABLE_POLLER is not a reload signal; it's orthogonal.
        assert is_reload_mode(env={"SRUNX_DISABLE_POLLER": "1"}, argv=[]) is False


class TestShouldStartPollers:
    """Coverage for :func:`should_start_pollers`."""

    def test_clean_env_starts_pollers(self) -> None:
        assert should_start_pollers(env={}, argv=["uvicorn"]) is True

    def test_reload_env_var_blocks_pollers(self) -> None:
        assert (
            should_start_pollers(env={"UVICORN_RELOAD": "1"}, argv=["uvicorn"]) is False
        )

    def test_reload_argv_blocks_pollers(self) -> None:
        assert should_start_pollers(env={}, argv=["uvicorn", "--reload"]) is False

    def test_disable_poller_flag_blocks_pollers(self) -> None:
        assert (
            should_start_pollers(env={"SRUNX_DISABLE_POLLER": "1"}, argv=["uvicorn"])
            is False
        )

    def test_disable_poller_zero_does_not_block(self) -> None:
        # Only the literal string "1" opts out; "0" is treated as not set.
        assert (
            should_start_pollers(env={"SRUNX_DISABLE_POLLER": "0"}, argv=["uvicorn"])
            is True
        )

    def test_disable_poller_unset_does_not_block(self) -> None:
        assert should_start_pollers(env={}, argv=["uvicorn"]) is True

    def test_uvicorn_reload_empty_string_does_not_block(self) -> None:
        # Empty string env var is falsy, should still start pollers.
        assert (
            should_start_pollers(env={"UVICORN_RELOAD": ""}, argv=["uvicorn"]) is True
        )

    def test_both_reload_and_disable_still_blocks(self) -> None:
        assert (
            should_start_pollers(
                env={"UVICORN_RELOAD": "1", "SRUNX_DISABLE_POLLER": "1"},
                argv=["uvicorn", "--reload"],
            )
            is False
        )
