"""Tests for srunx.common.exceptions transport selection error."""

from __future__ import annotations

from srunx.common.exceptions import TransportSelectionError


class TestTransportSelectionError:
    def test_is_value_error(self) -> None:
        """Subclasses ValueError so broad ``except ValueError`` still works."""
        assert issubclass(TransportSelectionError, ValueError)

    def test_message_preserved(self) -> None:
        exc = TransportSelectionError("bad selection")
        assert str(exc) == "bad selection"

    def test_param_hint_defaults_none(self) -> None:
        exc = TransportSelectionError("bad selection")
        assert exc.param_hint is None

    def test_param_hint_carried(self) -> None:
        exc = TransportSelectionError("conflict", param_hint="--profile / --local")
        assert exc.param_hint == "--profile / --local"
