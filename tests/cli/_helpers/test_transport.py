"""Tests for the CLI-boundary transport resolver wrappers.

The wrappers add exactly one thing on top of the registry resolvers: a
``TransportSelectionError`` -> ``typer.BadParameter`` translation so Typer
renders bad ``--profile`` / ``--local`` combinations as normal flag errors.
"""

from __future__ import annotations

import pytest
import typer

from srunx.cli._helpers.transport import (
    peek_scheduler_key,
    resolve_transport,
    resolve_transport_source,
)


class TestBadParameterTranslation:
    def test_resolve_transport_conflict_becomes_bad_parameter(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with pytest.raises(typer.BadParameter) as excinfo:
            with resolve_transport(profile="foo", local=True):
                pass
        assert excinfo.value.param_hint == "--profile / --local"

    def test_peek_scheduler_key_conflict_becomes_bad_parameter(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with pytest.raises(typer.BadParameter):
            peek_scheduler_key(profile="foo", local=True)

    def test_resolve_transport_source_conflict_becomes_bad_parameter(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with pytest.raises(typer.BadParameter):
            resolve_transport_source(profile="foo", local=True)

    def test_empty_profile_becomes_bad_parameter(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with pytest.raises(typer.BadParameter) as excinfo:
            peek_scheduler_key(profile="   ", local=False)
        assert excinfo.value.param_hint == "--profile"


class TestHappyPathPassThrough:
    def test_default_resolves_local(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with resolve_transport(profile=None, local=False) as rt:
            assert rt.scheduler_key == "local"

    def test_peek_local(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        assert peek_scheduler_key() == "local"
