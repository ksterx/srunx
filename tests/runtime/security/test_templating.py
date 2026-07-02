"""Tests for the sandboxed Jinja2 renderer (SSTI prevention)."""

import jinja2
import pytest

from srunx.runtime.security import sandboxed_template


class TestSandboxedTemplate:
    def test_renders_normal_variables(self):
        tmpl = sandboxed_template("hello {{ name }}")
        assert tmpl.render(name="world") == "hello world"

    def test_keep_trailing_newline(self):
        tmpl = sandboxed_template("x\n", keep_trailing_newline=True)
        assert tmpl.render() == "x\n"

    def test_blocks_globals_ssti(self):
        """cycler.__init__.__globals__ escape must raise SecurityError."""
        tmpl = sandboxed_template(
            "{{ cycler.__init__.__globals__['os'].system('id') }}"
        )
        with pytest.raises(jinja2.exceptions.SecurityError):
            tmpl.render()

    def test_blocks_mro_subclasses_ssti(self):
        tmpl = sandboxed_template("{{ ''.__class__.__mro__[1].__subclasses__() }}")
        with pytest.raises(jinja2.exceptions.SecurityError):
            tmpl.render()
