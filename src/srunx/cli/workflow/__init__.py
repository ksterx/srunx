"""Workflow CLI subpackage.

The user-facing entry points (``srunx flow run`` etc.) are wired through
:mod:`srunx.cli.main`, which lazily imports
:func:`srunx.cli.workflow.orchestrator._execute_workflow`. The standalone
``app`` Typer instance in :mod:`srunx.cli.workflow.orchestrator` exists
so tests can drive ``_execute_workflow`` through Typer without spinning
up the full ``srunx`` root app.

This ``__init__`` deliberately does NOT re-export submodule contents.
Re-exporting helper symbols (``WorkflowRunner``, ``SweepOrchestrator``,
``_execute_workflow``) shadows the submodules themselves and routes
``@patch`` calls to the wrong site, breaking tests in subtle ways.
Tests should import from the specific submodule (``srunx.cli.workflow.orchestrator``,
``srunx.cli.workflow.sweep``, etc.) and ``@patch`` at that lookup site.
"""
