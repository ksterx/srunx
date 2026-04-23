"""Service layer for ``/api/workflows/*`` endpoints.

Framework-agnostic business logic — FastAPI ``Depends`` wiring and HTTP
response shaping stay in the router module. Services accept plain
arguments (DB connection, adapter, Pydantic schema instance) and expose
async methods that internally wrap blocking calls with
``anyio.to_thread.run_sync``.
"""
