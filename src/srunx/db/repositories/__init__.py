"""Repositories for the srunx state DB.

Each repository wraps a single table and accepts a ``sqlite3.Connection``
in its constructor so that callers can compose multiple repositories
inside a single transaction.
"""
