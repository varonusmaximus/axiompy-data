"""
Arrow database error classes.

Provides a hierarchy of exceptions for Arrow database operations.
"""


class ArrowDatabaseError(Exception):
    """Base exception for Arrow database errors."""

    pass


class ArrowConnectionError(ArrowDatabaseError):
    """Connection-related errors (failed to connect, authentication, etc.)."""

    pass


class ArrowQueryError(ArrowDatabaseError):
    """Query execution errors (syntax errors, invalid columns, etc.)."""

    pass
