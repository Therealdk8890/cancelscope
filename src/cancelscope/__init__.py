"""Cooperative cancellation, timeouts, and cancel scopes for synchronous Python."""

from ._impl import (
    Cancelled,
    CancelScope,
    bind,
    checkpoint,
    current_scope,
    fail_after,
    guard,
    is_cancelled,
    move_on_after,
    sleep,
)

__version__ = "0.1.0"

__all__ = [
    "Cancelled",
    "CancelScope",
    "bind",
    "checkpoint",
    "current_scope",
    "fail_after",
    "guard",
    "is_cancelled",
    "move_on_after",
    "sleep",
]
