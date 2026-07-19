"""Core implementation of cancelscope.

Model
-----
A :class:`CancelScope` is a context manager that marks a region of code as
cancellable.  Cancellation is *cooperative*: nothing is interrupted
preemptively.  Code observes cancellation by calling :func:`checkpoint`
(or :func:`sleep`, or iterating via :func:`guard`), which raises
:class:`Cancelled` if the innermost active scope — or any enclosing,
non-shielded scope — has been cancelled.

Cancellation sources:

* ``scope.cancel(reason)`` — explicit, callable from any thread.
* A deadline (``timeout=``/``deadline=``) — enforced by a single shared
  daemon thread, so expiry also wakes threads blocked in :func:`sleep`
  and fires ``on_cancel`` callbacks at the deadline, not merely at the
  next checkpoint.

Cancelling a scope cancels every scope nested inside it, except scopes
entered with ``shield=True``, which detach from their parent.  A raised
:class:`Cancelled` is bound to the *outermost* directly-cancelled scope
and is swallowed by that scope's ``__exit__`` (setting
``cancelled_caught``), matching trio's semantics.
"""

from __future__ import annotations

import heapq
import itertools
import logging
import threading
import time
from collections import deque
from contextvars import ContextVar
from typing import Any, Callable, Iterable, Iterator, List, Optional, Set, Tuple, TypeVar

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

_T = TypeVar("_T")

_logger = logging.getLogger("cancelscope")

_current: ContextVar[Optional["CancelScope"]] = ContextVar(
    "cancelscope_current", default=None
)


class Cancelled(BaseException):
    """Raised at a checkpoint inside a cancelled scope.

    Inherits from :class:`BaseException` (like ``KeyboardInterrupt``) so
    that a bare ``except Exception`` cannot accidentally swallow a
    cancellation on its way to the scope that owns it.

    Attributes:
        scope: The outermost directly-cancelled :class:`CancelScope` this
            cancellation belongs to.  That scope's ``__exit__`` catches
            the exception; enclosing and intermediate scopes let it pass.
        reason: Optional human-readable reason passed to ``cancel()``,
            or ``"deadline expired"`` for timeouts.
    """

    def __init__(self, scope: "CancelScope", reason: Optional[str] = None) -> None:
        super().__init__(reason or "cancelled")
        self.scope = scope
        self.reason = reason


class _DeadlineService:
    """A process-wide daemon thread that fires scope deadlines.

    Scopes with a deadline register here.  Entries are lazily invalidated:
    if a scope exits or its deadline changes, the stale heap entry is
    skipped when it surfaces.  Cancellation side effects (event set,
    callbacks, child propagation) run outside the service lock.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._heap: List[Tuple[float, int, CancelScope]] = []
        self._counter = itertools.count()
        self._thread: Optional[threading.Thread] = None
        self._compact_at = 128

    def register(self, scope: "CancelScope") -> None:
        deadline = scope._deadline
        if deadline is None:
            return
        with self._cond:
            heapq.heappush(self._heap, (deadline, next(self._counter), scope))
            if len(self._heap) >= self._compact_at:
                # The monitor only pops stale entries when they surface at
                # the top, so stale entries sorted below a live far-future
                # entry would otherwise pin their (exited) scopes until it
                # fires.  Pruning at a doubling threshold keeps that
                # retention bounded at amortized O(1) per registration.
                self._heap = [
                    (d, c, s)
                    for (d, c, s) in self._heap
                    if not (s._exited or s._deadline != d)
                ]
                heapq.heapify(self._heap)
                self._compact_at = max(128, 2 * len(self._heap))
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._run, name="cancelscope-deadline-monitor", daemon=True
                )
                self._thread.start()
            self._cond.notify()

    # Cap on a single Condition.wait: an infinite or astronomically large
    # deadline (deadline=math.inf is a natural "no deadline yet" idiom)
    # must not overflow the platform timestamp and kill this thread.  A
    # capped park just rescans and parks again.
    _MAX_WAIT = 3600.0

    def _run(self) -> None:
        while True:
            due: List[CancelScope] = []
            with self._cond:
                while not due:
                    now = time.monotonic()
                    while self._heap:
                        deadline, _, scope = self._heap[0]
                        if scope._exited or scope._deadline != deadline:
                            heapq.heappop(self._heap)  # stale entry
                            continue
                        if deadline <= now:
                            heapq.heappop(self._heap)
                            due.append(scope)
                            continue
                        break
                    if due:
                        break
                    if self._heap:
                        self._cond.wait(min(self._heap[0][0] - now, self._MAX_WAIT))
                    else:
                        self._cond.wait()
            for scope in due:
                # This thread is a process-wide singleton: whatever a user
                # callback raises (including BaseExceptions, which
                # _run_callback deliberately does not catch), the service
                # must survive or every other registered deadline is lost.
                try:
                    scope._set_cancelled(
                        direct=True, reason="deadline expired", deadline_expired=True
                    )
                except BaseException:
                    _logger.exception(
                        "cancelscope: error while firing deadline for %r", scope
                    )


_service = _DeadlineService()


class CancelScope:
    """A cancellable region of code.

    Args:
        timeout: Seconds after ``__enter__`` at which the scope
            self-cancels.  Mutually exclusive with ``deadline``.
        deadline: Absolute ``time.monotonic()`` timestamp at which the
            scope self-cancels.
        shield: If true, this scope does not inherit cancellation from
            enclosing scopes.  Its own ``cancel()``/deadline still work.
        name: Optional label used in ``repr`` for debugging.

    A scope object may be created in one thread, handed to another, and
    cancelled from anywhere; but ``__enter__``/``__exit__`` must run in
    the same thread, and a scope cannot be entered twice.
    """

    def __init__(
        self,
        *,
        timeout: Optional[float] = None,
        deadline: Optional[float] = None,
        shield: bool = False,
        name: Optional[str] = None,
    ) -> None:
        if timeout is not None and deadline is not None:
            raise ValueError("pass either timeout= or deadline=, not both")
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be >= 0")
        self.name = name
        self._pending_timeout = timeout
        self._deadline: Optional[float] = deadline
        self._shield = bool(shield)

        self._lock = threading.Lock()
        self._event = threading.Event()
        self._cancel_called = False
        self._deadline_expired = False
        self._reason: Optional[str] = None
        self.cancelled_caught = False

        self._parent: Optional[CancelScope] = None
        self._children: Set[CancelScope] = set()
        self._callbacks: List[Callable[[CancelScope], None]] = []

        self._entered = False
        self._exited = False
        self._ctx_token: Any = None

    # -- introspection ------------------------------------------------

    @property
    def cancelled(self) -> bool:
        """True once this scope is effectively cancelled (directly or inherited)."""
        return self._event.is_set()

    @property
    def cancel_called(self) -> bool:
        """True once this scope itself was cancelled (explicitly or by its deadline)."""
        return self._cancel_called

    @property
    def reason(self) -> Optional[str]:
        return self._reason

    @property
    def shield(self) -> bool:
        return self._shield

    @property
    def deadline(self) -> Optional[float]:
        """Absolute ``time.monotonic()`` deadline, or None."""
        return self._deadline

    @deadline.setter
    def deadline(self, value: Optional[float]) -> None:
        # An explicit deadline always wins over a not-yet-applied timeout=,
        # so setting it before __enter__ is not silently overwritten.
        self._pending_timeout = None
        self._deadline = value
        if value is not None and self._entered and not self._exited:
            _service.register(self)

    def remaining(self) -> Optional[float]:
        """Seconds until this scope's own deadline, or None if it has none."""
        if self._deadline is None:
            return None
        return max(0.0, self._deadline - time.monotonic())

    def __repr__(self) -> str:
        bits = []
        if self.name:
            bits.append(f"name={self.name!r}")
        if self._deadline is not None:
            bits.append(f"remaining={self.remaining():.3f}s")
        if self._shield:
            bits.append("shield=True")
        if self._event.is_set():
            bits.append(f"cancelled reason={self._reason!r}")
        return f"<CancelScope {' '.join(bits) or 'active'}>"

    # -- cancellation -------------------------------------------------

    def cancel(self, reason: Optional[str] = None) -> bool:
        """Cancel this scope and everything nested inside it.

        Thread-safe; may be called before the scope is entered, from any
        thread, or more than once.  Returns True if this call performed
        the cancellation, False if the scope was already cancelled.
        """
        return self._set_cancelled(direct=True, reason=reason)

    def on_cancel(
        self, callback: Callable[["CancelScope"], None]
    ) -> Callable[[], None]:
        """Register ``callback(scope)`` to run when this scope is cancelled.

        This is the escape hatch for unblocking third-party blocking
        calls (e.g. close a socket, terminate a subprocess).  If the
        scope is already cancelled the callback runs immediately in the
        calling thread; otherwise it runs in whichever thread triggers
        the cancellation (which is the deadline-monitor thread for
        timeouts) — keep callbacks short and thread-safe.  Exceptions
        raised by callbacks are logged and suppressed.

        Callbacks never fire after the scope has exited: ``__exit__``
        discards them, and registering on an exited (uncancelled) scope
        is a no-op.

        Returns a zero-argument function that unregisters the callback.
        Unregistration cannot outrace a cancellation already in flight:
        if ``cancel()`` has begun dispatching in another thread, the
        callback may still run (even concurrently with, or after, the
        unregister call returning).
        """
        with self._lock:
            if not self._event.is_set():
                if self._exited:
                    # The scope's body already completed; there is nothing
                    # left to unblock, so never fire.
                    return lambda: None
                self._callbacks.append(callback)

                def unregister() -> None:
                    with self._lock:
                        try:
                            self._callbacks.remove(callback)
                        except ValueError:
                            pass

                return unregister
        self._run_callback(callback)
        return lambda: None

    def _run_callback(self, callback: Callable[["CancelScope"], None]) -> None:
        try:
            callback(self)
        except Exception:
            _logger.exception("cancelscope: on_cancel callback raised")

    def _set_cancelled(
        self,
        *,
        direct: bool,
        reason: Optional[str] = None,
        deadline_expired: bool = False,
    ) -> bool:
        # Iterative traversal: recursing per nesting level would hit the
        # recursion limit on deep scope chains, leaving descendants
        # uncancelled (and killing the deadline-monitor thread when the
        # cancel came from a deadline).
        state = self._mark_cancelled(
            direct=direct, reason=reason, deadline_expired=deadline_expired
        )
        if state is None:
            return False
        pending = deque([(self, state)])
        while pending:
            scope, (callbacks, children) = pending.popleft()
            for callback in callbacks:
                scope._run_callback(callback)
            for child in children:
                child_state = child._mark_cancelled(direct=False)
                if child_state is not None:
                    pending.append((child, child_state))
        return True

    def _mark_cancelled(
        self,
        *,
        direct: bool,
        reason: Optional[str] = None,
        deadline_expired: bool = False,
    ) -> Optional[Tuple[List[Callable[["CancelScope"], None]], List["CancelScope"]]]:
        """Flip this one scope to cancelled under its lock.

        Returns the (callbacks, children) snapshot to process outside the
        lock, or None if the scope was already cancelled.
        """
        with self._lock:
            if self._event.is_set():
                return None
            if direct:
                self._cancel_called = True
                self._deadline_expired = deadline_expired
                self._reason = reason
            self._event.set()
            callbacks, self._callbacks = self._callbacks, []
            return callbacks, list(self._children)

    # -- context management -------------------------------------------

    def __enter__(self) -> "CancelScope":
        if self._entered:
            raise RuntimeError("CancelScope may only be entered once")
        self._entered = True
        parent = None if self._shield else _current.get()
        self._ctx_token = _current.set(self)
        if parent is not None:
            self._parent = parent
            with parent._lock:
                parent._children.add(self)
                parent_cancelled = parent._event.is_set()
            if parent_cancelled:
                self._set_cancelled(direct=False)
        if self._pending_timeout is not None:
            self._deadline = time.monotonic() + self._pending_timeout
        if self._deadline is not None:
            _service.register(self)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self._exited = True
        with self._lock:
            # A cancellation that lands after this point (late explicit
            # cancel(), a deadline racing a clean exit, or parent
            # propagation racing our removal below) must not run
            # unblock-callbacks for a body that already completed.
            self._callbacks.clear()
        _current.reset(self._ctx_token)
        parent = self._parent
        if parent is not None:
            with parent._lock:
                parent._children.discard(self)
        if isinstance(exc, Cancelled) and exc.scope is self:
            self.cancelled_caught = True
            return True
        return False

    # -- observation --------------------------------------------------

    def _checkpoint(self) -> None:
        if not self._event.is_set():
            return
        target: Optional[CancelScope] = None
        node: Optional[CancelScope] = self
        while node is not None:
            if node._cancel_called:
                target = node
            node = node._parent
        if target is None:
            target = self
        raise Cancelled(target, target._reason)

    def _nearest_deadline(self) -> Optional[float]:
        nearest: Optional[float] = None
        node: Optional[CancelScope] = self
        while node is not None:
            # An exited ancestor (reachable via bind() when the parent
            # finishes in its own thread first) has a dead deadline; once
            # it lies in the past it would otherwise pin sleep() into a
            # 1ms poll loop.
            d = None if node._exited else node._deadline
            if d is not None and (nearest is None or d < nearest):
                nearest = d
            node = node._parent
        return nearest


# -- module-level API -------------------------------------------------


def current_scope() -> Optional[CancelScope]:
    """The innermost active scope in this thread's context, or None."""
    return _current.get()


def checkpoint() -> None:
    """Raise :class:`Cancelled` if the surrounding scope has been cancelled.

    No-op (and cheap: one Event check) when not cancelled or when called
    outside any scope.  Sprinkle this through loops and between units of
    work — it is the only place explicit cancellation is observed.
    """
    scope = _current.get()
    if scope is not None:
        scope._checkpoint()


def is_cancelled() -> bool:
    """Non-raising check: is the surrounding scope cancelled?"""
    scope = _current.get()
    return scope is not None and scope.cancelled


def sleep(seconds: float) -> None:
    """Like :func:`time.sleep`, but wakes immediately on cancellation.

    Outside any scope this is exactly ``time.sleep``.  Inside a scope it
    waits on the scope's cancellation event, so an explicit ``cancel()``
    from another thread or a deadline firing interrupts it right away
    (raising :class:`Cancelled`).
    """
    scope = _current.get()
    if scope is None:
        time.sleep(seconds)
        return
    wake_at = time.monotonic() + seconds
    while True:
        scope._checkpoint()
        now = time.monotonic()
        remaining = wake_at - now
        if remaining <= 0:
            return
        nearest = scope._nearest_deadline()
        if nearest is not None:
            remaining = min(remaining, max(0.0, nearest - now) + 0.001)
        scope._event.wait(remaining)


def guard(iterable: Iterable[_T]) -> Iterator[_T]:
    """Yield from ``iterable`` with a :func:`checkpoint` before each item.

    ``for item in guard(work):`` makes any loop cancellable.  The
    checkpoint runs *before* the underlying iterator is advanced, so
    cancellation never consumes — and silently drops — an item from a
    one-shot source such as a queue consumer or database cursor.
    """
    it = iter(iterable)
    while True:
        checkpoint()
        try:
            item = next(it)
        except StopIteration:
            return
        yield item


def bind(fn: Callable[..., _T]) -> Callable[..., _T]:
    """Capture the current innermost scope for use in another thread.

    Cancel scopes propagate via context variables, which do not flow
    into threads started with ``threading.Thread`` or executor workers.
    ``bind`` closes over the scope that is current *now* and returns a
    callable that reinstates it around ``fn``::

        with CancelScope() as scope:
            fut = executor.submit(bind(worker), job)

    Checkpoints inside ``worker`` then observe ``scope``.
    """
    scope = _current.get()

    def bound(*args: Any, **kwargs: Any) -> _T:
        if scope is None:
            return fn(*args, **kwargs)
        token = _current.set(scope)
        try:
            return fn(*args, **kwargs)
        finally:
            _current.reset(token)

    return bound


def move_on_after(
    seconds: float, *, shield: bool = False, name: Optional[str] = None
) -> CancelScope:
    """A scope that silently self-cancels after ``seconds``.

    ``with move_on_after(5) as scope: ...`` — afterwards check
    ``scope.cancelled_caught`` to see whether the timeout hit.
    """
    return CancelScope(timeout=seconds, shield=shield, name=name)


class fail_after:
    """Like :func:`move_on_after`, but raises :class:`TimeoutError` on expiry.

    An explicit ``cancel()`` of the scope is still swallowed silently;
    only the deadline converts to ``TimeoutError``.
    """

    def __init__(
        self, seconds: float, *, shield: bool = False, name: Optional[str] = None
    ) -> None:
        self._scope = CancelScope(timeout=seconds, shield=shield, name=name)

    def __enter__(self) -> CancelScope:
        return self._scope.__enter__()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        suppressed = self._scope.__exit__(exc_type, exc, tb)
        if suppressed and self._scope._deadline_expired:
            raise TimeoutError(
                "operation did not complete within the allotted time"
            ) from exc
        return suppressed
