import contextlib
import math
import queue
import threading
import time

import pytest

import cancelscope as cs


# -- basics -----------------------------------------------------------


def test_checkpoint_outside_scope_is_noop():
    cs.checkpoint()  # must not raise


def test_explicit_cancel_raises_at_checkpoint_and_is_caught():
    ran_after = False
    with cs.CancelScope() as scope:
        scope.cancel("stop it")
        cs.checkpoint()
        ran_after = True  # pragma: no cover - unreachable
    assert not ran_after
    assert scope.cancelled_caught
    assert scope.cancel_called
    assert scope.reason == "stop it"


def test_cancel_before_enter():
    scope = cs.CancelScope()
    assert scope.cancel() is True
    assert scope.cancel() is False  # idempotent
    body_ran = False
    with scope:
        cs.checkpoint()
        body_ran = True  # pragma: no cover - unreachable
    assert not body_ran
    assert scope.cancelled_caught


def test_body_completing_without_checkpoint_is_not_interrupted():
    with cs.CancelScope() as scope:
        scope.cancel()
        result = 1 + 1  # no checkpoint: cooperative, so this runs fine
    assert result == 2
    assert scope.cancelled
    assert not scope.cancelled_caught


def test_cancelled_is_base_exception():
    assert issubclass(cs.Cancelled, BaseException)
    assert not issubclass(cs.Cancelled, Exception)
    with cs.CancelScope() as scope:
        scope.cancel()
        try:
            try:
                cs.checkpoint()
            except Exception:  # must NOT swallow Cancelled
                pytest.fail("Cancelled was caught by `except Exception`")
        except cs.Cancelled:
            raise  # let the scope catch it
    assert scope.cancelled_caught


def test_is_cancelled_and_properties():
    assert cs.is_cancelled() is False
    with cs.CancelScope(name="outer") as scope:
        assert cs.current_scope() is scope
        assert cs.is_cancelled() is False
        scope.cancel()
        assert cs.is_cancelled() is True
        assert scope.cancelled
        assert "outer" in repr(scope)
    assert cs.current_scope() is None


def test_scope_not_reentrant():
    scope = cs.CancelScope()
    with scope:
        pass
    with pytest.raises(RuntimeError):
        with scope:
            pass


def test_timeout_and_deadline_mutually_exclusive():
    with pytest.raises(ValueError):
        cs.CancelScope(timeout=1, deadline=time.monotonic() + 1)


# -- timeouts ---------------------------------------------------------


def test_move_on_after_interrupts_sleep():
    start = time.monotonic()
    with cs.move_on_after(0.2) as scope:
        cs.sleep(10)
    elapsed = time.monotonic() - start
    assert scope.cancelled_caught
    assert elapsed < 5


def test_move_on_after_body_finishes_in_time():
    with cs.move_on_after(10) as scope:
        pass
    assert not scope.cancelled_caught
    assert not scope.cancelled


def test_fail_after_raises_timeout_error():
    with pytest.raises(TimeoutError):
        with cs.fail_after(0.1):
            cs.sleep(10)


def test_fail_after_explicit_cancel_stays_silent():
    with cs.fail_after(10) as scope:
        scope.cancel("changed my mind")
        cs.checkpoint()
    assert scope.cancelled_caught


def test_deadline_property_and_remaining():
    with cs.move_on_after(30) as scope:
        assert scope.remaining() == pytest.approx(30, abs=1)
        scope.deadline = time.monotonic() + 0.1  # tighten
        start = time.monotonic()
        cs.sleep(10)
    assert scope.cancelled_caught
    assert time.monotonic() - start < 5


def test_deadline_fires_between_checkpoints():
    # No sleep(): busy code that only checkpoints.  The monitor thread
    # must set the event so a later checkpoint observes it.
    with cs.move_on_after(0.1) as scope:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            cs.checkpoint()
            time.sleep(0.01)
    assert scope.cancelled_caught


def test_sleep_outside_scope():
    start = time.monotonic()
    cs.sleep(0.05)
    assert time.monotonic() - start >= 0.04


# -- nesting and shielding -------------------------------------------


def test_cancel_outer_unwinds_through_inner():
    with cs.CancelScope() as outer:
        with cs.CancelScope() as inner:
            outer.cancel()
            cs.checkpoint()
        pytest.fail("inner scope must not catch outer's cancellation")
    assert outer.cancelled_caught
    assert not inner.cancelled_caught
    assert inner.cancelled  # inherited


def test_cancel_inner_leaves_outer_running():
    after_inner = False
    with cs.CancelScope() as outer:
        with cs.CancelScope() as inner:
            inner.cancel()
            cs.checkpoint()
        after_inner = True
        cs.checkpoint()  # outer not cancelled: fine
    assert after_inner
    assert inner.cancelled_caught
    assert not outer.cancelled


def test_cancelled_raised_at_outermost_cancelled_scope():
    with cs.CancelScope() as outer:
        with cs.CancelScope() as inner:
            inner.cancel()
            outer.cancel()
            with pytest.raises(cs.Cancelled) as info:
                cs.checkpoint()
            assert info.value.scope is outer
            raise info.value
    assert outer.cancelled_caught


def test_entering_child_of_cancelled_parent():
    with cs.CancelScope() as outer:
        outer.cancel()
        with cs.CancelScope():
            cs.checkpoint()
            pytest.fail("child of a cancelled parent must observe cancellation")
    assert outer.cancelled_caught


def test_shield_blocks_outer_cancellation():
    shielded_ran = False
    with cs.CancelScope() as outer:
        outer.cancel()
        with cs.CancelScope(shield=True) as inner:
            cs.checkpoint()  # outer's cancellation must not be visible
            shielded_ran = True
        cs.checkpoint()  # back outside the shield: now it raises
        pytest.fail("unreachable")
    assert shielded_ran
    assert not inner.cancelled
    assert outer.cancelled_caught


def test_shield_own_cancel_still_works():
    with cs.CancelScope() as outer:
        with cs.CancelScope(shield=True) as inner:
            inner.cancel()
            cs.checkpoint()
            pytest.fail("unreachable")
    assert inner.cancelled_caught
    assert not outer.cancelled


# -- threads ----------------------------------------------------------


def test_cross_thread_cancel():
    scope = cs.CancelScope()
    observed = threading.Event()

    def worker():
        with scope:
            while True:
                cs.checkpoint()
                time.sleep(0.005)
        observed.set()

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.05)
    scope.cancel("main thread says stop")
    t.join(timeout=5)
    assert not t.is_alive()
    assert observed.is_set()
    assert scope.cancelled_caught
    assert scope.reason == "main thread says stop"


def test_cancel_interrupts_sleep_from_other_thread():
    scope = cs.CancelScope()
    elapsed = None

    def worker():
        nonlocal elapsed
        start = time.monotonic()
        with scope:
            cs.sleep(10)
        elapsed = time.monotonic() - start

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.1)
    scope.cancel()
    t.join(timeout=5)
    assert not t.is_alive()
    assert elapsed is not None and elapsed < 5


def test_bind_propagates_scope_into_thread():
    results = []
    with cs.CancelScope() as scope:

        def worker():
            results.append(cs.current_scope())
            try:
                while True:
                    cs.checkpoint()
                    time.sleep(0.005)
            except cs.Cancelled:
                results.append("cancelled")

        t = threading.Thread(target=cs.bind(worker))
        t.start()
        time.sleep(0.05)
        scope.cancel()
        t.join(timeout=5)
    assert results[0] is scope
    assert results[1] == "cancelled"


def test_bind_outside_scope_is_passthrough():
    def probe():
        return cs.current_scope()

    assert cs.bind(probe)() is None


def test_many_threads_all_observe_cancel():
    scope = cs.CancelScope()
    n = 8
    done = []
    lock = threading.Lock()

    def worker():
        try:
            while True:
                cs.checkpoint()
                time.sleep(0.001)
        except cs.Cancelled:
            with lock:
                done.append(True)

    with scope:
        # bind() must be called while the scope is current
        threads = [
            threading.Thread(target=cs.bind(worker), daemon=True) for _ in range(n)
        ]
        for t in threads:
            t.start()
        time.sleep(0.05)
        scope.cancel()
    for t in threads:
        t.join(timeout=5)
    assert len(done) == n


def test_child_enter_during_parent_cancel_race():
    # Hammer the enter-child / cancel-parent race: the child must always
    # observe the cancellation regardless of interleaving.
    for _ in range(100):
        parent = cs.CancelScope()
        raised = threading.Event()

        def worker():
            with parent:
                try:
                    while True:
                        with cs.CancelScope():
                            cs.checkpoint()
                except cs.Cancelled:
                    raised.set()
                    raise

        t = threading.Thread(target=worker)
        t.start()
        parent.cancel()
        t.join(timeout=5)
        assert not t.is_alive()
        assert raised.is_set()


# -- on_cancel callbacks ----------------------------------------------


def test_on_cancel_fires_on_cancel():
    fired = []
    with cs.CancelScope() as scope:
        scope.on_cancel(lambda s: fired.append(s))
        scope.cancel()
        assert fired == [scope]


def test_on_cancel_fires_immediately_if_already_cancelled():
    fired = []
    scope = cs.CancelScope()
    scope.cancel()
    scope.on_cancel(lambda s: fired.append(s))
    assert fired == [scope]


def test_on_cancel_unregister():
    fired = []
    scope = cs.CancelScope()
    unregister = scope.on_cancel(lambda s: fired.append(s))
    unregister()
    scope.cancel()
    assert fired == []


def test_on_cancel_callback_exception_is_swallowed():
    scope = cs.CancelScope()

    def bad(_):
        raise RuntimeError("boom")

    fired = []
    scope.on_cancel(bad)
    scope.on_cancel(lambda s: fired.append(True))
    scope.cancel()  # must not raise
    assert fired == [True]


def test_on_cancel_fires_at_deadline_not_at_checkpoint():
    fired = threading.Event()
    with cs.move_on_after(0.1) as scope:
        scope.on_cancel(lambda s: fired.set())
        # Block without checkpointing: callback must still fire (via the
        # monitor thread) at the deadline.
        assert fired.wait(timeout=5)
        cs.checkpoint()
    assert scope.cancelled_caught


def test_on_cancel_propagates_to_children():
    fired = []
    with cs.CancelScope() as outer:
        with cs.CancelScope() as inner:
            inner.on_cancel(lambda s: fired.append("inner"))
            outer.cancel()
            assert fired == ["inner"]
            cs.checkpoint()
    assert outer.cancelled_caught


# -- guard ------------------------------------------------------------


def test_guard_yields_items_and_cancels():
    seen = []
    with cs.CancelScope() as scope:
        for i in cs.guard(range(100)):
            seen.append(i)
            if i == 4:
                scope.cancel()
    assert seen == [0, 1, 2, 3, 4]
    assert scope.cancelled_caught


def test_guard_passthrough_when_not_cancelled():
    assert list(cs.guard([1, 2, 3])) == [1, 2, 3]


def test_guard_does_not_consume_item_on_cancel():
    # guard() must checkpoint BEFORE advancing the iterator: cancelling
    # mid-loop over a destructive source (queue, cursor) must not pull
    # and silently drop the next item.
    q = queue.Queue()
    for i in range(5):
        q.put(i)
    q.put(None)
    consumed = []
    with cs.CancelScope() as scope:
        for item in cs.guard(iter(q.get_nowait, None)):
            consumed.append(item)
            if item == 1:
                scope.cancel()
    assert scope.cancelled_caught
    assert consumed == [0, 1]
    remaining = []
    while (v := q.get_nowait()) is not None:
        remaining.append(v)
    assert remaining == [2, 3, 4]  # nothing lost


# -- exception passthrough --------------------------------------------


def test_body_exception_propagates_through_cancelled_scope():
    with pytest.raises(ValueError):
        with cs.CancelScope() as scope:
            scope.cancel()
            raise ValueError("boom")
    assert not scope.cancelled_caught


def test_fail_after_body_exception_passthrough_after_expiry():
    # The deadline expires (no checkpoint), then the body raises its own
    # error: fail_after must NOT mask it with TimeoutError.
    with pytest.raises(KeyError):
        with cs.fail_after(0.05):
            time.sleep(0.15)
            raise KeyError("k")


def test_fail_after_expired_without_checkpoint_exits_silently():
    with cs.fail_after(0.05) as scope:
        time.sleep(0.15)  # never checkpoints; body completes normally
    assert scope.cancelled
    assert not scope.cancelled_caught  # and no TimeoutError was raised


# -- deadline mutation ------------------------------------------------


def test_deadline_extend():
    with cs.move_on_after(0.15) as scope:
        scope.deadline = time.monotonic() + 30
        end = time.monotonic() + 0.5
        while time.monotonic() < end:
            cs.checkpoint()
            time.sleep(0.02)
    assert not scope.cancelled
    assert not scope.cancelled_caught


def test_deadline_cleared():
    with cs.move_on_after(0.15) as scope:
        scope.deadline = None
        assert scope.remaining() is None
        end = time.monotonic() + 0.5
        while time.monotonic() < end:
            cs.checkpoint()
            time.sleep(0.02)
    assert not scope.cancelled


def test_deadline_set_before_enter_overrides_timeout():
    scope = cs.CancelScope(timeout=30)
    scope.deadline = time.monotonic() + 0.1
    start = time.monotonic()
    with scope:
        cs.sleep(10)
    assert scope.cancelled_caught
    assert time.monotonic() - start < 5


# -- on_cancel unregister edge cases ----------------------------------


def test_on_cancel_unregister_after_cancel_is_noop():
    scope = cs.CancelScope()
    fired = []
    unreg = scope.on_cancel(lambda s: fired.append(s))
    scope.cancel()
    assert fired == [scope]
    unreg()
    unreg()  # idempotent, must not raise
    unreg2 = scope.on_cancel(lambda s: fired.append(s))  # fires immediately
    unreg2()  # must not raise
    assert len(fired) == 2


# -- concurrency regressions ------------------------------------------


def test_infinite_timeout_does_not_break_deadline_service():
    # Regression: an inf (or > platform time_t) deadline used to make the
    # monitor thread call Condition.wait(inf), which raised OverflowError
    # and killed the thread — stranding every other registered deadline
    # (a fail_after registered while the thread was dying never fired).
    from cancelscope._impl import _service

    with cs.CancelScope(timeout=math.inf):
        # Settle first: the monitor must actually reach the park before
        # its liveness means anything.  Without this the assertion races
        # the thread's death and passes even against the broken version.
        time.sleep(0.25)
        assert _service._thread is not None and _service._thread.is_alive()
        # And the service must still be able to fire other deadlines.
        with pytest.raises(TimeoutError):
            with cs.fail_after(0.1):
                cs.sleep(5)
    assert _service._thread.is_alive()


def test_on_cancel_does_not_fire_after_clean_exit():
    # Regression: __exit__ used to leave callbacks registered, so a late
    # cancel() (e.g. from a thread that doesn't know the scope finished)
    # ran "unblock the body" callbacks after the body had completed.
    fired = []
    with cs.CancelScope() as scope:
        scope.on_cancel(lambda s: fired.append(s))
    scope.cancel()
    assert fired == []
    assert scope.cancelled  # the cancellation itself still takes effect


def test_on_cancel_registered_after_exit_never_fires():
    fired = []
    with cs.CancelScope() as scope:
        pass
    unreg = scope.on_cancel(lambda s: fired.append(s))
    scope.cancel()
    unreg()  # must not raise
    assert fired == []


def test_deadline_expiry_racing_clean_exit_does_not_fire_callback():
    # Regression: two scopes sharing one deadline are popped by the
    # monitor in a single batch, first-registered first.  A's slow
    # callback used to delay B's dispatch past B's clean exit, so B's
    # close-the-socket callback fired ~150ms after B.__exit__ returned.
    deadline = time.monotonic() + 0.05
    fired_after_exit = []
    exited = threading.Event()

    a = cs.CancelScope(deadline=deadline, name="a")
    with a:
        a.on_cancel(lambda s: time.sleep(0.25))

        def worker():
            with cs.CancelScope(deadline=deadline, name="b") as b:
                b.on_cancel(
                    lambda s: fired_after_exit.append(True)
                    if exited.is_set()
                    else None
                )
                end = time.monotonic() + 0.15
                while time.monotonic() < end:
                    time.sleep(0.005)
            exited.set()

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=5)
        time.sleep(0.3)  # let the monitor drain the batch
    assert not t.is_alive()
    assert fired_after_exit == []


def test_deep_nesting_cancel_does_not_hit_recursion_limit():
    # Regression: child propagation recursed once per nesting level, so
    # cancelling the root of a chain deeper than the recursion limit
    # raised RecursionError and left the deepest scopes uncancelled.
    depth = 1500
    with contextlib.ExitStack() as stack:
        root = stack.enter_context(cs.CancelScope())
        deepest = root
        for _ in range(depth):
            deepest = stack.enter_context(cs.CancelScope())
        root.cancel()  # must not raise
        assert deepest.cancelled


def test_deep_nesting_deadline_wakes_sleeper():
    # Deadline-driven variant of the above: the RecursionError used to
    # kill the monitor thread, so the sleeper slept its full duration.
    start = time.monotonic()
    with contextlib.ExitStack() as stack:
        root = stack.enter_context(cs.move_on_after(0.15))
        for _ in range(1500):
            stack.enter_context(cs.CancelScope())
        cs.sleep(10)
    assert root.cancelled_caught
    assert time.monotonic() - start < 5


def test_heap_does_not_pin_exited_scopes_behind_live_entry():
    # Regression: stale heap entries are only popped when they surface at
    # the top, so exited scopes whose deadlines sorted below a live
    # far-future entry were pinned (50k scopes retained in one measured
    # run).  register() now compacts at a doubling threshold.
    from cancelscope._impl import _service

    with cs.CancelScope(timeout=100):  # live entry at the heap top
        for _ in range(2000):
            with cs.move_on_after(200):  # sorts below the live entry
                pass
        with _service._cond:
            heap_len = len(_service._heap)
    assert heap_len < 300


def test_sleep_does_not_busy_poll_after_bound_ancestor_exits():
    # Regression: via bind(), a parent can exit (in its own thread) while
    # a worker still sleeps under it.  The parent's stale deadline, once
    # in the past, used to pin sleep() into a ~1ms poll loop (hundreds of
    # wakeups per second).
    wait_calls = []

    def worker():
        with cs.CancelScope() as child:
            real_wait = child._event.wait

            def counting_wait(timeout=None):
                wait_calls.append(timeout)
                return real_wait(timeout)

            child._event.wait = counting_wait
            cs.sleep(0.5)

    parent = cs.CancelScope(timeout=0.1)
    with parent:
        t = threading.Thread(target=cs.bind(worker))
        t.start()
        time.sleep(0.02)
    t.join(timeout=5)
    assert not t.is_alive()
    assert len(wait_calls) < 20


def test_baseexception_callback_does_not_interrupt_child_cancellation():
    seen = []
    with cs.CancelScope() as parent:
        with cs.CancelScope() as child:
            parent.on_cancel(lambda scope: (_ for _ in ()).throw(SystemExit("callback")))
            parent.on_cancel(lambda scope: seen.append("second callback"))
            with pytest.raises(SystemExit, match="callback"):
                parent.cancel()
            assert child.cancelled
            assert seen == ["second callback"]
