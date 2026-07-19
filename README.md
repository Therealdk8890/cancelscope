# cancelscope

**Cooperative cancellation, timeouts, and cancel scopes for synchronous Python.**

Python has no good story for cancelling synchronous work: `KeyboardInterrupt` is a
signal-handler hack, threads can't be killed, and trio's lovely cancel scopes are
async-only. `cancelscope` brings trio-style cancellation semantics — scopes,
checkpoints, deadlines, shielding — to plain blocking code and threads.

- **Cooperative:** nothing is interrupted preemptively. Code observes cancellation
  at explicit `checkpoint()` calls (or inside `sleep()` / `guard()`), so you are
  never left with a lock half-held or a file half-written.
- **Thread-safe:** cancel a scope from any thread; workers observe it at their
  next checkpoint. `sleep()` wakes immediately.
- **Real deadlines:** timeouts are enforced by a shared monitor thread, so a
  deadline fires *at* the deadline — waking sleepers and running `on_cancel`
  callbacks — not merely at the next checkpoint.
- **Zero dependencies**, fully typed, works on Python 3.9+.

## Install

```bash
pip install cancelscope
```

## Quickstart

```python
import cancelscope as cs

# A timeout around blocking work
with cs.move_on_after(5) as scope:
    for row in cs.guard(rows):          # checkpoint before each item
        process(row)
if scope.cancelled_caught:
    print("timed out, partial results kept")

# Or raise on timeout
with cs.fail_after(5):                  # raises TimeoutError if too slow
    crunch()
```

Cancel from another thread (the classic "Stop" button):

```python
scope = cs.CancelScope()

def worker():
    with scope:
        while True:
            cs.checkpoint()             # raises cs.Cancelled once cancelled
            step()

threading.Thread(target=worker).start()
...
scope.cancel("user clicked stop")       # from any thread
```

Propagate a scope into a thread pool:

```python
with cs.CancelScope() as scope:
    # NB: bind() captures the scope that is current at the moment you
    # call it — call it *inside* the `with` block, or it captures nothing
    # and the worker silently becomes uncancellable.
    futures = [pool.submit(cs.bind(handle), job) for job in jobs]
    ...
    scope.cancel()                      # every worker's next checkpoint raises
```

Unblock third-party blocking calls via `on_cancel`:

```python
with cs.fail_after(10) as scope:
    sock = socket.create_connection(addr)
    # shutdown() reliably wakes a thread blocked in recv() on all major
    # platforms (close() does not, and races on the file descriptor).
    scope.on_cancel(lambda s: sock.shutdown(socket.SHUT_RDWR))
    data = sock.recv(65536)   # returns b'' once shut down
    cs.checkpoint()           # surfaces the timeout as TimeoutError
```

The callback only *unblocks* the call — after `shutdown()`, `recv()`
returns `b''` (or raises `OSError` on some paths). The `checkpoint()`
after it is what turns the cancellation into the `TimeoutError` that
`fail_after` promises; without it the block would end as a phantom EOF.

## Semantics (the trio model, sync)

- `CancelScope(timeout=..., deadline=..., shield=..., name=...)` is a context
  manager. Cancelling a scope cancels everything nested inside it.
- `checkpoint()` raises `Cancelled` — a `BaseException`, so stray
  `except Exception` blocks can't eat it — bound to the *outermost* cancelled
  scope. That scope's `__exit__` catches it and sets `cancelled_caught`;
  intermediate scopes let it pass through.
- `shield=True` detaches a scope from its parent: cleanup code inside a shield
  keeps running even while everything around it is being cancelled.
- Deadlines are absolute `time.monotonic()` times; `scope.deadline` is readable
  and writable while the scope is active (extend or tighten at will).
- If the body finishes before anyone checkpoints, cancellation simply has no
  effect — cooperative means never yanking the rug.

## API

| Name | What it does |
| --- | --- |
| `CancelScope(timeout=, deadline=, shield=, name=)` | the scope context manager |
| `scope.cancel(reason=None)` | cancel from any thread; idempotent |
| `scope.cancelled` / `scope.cancel_called` / `scope.cancelled_caught` | state |
| `scope.deadline` / `scope.remaining()` | inspect or move the deadline |
| `scope.on_cancel(fn)` | callback on cancellation; returns an unregister function |
| `checkpoint()` | raise `Cancelled` here if cancelled (cheap when not) |
| `sleep(seconds)` | `time.sleep` that wakes instantly on cancellation |
| `guard(iterable)` | checkpoint before each item |
| `bind(fn)` | carry the current scope into another thread |
| `move_on_after(seconds)` | timeout scope that exits silently |
| `fail_after(seconds)` | timeout scope that raises `TimeoutError` |
| `current_scope()` / `is_cancelled()` | introspection |

## Caveats

- This is *cooperative* cancellation: code that never checkpoints (a C
  extension crunching for minutes, a blocking socket read) is not interrupted.
  Use `on_cancel` to unblock such calls out-of-band — and note the callback
  only unblocks the call; checkpoint afterwards to raise. Prefer wake-up
  mechanisms like `socket.shutdown()` over `close()`, which does not wake a
  blocked `recv()` on Linux and races on the descriptor.
- The first deadline lazily starts one daemon monitor thread for the whole
  process; it stays alive thereafter.
- `on_cancel` callbacks run in whichever thread triggers the cancellation
  (the monitor thread, for deadlines): keep them short and thread-safe.

## License

MIT
