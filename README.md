# local-snapshotter-context

Captures a context-local dict of key-value pairs and returns a serialized token string suitable for propagation across process boundaries.

```python
from local_snapshotter_context import Snapshotter

s = Snapshotter()
s.set("request_id", "abc-123")
s.set("user", {"id": 7, "roles": ["ops"]})

token = s.snapshot()          # 'lsc1.<base64url payload>'
# ...pass token over a queue, HTTP header, log line, etc....

s2 = Snapshotter()
ctx = s2.restore(token)        # {'request_id': 'abc-123', 'user': {'id': 7, 'roles': ['ops']}}
s2.get("user")["id"]           # 7

Snapshotter.peek(token)        # same dict, without touching thread-local state
```

## Why this exists

When a request flows through threads or processes, you often want to carry a small bag of diagnostic key-value pairs (correlation IDs, tenant, user id) without threading an explicit argument through every function. This library stores that bag in `threading.local` so each thread sees its own context, and produces a self-contained token you can hand to a worker process, shove into an AMQP header, or write to a log.

The trade-off: values must be JSON-serializable (`dict`, `list`, `str`, `int`, `float`, `bool`, `None`, nested). Bytes and arbitrary objects are deliberately unsupported — any encoding convention for them would surprise at least one caller, so the contract stays narrow. The token is zlib-compressed JSON with a `lsc1.` version prefix, base64url-encoded so it is safe in URLs and headers without further escaping.

## Edge you will hit

`float('nan')` and `float('inf')` are accepted by `json.dumps` (it emits `NaN`/`Infinity` tokens) but are not valid JSON and will not round-trip through `restore`. If you need to carry numeric values, stick to finite floats or integers. If this becomes a problem in practice, validate before calling `set`.

The context is thread-local, not asyncio-task-local. Two tasks sharing one thread (the default event-loop case) will see the same context. If you need per-task isolation, run each task on its own thread or wrap context switches explicitly with `snapshot`/`restore`.

## Exported names

- `Snapshotter` — the class.
- `SnapshotError` — raised by `restore` and `peek` on malformed tokens.

`Snapshotter` methods: `set(key, value)`, `get(key, default=None)`, `remove(key)`, `clear()`, `snapshot() -> str`, `restore(token) -> dict`, `context() -> dict`, and the static `peek(token) -> dict`.
