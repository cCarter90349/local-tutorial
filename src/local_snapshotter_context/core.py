import base64
import json
import zlib
from threading import local
from typing import Any, Dict, Mapping, Optional


class SnapshotError(Exception):
    """Raised when a snapshot token is malformed or cannot be restored.

    A dedicated exception type lets callers distinguish context-restoration
    failures from unrelated ValueError/KeyError noise in their own code.
    """


class Snapshotter:
    """Captures a context-local dict of key-value pairs and returns a
    serialized token string suitable for propagation across process boundaries.

    Context-locality is implemented with threading.local so each thread (or
    asyncio task that runs on its own thread) sees an independent snapshot.
    The token is a compact, self-describing string encoding the JSON form of
    the dict, compressed with zlib and base64url-encoded so it is safe to
    carry in headers, query parameters, or log lines.

    Values must be JSON-serializable: dict, list, str, int, float (non-NaN,
    non-infinity), bool, None, or nested combinations thereof. Bytes are not
    supported because round-tripping bytes through JSON requires a convention
    that would surprise callers; keeping the contract narrow avoids that.
    """

    __slots__ = ("_local",)

    def __init__(self) -> None:
        # threading.local gives us per-thread isolation without any shared
        # mutable global state. Each thread starts with no context attached
        # until set_context is called.
        self._local = local()

    # --- internal helper -------------------------------------------------

    def _get_context(self) -> Dict[str, Any]:
        ctx = getattr(self._local, "context", None)
        if ctx is None:
            ctx = {}
            self._local.context = ctx
        return ctx

    # --- public API ------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        """Set a single key in the current thread's context.

        Setting a key to None is treated as an explicit value, not as a
        deletion, because callers who want deletion have a dedicated
        ``remove`` method and conflating the two is a common source of bugs.
        """
        if not isinstance(key, str):
            raise TypeError(f"key must be str, got {type(key).__name__}")
        ctx = self._get_context()
        ctx[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value for *key* in the current context, or *default*."""
        ctx = self._get_context()
        return ctx.get(key, default)

    def remove(self, key: str) -> None:
        """Remove *key* from the current context.

        Removing a key that was never set is a no-op rather than an error;
        this mirrors dict.pop(key, None) and avoids forcing callers to guard
        with a membership check.
        """
        ctx = self._get_context()
        ctx.pop(key, None)

    def clear(self) -> None:
        """Discard every key in the current thread's context."""
        self._local.context = {}

    def snapshot(self) -> str:
        """Serialize the current context into a propagation token.

        Returns a string of the form ``lsc1.<base64url payload>`` where the
        payload is zlib-compressed UTF-8 JSON. The version prefix lets us
        evolve the encoding later without ambiguity.
        """
        ctx = self._get_context()
        raw = json.dumps(ctx, sort_keys=True, separators=(",", ":")).encode("utf-8")
        compressed = zlib.compress(raw, level=9)
        payload = base64.urlsafe_b64encode(compressed).rstrip(b"=").decode("ascii")
        return f"lsc1.{payload}"

    def restore(self, token: str) -> Dict[str, Any]:
        """Restore a context from a token produced by :meth:`snapshot`.

        Replaces the current thread's context entirely with the decoded
        contents. Raises :class:`SnapshotError` if the token is malformed,
        has the wrong version prefix, or contains invalid JSON.
        """
        if not isinstance(token, str):
            raise SnapshotError("token must be a string")
        prefix = "lsc1."
        if not token.startswith(prefix):
            raise SnapshotError("token is missing the expected version prefix")
        payload = token[len(prefix):]
        # Re-pad the base64url string; rstrip removed trailing '=' during
        # encoding and urlsafe_b64decode is strict about padding.
        pad = (-len(payload)) % 4
        try:
            compressed = base64.urlsafe_b64decode(payload + ("=" * pad))
            raw = zlib.decompress(compressed)
            ctx = json.loads(raw.decode("utf-8"))
        except (ValueError, zlib.error, UnicodeDecodeError) as exc:
            raise SnapshotError(f"failed to decode token: {exc}") from exc
        if not isinstance(ctx, dict):
            raise SnapshotError("decoded token payload is not a JSON object")
        # Store an independent copy internally and return a separate copy so
        # mutations through the returned dict don't leak into thread-local
        # state (and vice versa).
        self._local.context = json.loads(json.dumps(ctx))
        return json.loads(json.dumps(ctx))

    def context(self) -> Mapping[str, Any]:
        """Return a read-only view of the current thread's context."""
        return dict(self._get_context())

    @staticmethod
    def peek(token: str) -> Dict[str, Any]:
        """Decode a token without touching any thread-local state.

        Useful for inspecting or validating a token in a logging pipeline
        where you don't want to disturb the active context.
        """
        if not isinstance(token, str):
            raise SnapshotError("token must be a string")
        prefix = "lsc1."
        if not token.startswith(prefix):
            raise SnapshotError("token is missing the expected version prefix")
        payload = token[len(prefix):]
        pad = (-len(payload)) % 4
        try:
            compressed = base64.urlsafe_b64decode(payload + ("=" * pad))
            raw = zlib.decompress(compressed)
            ctx = json.loads(raw.decode("utf-8"))
        except (ValueError, zlib.error, UnicodeDecodeError) as exc:
            raise SnapshotError(f"failed to decode token: {exc}") from exc
        if not isinstance(ctx, dict):
            raise SnapshotError("decoded token payload is not a JSON object")
        return dict(ctx)
