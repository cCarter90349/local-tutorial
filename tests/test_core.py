import threading
import unittest

from local_snapshotter_context import Snapshotter, SnapshotError


class SnapshotterTests(unittest.TestCase):
    def test_set_and_get_roundtrip(self):
        s = Snapshotter()
        s.set("request_id", "abc-123")
        self.assertEqual(s.get("request_id"), "abc-123")

    def test_get_missing_key_returns_default(self):
        s = Snapshotter()
        self.assertIsNone(s.get("nope"))
        self.assertEqual(s.get("nope", "fallback"), "fallback")

    def test_set_none_is_a_value_not_deletion(self):
        s = Snapshotter()
        s.set("k", None)
        self.assertIsNone(s.get("k"))
        self.assertIn("k", s.context())

    def test_remove_then_clear(self):
        s = Snapshotter()
        s.set("a", 1)
        s.set("b", 2)
        s.remove("a")
        self.assertNotIn("a", s.context())
        self.assertEqual(s.get("b"), 2)
        s.clear()
        self.assertEqual(s.context(), {})

    def test_remove_nonexistent_is_noop(self):
        s = Snapshotter()
        s.remove("ghost")  # must not raise
        self.assertEqual(s.context(), {})

    def test_snapshot_and_restore_roundtrip(self):
        s = Snapshotter()
        s.set("user", "alice")
        s.set("roles", ["admin", "ops"])
        s.set("meta", {"tenant": "t1", "flags": {"vip": True}})
        token = s.snapshot()
        self.assertTrue(token.startswith("lsc1."))
        # Simulate crossing a process boundary: fresh snapshotter/thread.
        s2 = Snapshotter()
        restored = s2.restore(token)
        self.assertEqual(restored, {"user": "alice",
                                    "roles": ["admin", "ops"],
                                    "meta": {"tenant": "t1", "flags": {"vip": True}}})
        self.assertEqual(s2.get("user"), "alice")

    def test_restore_replaces_existing_context(self):
        s = Snapshotter()
        s.set("stale", "data")
        s.set("keep", "no")
        orig = Snapshotter()
        orig.set("user", "bob")
        token = orig.snapshot()
        restored = s.restore(token)
        self.assertEqual(restored, {"user": "bob"})
        self.assertNotIn("stale", s.context())
        self.assertNotIn("keep", s.context())

    def test_snapshot_is_deterministic(self):
        s = Snapshotter()
        s.set("b", 2)
        s.set("a", 1)
        t1 = s.snapshot()
        t2 = s.snapshot()
        self.assertEqual(t1, t2)

    def test_peek_does_not_mutate_context(self):
        s = Snapshotter()
        s.set("x", 1)
        token = s.snapshot()
        result = Snapshotter.peek(token)
        self.assertEqual(result, {"x": 1})
        # context unchanged by peek
        self.assertEqual(s.context(), {"x": 1})
        # peek returned an independent copy
        result["x"] = 999
        self.assertEqual(s.get("x"), 1)

    def test_restore_returns_independent_copy(self):
        s = Snapshotter()
        s.set("k", [1, 2])
        token = s.snapshot()
        s2 = Snapshotter()
        restored = s2.restore(token)
        restored["k"].append(3)
        self.assertEqual(s2.get("k"), [1, 2])

    def test_threads_are_isolated(self):
        s = Snapshotter()
        s.set("who", "main")
        errors = []

        def worker():
            try:
                # A fresh thread sees an empty context, not the main thread's.
                self.assertEqual(s.context(), {})
                s.set("who", "worker")
                self.assertEqual(s.get("who"), "worker")
            except Exception as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        t = threading.Thread(target=worker)
        t.start()
        t.join()
        self.assertEqual(errors, [])
        self.assertEqual(s.get("who"), "main")

    def test_unicode_keys_and_values_roundtrip(self):
        s = Snapshotter()
        s.set("café", "naïve — ☕")
        token = s.snapshot()
        s2 = Snapshotter()
        self.assertEqual(s2.restore(token), {"café": "naïve — ☕"})

    def test_integer_and_bool_values_roundtrip(self):
        s = Snapshotter()
        s.set("count", 42)
        s.set("flag", True)
        token = s.snapshot()
        s2 = Snapshotter()
        restored = s2.restore(token)
        self.assertEqual(restored["count"], 42)
        self.assertIs(restored["flag"], True)

    def test_empty_context_snapshots_to_valid_token(self):
        s = Snapshotter()
        token = s.snapshot()
        self.assertTrue(token.startswith("lsc1."))
        s2 = Snapshotter()
        self.assertEqual(s2.restore(token), {})

    # --- error paths -----------------------------------------------------

    def test_set_non_string_key_raises_type_error(self):
        s = Snapshotter()
        with self.assertRaises(TypeError):
            s.set(123, "x")

    def test_restore_garbage_raises_snapshot_error(self):
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.restore("lsc1.not-real-base64@@@")

    def test_restore_wrong_prefix_raises_snapshot_error(self):
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.restore("v2.something")

    def test_restore_non_string_raises_snapshot_error(self):
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.restore(12345)

    def test_restore_payload_that_is_not_a_json_object(self):
        import base64
        import json
        import zlib
        # Encode a JSON array, which is valid JSON but not a dict.
        raw = json.dumps([1, 2, 3]).encode("utf-8")
        payload = base64.urlsafe_b64encode(zlib.compress(raw)).rstrip(b"=").decode("ascii")
        token = f"lsc1.{payload}"
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.restore(token)

    def test_peek_garbage_raises_snapshot_error(self):
        with self.assertRaises(SnapshotError):
            Snapshotter.peek("lsc1.@@@")


if __name__ == "__main__":
    unittest.main()
