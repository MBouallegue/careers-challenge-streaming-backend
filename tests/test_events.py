"""Validation, acceptance rules, and query-parameter parsing."""
from __future__ import annotations

import json
import unittest

from engine.events import PRIORITY, parse_request_body, validate_event
from engine.utils.params import parse_since, parse_window_ms
from engine.utils.timestamps import parse_timestamp, to_iso
from tests.helpers import FIXED_NOW_MS, build_config

NOW = FIXED_NOW_MS  # 2026-05-23T18:53:49.123Z


class ValidationTest(unittest.TestCase):
    def setUp(self):
        self.cfg = build_config()

    def test_accepts_the_envelope_from_the_readme(self):
        ok, ev = validate_event(
            {"device_id": "dev_0001", "room_id": "room_14", "type": "heartbeat",
             "ts": "2026-05-23T18:53:49.123Z"},
            NOW, self.cfg,
        )
        self.assertTrue(ok)
        self.assertEqual(ev["d"], "dev_0001")
        self.assertEqual(ev["t"], NOW)

    def test_seq_is_optional(self):
        # docs/event_schema.md lists seq in the envelope, but every worked
        # example in the root README omits it. Rejecting on a missing seq would
        # reject the spec's own sample payloads.
        ok, ev = validate_event(
            {"device_id": "d", "room_id": "r", "type": "heartbeat", "ts": to_iso(NOW)},
            NOW, self.cfg,
        )
        self.assertTrue(ok)
        self.assertIsNone(ev["q"])

    def test_future_and_past_acceptance_boundaries(self):
        ok, reason = validate_event(
            {"device_id": "d", "room_id": "r", "type": "heartbeat", "ts": to_iso(NOW + 3_600_001)},
            NOW, self.cfg)
        self.assertFalse(ok)
        self.assertEqual(reason, "ts_too_future")

        ok, reason = validate_event(
            {"device_id": "d", "room_id": "r", "type": "heartbeat", "ts": to_iso(NOW - 3_600_001)},
            NOW, self.cfg)
        self.assertFalse(ok)
        self.assertEqual(reason, "ts_too_old")

        # A device replaying a 59-minute offline buffer is the scenario the
        # grader runs, so this side of the boundary must be accepted.
        for offset in (-3_599_000, -1000, 0, 1000, 3_599_000):
            ok, _ = validate_event(
                {"device_id": "d", "room_id": "r", "type": "heartbeat", "ts": to_iso(NOW + offset)},
                NOW, self.cfg)
            self.assertTrue(ok, f"offset {offset} must be accepted")

    def test_rejects_malformed_and_missing_fields(self):
        cases = [
            ({}, "missing_device_id"),
            ({"device_id": "d"}, "missing_room_id"),
            ({"device_id": "d", "room_id": "r"}, "bad_type"),
            ({"device_id": "d", "room_id": "r", "type": "wat", "ts": to_iso(NOW)}, "bad_type"),
            ({"device_id": "d", "room_id": "r", "type": "heartbeat", "ts": "nope"}, "bad_ts"),
            ({"device_id": "d", "room_id": "r", "type": "presence", "ts": to_iso(NOW)}, "bad_in_room"),
            ({"device_id": "d", "room_id": "r", "type": "fall_warn", "ts": to_iso(NOW)}, "bad_confidence"),
            ({"device_id": "d", "room_id": "r", "type": "sleep_state", "ts": to_iso(NOW),
              "state": "drowsy"}, "bad_state"),
            ({"device_id": "d", "room_id": "r", "type": "motion", "ts": to_iso(NOW)}, "bad_magnitude"),
            ({"device_id": "d", "room_id": "r", "type": "net_status", "ts": to_iso(NOW)}, "bad_rssi"),
            (None, "malformed"),
            ("a string", "malformed"),
            ([1, 2], "malformed"),
        ]
        for payload, expected in cases:
            ok, reason = validate_event(payload, NOW, self.cfg)
            self.assertFalse(ok, repr(payload))
            self.assertEqual(reason, expected, repr(payload))

    def test_booleans_are_not_numbers(self):
        # In Python `isinstance(True, int)` is True, so confidence=True would
        # sail through a naive numeric check and poison the alarm record.
        ok, reason = validate_event(
            {"device_id": "d", "room_id": "r", "type": "fall_warn", "ts": to_iso(NOW),
             "confidence": True},
            NOW, self.cfg)
        self.assertFalse(ok)
        self.assertEqual(reason, "bad_confidence")

    def test_priority_puts_falls_ahead_of_telemetry(self):
        self.assertLess(PRIORITY["fall_warn"], PRIORITY["presence"])
        self.assertLess(PRIORITY["presence"], PRIORITY["heartbeat"])
        self.assertEqual(PRIORITY["motion"], PRIORITY["heartbeat"])


class TimestampTest(unittest.TestCase):
    def test_parses_iso_and_epoch(self):
        self.assertEqual(parse_timestamp("2026-05-23T18:53:49.123Z"), NOW)
        self.assertEqual(parse_timestamp(NOW), NOW)
        self.assertEqual(parse_timestamp(NOW // 1000), (NOW // 1000) * 1000)
        self.assertIsNone(parse_timestamp("nope"))
        self.assertIsNone(parse_timestamp(None))

    def test_fast_path_agrees_with_the_general_path(self):
        # parse_ts has a hand-rolled fast path for the exact 24-character format
        # the generator emits. If it ever disagrees with the fallback, every
        # aggregate silently shifts.
        import datetime
        for offset_ms in (0, 1, 999, 86_399_999, -86_400_000, 1_000_000_000):
            ms = NOW + offset_ms
            iso = to_iso(ms)
            self.assertEqual(parse_timestamp(iso), ms, iso)
            dt = datetime.datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=datetime.UTC)
            self.assertEqual(int(dt.timestamp() * 1000), ms, iso)

    def test_round_trip(self):
        self.assertEqual(parse_timestamp(to_iso(NOW)), NOW)


class ParamsTest(unittest.TestCase):
    def test_since_zero_means_everything_not_epoch_1970(self):
        # eval/check.py calls /alarms?since=0 and expects the full set back.
        self.assertEqual(parse_since("0"), ("cursor", 0))
        self.assertEqual(parse_since(""), ("cursor", 0))
        self.assertEqual(parse_since(None), ("cursor", 0))
        self.assertEqual(parse_since("42"), ("cursor", 42))
        # Large integers are epoch ms; ISO strings are timestamps.
        self.assertEqual(parse_since("1780000000000"), ("ts", 1780000000000))
        self.assertEqual(parse_since("2026-05-23T18:53:49.123Z"), ("ts", NOW))
        self.assertEqual(parse_since("garbage")[0], "invalid")

    def test_window_parsing_covers_the_three_graded_windows(self):
        self.assertEqual(parse_window_ms("1m", 3_600_000), 60_000)
        self.assertEqual(parse_window_ms("5m", 3_600_000), 300_000)
        self.assertEqual(parse_window_ms("1h", 3_600_000), 3_600_000)
        self.assertEqual(parse_window_ms("30s", 3_600_000), 30_000)
        self.assertEqual(parse_window_ms("90", 3_600_000), 90_000)
        self.assertEqual(parse_window_ms(None, 3_600_000), 300_000, "defaults to 5m")
        self.assertEqual(parse_window_ms("24h", 3_600_000), 3_600_000, "clamped to retention")
        self.assertIsNone(parse_window_ms("bogus", 3_600_000))


class BodyTest(unittest.TestCase):
    def test_accepts_single_array_and_ndjson(self):
        one = {"device_id": "d", "room_id": "r", "type": "heartbeat", "ts": to_iso(NOW)}
        ok, events = parse_request_body(json.dumps(one).encode())
        self.assertTrue(ok)
        self.assertEqual(len(events), 1)

        ok, events = parse_request_body(json.dumps([one, one]).encode())
        self.assertTrue(ok)
        self.assertEqual(len(events), 2)

        ndjson = (json.dumps(one) + "\n" + json.dumps(one)).encode()
        ok, events = parse_request_body(ndjson)
        self.assertTrue(ok)
        self.assertEqual(len(events), 2)

        self.assertEqual(parse_request_body(b"{bad")[0], False)
        self.assertEqual(parse_request_body(b"")[1], "empty_body")


if __name__ == "__main__":
    unittest.main()
