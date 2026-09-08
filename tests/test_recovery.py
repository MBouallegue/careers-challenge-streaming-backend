"""Restart correctness: snapshot, write-ahead log, and replay.

These exercise the engine directly rather than over HTTP, because the property
under test is "state is a deterministic fold over the log" and that is an engine
invariant. ``client/restart_check.py`` covers the same ground end-to-end against
a real ``kill -9``.
"""

from __future__ import annotations

import json
import unittest

from engine.service import StreamingEngine
from engine.storage.wal import segment_filename
from tests.helpers import (
    FIXED_NOW_MS,
    build_temp_config,
    fall_event,
    heartbeat_event,
    presence_event,
    remove_data_dir,
)

NOW = FIXED_NOW_MS


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.config = build_temp_config()
        self.addCleanup(remove_data_dir, self.config)

    def _start(self) -> StreamingEngine:
        engine = StreamingEngine(self.config)
        engine.recover()
        return engine

    def _populate(self, engine: StreamingEngine) -> None:
        """A representative slice of traffic: heartbeats, presence, a jittered fall."""
        for i in range(120):
            engine.accept(heartbeat_event("dev_1", NOW - i * 1000), NOW)
        engine.accept(presence_event("room_1", NOW - 60_000, True), NOW)
        engine.accept(presence_event("room_1", NOW - 30_000, False), NOW)
        # One physical fall, delivered as three jitter copies.
        for seq in (2, 3, 1):
            engine.accept(fall_event("dev_1", NOW - 5_000, seq), NOW)
        engine.drain_all()

    def _assert_state_matches(self, engine: StreamingEngine, expected: dict) -> None:
        self.assertEqual(len(engine.alarms), expected["alarms"])
        self.assertEqual(
            engine.devices.health("dev_1", NOW)["heartbeats_5m"], expected["heartbeats"]
        )
        self.assertAlmostEqual(
            engine.rooms.occupancy("room_1", 60_000, NOW)["occupied_seconds"],
            expected["occupied_seconds"],
            places=3,
        )

    def _expected(self, engine: StreamingEngine) -> dict:
        return {
            "alarms": len(engine.alarms),
            "heartbeats": engine.devices.health("dev_1", NOW)["heartbeats_5m"],
            "occupied_seconds": engine.rooms.occupancy("room_1", 60_000, NOW)["occupied_seconds"],
        }

    def test_recovers_from_snapshot_plus_wal(self):
        first = self._start()
        self._populate(first)
        first.write_snapshot()
        expected = self._expected(first)
        first.wal.close()

        second = self._start()
        self._assert_state_matches(second, expected)
        self.assertIsNotNone(second.recovery["snapshot"])

    def test_recovers_from_the_wal_alone(self):
        """No snapshot ever written: the log must be sufficient by itself."""
        first = self._start()
        self._populate(first)
        first.wal.flush()
        expected = self._expected(first)
        first.wal.close()

        second = self._start()
        self.assertIsNone(second.recovery["snapshot"])
        self.assertGreater(second.recovery["replayed"], 0)
        self._assert_state_matches(second, expected)

    def test_events_accepted_but_not_yet_applied_survive_a_crash(self):
        """The reason the WAL is written at accept time rather than apply time.

        Events are accepted and left sitting in the queue, then the process
        "dies" without draining. A log written at apply time would lose all of
        them; ours does not.
        """
        first = self._start()
        for seq in (2, 3, 1):
            first.accept(fall_event("dev_1", NOW - 5_000, seq), NOW)
        self.assertGreater(first.queue.size, 0, "events must still be queued")
        first.wal.flush()          # group commit, as the fsync timer would do
        first.wal.close()          # no drain, no snapshot: simulate a hard kill

        second = self._start()
        self.assertEqual(len(second.alarms), 1, "the fall survived and still deduplicates")

    def test_replay_overlap_does_not_duplicate_alarms(self):
        """Recovery replays a conservative, overlapping slice of the log.

        That is only safe because applying an event is idempotent. Here the
        snapshot is deliberately rewound to the start of the log so every event
        is replayed on top of state that already contains it.
        """
        first = self._start()
        self._populate(first)
        first.write_snapshot()
        expected = self._expected(first)

        snapshot_path = first.snapshots.current_path
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot["wal_pos"] = {"seg": 1, "off": 0}
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        first.wal.close()

        second = self._start()
        self.assertGreater(second.recovery["replayed"], 0, "the whole log was replayed")
        self._assert_state_matches(second, expected)

    def test_torn_tail_record_is_skipped_not_fatal(self):
        """A hard kill can leave a half-written final line in the log."""
        first = self._start()
        self._populate(first)
        first.wal.flush()
        expected = self._expected(first)
        first.wal.close()

        segment = first.wal.directory / segment_filename(first.wal.segment)
        with segment.open("ab") as handle:
            handle.write(b'{"d":"dev_1","r":"room_1","y":"heart')  # torn mid-write

        second = self._start()
        self.assertEqual(second.recovery["torn_records_skipped"], 1)
        self._assert_state_matches(second, expected)

    def test_corrupt_snapshot_falls_back_to_the_previous_generation(self):
        first = self._start()
        self._populate(first)
        first.write_snapshot()   # generation 1
        first.write_snapshot()   # generation 2; generation 1 becomes .prev
        expected = self._expected(first)
        first.wal.close()

        first.snapshots.current_path.write_text("{ truncated", encoding="utf-8")

        second = self._start()
        self.assertTrue(second.recovery["snapshot"].endswith("snapshot.prev.json"))
        self._assert_state_matches(second, expected)

    def test_alarm_cursors_are_stable_across_restart(self):
        """A subscriber's resume cursor must still mean the same thing."""
        first = self._start()
        self._populate(first)
        for offset in (60_000, 120_000, 180_000):
            first.accept(fall_event("dev_2", NOW - offset, 1), NOW)
        first.drain_all()
        first.write_snapshot()
        before = [(a["event_id"], a["cursor"]) for a in first.alarms.log]
        first.wal.close()

        second = self._start()
        after = [(a["event_id"], a["cursor"]) for a in second.alarms.log]
        self.assertEqual(before, after)

    def test_snapshot_truncates_superseded_wal_segments(self):
        engine = self._start()
        self._populate(engine)
        engine.write_snapshot()
        self.assertEqual(len(engine.wal.segments()), 1)
        engine.wal.close()

    def test_recovering_an_empty_data_dir_is_a_clean_start(self):
        engine = self._start()
        self.assertEqual(engine.recovery["replayed"], 0)
        self.assertEqual(len(engine.alarms), 0)
        self.assertIsNone(engine.recovery["snapshot"])


if __name__ == "__main__":
    unittest.main()
