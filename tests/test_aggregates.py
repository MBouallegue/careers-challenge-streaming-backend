"""Room occupancy and device health, including the late-event repair cases."""
from __future__ import annotations

import unittest

from engine.aggregates import DeviceStore, RoomStore
from engine.events import validate_event
from engine.queues import PriorityQueue
from tests.helpers import (
    FIXED_NOW_MS,
    build_config,
    heartbeat_event,
    normalised,
    presence_event,
)

NOW = FIXED_NOW_MS
MIN = 60_000


def ev(raw, cfg):
    ok, result = validate_event(raw, NOW, cfg)
    assert ok, result
    return result


class OccupancyTest(unittest.TestCase):
    def setUp(self):
        self.cfg = build_config()
        self.rooms = RoomStore(self.cfg)

    def apply(self, ts, in_room, room="room_1"):
        self.rooms.apply(ev(presence_event(room, ts, in_room), self.cfg))

    def test_occupancy_is_time_integrated_not_event_counted(self):
        # Occupied for the first 30s of the last minute, then vacated.
        self.apply(NOW - 60_000, True)
        self.apply(NOW - 30_000, False)
        result = self.rooms.occupancy("room_1", MIN, NOW)
        self.assertAlmostEqual(result["occupied_pct"], 0.5, places=4)
        self.assertAlmostEqual(result["occupied_seconds"], 30.0, places=3)
        self.assertFalse(result["in_room"])

    def test_repeated_identical_states_do_not_inflate_occupancy(self):
        # The generator emits in_room as a coin flip, so identical consecutive
        # states are common. An implementation that counts events instead of
        # integrating time reports nonsense here.
        for i in range(10):
            self.apply(NOW - 60_000 + i * 1000, True)
        result = self.rooms.occupancy("room_1", MIN, NOW)
        self.assertAlmostEqual(result["occupied_pct"], 1.0, places=4)

    def test_state_carries_into_the_window(self):
        # The only presence event is an hour before the window opens. The room
        # is still occupied for the whole window.
        self.apply(NOW - 3_600_000, True)
        result = self.rooms.occupancy("room_1", MIN, NOW)
        self.assertAlmostEqual(result["occupied_pct"], 1.0, places=4)

    def test_unknown_before_window_counts_as_unoccupied(self):
        self.apply(NOW - 10_000, False)
        result = self.rooms.occupancy("room_1", MIN, NOW)
        self.assertEqual(result["occupied_pct"], 0.0)

    def test_late_event_repairs_history(self):
        """The graded scenario: a device replays a 20-minute offline buffer.

        The room was actually occupied during the gap, so the 1h window must
        reflect that after the replay lands.
        """
        self.apply(NOW - 10_000, False)
        before = self.rooms.occupancy("room_1", 3_600_000, NOW)["occupied_seconds"]

        # Now the buffered truth arrives, out of order and 20 minutes late.
        self.apply(NOW - 40 * MIN, True)
        self.apply(NOW - 20 * MIN, False)

        after = self.rooms.occupancy("room_1", 3_600_000, NOW)["occupied_seconds"]
        self.assertAlmostEqual(before, 0.0, places=3)
        self.assertAlmostEqual(after, 20 * 60.0, places=3, msg="20 minutes of occupancy recovered")

    def test_out_of_order_insert_keeps_the_timeline_sorted(self):
        for ts in (NOW - 10_000, NOW - 50_000, NOW - 30_000, NOW - 20_000):
            self.apply(ts, True)
        room = self.rooms.rooms["room_1"]
        self.assertEqual(room.ts, sorted(room.ts))

    def test_same_timestamp_last_writer_wins(self):
        self.apply(NOW - 30_000, True)
        self.apply(NOW - 30_000, False)
        room = self.rooms.rooms["room_1"]
        self.assertEqual(len(room.ts), 1)
        self.assertFalse(room.val[0])

    def test_latest_presence_by_ts_wins_for_current_state(self):
        # Two devices in one room with skewed clocks. The spec says ts is
        # authoritative, so the later ts wins even if it arrived first.
        self.apply(NOW - 1000, True, room="room_2")
        self.apply(NOW - 5000, False, room="room_2")
        self.assertTrue(self.rooms.occupancy("room_2", MIN, NOW)["in_room"])

    def test_windows_are_independent(self):
        self.apply(NOW - 30 * MIN, True)
        self.apply(NOW - 29 * MIN, False)
        one_min = self.rooms.occupancy("room_1", MIN, NOW)
        one_hour = self.rooms.occupancy("room_1", 3_600_000, NOW)
        self.assertEqual(one_min["occupied_pct"], 0.0)
        self.assertAlmostEqual(one_hour["occupied_seconds"], 60.0, places=3)

    def test_prune_keeps_the_carry_in_state(self):
        # Five hours old, so it is built directly rather than through the
        # acceptance rules, which would reject it as too old. Retention and
        # acceptance are separate concerns and this test is about retention.
        self.rooms.apply(normalised("presence", "room_1", NOW - 5 * 3_600_000, True))
        self.rooms.prune(NOW)
        # Pruning must not make the room look empty: that single old event is
        # the only thing that says it is occupied.
        self.assertAlmostEqual(
            self.rooms.occupancy("room_1", 3_600_000, NOW)["occupied_pct"], 1.0, places=4)

    def test_unknown_room_is_reported_not_errored(self):
        result = self.rooms.occupancy("nope", MIN, NOW)
        self.assertFalse(result["known"])
        self.assertEqual(result["occupied_pct"], 0.0)


class DeviceHealthTest(unittest.TestCase):
    def setUp(self):
        self.cfg = build_config()
        self.devices = DeviceStore(self.cfg)

    def beat(self, ts, device="dev_1"):
        self.devices.apply(ev(heartbeat_event(device, ts), self.cfg))

    def test_availability_is_second_coverage(self):
        for i in range(150):
            self.beat(NOW - i * 1000)
        health = self.devices.health("dev_1", NOW)
        self.assertEqual(health["heartbeats_5m"], 150)
        self.assertAlmostEqual(health["availability_5m"], 0.5, places=4)

    def test_duplicate_heartbeats_in_one_second_collapse(self):
        # Otherwise a replay burst would report availability above 1.0.
        for _ in range(20):
            self.beat(NOW - 5000)
        health = self.devices.health("dev_1", NOW)
        self.assertEqual(health["heartbeats_5m"], 1)

    def test_full_coverage_never_exceeds_one(self):
        for i in range(600):
            self.beat(NOW - i * 1000)
        self.assertLessEqual(self.devices.health("dev_1", NOW)["availability_5m"], 1.0)

    def test_late_replay_repairs_availability(self):
        # Device was offline; nothing recorded. Then it replays its buffer.
        self.assertEqual(self.devices.health("dev_1", NOW)["availability_5m"], 0)
        for i in range(60):
            self.beat(NOW - 120_000 - i * 1000)
        self.assertEqual(self.devices.health("dev_1", NOW)["heartbeats_5m"], 60)

    def test_stale_device_decays_to_zero(self):
        # Availability is anchored to query time, not to the device's own last
        # heartbeat, so a device that died 10 minutes ago reads 0.
        for i in range(300):
            self.beat(NOW - 600_000 - i * 1000)
        self.assertEqual(self.devices.health("dev_1", NOW)["availability_5m"], 0)
        self.assertIsNotNone(self.devices.health("dev_1", NOW)["last_heartbeat_ts"])

    def test_snapshot_round_trip_preserves_availability(self):
        for i in range(150):
            self.beat(NOW - i * 1000)
        import json
        restored = DeviceStore(self.cfg)
        restored.restore(json.loads(json.dumps(self.devices.snapshot())))
        self.assertEqual(
            restored.health("dev_1", NOW)["heartbeats_5m"],
            self.devices.health("dev_1", NOW)["heartbeats_5m"],
        )

    def test_new_device_needs_no_registration(self):
        self.beat(NOW, device="dev_5001")
        self.assertTrue(self.devices.health("dev_5001", NOW)["known"])

    def test_unknown_device_is_reported_not_errored(self):
        self.assertFalse(self.devices.health("nope", NOW)["known"])


class PriorityQueueTest(unittest.TestCase):
    def test_falls_drain_before_telemetry(self):
        q = PriorityQueue()
        for i in range(100):
            q.push(2, ("heartbeat", i))
        q.push(0, ("fall_warn", "urgent"))
        out = []
        q.drain(out, 10)
        self.assertEqual(out[0], ("fall_warn", "urgent"))

    def test_telemetry_is_not_starved_by_a_state_flood(self):
        # Strict priority would let a sustained tier-1 flood starve tier 2
        # forever, which would quietly break device availability during a burst.
        q = PriorityQueue()
        for i in range(10_000):
            q.push(1, ("presence", i))
        for i in range(10_000):
            q.push(2, ("heartbeat", i))
        out = []
        q.drain(out, 1000)
        telemetry = sum(1 for kind, _ in out if kind == "heartbeat")
        self.assertGreaterEqual(telemetry, 250, "tier 2 must keep its guaranteed floor")

    def test_size_tracks_drain(self):
        q = PriorityQueue()
        for i in range(500):
            q.push(i % 3, i)
        self.assertEqual(q.size, 500)
        out = []
        taken = q.drain(out, 200)
        self.assertEqual(taken, 200)
        self.assertEqual(q.size, 300)
        self.assertEqual(len(out), 200)

    def test_unused_tier2_floor_goes_back_to_tier1(self):
        q = PriorityQueue()
        for i in range(100):
            q.push(1, i)
        out = []
        self.assertEqual(q.drain(out, 100), 100)


if __name__ == "__main__":
    unittest.main()
