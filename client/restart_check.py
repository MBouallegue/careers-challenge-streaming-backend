"""End-to-end restart verification against a real hard kill.

The unit tests in ``tests/test_recovery.py`` prove the engine folds the log
deterministically. This proves the whole service does it: a real process, a real
``SIGKILL`` (``taskkill /F`` on Windows), a real restart, and a consumer that
resumes the alarm feed from the cursor it held when the process died.

It checks the three things the brief asks for by name:

1. per-device health, per-room occupancy and recent alarms survive the kill;
2. a consumer resumes "without missing alarms generated during the gap", meaning
   every alarm past its cursor is still delivered after the restart;
3. no alarm is duplicated by recovery, which is the failure mode a naive replay
   produces.

Usage::

    python -m client.restart_check
    python -m client.restart_check --keep-data-dir --port 8090
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
FALL_DEVICE = "dev_restart"
FALL_ROOM = "room_restart"


class Failure(Exception):
    """A checked expectation did not hold."""


# ----------------------------------------------------------------- transport


def post_event(base_url: str, event: dict[str, Any], timeout: float = 10) -> int:
    request = urllib.request.Request(
        f"{base_url}/events",
        data=json.dumps(event).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response.read()
            return response.status
    except urllib.error.HTTPError as exc:
        exc.read()
        return exc.code


def get_json(base_url: str, path: str, timeout: float = 10) -> dict[str, Any]:
    with urllib.request.urlopen(f"{base_url}{path}", timeout=timeout) as response:
        return json.loads(response.read())


def to_iso(epoch_ms: int) -> str:
    seconds, milliseconds = divmod(epoch_ms, 1000)
    return f"{time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(seconds))}.{milliseconds:03d}Z"


# ------------------------------------------------------------ process control


def start_service(port: int, data_dir: Path, python: str) -> subprocess.Popen:
    environment = {**os.environ, "PORT": str(port), "DATA_DIR": str(data_dir)}
    process = subprocess.Popen(
        [python, str(REPO_ROOT / "run.py")],
        cwd=REPO_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return process


def wait_until_ready(base_url: str, process: subprocess.Popen, timeout: float = 45) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read().decode("utf-8", "replace")[-2000:]
            raise Failure(f"service exited during startup:\n{stderr}")
        try:
            get_json(base_url, "/healthz", timeout=2)
            return
        except Exception:
            time.sleep(0.25)
    raise Failure(f"service did not become ready within {timeout}s")


def hard_kill(process: subprocess.Popen) -> None:
    """SIGKILL, with no chance for the process to flush or snapshot."""
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(process.pid)],
            capture_output=True,
            check=False,
        )
    else:
        process.send_signal(signal.SIGKILL)
    process.wait(timeout=30)


# ------------------------------------------------------------------- the run


def seed_traffic(base_url: str, now_ms: int) -> None:
    """Heartbeats, a presence interval, and one jittered fall."""
    for index in range(120):
        post_event(base_url, {
            "device_id": FALL_DEVICE, "room_id": FALL_ROOM, "type": "heartbeat",
            "ts": to_iso(now_ms - index * 1000), "seq": index + 1,
        })
    post_event(base_url, {
        "device_id": FALL_DEVICE, "room_id": FALL_ROOM, "type": "presence",
        "ts": to_iso(now_ms - 60_000), "in_room": True,
    })
    post_event(base_url, {
        "device_id": FALL_DEVICE, "room_id": FALL_ROOM, "type": "presence",
        "ts": to_iso(now_ms - 30_000), "in_room": False,
    })
    for seq in (601, 602, 600):  # jitter copies, original last, as the generator does
        post_event(base_url, {
            "device_id": FALL_DEVICE, "room_id": FALL_ROOM, "type": "fall_warn",
            "ts": to_iso(now_ms - 5_000), "seq": seq, "confidence": 0.91,
        })


def send_gap_falls(base_url: str, now_ms: int, count: int) -> None:
    """Falls a consumer has not yet acknowledged when the process dies."""
    for index in range(count):
        post_event(base_url, {
            "device_id": f"{FALL_DEVICE}_{index}", "room_id": FALL_ROOM, "type": "fall_warn",
            "ts": to_iso(now_ms + index * 5_000), "seq": 900 + index, "confidence": 0.8,
        })


def snapshot_state(base_url: str) -> dict[str, Any]:
    return {
        "alarms": get_json(base_url, "/alarms?since=0"),
        "health": get_json(base_url, f"/devices/{FALL_DEVICE}/health"),
        "occupancy": get_json(base_url, f"/rooms/{FALL_ROOM}/occupancy?window=5m"),
    }


def check(label: str, condition: bool, detail: str = "") -> bool:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}{f' - {detail}' if detail else ''}")
    return condition


def run(args: argparse.Namespace) -> int:
    python = args.python or sys.executable
    data_dir = Path(args.data_dir or REPO_ROOT / "data-restart-check")
    base_url = f"http://localhost:{args.port}"

    if not args.keep_data_dir:
        shutil.rmtree(data_dir, ignore_errors=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [python, str(REPO_ROOT / "manage.py"), "migrate", "--no-input"],
        cwd=REPO_ROOT,
        env={**os.environ, "DATA_DIR": str(data_dir)},
        capture_output=True,
        check=False,
    )

    print(f"restart check: port={args.port} data_dir={data_dir}")
    print("\n1. starting service")
    process = start_service(args.port, data_dir, python)
    try:
        wait_until_ready(base_url, process)

        now_ms = int(time.time() * 1000)
        print("2. seeding traffic (120 heartbeats, 2 presence, 1 jittered fall)")
        seed_traffic(base_url, now_ms)
        time.sleep(1.0)

        before = snapshot_state(base_url)
        cursor_before = before["alarms"]["cursor"]
        print(f"   consumer cursor at {cursor_before}")

        print(f"3. emitting {args.gap_falls} more falls, then killing immediately")
        send_gap_falls(base_url, now_ms, args.gap_falls)
        # No sleep: the point is to kill while these are still in flight.
        expected_total = get_json(base_url, "/alarms?since=0")["cursor"]

        print("4. hard kill (SIGKILL / taskkill /F)")
        hard_kill(process)

        print("5. restarting")
        process = start_service(args.port, data_dir, python)
        wait_until_ready(base_url, process)
        after = snapshot_state(base_url)

        print("\n=== results ===")
        results = []

        recovered_alarms = after["alarms"]["count"]
        results.append(check(
            "alarms survived the kill",
            recovered_alarms >= expected_total,
            f"{recovered_alarms} present, {expected_total} acknowledged before kill",
        ))

        event_ids = [alarm["event_id"] for alarm in after["alarms"]["alarms"]]
        results.append(check(
            "no alarm duplicated by replay",
            len(event_ids) == len(set(event_ids)),
            f"{len(event_ids)} alarms, {len(set(event_ids))} distinct",
        ))

        jitter_alarm = next(
            (a for a in after["alarms"]["alarms"] if a["device_id"] == FALL_DEVICE), None
        )
        results.append(check(
            "dedup state survived: jitter copies still collapsed",
            jitter_alarm is not None and jitter_alarm["duplicates_collapsed"] == 2,
            f"duplicates_collapsed={jitter_alarm['duplicates_collapsed']}"
            if jitter_alarm
            else "missing",
        ))

        resumed = get_json(base_url, f"/alarms?since={cursor_before}")
        missed = expected_total - cursor_before
        results.append(check(
            "consumer resumes from its cursor with no gap",
            resumed["count"] >= missed,
            f"{resumed['count']} delivered from cursor {cursor_before},"
            f" {missed} expected",
        ))

        cursors = [alarm["cursor"] for alarm in after["alarms"]["alarms"]]
        results.append(check(
            "cursors stay dense and ordered",
            cursors == sorted(cursors) and cursors == list(range(1, len(cursors) + 1)),
        ))

        results.append(check(
            "device health survived",
            after["health"]["heartbeats_5m"] == before["health"]["heartbeats_5m"],
            f"{after['health']['heartbeats_5m']} vs"
            f" {before['health']['heartbeats_5m']} heartbeat-seconds",
        ))

        occupancy_delta = abs(
            after["occupancy"]["occupied_seconds"] - before["occupancy"]["occupied_seconds"]
        )
        results.append(check(
            "room occupancy survived",
            occupancy_delta < 1.0,
            f"{after['occupancy']['occupied_seconds']}s vs"
            f" {before['occupancy']['occupied_seconds']}s",
        ))

        stats = get_json(base_url, "/stats")
        recovery = stats.get("recovery", {})
        print(
            f"\n  recovery: replayed={recovery.get('replayed')} events, "
            f"snapshot={'yes' if recovery.get('snapshot') else 'none'}, "
            f"torn_records_skipped={recovery.get('torn_records_skipped')}, "
            f"took {recovery.get('duration_ms')}ms"
        )

        passed = all(results)
        print(f"\n{'ALL CHECKS PASSED' if passed else 'SOME CHECKS FAILED'}")
        return 0 if passed else 1

    finally:
        if process.poll() is None:
            hard_kill(process)
        if not args.keep_data_dir:
            shutil.rmtree(data_dir, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--python", default=None, help="interpreter to run the service with")
    parser.add_argument("--gap-falls", type=int, default=5)
    parser.add_argument("--keep-data-dir", action="store_true")
    args = parser.parse_args(argv)

    try:
        return run(args)
    except Failure as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
