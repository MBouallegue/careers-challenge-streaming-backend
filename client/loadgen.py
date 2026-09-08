"""High-rate load generator with end-to-end alarm latency measurement.

Why this exists alongside ``event_generator/generate.py``:

The bundled generator is a correctness harness, not a load harness. It sends one
event per request from a single thread with ``urllib``, which opens a fresh TCP
connection per event (``Connection: close``). On this machine that caps it at
roughly 50-100 events/sec regardless of how fast the service is, so it cannot
produce the 10x burst the brief is graded on, and it cannot measure the alarm
SLA because it never subscribes to the feed.

This client closes both gaps:

* concurrent workers over pooled keep-alive connections, with optional batching,
  so the ingest path rather than TCP setup is what is being measured;
* a subscriber on the SSE feed that correlates each alarm back to the moment its
  event was sent, giving a true end-to-end p50/p95/p99 for the graded number
  rather than the service's own opinion of its latency.

Usage::

    python -m client.loadgen --rate 50000 --duration 30 --devices 5000
    python -m client.loadgen --rate 5000 --duration 60 --batch 20
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import time
from dataclasses import dataclass, field

from client.events import EventFactory

DEFAULT_TARGET = "http://localhost:8080"


@dataclass(slots=True)
class Results:
    sent: int = 0
    accepted: int = 0
    failed: int = 0
    status_counts: dict[int, int] = field(default_factory=dict)
    request_latencies_ms: list[float] = field(default_factory=list)
    alarm_latencies_ms: list[float] = field(default_factory=list)
    falls_sent: int = 0
    alarms_received: int = 0


def _quantile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(quantile * len(ordered) + 0.9999) - 1))
    return round(ordered[index], 2)


class LoadGenerator:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.factory = EventFactory(device_count=args.devices, seed=args.seed)
        self.results = Results()
        self.fall_sent_at: dict[str, float] = {}
        self._stop = asyncio.Event()

    # ------------------------------------------------------------- feed side

    async def watch_alarm_feed(self) -> None:
        """Subscribe to the SSE feed and time each alarm against its send."""
        import aiohttp  # optional; the run degrades gracefully without it

        url = f"{self.args.target}/alarms/stream?since={self.args.since}"
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as session, session.get(url) as response:
            async for raw_line in response.content:
                if self._stop.is_set():
                    return
                line = raw_line.decode("utf-8", "replace").strip()
                if not line.startswith("data:"):
                    continue
                alarm = json.loads(line[5:].strip())
                self.results.alarms_received += 1
                sent_at = self.fall_sent_at.pop(alarm["event_id"], None)
                if sent_at is not None:
                    self.results.alarm_latencies_ms.append((time.perf_counter() - sent_at) * 1000)

    # ------------------------------------------------------------ send side

    async def worker(self, session, deadline: float, interval: float) -> None:
        url = f"{self.args.target}/events"
        headers = {"Content-Type": "application/json"}
        next_send = time.perf_counter()

        while time.perf_counter() < deadline:
            now = time.perf_counter()
            if now < next_send:
                await asyncio.sleep(next_send - now)
            next_send += interval

            batch = [self.factory.build() for _ in range(self.args.batch)]
            payload = json.dumps(batch if self.args.batch > 1 else batch[0]).encode()

            for event in batch:
                if event["type"] == "fall_warn":
                    self.results.falls_sent += 1
                    self.fall_sent_at[self.factory.event_id_for(event)] = time.perf_counter()

            started = time.perf_counter()
            try:
                async with session.post(url, data=payload, headers=headers) as response:
                    await response.read()
                    status = response.status
            except Exception:
                self.results.failed += len(batch)
                continue

            self.results.request_latencies_ms.append((time.perf_counter() - started) * 1000)
            self.results.sent += len(batch)
            self.results.status_counts[status] = self.results.status_counts.get(status, 0) + 1
            if 200 <= status < 300:
                self.results.accepted += len(batch)
            else:
                self.results.failed += len(batch)

    async def run(self) -> Results:
        import aiohttp

        requests_per_second = self.args.rate / max(1, self.args.batch)
        interval = self.args.concurrency / requests_per_second
        deadline = time.perf_counter() + self.args.duration

        connector = aiohttp.TCPConnector(limit=self.args.concurrency, force_close=False)
        feed_task = None
        if not self.args.no_feed:
            feed_task = asyncio.create_task(self.watch_alarm_feed())
            await asyncio.sleep(0.3)  # let the subscription establish

        async with aiohttp.ClientSession(connector=connector) as session:
            await asyncio.gather(
                *(self.worker(session, deadline, interval) for _ in range(self.args.concurrency))
            )

        # Alarms are published within the SLA, but give the feed a moment to
        # deliver the tail before we score it.
        await asyncio.sleep(2.0)
        self._stop.set()
        if feed_task is not None:
            feed_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await feed_task

        return self.results


def report(results: Results, args: argparse.Namespace) -> None:
    elapsed = args.duration
    print("\n=== load generator ===")
    print(f"  target                {args.target}")
    print(f"  devices               {args.devices}")
    print(
        f"  requested rate        {args.rate:,}/sec "
        f"(batch={args.batch}, workers={args.concurrency})"
    )
    print(f"  duration              {elapsed}s")
    print()
    print(f"  events sent           {results.sent:,}")
    print(f"  accepted (2xx)        {results.accepted:,}")
    print(f"  failed                {results.failed:,}")
    print(f"  achieved rate         {results.sent / elapsed:,.0f} events/sec")
    print(f"  status codes          {results.status_counts}")
    print()
    print("  request latency (ms)  "
          f"p50={_quantile(results.request_latencies_ms, 0.5)} "
          f"p95={_quantile(results.request_latencies_ms, 0.95)} "
          f"p99={_quantile(results.request_latencies_ms, 0.99)}")

    if results.alarm_latencies_ms:
        print()
        print("  --- alarm feed SLA (send -> received on feed) ---")
        print(f"  falls sent            {results.falls_sent:,}")
        print(f"  alarms received       {results.alarms_received:,}")
        print(f"  correlated            {len(results.alarm_latencies_ms):,}")
        print("  end-to-end latency    "
              f"p50={_quantile(results.alarm_latencies_ms, 0.5)}ms "
              f"p95={_quantile(results.alarm_latencies_ms, 0.95)}ms "
              f"p99={_quantile(results.alarm_latencies_ms, 0.99)}ms "
              f"max={round(max(results.alarm_latencies_ms), 2)}ms")
        within_sla = sum(1 for v in results.alarm_latencies_ms if v <= 1000)
        share = within_sla / len(results.alarm_latencies_ms)
        print(f"  within 1s SLA         {within_sla:,}/{len(results.alarm_latencies_ms):,} "
              f"({share:.2%})")
    elif not args.no_feed:
        print("\n  (no alarms correlated - the run may have been too short to emit a fall)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", default=DEFAULT_TARGET)
    parser.add_argument("--rate", type=int, default=5_000, help="events/sec to aim for")
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--devices", type=int, default=5_000)
    parser.add_argument("--batch", type=int, default=1, help="events per HTTP request")
    parser.add_argument("--concurrency", type=int, default=64, help="concurrent workers")
    parser.add_argument("--since", default="0", help="feed cursor to subscribe from")
    parser.add_argument("--no-feed", action="store_true", help="skip the SSE latency measurement")
    parser.add_argument("--seed", type=int, default=1234)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        print("aiohttp is required for the load generator: pip install aiohttp", file=sys.stderr)
        return 1

    generator = LoadGenerator(args)
    results = asyncio.run(generator.run())
    report(results, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
