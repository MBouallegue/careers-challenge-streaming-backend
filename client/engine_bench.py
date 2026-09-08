"""Engine throughput without HTTP.

Isolates the pipeline from the transport so the two can be reasoned about
separately. The HTTP layer is measured by ``client/loadgen.py``; this measures
what the HTTP layer is feeding.

Usage::

    python -m client.engine_bench
    python -m client.engine_bench --events 500000 --devices 5000
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
import time

from client.events import EventFactory
from engine.config import EngineConfig
from engine.service import StreamingEngine


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--events", type=int, default=200_000)
    parser.add_argument("--devices", type=int, default=5_000)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args(argv)

    data_dir = tempfile.mkdtemp(prefix="teton-bench-")
    # Background timers disabled: this measures the pipeline, and flush and
    # snapshot are timed explicitly below.
    config = EngineConfig(data_dir=data_dir, fsync_ms=3_600_000, snapshot_ms=3_600_000)
    engine = StreamingEngine(config)
    engine.recover()

    factory = EventFactory(device_count=args.devices, seed=args.seed)
    events = [factory.build() for _ in range(args.events)]
    now_ms = int(time.time() * 1000)

    started = time.perf_counter()
    for event in events:
        engine.accept(event, now_ms)
    accept_seconds = time.perf_counter() - started

    started = time.perf_counter()
    engine.drain_all()
    drain_seconds = time.perf_counter() - started

    started = time.perf_counter()
    engine.wal.flush()
    flush_seconds = time.perf_counter() - started

    started = time.perf_counter()
    engine.write_snapshot()
    snapshot_seconds = time.perf_counter() - started

    total = accept_seconds + drain_seconds
    print("\n=== engine throughput (no HTTP) ===")
    print(f"  events                        {args.events:,} across {args.devices:,} devices")
    print(
        f"  accept (validate+WAL+enqueue) {accept_seconds:6.2f}s"
        f" -> {args.events / accept_seconds:11,.0f} events/sec"
    )
    print(
        f"  drain  (aggregate+dedup)      {drain_seconds:6.2f}s"
        f" -> {args.events / drain_seconds:11,.0f} events/sec"
    )
    print(
        f"  end to end                    {total:6.2f}s"
        f" -> {args.events / total:11,.0f} events/sec"
    )
    print()
    print(
        f"  wal flush + fsync             {flush_seconds * 1000:7.1f}ms"
        f" for {engine.wal.bytes_written / 1e6:.1f} MB"
    )
    print(f"  snapshot write                {snapshot_seconds * 1000:7.1f}ms "
          f"({len(engine.devices):,} devices, {len(engine.rooms):,} rooms)")
    print(f"  alarms                        {len(engine.alarms):,} "
          f"({engine.alarms.duplicates_collapsed:,} jitter copies collapsed)")

    engine.wal.close()
    shutil.rmtree(data_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
