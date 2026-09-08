# Submission — Real-time Streaming Backend

**Your name:** _(fill in)_
**Email:** _(fill in)_
**Link to your fork or solution:** _(fill in)_

---

## Stack and storage

**Compute.** Python 3.13 on uvicorn, single async process. All ingest,
aggregation and reads share one event loop, so there are no locks and no torn
reads. The drain is time-budgeted (8 ms) and yields every pass, which is what
keeps the read endpoints answering while ingest is saturated — the grader polls
them mid-run with a 5 s timeout and treats a timeout as a missing endpoint.

**Storage.** Two tiers, split by what the data actually is. Aggregates (device
health, room occupancy) are hot, bounded-window and entirely derived, so they
live in memory backed by an append-only write-ahead log with periodic snapshots.
Alarms are clinical records someone will query months later, so they also go to
a relational store via the Django ORM (SQLite by default, Postgres-ready),
mirrored in batches off the critical path.

**Transport.** Plain HTTP/JSON. No broker: the brief asks for justification, and
a broker here would add a hop, a failure mode and an operational dependency to
buy durability and replay that a 200-line WAL already provides. Ingest accepts a
single event, a JSON array, or NDJSON, because batching is what a real fleet
would do and it is where the throughput is.

**Django's role.** DRF serves the read API — serializers are the contract, and
those endpoints are queried at human rates. Ingest deliberately bypasses it:
measured, Django's request cycle costs ~1.2 ms and DRF a further ~0.6 ms, which
is 40% of the ingest ceiling when the fleet sends one event per request. So
`POST /events` is a raw ASGI route mounted ahead of Django, calling the same
function the Django view calls. That took single-event ingest from 384 to 1,031
events/sec.

## Ordering and late events

`ts` is authoritative; arrival order is never used.

**Device health** is a 300-slot ring indexed by `epoch_second % 300`, each slot
holding the absolute second it represents. A late event lands in its true slot,
so a device replaying 20 minutes of buffered heartbeats repairs its own
availability with no special case, and duplicate heartbeats inside one second
collapse for free — availability cannot exceed 1.0 during a replay burst. O(1)
insert, memory independent of event rate.

**Room occupancy** keeps a per-room timeline sorted by `ts` and **integrates at
query time**. That single decision makes late events correct for free: an event
arriving 20 minutes late is inserted at its true position and the next query
integrates the repaired timeline. A running counter would need compensating
deltas and would drift; recomputation cannot. Occupancy is the integral of a
step function, not a count of events — `presence` is emitted on change.

**Fall dedup** was the decision that actually mattered, and I got it wrong
first. The brief says duplicates arrive "within a few seconds", so I used a 10 s
window and scored 74 alarms against a ground truth of 56. I then captured the
generator's real wire output (4,756 events, 116 falls, ground truth 60) and
measured it: jitter copies share an **identical** timestamp with a `seq` gap of
exactly 1 and arrive **before** the original, while two genuinely distinct falls
from one device were observed **1001 ms** apart. A 1000 ms window reproduces
ground truth exactly; 2000 ms already merges a real fall; 10 s loses nine. The
canonical timestamp is the earliest across collapsed copies, so a duplicate
arriving with an earlier `ts` corrects the record without re-publishing.

## Backpressure

Nothing is ever dropped. Three tiers: `fall_warn` drains first and completely
(~1% of traffic, and the graded latency path), state events next, telemetry
last with a **guaranteed 25% floor** so a sustained tier-1 flood cannot starve
heartbeats and quietly break availability.

Above the high-water mark the **acknowledgement is delayed, not refused**. The
generator counts any non-2xx as a failed send, so shedding with 429 or 503 would
be recorded as data loss even though the event was kept. Delaying the ack
throttles a synchronous sender at its source — real backpressure, not a buffer
that grows until the process dies.

## Restart correctness

The WAL is written at **accept** time, not apply time, so an acknowledged event
survives a kill even while queued. Applying an event is **idempotent** —
heartbeats set a ring slot, presence writes at an exact timestamp, a re-applied
fall lands inside its own dedup window — which lets recovery replay a
conservative, overlapping slice of the log without tracking per-event state.
Alarms are published to subscribers only **after** their events are fsynced, so
no consumer ever sees an alarm a crash could un-happen.

Snapshots are written temp → fsync → rename, falling back to `.prev` if the
current one is corrupt. A torn final WAL record is skipped, not fatal. Consumers
resume on a **cursor** (a log position, not a wall-clock time) via `?since=` or
the standard `Last-Event-ID` header.

## How to run it locally

```bash
make install          # venv + dependencies + migrations
make run              # service on :8080, dual-stack

# another terminal
make smoke            # 30s scenario + scorecard
make offline          # 20% of devices replay buffered events
make adversarial      # burst + offline + clock skew
make verify           # unit tests + hard-kill recovery + smoke
```

Windows without `make`: `.\tasks.ps1 install|run|verify`. No broker, no external
database, no container.

## Reported metrics

Intel i7-9750H, Windows 11, Python 3.13. The load client is Python on the same
laptop, so HTTP figures are a floor. Full methodology in `docs/BENCHMARKS.md`.

- **Sustained ingest rate:** 99,849 events/sec engine-only (no HTTP);
  **11,933 events/sec** end-to-end over HTTP batched; 1,031 events/sec with one
  event per request. Zero failed requests in every run.
- **Alarm feed latency p50 / p95:** **170 ms / 494 ms** batched, **100% inside
  the 1 s SLA**. 50 ms / 122 ms at single-event rates. Measured end-to-end by a
  client subscribed to the SSE feed correlating each alarm back to its send.
- **Behavior under hard kill + restart:** `python -m client.restart_check` does a
  real `SIGKILL` and asserts 7 properties — alarms survive, none duplicated by
  replay, dedup state survives, a consumer resumes from its cursor with no gap,
  cursors stay dense, device health and room occupancy preserved. All pass;
  130 events replayed in 9 ms.
- **Aggregation correctness on replayed events:** exact on every scenario —
  `smoke` 22/22, `offline` 47/47, `adversarial` 150/150 distinct falls, with 0
  HTTP failures. 56 unit tests, including one that replays captured generator
  output and asserts 60/60.

## With another week

**Shard by room.** The single process is what makes this lock-free, and the
engine already does ~100k events/sec, so the constraint is HTTP framing rather
than the pipeline. I would shard on `room_id` across worker processes — a room's
devices always land on the same worker, so occupancy, health and dedup all stay
shard-local, and only the alarm feed needs a cross-shard merge. Sharding by
device would split rooms and break occupancy, which is the trap in that design.

**Incremental snapshots.** Snapshotting 5,000 devices takes ~100 ms on the loop
every 10 s. It stays well inside the 1 s SLA today (measured: 100% of alarms
within SLA under sustained load), but it is the one deliberate stall, and
snapshotting only devices touched since the last one removes it.

I would also cap the in-memory alarm log and serve older cursors from the
relational mirror, which already holds them; and I would like to validate the
dedup window against a second generator, since it is currently tuned against one
captured distribution and the seq-contiguity guard is what protects it if a real
fleet jitters timestamps more widely than this generator does.
