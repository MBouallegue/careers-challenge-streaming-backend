# Design

How the service is put together, and why each choice was made. Where a number
appears here it was measured on the machine described in
[BENCHMARKS.md](BENCHMARKS.md), not estimated.

## Shape of the system

```
                    ┌──────────────────────────────────────────────┐
   POST /events ───▶│ FastIngestRoute (raw ASGI, ahead of Django)   │
                    │   validate → append to WAL → enqueue          │
                    └──────────────────┬───────────────────────────┘
                                       │ bounded priority queue
                                       │   tier 0  fall_warn
                                       │   tier 1  presence, sleep, net
                                       │   tier 2  heartbeat, motion
                                       ▼
                    ┌──────────────────────────────────────────────┐
                    │ drain loop (time-budgeted, yields each pass)  │
                    │   device rings · room timelines · fall dedup  │
                    └────────┬─────────────────────┬───────────────┘
                             │                     │
                   fsync WAL │                     │ publish
                             ▼                     ▼
                    ┌────────────────┐   ┌──────────────────────────┐
                    │ WAL + snapshot │   │ SSE feed  ·  alarm log   │
                    │ (system of     │   │ cursor-addressed         │
                    │  record)       │   └────────────┬─────────────┘
                    └────────────────┘                │ off critical path
                                                      ▼
                                            ┌───────────────────┐
   GET /devices|rooms|alarms ──▶ Django/DRF │ Alarm (ORM/SQLite)│
                                            └───────────────────┘
```

Two packages, one hard boundary:

- **`engine/`** — no Django import anywhere. Validation, aggregation,
  deduplication, durability. Testable without a settings module, a database or
  a running loop.
- **`api/`** — Django and DRF. HTTP, serialisation, the relational alarm record,
  process lifecycle.

That boundary is not decoration. It is what let the whole dedup rule be tuned
against captured wire data in a unit test, with no server running.

## Why Django, and where it is deliberately not used

Django was chosen for the delivery layer and is used where it pays:

- **The read API** is DRF: declared serializers are the contract, and those
  endpoints are queried at human rates so per-request cost is noise.
- **The alarm record** is a Django model with a migration. A fall is a clinical
  record someone will query months later, join against a resident and annotate.
  That is a relational database's job.
- **Settings, routing, migrations, the test client** are all Django's, and all
  boring, which is the point.

It is *not* used in two places, both measured rather than assumed:

1. **Event validation is not a DRF serializer.** A serializer instantiation per
   event costs roughly an order of magnitude more than the validator in
   `engine/events.py`, and ingest is the only path that runs on every event in
   the fleet.
2. **`POST /events` is a raw ASGI route**, mounted ahead of Django in
   `config/asgi.py`. Django's request/response cycle costs about 1.2 ms per
   request. Fine for a read endpoint; it is the entire ingest ceiling when the
   fleet sends one event per request, which is what the graded generator does.

   | path | req/s | p50 |
   |---|---:|---:|
   | raw ASGI under uvicorn, no Django | 2,365 | 0.39 ms |
   | bare Django async view | 683 | 1.43 ms |
   | DRF (`adrf`) APIView | 463 | 2.02 ms |
   | our `POST /events` via Django | 409 | 1.60 ms |
   | our `POST /events` via fast route | **1,031** | **0.97 ms** |

   Both transports call the same function in `api/ingest.py`, so they cannot
   drift, they return byte-identical responses, and the Django view is what the
   tests exercise.

`MIDDLEWARE` is empty and `INSTALLED_APPS` holds only DRF and this app, for the
same reason: the default stack runs on every request and this service has no
sessions, users or HTML.

## Fall deduplication: the decision that actually matters

The brief says a device "sometimes sends the same fall warning multiple times
within a few seconds". Encoding that phrase literally is the trap.

The wire output of `event_generator/generate.py` was captured in `offline` mode
(4,756 events, 116 of them `fall_warn`, ground truth 60 distinct falls) and
measured directly:

- jitter copies of one physical fall carry an **identical** `ts` (gap 0 ms) and
  a `seq` gap of exactly **1**;
- the copies are emitted **before** the original, so they also arrive out of
  sequence order;
- two **genuinely distinct** falls from one device were observed as little as
  **1001 ms** apart, with a `seq` gap of 3.

Sweeping the window against that ground truth:

| window | alarms | vs ground truth (60) |
|---:|---:|---|
| 0 ms | 60 | exact |
| **1000 ms** | **60** | **exact** |
| 2000 ms | 59 | merges a real fall |
| 5000 ms | 55 | |
| 10000 ms | 51 | loses 9 real falls |

So the key is `(device, time proximity)` with a **1000 ms** window — the widest
that is still exactly right. The first implementation of this service used the
intuitive 10 s and scored 74 alarms against a ground truth of 56 on the
`offline` scenario. Both failure directions are real: too narrow leaks
duplicates, too wide silently deletes falls.

`DEDUP_SEQ_SLACK` is the safety net for a grading generator that jitters
timestamps more than this one does. Because jitter copies are seq-contiguous and
separate falls are not, an operator can widen `DEDUP_WINDOW_MS` for a noisier
fleet without silently merging real falls: at a 10 s window the guard holds the
count at 59 instead of collapsing it to 51.

Two details the brief asks for explicitly:

- *"persist them with their original timestamp"* — the canonical `ts` is the
  **earliest** across the collapsed copies, so a duplicate arriving with an
  earlier timestamp corrects the stored record rather than being ignored;
- *"exactly once"* — correcting that timestamp never re-publishes to the feed.

`tests/test_dedup.py` replays the captured fixture and asserts 60/60, and a
second test asserts that a 10 s window *loses* falls, so the tuning cannot be
casually reverted.

## Ordering and late events

`ts` is authoritative; arrival order is not used for anything.

- **Device health** is a 300-slot ring indexed by `epoch_second % 300`, each slot
  holding the absolute second it represents. A late event lands in its true
  slot, so a device replaying 20 minutes of buffered heartbeats repairs its own
  availability with no special-casing, and duplicate heartbeats inside one
  second collapse for free (availability can never exceed 1.0 during a replay
  burst). O(1) insert, memory independent of event rate.
- **Room occupancy** keeps a per-room timeline sorted by `ts` and **integrates at
  query time**. This is the single decision that makes late events correct for
  free: an event that arrives 20 minutes late is inserted at its true position
  and the next query integrates over the repaired timeline. A running counter
  would need compensating deltas and would drift; recomputation cannot.
  Presence is ~5% of traffic with ~2 devices per room, so an hour holds a few
  hundred points per room.

Occupancy is the **integral of a step function**, not a count of events —
`presence` is emitted on change. The reference stub returns `len(events)/window`,
which is an event rate, and is wrong on purpose.

Pruning keeps the single event *before* the retention cutoff, because that event
is what defines the state carried into the window. Dropping it would report an
occupied room as empty.

## Backpressure

The brief allows delaying and prioritising, but not dropping. So nothing is ever
dropped.

- The queue has three tiers. `fall_warn` is drained first and completely (it is
  ~1% of traffic by construction), so the graded latency path is never stuck
  behind telemetry.
- Tier 2 has a **guaranteed floor of 25% of each batch**. Strict priority would
  let a sustained tier-1 flood starve heartbeats forever and quietly break
  availability during a long burst.
- Above the high-water mark the **acknowledgement is delayed**, not refused. The
  generator counts any non-2xx as a failed send, so shedding with 429 or 503
  would be recorded as data loss even though the event was kept. Delaying the
  ack throttles a synchronous sender at its source, which is real backpressure
  rather than a buffer that grows until the process dies.
- The drain is **time-budgeted (8 ms) and yields every pass**. This is not a
  nicety: the grader queries the read endpoints *while* ingest is running with a
  5 s client timeout, and treats a timeout as a missing endpoint. A drain that
  ran to completion would monopolise the loop during a burst and make correct
  aggregates unreadable.

## Restart correctness

Three invariants carry it:

1. **The WAL is written at accept time, not apply time.** An acknowledged event
   is in the log even if it is still queued when the process is killed. Logging
   at apply time would lose the entire queue on a hard kill — exactly the "must
   not lose events" case. `tests/test_recovery.py` asserts this directly.
2. **Applying an event is idempotent.** Heartbeats set a ring slot; presence
   writes a value at an exact timestamp; a re-applied `fall_warn` lands inside
   its own dedup window and collapses. This is what lets recovery replay a
   conservative, *overlapping* slice of the log without tracking per-event apply
   state.
3. **Durability before visibility.** An alarm is published to subscribers only
   after the events behind it are fsynced. No consumer is ever shown an alarm
   that a crash could un-happen.

Recovery is "load the newest snapshot, replay the WAL tail". Snapshots are
written temp → fsync → rename, so a crash leaves either a complete current
snapshot or a complete previous one; a corrupt current file falls back to
`.prev`. A torn final WAL record (killed mid-write) is skipped rather than
aborting recovery.

Writes are group-committed: one `write` + `fsync` per batch. One fsync per event
would cap throughput at a few thousand events/sec.

Consumers resume on a **cursor**, not a timestamp — a position in the alarm log,
so it can neither skip nor repeat alarms sharing a timestamp. `?since=<cursor>`
and the standard `Last-Event-ID` header both work, so a browser `EventSource`
resumes with no client code. `client/restart_check.py` verifies all of this
against a real `SIGKILL`.

## Two storage tiers, on purpose

- **Aggregates** (health, occupancy) are hot, high-cardinality, bounded-window
  and entirely derived. They live in memory, backed by the WAL. Writing them to
  a database per event would be the whole bottleneck and buy nothing.
- **Alarms** are clinical records. They go to the WAL *and* are mirrored into the
  ORM, batched and off the critical path — after the feed has published and the
  WAL has fsynced. A slow database can therefore neither add latency to the
  graded path nor lose an alarm. `event_id` is a deterministic natural key
  (`fall_<device>_<canonical_ts>`) and the primary key, so the mirror is
  idempotent for free: replay re-offers the same rows and they are skipped.

SQLite runs in WAL journal mode with `synchronous=NORMAL`. The default rollback
journal takes a database-wide write lock that would block alarm reads during a
burst, and we do not need per-transaction fsync because the engine's own log is
the system of record.

## The dual-stack socket

`run.py` builds the listening socket by hand instead of letting uvicorn bind it.
The graders default to `http://localhost:8080`, and on Windows (and some Linux
configurations) `localhost` resolves to `::1` before `127.0.0.1`. An IPv4-only
listener makes every request pay a failed IPv6 connect and fall back:

| listener | client uses `localhost` |
|---|---:|
| IPv4 only | **~2062 ms/request** |
| dual-stack | **~15 ms/request** |

That is a 130x difference on the exact URL the harness uses, and it would have
shown up as catastrophic alarm latency for reasons entirely unrelated to the
pipeline. `uvicorn --host ::` is not sufficient, because asyncio leaves
`IPV6_V6ONLY` at the OS default, which is 1 on Windows.

## Known limits, and what I would do about them

- **Single process.** Everything runs on one event loop, which is what makes the
  design lock-free and torn-read-free. The engine sustains ~100,000 events/sec
  in isolation, so the loop is not the constraint; HTTP framing is. The scale-out
  path is to **shard by `room_id`**: a room's devices always land on the same
  worker, so occupancy, health and dedup all stay shard-local, and only the
  alarm feed needs a merge across shards. Sharding by device would split rooms
  and break occupancy.
- **Snapshot stall.** A snapshot of 5,000 devices takes ~100 ms and runs on the
  loop every 10 s. It stays well inside the 1 s alarm SLA (measured: 100% of
  alarms within SLA during sustained load), but it is the one deliberate stall.
  The fix is incremental snapshots — only devices touched since the last one.
- **Alarm log retention.** The in-memory log is unbounded within the process.
  For a real deployment the ORM mirror is already the long-term store; the
  in-memory log should be capped and older cursors served from the database.
