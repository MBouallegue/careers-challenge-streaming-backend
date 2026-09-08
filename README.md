# Teton Challenge — Real-time Streaming Backend

A solution to [Teton-ai/careers-challenge-streaming-backend](https://github.com/Teton-ai/careers-challenge-streaming-backend).

Ingests sensor events from a fleet of care-room devices, aggregates them in real
time, deduplicates fall warnings, and serves a resumable live alarm feed.

**Stack:** Python 3.13 · Django 5.2 · Django REST Framework · uvicorn ·
SQLite (Postgres-ready) · no message broker.

---

## Quickstart

```bash
make install     # venv + dependencies + migrations
make run         # service on :8080

# in another terminal
make smoke       # 30s scenario + scorecard
make verify      # unit tests + hard-kill recovery + smoke
```

Without `make` (Windows):

```powershell
.\tasks.ps1 install
.\tasks.ps1 run
.\tasks.ps1 verify
```

Manually:

```bash
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python manage.py migrate
.venv/bin/python run.py
```

Nothing else is required: no broker, no external database, no container.

## Results

Against the harness in this repository, 50 devices per run:

| scenario | events | ground truth falls | alarms returned | HTTP failed |
|---|---:|---:|---:|---:|
| `smoke` | 1,424 | 22 | **22** ✓ | 0 |
| `offline` (20% replay) | 3,401 | 47 | **47** ✓ | 0 |
| `adversarial` (burst + offline + skew) | 10,789 | 150 | **150** ✓ | 0 |

Exact dedup on every scenario, zero rejected sends.

| measurement | result |
|---|---|
| Engine throughput (no HTTP) | **99,849 events/sec** |
| Over HTTP, batched | **11,933 events/sec**, alarm p95 **494 ms**, 100% inside the 1 s SLA |
| Over HTTP, one event per request | 1,031 events/sec, alarm p95 122 ms |
| Hard kill (`SIGKILL`) + restart | 7/7 checks pass, 130 events replayed in 9 ms |
| Unit tests | 56 passing |

Full numbers and methodology: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## API

Two mounts, deliberately. `/api/v1/` is the API as it should be named; the
unversioned paths are the fixed contract the grading harness calls, pointing at
the same views.

| method | path | |
|---|---|---|
| `POST` | `/events` | one event, a JSON array, or NDJSON → `202` |
| `GET` | `/devices/{device_id}/health` | latest heartbeat + 5-minute availability |
| `GET` | `/rooms/{room_id}/occupancy?window=1m\|5m\|1h` | current presence + time-weighted occupancy |
| `GET` | `/alarms?since=<cursor\|ts>` | deduplicated falls, replayable |
| `GET` | `/alarms/stream?since=<cursor>` | live SSE feed, resumable |
| `GET` | `/healthz` `/readyz` `/stats` `/metrics` | ops; also under `/api/v1/ops/` |

`?since=` takes a **feed cursor** (`0` means everything), an epoch in
milliseconds, or an ISO-8601 timestamp. The cursor is a position in the alarm
log, which is what makes resume-after-restart exact.

```bash
curl -X POST localhost:8080/events -H 'content-type: application/json' \
  -d '{"device_id":"dev_0001","room_id":"room_14","type":"fall_warn","ts":"2026-05-23T18:53:49.123Z","confidence":0.92}'

curl localhost:8080/alarms?since=0
curl -N localhost:8080/alarms/stream       # live feed
curl localhost:8080/stats                  # queue depth, latency quantiles, recovery info
```

## Layout

```
engine/            framework-independent core — no Django import anywhere
  events.py          validation + acceptance rules
  queues.py          bounded priority queue (backpressure)
  service.py         StreamingEngine: accept → WAL → drain → publish
  aggregates/        device health rings, room occupancy timelines
  alarms/            fall dedup, alarm log, live feed subscribers
  storage/           write-ahead log, snapshots
  utils/             timestamps, query-parameter parsing
  metrics.py         counters, gauges, latency histograms

api/               Django + DRF delivery layer
  ingest.py          shared ingest implementation (hot path)
  views/             ingest · telemetry · alarms · ops
  serializers.py     response contracts
  services.py        engine lifecycle + relational alarm mirror
  models.py          Alarm (durable clinical record)

config/            settings, URLs, ASGI (lifespan + fast ingest route)
client/            load generator, restart verifier, engine benchmark
tests/             56 unit tests, incl. a captured-wire dedup fixture
docs/              DESIGN.md · BENCHMARKS.md · event_schema.md
```

`engine/` never imports Django. That boundary is why the deduplication rule
could be tuned against captured wire data in a unit test with no server running.

## The part that decided the score

`fall_warn` deduplication. The brief says duplicates arrive "within a few
seconds", and encoding that phrase literally is wrong.

I captured the real wire output of `event_generator/generate.py` (4,756 events,
116 of them falls, ground truth 60 distinct) and measured it:

- jitter copies of one fall share an **identical** timestamp and a `seq` gap of
  exactly 1, and are sent **before** the original;
- two **genuinely distinct** falls from one device were observed **1001 ms**
  apart.

| dedup window | alarms | vs ground truth (60) |
|---:|---:|---|
| **1000 ms** | **60** | **exact** |
| 2000 ms | 59 | merges a real fall |
| 10000 ms | 51 | loses 9 real falls |

My first implementation used the intuitive 10 s and reported 74 alarms against a
ground truth of 56. Both directions fail: too narrow leaks duplicates, too wide
silently deletes falls. `tests/test_dedup.py` replays the captured fixture and
asserts 60/60, plus a second test asserting that a 10 s window loses falls, so
the tuning cannot be quietly reverted.

The reasoning behind every other decision — WAL at accept time, idempotent
apply, delayed acks instead of 429s, query-time occupancy integration, and why
the hot path skips Django while everything else keeps DRF — is in
[docs/DESIGN.md](docs/DESIGN.md).

## Verifying it yourself

```bash
make test           # 56 unit tests, no server needed
make restart-check  # starts the service, kill -9, restarts, asserts recovery
make bench-engine   # engine throughput without HTTP
make load           # end-to-end load + measured alarm-feed SLA
make adversarial    # burst + offline + clock skew against the running service
```

`client/loadgen.py` exists because the bundled generator cannot produce the
graded burst: it sends one event per request from a single thread over a fresh
TCP connection each time, capping at ~50-100 events/sec. It also never
subscribes to the feed, so it cannot measure the alarm SLA. The load client does
both, correlating each received alarm back to the moment its event was sent.

## Configuration

All environment variables, all optional.

| variable | default | |
|---|---|---|
| `PORT` | `8080` | |
| `DATA_DIR` | `data` | WAL, snapshots, SQLite |
| `DEDUP_WINDOW_MS` | `1000` | fall dedup window — see above |
| `DEDUP_SEQ_SLACK` | `2` | guard against over-merging at wider windows |
| `FSYNC_MS` | `200` | group-commit interval |
| `SNAPSHOT_MS` | `10000` | snapshot cadence |
| `QUEUE_HIGH_WATER` | `200000` | begin delaying acknowledgements |
| `MAX_ACK_DELAY_MS` | `4000` | cap on ack delay (client timeout is 5 s) |
| `ALARM_DB_MIRROR` | `1` | mirror alarms to the relational store |
| `LOG_LEVEL` | `INFO` | |

## Notes for the reviewer

- Upstream files (`event_generator/`, `eval/`, `example_solution/`,
  `docs/event_schema.md`) are **unmodified**, so the harness runs as shipped.
- One finding worth flagging regardless of this submission: binding the listener
  IPv4-only makes every `localhost` request pay a failed IPv6 connect —
  **~2062 ms vs ~15 ms per request** on this machine. `eval/check.py` defaults to
  `http://localhost:8080`, so an otherwise-correct submission can look
  catastrophically slow for reasons unrelated to its pipeline. `run.py` binds
  dual-stack explicitly; `uvicorn --host ::` is not sufficient, because asyncio
  leaves `IPV6_V6ONLY` at the OS default.
- `eval/check.py` crashes on Windows consoles when printing `⚠`/`✓`
  (cp1252). `PYTHONIOENCODING=utf-8` works around it. Not changed, since the
  file is yours.
