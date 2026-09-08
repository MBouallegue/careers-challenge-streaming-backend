# Benchmarks

Every number in this repository was measured on the machine below. Nothing here
is estimated or extrapolated, and where a result is unflattering it is reported
as measured.

## Machine

| | |
|---|---|
| CPU | Intel Core i7-9750H @ 2.60GHz, 6 cores / 12 threads |
| RAM | 15.8 GB |
| OS | Windows 11 (10.0.22000) |
| Python | 3.13.15 |
| Stack | Django 5.2.17, DRF 3.18.1, uvicorn 0.52.4 |

Two caveats that matter when reading these:

1. **The load client is Python and runs on the same laptop.** It competes with
   the service for CPU, so the HTTP figures are a floor, not a ceiling.
2. **Windows has no `uvloop` and no `httptools`.** uvicorn falls back to
   asyncio's Proactor loop and its pure-Python HTTP parser. On Linux the same
   code gets a materially faster transport for free.

## Engine throughput, without HTTP

The pipeline in isolation: validate, append to the WAL, enqueue, aggregate,
deduplicate. 200,000 synthesised events across 5,000 devices.

| stage | rate |
|---|---:|
| accept (validate + WAL append + enqueue) | **124,285 events/sec** |
| drain (aggregate + dedup) | **507,839 events/sec** |
| **end to end** | **99,849 events/sec** |

| operation | cost |
|---|---:|
| WAL flush + fsync | 16.4 MB in <1 ms |
| snapshot write (5,000 devices, 2,467 rooms) | 100 ms |

The engine sustains roughly **twice the 50,000 events/sec** the brief asks for,
in a single CPython process. Per event that is about **8 microseconds**. The
engine is not the bottleneck; HTTP framing is.

## HTTP layer, attributed

Single warm keep-alive connection, 400-600 requests per row, so this measures
per-request cost rather than concurrency behaviour.

| path | req/s | p50 |
|---|---:|---:|
| raw ASGI app under uvicorn, no Django | 2,365 | 0.39 ms |
| bare Django async view | 683 | 1.43 ms |
| DRF (`adrf`) `APIView` | 463 | 2.02 ms |
| `POST /events` through Django + engine | 409 | 1.60 ms |
| `POST /events` through the fast ASGI route | **1,031** | **0.97 ms** |

Reading this:

- uvicorn and the platform can do ~2,365 req/s, so they are not the limit.
- Django's request/response cycle costs roughly **1.2 ms** per request.
- DRF adds a further **~0.6 ms** on top of a bare Django view.
- The engine contributes ~0.008 ms.

Hence the split described in [DESIGN.md](DESIGN.md): DRF on the read API where
0.6 ms is irrelevant, and a raw ASGI route for ingest where it is 40% of the
achievable event rate. Moving ingest off Django took single-event throughput
from 384 to 1,031 events/sec, and p50 request latency from 74 ms to 11 ms.

## End-to-end, over HTTP, with alarm SLA

Measured with `client/loadgen.py`, which subscribes to the SSE feed and
correlates each received alarm back to the moment its event was sent. This is a
true end-to-end latency, not the service's opinion of its own latency.

| batch | workers | events/sec | alarm p50 | alarm p95 | within 1 s SLA |
|---:|---:|---:|---:|---:|---:|
| 1 | 32 | 1,031 | 50 ms | 122 ms | **100%** |
| 50 | 8 | 2,957 | 155 ms | 343 ms | 98.3% |
| 200 | 8 | 7,613 | 231 ms | 910 ms | 98.0% |
| **500** | **4** | **11,933** | **170 ms** | **494 ms** | **100%** |

Zero failed requests in every run; every response was `202`.

The honest summary: **~12,000 events/sec sustained over HTTP with the full alarm
SLA met**, against an engine that can take 100,000. The gap is per-request
overhead, and batching is what closes it — which is also what a real fleet would
do rather than opening a TCP connection per heartbeat.

To reach 50,000 events/sec over HTTP the route is horizontal: shard by
`room_id` across worker processes, so occupancy, health and dedup all stay
shard-local. That is designed but not implemented; see the limits section of
[DESIGN.md](DESIGN.md).

## The `localhost` finding

Not a tuning result, but the single largest performance number in this project.

| listener | client uses `localhost` |
|---|---:|
| IPv4-only (`0.0.0.0`) | ~2062 ms/request |
| dual-stack (`::` with `IPV6_V6ONLY=0`) | ~15 ms/request |

`localhost` resolves to `::1` first on this platform. Against an IPv4-only
listener every request pays a failed IPv6 connect before falling back. The
bundled harness defaults to `http://localhost:8080`, so this alone moved the
provided generator from ~1.7 to ~47 events/sec against the reference stub.

`uvicorn --host ::` does **not** fix it: asyncio leaves `IPV6_V6ONLY` at the OS
default, which is 1 on Windows. `run.py` builds the socket explicitly.

## Reproducing

```bash
make bench-engine                 # engine throughput, no HTTP
make load                         # default: batch 500, 4 workers
python -m client.loadgen --rate 50000 --duration 30 --devices 5000 --batch 500 --concurrency 4
python -m client.restart_check    # hard-kill recovery, 7 assertions
```
