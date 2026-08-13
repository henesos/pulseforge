# PulseForge

A distributed, self-hosted load testing engine. You describe an HTTP load scenario; PulseForge
spreads it across N workers, generates open-loop load, streams the measurements back, and answers
with global p50/p95/p99 latency and throughput.

This is a reference implementation built to demonstrate distributed-systems design, not a
production product. The interesting parts are the measurement decisions, not the feature list —
see [Design Decisions](#design-decisions).

> **Status: Phase 1 of 4 complete.** The full stack builds and runs; the target service and the
> control plane's status API are functional. Load generation, metric ingestion and result queries
> land in Phase 2. See [Roadmap](#roadmap).

---

## Quick start

Requires only Docker. No JDK or Gradle on the host — the build runs inside the image.

```bash
git clone https://github.com/<owner>/pulseforge.git
cd pulseforge/docker
docker compose up -d --build
```

First build takes a few minutes (Gradle resolves dependencies once, then caches them). When it
settles, all eight containers report healthy:

```
$ docker compose ps --format "table {{.Service}}\t{{.Status}}"
SERVICE            STATUS
clickhouse         Up 9 minutes (healthy)
control-plane      Up 44 seconds (healthy)
load-worker        Up 9 minutes (healthy)
metrics-ingestor   Up 44 seconds (healthy)
nats               Up 9 minutes (healthy)
postgres           Up 15 minutes (healthy)
redis              Up 15 minutes (healthy)
target-service     Up 9 minutes (healthy)
```

Verify the deployment can actually run a test — one call, every dependency:

```bash
$ curl -s localhost:8080/api/v1/system/status | jq
{
  "status": "UP",
  "version": "0.1.0",
  "checkedAt": "2026-08-13T19:28:01.650743560Z",
  "components": {
    "clickhouse": "UP",
    "db": "UP",
    "diskSpace": "UP",
    "nats": "UP",
    "ping": "UP",
    "redis": "UP"
  }
}
```

The endpoint returns **503** if any dependency is down, so a CI smoke test can rely on the HTTP
status alone.

Scale the generator fleet horizontally:

```bash
docker compose up -d --scale load-worker=5
```

### The target under test

The project is self-contained: load is applied to a bundled service with tunable latency and
failure behaviour, never to a third-party host.

| Endpoint      | Behaviour                                | Default          |
|---------------|------------------------------------------|------------------|
| `/api/fast`   | near-instant, the measurement baseline   | 0 ms + 2 ms jitter   |
| `/api/slow`   | slow enough to make percentile tails visible | 120 ms + 60 ms jitter |
| `/api/flaky`  | fails a fixed share of requests with 503 | 20 ms, 10 % errors |

All three are configured under `pulseforge.target.*` in `target-service/src/main/resources/application.yml`.

```bash
$ for i in $(seq 40); do curl -s -o /dev/null -w "%{http_code}\n" localhost:8081/api/flaky; done | sort | uniq -c
     37 200
      3 503
```

---

## Architecture

```mermaid
flowchart LR
    user([Operator / CI]) -->|REST| CP

    subgraph control["Control plane"]
        CP[control-plane<br/>:8080]
        PG[(PostgreSQL<br/>scenarios, runs)]
        RD[(Redis<br/>heartbeats, live state)]
    end

    CP --- PG
    CP --- RD

    CP -->|run commands| NATS{{NATS}}

    subgraph fleet["Load fleet (scaled with --scale)"]
        W1[load-worker 1]
        W2[load-worker 2]
        W3[load-worker N]
    end

    NATS --> W1
    NATS --> W2
    NATS --> W3

    W1 -->|HTTP load| T[target-service<br/>:8081]
    W2 -->|HTTP load| T
    W3 -->|HTTP load| T

    W1 -.->|heartbeat| RD
    W2 -.->|heartbeat| RD
    W3 -.->|heartbeat| RD

    W1 -->|histogram snapshots<br/>1/sec| NATS
    W2 --> NATS
    W3 --> NATS

    NATS --> ING[metrics-ingestor]
    ING -->|batched inserts| CH[(ClickHouse<br/>raw histograms)]
    CP -->|percentile queries| CH
```

The split is deliberate: **Postgres holds the small mutable state** (scenario definitions, run
status) where transactions and foreign keys matter, while **ClickHouse holds the large append-only
measurement stream** where columnar scans and merge-tree aggregation matter. Neither database is
asked to do the other's job.

### Modules

| Module             | Responsibility                                                            |
|--------------------|---------------------------------------------------------------------------|
| `common`           | Domain model, wire protocol, shared NATS plumbing                          |
| `control-plane`    | Public REST API; scenario CRUD, run lifecycle, result queries              |
| `load-worker`      | Consumes run commands, generates HTTP load, aggregates and ships histograms |
| `metrics-ingestor` | Consumes snapshots, batches them, writes to ClickHouse                     |
| `target-service`   | The bundled system under test                                              |

`common` is split on purpose: `io.pulseforge.common.domain` is plain Java with no framework
annotations and is unit-testable without a container, while `io.pulseforge.common.nats` is
explicitly Spring-aware infrastructure that services opt into with `@Import`.

---

## Scenario format

```yaml
name: checkout-flow-baseline
target: http://target-service:8081
duration: 120s
rampUp: 30s
arrivalRate: 400          # requests per second — an arrival rate, not a virtual-user count
steps:
  - method: GET
    path: /api/fast
    weight: 70
  - method: POST
    path: /api/slow
    weight: 30
    body: '{"item":"sku-1"}'
assertions:
  - p95 < 250ms
  - errorRate < 1%
```

Weights are relative, not percentages, so adding a step does not require rebalancing the others.
Assertions are evaluated when the run ends and decide its PASS/FAIL verdict — reflected in the
process exit code so the tool drops into a CI pipeline without a wrapper script.

---

## Design Decisions

### 1. Workers aggregate locally; they never ship one message per request

**Problem.** At 10 000 requests/second across 5 workers, emitting one metric message per request
means 10 000 messages/second through NATS and 10 000 rows/second into ClickHouse. The measurement
layer collapses before the target does, and you end up load-testing your own telemetry.

**Options.** (a) One message per request, aggregate centrally — simple, correct, and fatally
slow. (b) Sampling — cheap, but throws away exactly the tail that percentile assertions exist to
measure. (c) Local aggregation into an HdrHistogram, ship a serialized snapshot on an interval.

**Choice.** (c). Each worker records latencies into an HdrHistogram in-process and publishes one
snapshot per second per step. Message volume becomes `workers × steps` per second — independent of
request rate.

**Trade-off.** Results are visible at snapshot granularity rather than instantly, and a worker that
dies mid-interval loses up to one second of its samples. Both are acceptable; a telemetry pipeline
that distorts the measurement is not. This is the single most important decision in the project.

### 2. Percentiles are merged from histograms, never averaged

**Problem.** With load split across 5 workers, each reports its own p99. It is tempting to average
them. **The mean of five p99 values is not the p99 of the combined population** — it is a number
with no statistical meaning, and it is systematically wrong whenever load is unevenly distributed.

**Options.** (a) Average the per-worker percentiles — wrong. (b) Ship every raw sample and compute
exactly — correct, but that is decision 1 all over again. (c) Merge the distributions, then compute
the percentile once over the merged population.

**Choice.** (c). Histograms are stored decomposed into `(bucket, count)` rows in the
`latency_buckets` table; a global percentile is the cumulative sum over buckets summed across all
workers. ClickHouse does the merge; the arithmetic happens once, over the whole population.

**Trade-off.** Precision is bounded by HdrHistogram's bucket resolution (configured to three
significant digits) rather than exact. That is a known, quantified error — unlike averaging, which
is unbounded and unquantifiable.

### 3. Backpressure drops samples; it never slows the generator

**Problem.** If the metric pipeline stalls, something has to give. Blocking the load generator
until the queue drains is the intuitive answer and the wrong one: the generator stops issuing
requests, the offered rate silently drops, and the entire run's numbers become fiction.

**Options.** (a) Block the generator — destroys the experiment. (b) Unbounded queue — moves the
failure to an OOM under exactly the conditions you were testing. (c) Bounded queue, drop on
overflow, count every drop.

**Choice.** (c). The outbound metric queue is bounded (`pulseforge.worker.metric-queue-capacity`).
On overflow the sample is discarded and `dropped_samples` is incremented, stored per snapshot in
ClickHouse and **surfaced in the run report**.

**Trade-off.** Results can be incomplete. That is the point: a run that reports
`dropped_samples: 12043` tells you its percentiles are suspect, whereas silent data loss produces
a clean-looking report that is quietly wrong.

### 4. Open-loop arrival rate, because coordinated omission destroys tail latency

**Problem.** The standard load generator keeps N virtual users in a request → wait → request loop.
When the target slows down, those users issue *fewer* requests — so the slow period is
under-sampled precisely when it matters. This is **coordinated omission**: the measurement
apparatus cooperates with the system under test to hide its worst behaviour. A target that stalls
for 10 seconds shows up as a handful of slow samples instead of thousands, and p99 comes out
looking healthy.

**Options.** (a) Closed-loop virtual users — simple, and systematically flattering. (b) Closed-loop
with post-hoc correction — better, but corrects a distortion instead of avoiding it.
(c) Open-loop: requests are scheduled at a fixed arrival rate, independent of response times.

**Choice.** (c). `arrivalRate` is a schedule, not a concurrency level. If the target slows, requests
keep being issued on time and latency is measured against the *intended* send time, so queueing
delay lands in the histogram where it belongs.

**Trade-off.** An open-loop generator against a collapsing target accumulates in-flight requests,
which is why `max-concurrent-requests` exists as a memory guard. It also does not model a real user
who would give up and go away — but modelling that is the job of the scenario, not the generator.

### 5. Worker failure produces DEGRADED, never a quietly incomplete result

**Problem.** With five workers each issuing a fifth of the load, one dying mid-run means the
achieved rate silently drops to 80 %. The run still finishes and still produces plausible
percentiles — from an experiment that never actually ran.

**Options.** (a) Ignore it. (b) Abort the whole run. (c) Detect it, finish the run, and mark the
result.

**Choice.** (c). Workers heartbeat into Redis with a TTL; the control plane watches for expiry. A
run that loses a worker terminates as `DEGRADED` with a `status_reason` naming the worker.
`DEGRADED` is a first-class terminal state alongside `COMPLETED`, not a flag hidden inside it.

**Trade-off.** Requires liveness tracking and a reaper the system would otherwise not need. Worth
it: the alternative is a report that looks correct and isn't.

### 6. Ramp-up is part of the load profile, not a client-side courtesy

**Problem.** Slamming a cold target with 500 RPS on the first tick measures JIT warm-up, cold
connection pools and empty caches — not steady-state behaviour.

**Options.** (a) Warm up manually before the run. (b) Discard the first N seconds of results.
(c) Model ramp-up in the profile itself.

**Choice.** (c). `rampUp` makes the target rate rise linearly from zero (`LoadProfile.rateAt`), and
the ramp is applied consistently across every worker so the fleet-wide rate follows the intended
curve.

**Trade-off.** The ramp window mixes warm-up with steady-state samples in the overall histogram.
Per-window snapshots keep both separable in analysis.

---

## Known Limitations

Deliberately out of scope for a portfolio reference implementation:

- **No authentication or authorization.** The REST API is fully open. Any real deployment needs at
  minimum an API key in front of the run-trigger endpoints — this thing generates traffic.
- **No TLS anywhere.** Plain HTTP between services and to the target; NATS is unauthenticated.
- **HTTP/1.1 only.** No gRPC, WebSocket or raw-TCP load. The step model assumes request/response.
- **Single region, single compose network.** No cross-region worker placement, so this measures
  application latency, not network geography.
- **No stateful scenarios.** No cookie jars, no correlation of a response value into the next
  request, no login flows. Steps are independent and weighted.
- **In-memory run state.** Redis persistence is off; a Redis restart mid-run loses liveness data.
- **No result retention policy beyond a 30-day TTL** on the ClickHouse tables.
- **Percentile precision is bounded** by histogram bucket resolution (see decision 2).

---

## Roadmap

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Gradle multi-module skeleton, compose stack, target service, control-plane status API | ✅ Done |
| 2 | Single worker end to end: YAML parsing, NATS dispatch, load generation, HdrHistogram, ClickHouse write, result query | Next |
| 3 | Distributed scaling: rate sharding across workers, Redis heartbeats, global percentile merge, dropped-sample accounting | Planned |
| 4 | Ramp-up, assertion engine, gRPC ingestion path, Testcontainers integration tests, architecture docs | Planned |

---

## Development

The Gradle build runs inside a container, so the host needs nothing but Docker:

```bash
docker run --rm -u $(id -u):$(id -g) \
  -v "$PWD":/workspace -v pulseforge-gradle:/gradle-home \
  -w /workspace -e GRADLE_USER_HOME=/gradle-home \
  gradle:8.11-jdk21 gradle build
```

A local JDK 21 works too, via `./gradlew build`.

### Tech stack

Java 21 · Spring Boot 3.3 · Gradle (Kotlin DSL) · PostgreSQL · ClickHouse · Redis · NATS ·
HdrHistogram · Docker Compose · JUnit 5 + Testcontainers
