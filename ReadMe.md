# Real-Time Customer Heartbeat Monitoring System

Simulated heart-rate sensors → Kafka → event-time stream processing with
watermarks and 1-day tumbling windows → PostgreSQL, with Prometheus and
Grafana for observability.

**Everything runs in Docker.**

```bash
make up      # start the whole stack
make urls    # every dashboard URL and credential
make help    # every target
```

---

## Quick start

```bash
cp .env.example .env   # optional, every value has a default
make up
make urls
```

| Service | URL | Credentials |
| --- | --- | --- |
| **Grafana** dashboards | http://localhost:3000 | `admin` / `admin_password` |
| **Adminer** (PostgreSQL) | http://localhost:8080 | server `postgres`, db `heartbeat`, user `heartbeat_user`, pass `heartbeat_password` |
| **Prometheus** | http://localhost:9090 | — |
| **Alertmanager** | http://localhost:9093 | — |
| Consumer metrics | http://localhost:8000/metrics | — |
| Notifier health | http://localhost:9091/health | — |

`make adminer`, `make grafana`, `make prometheus` print the URL and
credentials for each. Grafana's dashboard (**Heartbeat Pipeline**,
9 panels) and its datasource are provisioned automatically — nothing to
click.

---

## Technology choices

| Choice | Why |
| --- | --- |
| Kafka 4.0, **KRaft** | Durable replayable partitioned log.|
| **Python** consumer, no Flink | The windowing and watermark logic is the learning objective|
| PostgreSQL 17 | Indexed time-series storage with the `ON CONFLICT` upsert idempotency needs |
| Prometheus + Grafana | Application metrics belong in a time-series store, not the operational database |
| JSON Schema | One machine-checkable definition of a valid event |

Kafka runs two listeners — `kafka:29092` for containers, `localhost:9092`
for the host. A single listener cannot serve both: a container told to
reconnect to `localhost` would dial itself.

Topics are created by a `kafka-init` service that runs once and exits;
app services gate on it, and auto-creation is off.

| Topic | Partitions | Replication | Retention |
| --- | --- | --- | --- |
| `heart-rate-events` | 3 | 1 | 7 days |
| `heart-rate-events-dlq` | 3 | 1 | 30 days |

---

## Event schema

[`schemas/heart_rate_event.json`](schemas/heart_rate_event.json) is the
single source of truth, loaded and enforced at runtime.

```json
{
  "event_id": "3f2b1c94-...",
  "customer_id": "customer-0001",
  "heart_rate": 75,
  "event_time": "2026-08-30T10:00:00+00:00"
}
```

`event_id` is a UUID and the primary key. `customer_id` is the Kafka
partition key, so a customer's events stay ordered on one partition.
`heart_rate` is an integer 20–250. `event_time` needs a timezone offset.
Unknown fields are rejected.

**Abnormal is not invalid.** 45 or 180 bpm is stored and tagged
`ABNORMAL`. Only structurally broken events are dead-lettered.

---

## Testing the pipeline.

| Fault | Default rate | Models |
| --- | --- | --- |
| Abnormal reading | 5% | Valid event outside 60–100 bpm |
| Out of order | 5% | `event_time` backdated up to 10 min |
| Extreme late | 0.5% | Device offline for ~2 days, then synced |
| Out of range | 0.4% | Malfunctioning sensor, violates the schema |
| Invalid payload | 0.4% | Buggy device: bad UUID, missing field, wrong type |
| Duplicate | 1% | Device retrying a send it already made |

One command shows what the pipeline handled:

```bash
make show-faults
```

### On-demand injection

`make inject WHAT=<case>` fires one specific case immediately, for when
you need to demonstrate rather than wait for probability:

```bash
make inject WHAT=future        # +3d → advances watermark, closes windows
make inject WHAT=toolate       # -2d → raw kept, finalized window untouched
```

`WHAT` also accepts `normal`, `abnormal`, `invalid`, `outofrange`,
`late` and `duplicate`, though the simulator already produces all six.

The two above are **the only cases that cannot be automated**:

- **`future`** is not sensor behaviour. It is a tool for jumping the
  watermark so windows finalize inside a demo instead of after a real
  day. Making it probabilistic would repeatedly finalize open windows
  early and push every later event past the grace period — it would
  corrupt the aggregates rather than exercise them.
- **`toolate`, the finalized-window case,** needs a window that is *already*
  finalized. The simulator's extreme-late events produce huge lateness,
  but if their window was never opened the aggregator simply opens it,
  so `aggregated=false` cannot be produced reliably by chance. The
  `future` → `toolate` sequence is what demonstrates it.

Inspect the result:

```bash
make show-faults    # what the simulator produced and how it was handled
make show-raw       # recent events with status, is_late, lateness
make show-daily     # daily aggregates, open and finalized
make show-late      # late count and worst lateness
make show-dlq N=3   # read the dead letter topic
make reconcile      # aggregates vs raw counts
make metrics        # raw Prometheus metrics
make throughput     # rates, latency percentiles, watermark lag
```

### Walkthroughs for each guarantee

**Invalid → DLQ, and nothing reaches PostgreSQL**

```bash
make show-dlq
```

The DLQ already has entries — the simulator emits malformed payloads
continuously. No injection needed.

The DLQ payload carries the schema's own message, the original event,
and the source topic / partition / offset. The offset is committed
**only after** the DLQ write is acknowledged — if the DLQ fails, Kafka
redelivers rather than dropping.

**Late events are detected and measured**

```bash
make show-late
```

A non-zero late count is the real proof: it requires `event_time` to be
genuinely backdated, not merely delivered late.

**Windows finalize, then become immutable**

```bash
make show-daily                 # note event_count for customer-0001
make inject WHAT=future         # jump the watermark 3 days ahead
make show-daily                 # windows now is_finalized = true
make inject WHAT=toolate        # 240 bpm into the closed window
make show-raw                   # stored, is_late = true
make show-daily                 # aggregate UNCHANGED
```

**Idempotency under at-least-once delivery**

```bash
make show-faults   # duplicate_rows is 0 despite a 1% duplicate rate
```

**Aggregates reconcile with raw data**

```bash
docker compose stop producer    # let one flush interval pass
make reconcile                  # every open window: reconciles = t
docker compose start producer
```

Two things make this check meaningful only under those conditions:

- Stop the producer first. While it runs, the open-window snapshot
  legitimately trails the raw table by up to
  `WINDOW_FLUSH_INTERVAL_SECONDS`.
- `make reconcile` compares **open windows only**. A finalized window is
  immutable, so events arriving after it closed are stored raw and
  deliberately excluded from the aggregate. Comparing global totals
  after any window has finalized would report a mismatch that is in fact
  the policy working correctly.

**State survives a consumer restart**

```bash
make reconcile
docker compose restart consumer
make consumer          # look for window state being reloaded
make reconcile         # still reconciles, not reset to a partial
```

**Scaling and partition assignment**

```bash
make load-up RATE=1000 CONSUMERS=2
make lag               # per-partition lag and which consumer owns what
make throughput SAMPLE=90
make load-down
```

With 3 partitions and 4 consumers, one consumer is assigned zero
partitions and idles — partition count is the ceiling on useful
parallelism. Measured numbers: **[docs/performance.md](docs/performance.md)**.

### Automated tests

```bash
make test               # 96 unit tests, no stack needed
make test-integration   # 29 integration tests, needs `make up`
make test-all
make lint
```

Integration tests run the real consumer against real Kafka and
PostgreSQL. They are isolated from the running
pipeline on both axes — their own topics per test, their own PostgreSQL
schema built from the real DDL — so a test run cannot corrupt live
aggregates and the live pipeline cannot skew a test.

---

## How the stream processing works

**Watermark** — `max_event_time_seen − ALLOWED_OUT_OF_ORDERNESS_SECONDS`.
Moves forward, never backward, derived from event time only. An event is
never late relative to itself: lateness is judged against the watermark
*before* that event was applied. `consumer_watermark_lag_seconds` sits
at roughly the configured out-of-orderness on a healthy stream.

**Windows** — 1-day tumbling, `00:00 → 24:00` UTC, non-overlapping,
assigned from `event_time`. State is held in memory per
`(customer_id, window_start)` and written to `heart_rate_daily`. Open
windows are snapshotted every `WINDOW_FLUSH_INTERVAL_SECONDS` with
`is_finalized = false`, so today is queryable before it closes. A window
finalizes once the watermark passes `window_end + ALLOWED_LATENESS_SECONDS`.

**Late events** — three outcomes:

| Arrival | Raw row | Aggregate |
| --- | --- | --- |
| On time, or within allowed out-of-orderness | Stored, `is_late=false` | Updated |
| Late, window still open | Stored, `is_late=true`, lateness recorded | Updated — correct window, by event time |
| Past allowed lateness, window finalized | Stored, `is_late=true`, lateness recorded | **Unchanged** |

Raw data is never discarded; a finalized window is immutable. Late
events are never treated as invalid and never reach the DLQ — lateness
is a timing property, not a validity one.

The simulator creates genuinely out-of-order events by **backdating
`event_time`**. Delaying delivery alone would not work: `event_time`
would still increase monotonically, so nothing could ever be late.

**Restart** — offsets commit as events are processed, so a restarted
consumer never re-reads them. Window state is reloaded from PostgreSQL
lazily, on the first event for each window. Lazy loading is what makes
scaling safe: a worker only owns windows it actually receives events
for.

---

## Monitoring

`consumer_messages_processed_total`,
`consumer_messages_failed_total{reason}`, `consumer_dlq_messages_total`,
`consumer_abnormal_events_total`,
`consumer_late_events_total{aggregated}`,
`consumer_processing_seconds` (histogram),
`consumer_watermark_lag_seconds`, `consumer_windows_open`,
`consumer_windows_finalized_total`.

Prometheus discovers consumers by DNS, so scraping follows replicas when
scaled.

---

## Notifications

```
Prometheus → alert rules → Alertmanager → notifier → notifications table
```

Alerts cover **operational failures, not clinical anomalies**. An
abnormal heart rate is valid business data, already stored and tagged.
A dead consumer is not.

| Alert | Fires when | For | Severity |
| --- | --- | --- | --- |
| `ConsumerDown` | No healthy consumer instance | 1m | CRITICAL |
| `PostgresUnavailable` | Notifier cannot reach PostgreSQL | 1m | CRITICAL |
| `KafkaLagHigh` | Consumer group lag > 10,000 | 5m | WARNING |
| `ProcessingLatencyHigh` | p95 processing > 500ms | 5m | WARNING |
| `DLQRateHigh` | > 5% of events dead-lettered | 5m | WARNING |
| `WatermarkStuck` | Watermark lag > 900s | 10m | WARNING |

Thresholds are set against measured baselines from
[docs/performance.md](docs/performance.md), not guessed — p95 is normally
~25ms, watermark lag sits at ~300s, and the DLQ baseline is ~0.8% of
traffic. DLQRateHigh is a proportion rather than an absolute rate, so
it does not false-fire simply because throughput rose.

Every alert is recorded in PostgreSQL with a firing/resolved lifecycle,
so the system can answer *"show me every notification in the last 30
days."*

```bash
make alerts               # rules and their current state
make show-notifications   # alert history
make alert-demo           # stop the consumer, watch ConsumerDown fire and resolve
```

```sql
SELECT alert_type, severity, status, created_at, resolved_at
FROM notifications
WHERE created_at > now() - interval '30 days'
ORDER BY created_at DESC;
```

Alertmanager is at http://localhost:9093, the notifier at
http://localhost:9091/health.

**The notifier is a separate service on purpose.** `ConsumerDown` is one
of the alerts, so a recorder inside the consumer would die alongside the
thing it reports on. It also probes PostgreSQL independently, which is
where `PostgresUnavailable` gets its signal.

To add Slack or email, uncomment the receiver stub in
[config/alertmanager/alertmanager.yml](config/alertmanager/alertmanager.yml)
and put the webhook URL in your own `.env`. Nothing outbound is
configured by default.

---

## CI/CD

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) — deliberately
slim:

```
push / PR
  ├── quality      lint → unit tests → docker build
  └── integration  start stack → wait for first event → full pipeline
```

CI uses the same `make` targets you run locally, so it cannot drift.

---

## Configuration

Everything is environment-driven — see [.env.example](.env.example).
Nothing needs a source edit, including event rate, out-of-order
probability, watermark lag, allowed lateness, and flush interval.

---

## Known limitations

**Aggregates depend on the insert result, not on delivery.** A
redelivered event is refused by `ON CONFLICT` and is therefore *not*
folded into its window either — the consumer calls `observe()` for every
delivery but `add_to_window()` only when the insert actually created a
row. That keeps `event_count` equal to the raw count under
at-least-once delivery. Verified live: 331 deliveries, 2 duplicates
ignored, 329 rows, 329 distinct, every open window reconciling.

The residual gap is a hard crash *between* the insert and the in-memory
update, which would lose that event's contribution to the window until
the aggregate is rebuilt from raw. Raw data is always authoritative.

**Throughput caps at ~57–65 events/sec per consumer**, set by two
synchronous round trips per event (database commit, Kafka offset
commit). Batching would raise it at the cost of per-event durability.

**Memory grows with `customers × open windows`.** Fine at this scale; a
very large customer set with long allowed lateness would need spilling.

**Single-node infrastructure** — one broker, replication factor 1, one
PostgreSQL. No failover.

**The producer is single-threaded** and caps near 3,450 events/sec, so
the design doc's 5,000/sec target was not reached on the input side.

**No authentication** — Kafka is `PLAINTEXT`, and PostgreSQL, Grafana and
Adminer use development credentials. Local development only.

**Alert history shares its database with the data it monitors.** If
PostgreSQL is down, `PostgresUnavailable` cannot be written until it
returns; the notifier buffers in memory and always logs to stdout. A
production system would keep the alert store elsewhere.

**No outbound notification channel is configured.** Alerts land in
PostgreSQL. Slack and email are stubbed but need credentials.

**Naming differs from the original design notes**, which used
`heartbeat-events` and `heartbeat_event.json` where the implementation
uses `heart-rate-events` and `heart_rate_event.json`. The code is
internally consistent; the notes predate it.

---

## Layout

```
simulator/   Synthetic event generation
producer/    Kafka publishing
consumer/    Validation, classification, event time, windowing, persistence
notifier/    Alertmanager webhook sink, PostgreSQL health probe
schemas/     JSON Schema — the event contract
database/    Numbered DDL, applied automatically on first start
config/      Prometheus scrape config, Grafana provisioning
tests/       Unit tests, plus tests/integration against the live stack
scripts/     inject.py (crafted events), measure.py (throughput sampling)
docs/        Architecture (PlantUML), performance results, evidence checklist
```
