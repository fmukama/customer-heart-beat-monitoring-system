# Architecture

Diagrams are PlantUML sources with rendered PNG and SVG alongside:

| Diagram | Source | Rendered |
| --- | --- | --- |
| Data flow | [dataflow.puml](dataflow.puml) | [PNG](dataflow.png) · [SVG](dataflow.svg) |
| Deployment | [deployment.puml](deployment.puml) | [PNG](deployment.png) · [SVG](deployment.svg) |

Re-render after editing a `.puml`:

```bash
make diagrams
```

![Data flow](dataflow.png)

## Component responsibilities

| Component | Owns | Does not own |
| --- | --- | --- |
| `simulator/` | Event generation, rate, abnormal and out-of-order injection | Kafka; it writes JSON to stdout only |
| `producer/` | Publishing, partition keying, delivery acknowledgement | Event content or validity |
| `consumer/validation.py` | Structural validity, judged against the JSON Schema | Whether a reading is clinically normal |
| `consumer/anomaly.py` | NORMAL / ABNORMAL classification | Rejecting anything |
| `consumer/watermark.py` | How far event time has progressed | Window assignment |
| `consumer/windows.py` | Mapping an event time to its 1-day window | Aggregation |
| `consumer/window_state.py` | Accumulating one window's statistics | Window lifecycle |
| `consumer/aggregator.py` | Window lifecycle, lateness verdicts, finalization | Persistence |
| `consumer/repository.py` | All SQL, idempotency, upserts | Business rules |
| `consumer/dlq.py` | Publishing failures with diagnostic context | Deciding what is a failure |
| `consumer/retry.py` | Bounded exponential backoff with jitter | Classifying errors |

The split that matters most: **abnormal is not invalid.** A 180 bpm
reading is structurally valid and must be stored and tagged. Only
structurally broken events are dead-lettered.

## Failure paths

| Failure | Handling | Offset committed? |
| --- | --- | --- |
| Schema violation, bad UUID, bad timestamp | Published to DLQ with error type, message, source topic/partition/offset | Only after the DLQ write succeeds |
| DLQ publish fails | Exception propagates | No — Kafka redelivers |
| Transient database error | `retry_with_backoff`, up to `MAX_RETRY_ATTEMPTS`, jittered | Only after the insert succeeds |
| Retries exhausted | Exception propagates | No |
| Duplicate delivery | `ON CONFLICT (event_id) DO NOTHING` | Yes — the insert is idempotent |
| Consumer restart | Window state reloaded lazily, per key, on first event | Offsets already committed |
| Event past allowed lateness | Stored raw with `is_late` and `lateness_seconds`; finalized aggregate untouched | Yes |

Nothing is acknowledged to Kafka until it has been durably handled —
either stored or dead-lettered. That is the core delivery guarantee.

## Why event time, not arrival time

A window is chosen from `event_time` — when the heartbeat happened — not
`ingestion_time`, when we received it. Both are stored, so lag is
measurable. An event delayed in transit still lands in the window it
belongs to, which is the point of the watermark and the late-event
policy.

## Deployment

![Deployment](deployment.png)

Kafka advertises two listeners: `INTERNAL://kafka:29092` for containers
and `EXTERNAL://localhost:9092` for the host. A single listener cannot
serve both — an in-network client told to reconnect to `localhost` would
be dialling itself.

`kafka-init` creates both topics and exits. App services gate on
`service_completed_successfully`, so topics exist before anything uses
them, with `KAFKA_AUTO_CREATE_TOPICS_ENABLE` left off.

Prometheus discovers consumers by DNS rather than a static target, so
scraping follows replicas when the consumer is scaled. See
[performance.md](performance.md).
