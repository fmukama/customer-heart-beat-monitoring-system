# Performance and Scaling

Load testing and consumer-group scaling.

## How to reproduce

```bash
make load-up RATE=1000 CONSUMERS=2
```

`RATE` is simulator events/sec. `CONSUMERS` is *additional* workers
joining the group — the base `consumer` service is always present, so
total instances = `CONSUMERS + 1`.

```bash
make throughput SAMPLE=90    # sample Prometheus once rates settle
make lag                     # per-partition lag and ownership
make load-down               # back to the single-consumer stack
```

Always pass `SAMPLE` — containers are recreated on `load-up`, and both
Prometheus DNS discovery and the 1-minute `rate()` window need time to
settle. Measuring immediately reports numbers that are far too low.

## Environment

Single Docker Desktop host, Windows 11, Kafka 4.0 KRaft single broker,
3 partitions on `heart-rate-events`, PostgreSQL 17. All services share
one machine, so these figures are relative, not absolute capacity.

## Results

| Test | Rate | Consumers | Achieved input/s | Processed/s | p50 | p95 | p99 | Lag behaviour |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Baseline | 1 | 1 | 1.0 | 1.0 | 21.9ms | 47.0ms | 49.8ms | Stable at ~0 |
| Test 1 | 100 | 2 | 100 | 110 | 17.4ms | 24.5ms | 36.0ms | Drains backlog |
| Test 2 | 1,000 | 3 | 1,000 | 195 | 17.5ms | 24.5ms | 36.7ms | Grows ~800/s |
| Test 3 | 5,000 | 4 | ~3,450 | 206 | 17.5ms | 24.5ms | 36.2ms | Grows ~3,250/s |

## Where the bottleneck is

**The consumer, at roughly 57–65 events/sec per instance.**

p50 processing time is 17.5ms and essentially flat across every rate.
`1 / 0.0175 ≈ 57/s`, which matches measured per-instance throughput.
Latency not degrading under load means the consumer is not contended —
it is serialised. Each event costs two synchronous round trips:

1. `insert_event` runs one `INSERT` followed by `connection.commit()`
2. `self.consumer.commit()` synchronously commits the Kafka offset

Both are per event, so throughput is bounded by round-trip latency
rather than by CPU, Kafka, or PostgreSQL capacity.

**A second, separate ceiling sits in the producer** at ~3,450/s, which
is why Test 3 could not reach 5,000. Generation, JSON encoding, and the
`simulator | producer` pipe are all single-threaded Python.

Before this phase there was a third and much lower ceiling: the producer
waited for each message's acknowledgement with `future.get()`, capping
input at roughly `1 / linger_ms` ≈ 20/s. `KAFKA_SYNC_SEND=false` (set by
the load overlay) removes it.

## Answers to the load-testing questions

**Where does the bottleneck appear?** The consumer's per-event commits,
at ~57/s per instance. Not Kafka, not PostgreSQL capacity.

**Does Kafka accumulate messages?** Yes. Kafka ingested ~3,450/s
without difficulty while consumers drained ~206/s, so lag is purely a
consumer-side deficit. Kafka absorbing the difference is the design
working as intended.

**Does consumer lag increase?** Yes, once input exceeds aggregate
consumer throughput. At 1,000/s with 3 consumers, lag reached ~98,000
in about 2 minutes.

**Does PostgreSQL become the bottleneck?** Not by capacity. Single-row
inserts are trivial for PostgreSQL 17; the cost is the per-event
transaction commit. Batching inserts would move this ceiling
substantially, at the price of the current per-event durability.

**Can additional consumer instances improve throughput?** Yes, close to
linearly, up to the partition count:

| Instances | Processed/s | Per instance |
| --- | --- | --- |
| 1 | ~60 | 60 |
| 2 | 110 | 55 |
| 3 | 195 | 65 |
| 4 | 206 | 52 |

The gain from 3 to 4 is noise, not scaling — see below.

**Does partition count affect parallelism?** Yes, decisively.

## Deliverable 17 — partitions versus consumers

Partition count is the hard ceiling on useful consumers in a group.

**3 partitions, 2 consumers** — assignment is uneven, and so is load:

| Partition | Owner | Lag |
| --- | --- | --- |
| 0 | worker A | 805 |
| 1 | worker A | 1,107 |
| 2 | worker B | 1 |

Worker A holds two partitions and falls behind while worker B, holding
one, keeps pace. Aggregate throughput is limited by the busiest member.

**3 partitions, 3 consumers** — one partition each, evenly balanced.
This is the configuration that produced the best per-instance figures.

**3 partitions, 4 consumers** — the fourth consumer gets nothing:

```
CONSUMER-ID                              #PARTITIONS
kafka-python-3.0.11-74c0d180-...              1
kafka-python-3.0.11-5294d0d5-...              1
kafka-python-3.0.11-53d48fc3-...              1
kafka-python-3.0.11-7fe1cbc7-...              0
```

It joins the group, is assigned zero partitions, and idles. Throughput
rose only from 195/s to 206/s — within measurement noise. This is the
learning objective demonstrated directly:

> Kafka partitions provide the parallelism available to a consumer group.

To scale past 3 consumers, raise `KAFKA_TOPIC_PARTITIONS` and recreate
the topic.

## Correctness under scaling

Two properties make horizontal scaling safe here.

Events are keyed by `customer_id`, so all of a customer's events land on
one partition and therefore one consumer. A customer's window state is
never split across instances.

Window state is loaded lazily, per key, on the first event for that
window (`repository.load_window`). An earlier eager implementation
loaded *every* unfinalized window at startup, which under scaling meant
each worker held state for customers it did not own and re-persisted
stale values over its peers' aggregates. Lazy loading means a worker
only ever owns windows it actually receives events for.

## Known limits

- Figures come from one shared host; absolute numbers will differ
  elsewhere. The *ratios* and the identified bottleneck are the result.
- CPU and memory were not captured per container. `docker stats` during
  a run would add that.
- 5,000/s was not reached on the input side, so the consumer's ceiling
  above ~206/s aggregate is untested.
- Raising throughput meaningfully means batching offset commits and
  inserts, which trades away per-event durability. Not attempted, since
  the current guarantee is what deliverables 9 through 11 specify.
