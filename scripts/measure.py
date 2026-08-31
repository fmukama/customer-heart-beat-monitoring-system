"""
Sample pipeline throughput from Prometheus.

Run inside the dev service so prometheus is reachable by name:

    make throughput
    make throughput SAMPLE=60
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request

PROMETHEUS = os.getenv(
    "PROMETHEUS_URL",
    "http://prometheus:9090",
)

QUERIES = {
    "processed/sec": "sum(rate(consumer_messages_processed_total[1m]))",
    "failed/sec": "sum(rate(consumer_messages_failed_total[1m]))",
    "abnormal/sec": "sum(rate(consumer_abnormal_events_total[1m]))",
    "late/sec": "sum(rate(consumer_late_events_total[1m]))",
    "p50 latency (s)": (
        "histogram_quantile(0.50, sum by (le) "
        "(rate(consumer_processing_seconds_bucket[5m])))"
    ),
    "p95 latency (s)": (
        "histogram_quantile(0.95, sum by (le) "
        "(rate(consumer_processing_seconds_bucket[5m])))"
    ),
    "p99 latency (s)": (
        "histogram_quantile(0.99, sum by (le) "
        "(rate(consumer_processing_seconds_bucket[5m])))"
    ),
    "watermark lag (s)": "max(consumer_watermark_lag_seconds)",
    "windows open": "sum(consumer_windows_open)",
    "consumer instances": "count(up{job=\"heartbeat-consumer\"} == 1)",
}


def query(expression: str) -> float | None:
    url = (
        f"{PROMETHEUS}/api/v1/query?"
        + urllib.parse.urlencode({"query": expression})
    )

    with urllib.request.urlopen(url, timeout=10) as response:
        payload = json.load(response)

    result = payload.get("data", {}).get("result", [])

    if not result:
        return None

    return float(result[0]["value"][1])


def main() -> None:
    sample_seconds = int(os.getenv("SAMPLE", "0"))

    if sample_seconds:
        print(
            f"Letting rates settle for {sample_seconds}s...",
            file=sys.stderr,
        )

        time.sleep(sample_seconds)

    width = max(len(name) for name in QUERIES)

    for name, expression in QUERIES.items():
        value = query(expression)

        rendered = (
            "no data" if value is None else f"{value:,.4f}".rstrip("0").rstrip(".")
        )

        print(f"{name:<{width}}  {rendered}")


if __name__ == "__main__":
    main()
