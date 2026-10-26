"""Measure one uniquely identified producer-to-PostgreSQL run, with a bounded drain."""

import argparse
import json
import os
import platform
import subprocess
import time
from pathlib import Path

from common.storage import connect
from producer.main import publish


def benchmark(count, rate, timeout):
    if count < 1 or rate < 0 or timeout <= 0:
        raise ValueError("count and timeout must be positive; rate must be non-negative")
    started = time.perf_counter()
    producer = publish(count, rate)
    connection = connect()
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            deadline = time.monotonic() + timeout
            while True:
                cursor.execute("SELECT COUNT(*) FROM events WHERE run_id=%s", (producer["run_id"],))
                persisted = cursor.fetchone()[0]
                if persisted >= count or time.monotonic() >= deadline:
                    break
                time.sleep(0.1)
            elapsed = time.perf_counter() - started
            cursor.execute(
                """SELECT
                percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at-event_timestamp))*1000),
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at-event_timestamp))*1000),
                percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at-event_timestamp))*1000),
                MIN(EXTRACT(EPOCH FROM (ingested_at-event_timestamp))*1000)
                FROM events WHERE run_id=%s""",
                (producer["run_id"],),
            )
            p50, p95, p99, minimum = cursor.fetchone()
        try:
            revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
            dirty = bool(
                subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
            )
        except (OSError, subprocess.CalledProcessError):
            revision, dirty = "unknown", True
        return {
            "producer": producer,
            "persisted_unique_events": persisted,
            "complete": persisted == count,
            "end_to_end_seconds": elapsed,
            "persisted_events_per_second": persisted / elapsed,
            "latency_ms": {"p50": p50, "p95": p95, "p99": p99, "min": minimum},
            "clock_skew_detected": minimum is not None and minimum < 0,
            "revision": revision,
            "working_tree_dirty": dirty,
            "python": platform.python_version(),
            "os": platform.platform(),
            "logical_cpus": os.cpu_count(),
            "scope": "synthetic event creation to database INSERT timestamp; not dashboard aggregation",
        }
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--rate", type=float, default=0)
    parser.add_argument("--drain-timeout", type=float, default=60)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.count < 1 or args.rate < 0 or args.drain_timeout <= 0:
        parser.error("count and timeout must be positive; rate must be non-negative")
    result = benchmark(args.count, args.rate, args.drain_timeout)
    output = json.dumps(result, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n")
    print(output)
    if not result["complete"] or result["clock_skew_detected"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
