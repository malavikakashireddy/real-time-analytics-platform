"""Bounded event production; throughput here measures broker acknowledgement only."""

import argparse
import json
import platform
import random
import time
import uuid
from collections import deque
from datetime import UTC, datetime
from pathlib import Path

from kafka import KafkaProducer

from common.config import bootstrap_servers, topic

EVENT_TYPES = ["page_view", "search", "click", "purchase", "error"]
PAGES = ["/", "/products", "/search", "/pricing", "/dashboard", "/checkout"]


def generate_event(run_id=None):
    event_type = random.choice(EVENT_TYPES)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 100)}",
        "event_type": event_type,
        "page": random.choice(PAGES),
        "response_time_ms": random.randint(50, 1200),
        "status_code": random.choice([400, 500]) if event_type == "error" else 200,
        "timestamp": datetime.now(UTC).isoformat(),
        "run_id": run_id,
    }


def publish(count, rate=0, run_id=None):
    if count < 1 or rate < 0:
        raise ValueError("count must be positive and rate non-negative")
    run_id = str(uuid.UUID(run_id)) if run_id else str(uuid.uuid4())
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers(),
        acks="all",
        retries=5,
        linger_ms=10,
        batch_size=65536,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    pending = deque()
    acknowledged = 0
    started = time.perf_counter()
    try:
        for index in range(count):
            event = generate_event(run_id)
            pending.append(producer.send(topic(), key=event["user_id"].encode(), value=event))
            if len(pending) >= 1000:
                pending.popleft().get(timeout=30)
                acknowledged += 1
            if rate:
                remaining = (index + 1) / rate - (time.perf_counter() - started)
                if remaining > 0:
                    time.sleep(remaining)
        producer.flush(timeout=30)
        while pending:
            pending.popleft().get(timeout=30)
            acknowledged += 1
        elapsed = time.perf_counter() - started
        return {
            "run_id": run_id,
            "attempted": count,
            "acknowledged": acknowledged,
            "producer_seconds": elapsed,
            "broker_acked_events_per_second": acknowledged / elapsed,
            "requested_rate": rate,
            "python": platform.python_version(),
            "scope": "producer to broker; excludes consumer/database completion",
        }
    finally:
        producer.close(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=float, default=2, help="events/sec; 0 means unthrottled")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.count < 1 or args.rate < 0:
        parser.error("count must be positive and rate non-negative")
    result = json.dumps(publish(args.count, args.rate), indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(result + "\n")
    print(result)


if __name__ == "__main__":
    main()
