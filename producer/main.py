import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

EVENT_TYPES = [
    "page_view",
    "search",
    "click",
    "purchase",
    "error",
]

PAGES = [
    "/",
    "/products",
    "/search",
    "/pricing",
    "/dashboard",
    "/checkout",
]


def generate_event():
    event_type = random.choice(EVENT_TYPES)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 100)}",
        "event_type": event_type,
        "page": random.choice(PAGES),
        "response_time_ms": random.randint(50, 1200),
        "status_code": (
            random.choice([400, 500])
            if event_type == "error"
            else random.choices([200, 400], weights=[97, 3])[0]
        ),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print("Real-Time Analytics Event Producer")
    print("Sending events to Kafka...\n")

    while True:
        event = generate_event()

        producer.send("analytics-events", value=event)
        producer.flush()

        print(json.dumps(event))

        time.sleep(0.5)


if __name__ == "__main__":
    main()