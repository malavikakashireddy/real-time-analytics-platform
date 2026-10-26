"""At-least-once Kafka consumption with idempotent PostgreSQL storage."""

import json
import logging
import signal
import time

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata
from pydantic import ValidationError

from common.config import bootstrap_servers, group_id, topic
from common.events import Event
from common.storage import connect, persist_batch

logger = logging.getLogger(__name__)
MAX_EVENT_BYTES = 16_384


def process_poll(consumer, connection, batches):
    valid, rejected, offsets = [], [], {}
    polled = 0
    for partition, messages in batches.items():
        for message in messages:
            polled += 1
            try:
                if len(message.value) > MAX_EVENT_BYTES:
                    raise ValueError("oversize")
                event = Event.model_validate_json(message.value)
                valid.append(event)
            except (ValidationError, ValueError):
                # Store coordinates and a safe reason, never arbitrary payloads or credentials.
                rejected.append((message.topic, message.partition, message.offset, "invalid_event"))
            offsets[partition] = OffsetAndMetadata(message.offset + 1, "", -1)
    if not polled:
        return {"polled": 0, "inserted": 0, "duplicates": 0, "rejected": 0}
    inserted = persist_batch(connection, valid, rejected)
    # Explicit offsets cover only records included in the completed DB transaction.
    consumer.commit(offsets=offsets)
    return {
        "polled": polled,
        "inserted": inserted,
        "duplicates": len(valid) - inserted,
        "rejected": len(rejected),
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop)
    consumer = None
    connection = None
    try:
        connection = connect()
        consumer = KafkaConsumer(
            topic(),
            bootstrap_servers=bootstrap_servers(),
            group_id=group_id(),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=500,
            max_poll_interval_ms=300000,
        )
        while not stopping:
            started = time.perf_counter()
            result = process_poll(
                consumer, connection, consumer.poll(timeout_ms=1000, max_records=500)
            )
            if result["polled"]:
                logger.info(
                    json.dumps(
                        {
                            "event": "batch_persisted",
                            **result,
                            "batch_seconds": time.perf_counter() - started,
                        }
                    )
                )
    except (KafkaError, psycopg2.Error, OSError, RuntimeError) as exc:
        logger.error(json.dumps({"event": "consumer_failed", "error_type": type(exc).__name__}))
        raise SystemExit(1) from None
    finally:
        if consumer:
            consumer.close(autocommit=False)
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
