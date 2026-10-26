from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

from .config import database_url


def connect():
    return psycopg2.connect(database_url(), connect_timeout=5, options="-c statement_timeout=10000")


def persist_batch(connection, records, rejected):
    """A database commit must succeed before the caller acknowledges Kafka offsets."""
    with connection, connection.cursor() as cursor:
        inserted = 0
        if records:
            rows = [
                (
                    str(e.event_id),
                    e.user_id,
                    e.event_type,
                    e.page,
                    e.response_time_ms,
                    e.status_code,
                    e.timestamp,
                    str(e.run_id) if e.run_id else None,
                )
                for e in records
            ]
            inserted = len(
                execute_values(
                    cursor,
                    """INSERT INTO events (
                event_id, user_id, event_type, page, response_time_ms, status_code,
                event_timestamp, run_id) VALUES %s
                ON CONFLICT (event_id) DO NOTHING RETURNING event_id""",
                    rows,
                    fetch=True,
                )
            )
        if rejected:
            execute_values(
                cursor,
                """INSERT INTO rejected_events (topic, partition_id,
                offset_id, reason) VALUES %s
                ON CONFLICT (topic, partition_id, offset_id) DO NOTHING""",
                rejected,
            )
    return inserted


def initialize_schema():
    connection = connect()
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute((Path(__file__).parents[1] / "sql/schema.sql").read_text())
    finally:
        connection.close()


if __name__ == "__main__":
    initialize_schema()
