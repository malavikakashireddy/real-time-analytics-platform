import json
from collections import Counter

import psycopg2
from kafka import KafkaConsumer


def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="analytics",
        user="analytics",
        password="analytics",
    )


def insert_event(cursor, event):
    cursor.execute(
        """
        INSERT INTO events (
            event_id,
            user_id,
            event_type,
            page,
            response_time_ms,
            status_code,
            event_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
        """,
        (
            event["event_id"],
            event["user_id"],
            event["event_type"],
            event["page"],
            event["response_time_ms"],
            event["status_code"],
            event["timestamp"],
        ),
    )


def main():
    consumer = KafkaConsumer(
        "analytics-events",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="analytics-consumer-group-v3",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    connection = get_db_connection()
    cursor = connection.cursor()

    total_events = 0
    total_response_time = 0
    error_count = 0

    event_type_counts = Counter()
    page_counts = Counter()
    unique_users = set()

    print("Real-Time Analytics Consumer")
    print("Connected to PostgreSQL")
    print("Waiting for events...\n")

    try:
        for message in consumer:
            event = message.value

            insert_event(cursor, event)
            connection.commit()

            total_events += 1
            total_response_time += event["response_time_ms"]

            event_type_counts[event["event_type"]] += 1
            page_counts[event["page"]] += 1
            unique_users.add(event["user_id"])

            if event["status_code"] >= 400:
                error_count += 1

            average_response_time = total_response_time / total_events
            error_rate = (error_count / total_events) * 100

            print("\n===== REAL-TIME ANALYTICS =====")
            print(f"Total Events: {total_events}")
            print(f"Unique Users: {len(unique_users)}")
            print(f"Average Response Time: {average_response_time:.2f} ms")
            print(f"Errors: {error_count}")
            print(f"Error Rate: {error_rate:.2f}%")
            print(f"Events by Type: {dict(event_type_counts)}")
            print(f"Events by Page: {dict(page_counts)}")

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    finally:
        cursor.close()
        connection.close()
        consumer.close()


if __name__ == "__main__":
    main()