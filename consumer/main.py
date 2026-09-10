import json

from kafka import KafkaConsumer


def main():
    consumer = KafkaConsumer(
        "analytics-events",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="analytics-consumer-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print("Real-Time Analytics Consumer")
    print("Waiting for events...\n")

    for message in consumer:
        event = message.value
        print(json.dumps(event))


if __name__ == "__main__":
    main()