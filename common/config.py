"""Configuration is read at runtime; credentials are never logged."""

import os


def bootstrap_servers():
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")


def topic():
    return os.getenv("KAFKA_TOPIC", "analytics-events")


def group_id():
    return os.getenv("KAFKA_GROUP_ID", "analytics-consumer-v4")


def database_url():
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL is required; see .env.example")
    return value
