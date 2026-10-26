import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from kafka.structs import TopicPartition
from pydantic import ValidationError

from api.main import app
from common.events import Event
from consumer.main import process_poll
from producer.main import generate_event


def batch(values):
    partition = TopicPartition("analytics-events", 0)
    return {
        partition: [
            SimpleNamespace(topic=partition.topic, partition=0, offset=i, value=value)
            for i, value in enumerate(values)
        ]
    }


def test_generator_matches_contract():
    event = Event.model_validate(generate_event())
    assert event.timestamp.tzinfo is not None


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_id", "invalid"),
        ("timestamp", "2026-01-01"),
        ("status_code", 999),
        ("response_time_ms", -1),
        ("response_time_ms", True),
        ("event_type", "other"),
        ("user_id", ""),
        ("page", "x" * 501),
    ],
)
def test_event_validation(field, value):
    event = generate_event()
    event[field] = value
    with pytest.raises(ValidationError):
        Event.model_validate(event)


def test_db_commit_precedes_offset_commit(monkeypatch):
    order = []

    def persist(connection, valid, rejected):
        order.append("database_committed")
        assert len(valid) == 1 and rejected == []
        return 1

    monkeypatch.setattr("consumer.main.persist_batch", persist)
    consumer = MagicMock()
    consumer.commit.side_effect = lambda **kw: order.append("offset_committed")
    result = process_poll(consumer, object(), batch([json.dumps(generate_event()).encode()]))
    assert order == ["database_committed", "offset_committed"]
    assert result["inserted"] == 1
    offsets = consumer.commit.call_args.kwargs["offsets"]
    assert offsets[TopicPartition("analytics-events", 0)].offset == 1


def test_storage_failure_never_commits_offsets(monkeypatch):
    def fail(*args):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr("consumer.main.persist_batch", fail)
    consumer = MagicMock()
    with pytest.raises(RuntimeError):
        process_poll(consumer, object(), batch([json.dumps(generate_event()).encode()]))
    consumer.commit.assert_not_called()


def test_invalid_json_and_oversize_are_quarantined_before_commit(monkeypatch):
    def persist(connection, valid, rejected):
        assert not valid
        assert rejected == [
            ("analytics-events", 0, 0, "invalid_event"),
            ("analytics-events", 0, 1, "invalid_event"),
        ]
        return 0

    monkeypatch.setattr("consumer.main.persist_batch", persist)
    consumer = MagicMock()
    result = process_poll(consumer, object(), batch([b"not json", b"x" * 16385]))
    assert result["rejected"] == 2
    consumer.commit.assert_called_once()


def test_replays_count_as_duplicates(monkeypatch):
    monkeypatch.setattr("consumer.main.persist_batch", lambda *args: 0)
    result = process_poll(MagicMock(), object(), batch([json.dumps(generate_event()).encode()]))
    assert result["duplicates"] == 1


def test_empty_poll_does_not_touch_storage_or_offsets(monkeypatch):
    persist = MagicMock()
    monkeypatch.setattr("consumer.main.persist_batch", persist)
    consumer = MagicMock()
    assert process_poll(consumer, object(), {})["polled"] == 0
    consumer.commit.assert_not_called()
    persist.assert_not_called()


def test_explicit_offsets_for_multiple_partitions(monkeypatch):
    monkeypatch.setattr(
        "consumer.main.persist_batch", lambda connection, valid, rejected: len(valid)
    )
    p0, p1 = TopicPartition("analytics-events", 0), TopicPartition("analytics-events", 1)
    batches = batch([json.dumps(generate_event()).encode()])
    batches[p1] = [
        SimpleNamespace(
            topic=p1.topic, partition=1, offset=7, value=json.dumps(generate_event()).encode()
        )
    ]
    consumer = MagicMock()
    process_poll(consumer, object(), batches)
    offsets = consumer.commit.call_args.kwargs["offsets"]
    assert offsets[p0].offset == 1 and offsets[p1].offset == 8


def test_health_and_dependency_failure_are_distinct(monkeypatch):
    import psycopg2

    def fail():
        raise psycopg2.OperationalError("secret connection details")

    monkeypatch.setattr("api.main.connect", fail)
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        for route in [
            "/ready",
            "/analytics/summary",
            "/analytics/pages",
            "/analytics/event-types",
            "/metrics",
        ]:
            response = client.get(route)
            assert response.status_code == 503
            assert "secret" not in response.text
