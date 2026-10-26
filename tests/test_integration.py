"""Opt-in real PostgreSQL integration; no mocks or benchmark claims."""

import os
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from api.main import app
from common.events import Event
from common.storage import connect, persist_batch
from producer.main import generate_event

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="requires local services"),
]


def test_real_persistence_replay_rejection_and_api():
    run = str(uuid4())
    event = Event.model_validate(generate_event(run))
    connection = connect()
    rejection = ("integration-test-" + run, 0, 1, "invalid_event")
    try:
        assert persist_batch(connection, [event], [rejection]) == 1
        assert persist_batch(connection, [event], [rejection]) == 0
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM events WHERE run_id=%s", (run,))
            assert cursor.fetchone()[0] == 1
        with TestClient(app) as client:
            assert client.get("/ready").status_code == 200
            assert client.get("/analytics/summary").json()["total_events"] >= 1
            assert client.get("/metrics").status_code == 200
    finally:
        with connection, connection.cursor() as cursor:
            cursor.execute("DELETE FROM events WHERE run_id=%s", (run,))
            cursor.execute("DELETE FROM rejected_events WHERE topic=%s", (rejection[0],))
        connection.close()
