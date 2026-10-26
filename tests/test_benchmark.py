import json
from decimal import Decimal
from unittest.mock import MagicMock

from benchmarks.pipeline import benchmark


def test_database_decimals_are_json_serializable(monkeypatch):
    monkeypatch.setattr("benchmarks.pipeline.publish", lambda count, rate: {"run_id": "run"})
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [
        (2,),
        (Decimal("10.5"), Decimal("20.5"), Decimal("30.5"), Decimal("1.5")),
    ]
    monkeypatch.setattr("benchmarks.pipeline.connect", lambda: connection)
    result = benchmark(2, 0, 5)
    decoded = json.loads(json.dumps(result))
    assert decoded["complete"] is True
    assert decoded["latency_ms"] == {"p50": 10.5, "p95": 20.5, "p99": 30.5, "min": 1.5}
    connection.close.assert_called_once()


def test_empty_timeout_has_null_latency_and_incomplete_result(monkeypatch):
    monkeypatch.setattr("benchmarks.pipeline.publish", lambda count, rate: {"run_id": "run"})
    monkeypatch.setattr("benchmarks.pipeline.time.monotonic", MagicMock(side_effect=[0, 1]))
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [(0,), (None, None, None, None)]
    monkeypatch.setattr("benchmarks.pipeline.connect", lambda: connection)
    result = benchmark(2, 0, 0.1)
    assert not result["complete"]
    assert not result["clock_skew_detected"]
    assert all(value is None for value in result["latency_ms"].values())
    json.dumps(result)
