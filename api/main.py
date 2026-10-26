import logging
from contextlib import contextmanager

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from psycopg2.extras import RealDictCursor

from common.storage import connect

logger = logging.getLogger(__name__)
app = FastAPI(title="Real-Time Analytics API", version="1.1.0")


@contextmanager
def query_cursor():
    connection = None
    try:
        connection = connect()
        with connection, connection.cursor(cursor_factory=RealDictCursor) as cursor:
            yield cursor
    except (psycopg2.Error, RuntimeError) as exc:
        logger.error("analytics dependency failed: %s", type(exc).__name__)
        raise HTTPException(status_code=503, detail="Analytics storage unavailable") from None
    finally:
        if connection:
            connection.close()


@app.get("/")
def root():
    return {"message": "Real-Time Analytics API is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/ready")
def ready():
    with query_cursor() as cursor:
        cursor.execute("SELECT 1 FROM events LIMIT 1")
    return {"status": "ready"}


@app.get("/analytics/summary")
def analytics_summary():
    with query_cursor() as cursor:
        cursor.execute("""SELECT COUNT(*) AS total_events,
            COUNT(DISTINCT user_id) AS unique_users,
            COALESCE(ROUND(AVG(response_time_ms), 2), 0) AS average_response_time_ms,
            COUNT(*) FILTER (WHERE status_code >= 400) AS error_count,
            COALESCE(ROUND(100.0 * COUNT(*) FILTER (WHERE status_code >= 400)
                / NULLIF(COUNT(*), 0), 2), 0) AS error_rate FROM events""")
        return cursor.fetchone()


@app.get("/analytics/event-types")
def event_types():
    with query_cursor() as cursor:
        cursor.execute("""SELECT event_type, COUNT(*) AS event_count FROM events
            GROUP BY event_type ORDER BY event_count DESC, event_type""")
        return cursor.fetchall()


@app.get("/analytics/pages")
def page_analytics():
    with query_cursor() as cursor:
        cursor.execute("""SELECT page, COUNT(*) AS event_count,
            ROUND(AVG(response_time_ms), 2) AS average_response_time_ms FROM events
            GROUP BY page ORDER BY event_count DESC, page""")
        return cursor.fetchall()


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    with query_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM events")
        total = cursor.fetchone()["total"]
        cursor.execute("SELECT COUNT(*) AS total FROM rejected_events")
        rejected = cursor.fetchone()["total"]
    return (
        "# TYPE analytics_events_stored gauge\n"
        f"analytics_events_stored {total}\n"
        "# TYPE analytics_events_rejected gauge\n"
        f"analytics_events_rejected {rejected}\n"
    )
