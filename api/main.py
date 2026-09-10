from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor


app = FastAPI(
    title="Real-Time Analytics API",
    description="REST API for querying real-time application analytics",
    version="1.0.0",
)


def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="analytics",
        user="analytics",
        password="analytics",
    )


@app.get("/")
def root():
    return {
        "message": "Real-Time Analytics API is running"
    }


@app.get("/health")
def health():
    return {
        "status": "healthy"
    }


@app.get("/analytics/summary")
def analytics_summary():
    connection = get_db_connection()

    cursor = connection.cursor(
        cursor_factory=RealDictCursor
    )

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_events,
            COUNT(DISTINCT user_id) AS unique_users,
            ROUND(AVG(response_time_ms), 2) AS average_response_time_ms,
            COUNT(*) FILTER (
                WHERE status_code >= 400
            ) AS error_count,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE status_code >= 400
                ) / NULLIF(COUNT(*), 0),
                2
            ) AS error_rate
        FROM events;
        """
    )

    result = cursor.fetchone()

    cursor.close()
    connection.close()

    return result


@app.get("/analytics/event-types")
def event_types():
    connection = get_db_connection()
    cursor = connection.cursor(
        cursor_factory=RealDictCursor
    )

    cursor.execute(
        """
        SELECT
            event_type,
            COUNT(*) AS event_count
        FROM events
        GROUP BY event_type
        ORDER BY event_count DESC;
        """
    )

    results = cursor.fetchall()

    cursor.close()
    connection.close()

    return results


@app.get("/analytics/pages")
def page_analytics():
    connection = get_db_connection()
    cursor = connection.cursor(
        cursor_factory=RealDictCursor
    )

    cursor.execute(
        """
        SELECT
            page,
            COUNT(*) AS event_count,
            ROUND(AVG(response_time_ms), 2)
                AS average_response_time_ms
        FROM events
        GROUP BY page
        ORDER BY event_count DESC;
        """
    )

    results = cursor.fetchall()

    cursor.close()
    connection.close()

    return results