-- Fresh local database schema. Existing installations require a reviewed migration.
CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    page VARCHAR(500) NOT NULL,
    response_time_ms INTEGER NOT NULL CHECK (response_time_ms >= 0),
    status_code INTEGER NOT NULL CHECK (status_code BETWEEN 100 AND 599),
    event_timestamp TIMESTAMPTZ NOT NULL,
    run_id UUID,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS events_timestamp_idx ON events(event_timestamp);
CREATE INDEX IF NOT EXISTS events_run_id_idx ON events(run_id);
CREATE TABLE IF NOT EXISTS rejected_events (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    reason TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (topic, partition_id, offset_id)
);
