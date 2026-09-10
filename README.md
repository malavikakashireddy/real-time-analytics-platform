# Real-Time Analytics & Observability Platform

A real-time event analytics platform that ingests application events through Apache Kafka, processes streaming data with Python, and exposes analytics through a backend API and interactive dashboards.

## Overview

This project demonstrates a production-style streaming analytics pipeline for collecting, processing, storing, and visualizing application events in real time.

The platform is designed to answer questions such as:

* How many events are being generated?
* Which event types are most common?
* How many errors are occurring?
* What is the average response time?
* How many unique users are active?
* Which pages receive the most activity?

## Architecture

```text
Event Generator
      │
      ▼
   Apache Kafka
      │
      ▼
 Python Consumer
      │
      ├──────────► PostgreSQL
      │
      └──────────► Redis
                     │
                     ▼
                  FastAPI
                     │
                     ▼
              React Dashboard

              Grafana
                 │
                 ▼
        System Monitoring
```

## Tech Stack

* **Python** — event generation, stream processing, backend services
* **Apache Kafka** — event streaming and message ingestion
* **PostgreSQL** — persistent analytics storage
* **Redis** — fast-access counters and cached metrics
* **FastAPI** — analytics REST API
* **React** — interactive dashboard
* **Grafana** — monitoring and observability
* **Docker** — local infrastructure and service orchestration

## Current Progress

### Completed

* Kafka infrastructure running with Docker
* Python event producer
* Kafka topic for analytics events
* Python Kafka consumer
* Event serialization/deserialization
* Streaming events between producer and consumer

### In Progress

* Real-time metric aggregation
* PostgreSQL persistence
* Redis counters and caching
* FastAPI analytics endpoints
* React analytics dashboard
* Grafana monitoring

## Example Event

```json
{
  "event_id": "example-event-id",
  "user_id": "user_42",
  "event_type": "page_view",
  "page": "/dashboard",
  "response_time_ms": 245,
  "status_code": 200,
  "timestamp": "2026-09-01T18:30:00+00:00"
}
```

## Planned Analytics

The platform will calculate:

* Total events
* Events per minute
* Events by event type
* Error count and error rate
* Average response time
* Unique users
* Events by page
* Top active users/pages
* Time-series event activity

## Running Locally

### 1. Start Kafka

```bash
docker compose up -d
```

### 2. Start the producer

```bash
python3 producer/main.py
```

### 3. Start the consumer

```bash
python3 consumer/main.py
```

The producer generates application events and publishes them to Kafka. The consumer reads those events from the `analytics-events` topic for processing.

## Roadmap

* [x] Kafka infrastructure
* [x] Event producer
* [x] Event consumer
* [x] Streaming metric aggregation
* [x] PostgreSQL data storage
* [x] Redis caching
* [x] FastAPI analytics API
* [ ] React dashboard
* [ ] Grafana observability
* [ ] Load testing and performance benchmarking
* [ ] Automated tests and CI

## Goal

Build a complete real-time analytics pipeline that demonstrates practical experience with streaming systems, backend development, data processing, databases, observability, and scalable application architecture.

