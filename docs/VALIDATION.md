# Validation evidence — 2026-10-26

These results refer to the prepared review branch.

| Check | Actual result |
| --- | --- |
| Python 3.12.14 unit tests | **16 passed** |
| Full pytest invocation | **16 passed, 1 skipped** (explicit service-integration test) |
| Unit coverage over common/consumer/producer/api | **54% statements**; producer process, real DB, and loop paths remain gaps |
| Ruff lint and format | Passed |
| Event validation | Executed tests cover UUID, timestamp timezone, enums, bounds, booleans, strings |
| Delivery ordering | Unit tests exercise database persistence before explicit partition offset commit and no acknowledgement on storage failure |
| Malformed/oversize input | Unit tests exercise quarantine coordinates before acknowledgement |
| Replay accounting | Unit test exercises duplicate counts; real SQL uniqueness remains an integration check |
| Actual Kafka/PostgreSQL services | **Not run**: no Docker runtime |
| SQL schema and Compose healthchecks | Prepared; **not verified against live services** |
| End-to-end benchmark | Harness prepared; **not run** |
| Remote GitHub Actions | Unit/service-integration workflows prepared; check PR execution |
| 5,000+ events/sec / <2s aggregation / 500,000 events / six consumers / 99%+ success | **Not verified** |

The unit tests use controlled storage/consumer substitutes to verify ordering and error paths; they do not prove broker or database behavior. The opt-in integration test uses real PostgreSQL and cleans up its UUID-isolated rows. The CI smoke harness is designed to exercise Kafka through PostgreSQL once it runs.

The producer reports broker acknowledgement separately from the pipeline’s eventual unique-row completion. Event-to-insert latency is not aggregation/dashboard latency. See `benchmarks/README.md` for clocks, hardware, count reconciliation, repeats, and failure experiments. A Starlette TestClient deprecation warning is present locally; no tests fail because of it.
