# Validation evidence — 2026-10-26

These results refer to the prepared review branch.

| Check | Actual result |
| --- | --- |
| Python 3.12.14 unit tests | **18 passed** |
| Full pytest invocation | **18 passed, 1 skipped** (explicit service-integration test) |
| Unit coverage over common/consumer/producer/api/benchmarks | **56% statements**; producer process, real DB, and loop paths remain gaps |
| Ruff lint and format | Passed |
| Event validation | Executed tests cover UUID, timestamp timezone, enums, bounds, booleans, strings |
| Delivery ordering | Unit tests exercise database persistence before explicit partition offset commit and no acknowledgement on storage failure |
| Malformed/oversize input | Unit tests exercise quarantine coordinates before acknowledgement |
| Replay accounting | Unit test exercises duplicate counts; real SQL replay uniqueness passed the CI integration test |
| Actual Kafka/PostgreSQL services | Passed in GitHub CI; local environment has no Docker runtime |
| SQL schema and Compose healthchecks | Passed CI service setup and a real PostgreSQL integration test |
| End-to-end benchmark | Passed CI smoke: 500 attempted/acknowledged/persisted events at a requested 100 events/sec |
| Remote GitHub Actions | [Passed: run 40400540202](https://github.com/malavikakashireddy/real-time-analytics-platform/actions/runs/40400540202) |
| 5,000+ events/sec / <2s aggregation / 500,000 events / six consumers / 99%+ success | **Not verified** |

The unit tests use controlled storage/consumer substitutes to verify ordering and error paths; they do not prove broker or database behavior. The opt-in integration test uses real PostgreSQL and cleans up its UUID-isolated rows. The linked CI smoke executed Kafka through PostgreSQL successfully.

The producer reports broker acknowledgement separately from the pipeline’s eventual unique-row completion. Event-to-insert latency is not aggregation/dashboard latency. See `benchmarks/README.md` for clocks, hardware, count reconciliation, repeats, and failure experiments. A Starlette TestClient deprecation warning is present locally; no tests fail because of it.

The successful CI smoke (Python 3.12.15; 4 logical CPUs; Kafka 4.3.1 and PostgreSQL 17) reconciled all 500 unique events in about 5.03 seconds. At the deliberately imposed 100 events/sec input rate, measured persisted throughput was about 99.37 events/sec; p50/p95/p99 event-to-insert latency was about 13.41/122.38/123.14 ms. This is a short smoke test, not a saturation benchmark or evidence for a 5,000 events/sec claim. The JSON result is attached to the linked run and preserved in `benchmarks/evidence/ci-smoke-2026-10-26.json`. The revision is the temporary PR merge commit recorded in that artifact.

The first CI run caught a Decimal-to-JSON serialization failure in the benchmark; numeric latency values are now normalized, with regression tests for Decimal and empty/timeout results. The service integration test and the full broker-to-database smoke both pass after the fix. Restart/rebalance/load-failure scenarios remain unverified.
