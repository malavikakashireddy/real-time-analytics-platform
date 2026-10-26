# Throughput and latency methodology

No 5,000 events/sec or sub-two-second aggregation claim is currently verified.

1. Start the documented services and one consumer. Record git revision, dirty state, OS, CPU, RAM, Docker limits, service/client versions, topic partitions, consumer count, broker settings, and other workloads. Keep credentials out of results.
2. Run `python -m benchmarks.pipeline --count 1000 --rate 0` as a warm-up. Warm-up results are not measurement runs.
3. Run at least five 100,000-event measurements, both unthrottled and at explicitly chosen rates. Use `--output benchmarks/results/run-N.json`. The run UUID isolates counts from older events. Retain the JSON plus a short environment manifest.
4. The producer reports broker-acknowledged throughput. The pipeline harness separately waits for every unique event in PostgreSQL and reports persisted events/sec over producer start to observed completion, including polling granularity. Timeout exits nonzero and reports incomplete counts.
5. Latency p50/p95/p99 is event creation timestamp to SQL INSERT timestamp, with `clock_timestamp()` on the database. This is not dashboard refresh or aggregation latency. PostgreSQL inserts may precede transaction commit slightly; this is not a commit-latency measurement. Synchronize host/database clocks; negative latencies invalidate a run.
6. Repeat with 1, 2, and 3 consumers in the same group and record the exact processes. More than three consumers do not add partition parallelism with the current topic. Observe rejected counts, duplicate logs, consumer lag, CPU/memory, and database I/O. The harness does not collect all these automatically.
7. For reliability, kill/restart the consumer during a run; compare unique input IDs with persisted IDs, not only log-line counts. Inject malformed events and a database outage. Commit evidence and exact reproduction commands before quoting results on a resume.

Synthetic events and one local broker do not establish production capacity. Keep raw local run files out of routine commits until reviewed for reproducibility. The initial audit had no benchmark evidence. A subsequent [CI smoke result](evidence/ci-smoke-2026-10-26.json) reconciles 500 events at an imposed 100 events/sec; it is not a saturation benchmark.
