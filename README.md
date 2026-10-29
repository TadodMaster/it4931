# it4931
Lưu trữ và xử lý dữ liệu lớn 20261

## Lambda vs Kappa demo

Reproducible, laptop-scale benchmark. One e-wallet spending-statistics problem is implemented **twice**, as a Lambda and as a
Kappa architecture, on **byte-identical input** (the same generated Parquet files). Both are checked against an independent SQL
oracle (DuckDB over a ground-truth log) and measured. Nothing is hard-coded: the report is regenerated from raw data.

### The four requirements

| Id | Requirement | Demonstrated by |
|----|-------------|-----------------|
| Y1 | Fresh: a payment shows up in seconds | B1 freshness p95, B3 correctness over time |
| Y2 | 3-year spend report by merchant category | B8 report latency, batch duration, state size |
| Y3 | Spending rule changes (v1 → v2), recompute all history | B6 time-to-correct, B7 drift between codebases |
| Y4 | Exact money despite late POS (≤ 2 h), duplicates, crashes | B4, B5, `bench verify` (0 VND difference) |

### Architecture

Both pipelines read the **same** input: the generator writes Parquet once (plus a ground-truth log with every logical event
exactly once, before duplicates and lateness are injected), then loads those files into Kafka (Kappa source) and into the
Iceberg master (Lambda source). Live traffic goes to the Kafka topic `transactions`, keyed by `account_id`.

```
                          generator ──► truth/ (Parquet) ──► oracle (DuckDB, SQL rule) ──► /truth
                              │
                              ▼
                     Kafka  transactions  (6 partitions, infinite retention)
                      │                          │
        ┌─────────────┘                          └──────────────┐
        ▼  LAMBDA                                               ▼  KAPPA
 Spark archiver ─► Iceberg master (append-only)      Flink job (keyBy account, RocksDB state)
        │                │                              dedup · lateness · rule · merchant join
        │         nightly Spark batch                           │            │
        │         (rule, dedup, join)                           ▼            ▼
        │                │                              ClickHouse       late-events
        │                ▼                              kappa_vN_*       (side output)
        │         ClickHouse lambda_batch_*  (atomic swap)       │
        ▼                │                                      ▼
 Flink speed layer ─► Redis (events > t_k)              views kappa_current_*
        │                │                                      │
        └────────────────┴──────────────► query-api ◄───────────┘
                                   /lambda  /kappa  /truth  /trace
                                              │
                                  wallet UI: Lambda | Kappa | Truth
```

**Lambda** (batch layer + speed layer + serving merge)

* *Master dataset*: a Spark Structured Streaming job appends Kafka to the Iceberg table `master.transactions` (partitioned by day).
  It is never updated or deleted; only `append` snapshots are allowed, and `bench verify` checks that.
* *Batch layer*: once per simulated night a PySpark job reads everything with `event_time < t_k`, deduplicates by `event_id`,
  applies the spending rule, joins the merchant dimension and writes three views (spend per day, spend per category-month,
  balance). They are loaded into new versioned ClickHouse tables and switched in with one atomic `EXCHANGE TABLES`; the previous
  version stays for rollback and each run is recorded in `lambda_batch_runs` (`t_k`, rule version, duration).
* *Speed layer*: a Flink job applies the rule to events with `event_time ≥ t_k`, dedups with Redis `SET NX` + TTL and keeps
  per-(account, day) buckets and balance deltas in Redis. Older events are ignored; the next batch repairs them. After each swap,
  buckets older than the new `t_k` are expired.
* *Serving*: `lambda_spend(A, day) = batch(≤ t_k) + realtime(> t_k)`, using the exact `t_k` from `lambda_batch_runs`.
* *Cost*: the spending rule exists in two codebases (PySpark and Java), so a rule change touches both and they can drift.

**Kappa** (one streaming job, the log is the source of truth)

* *Single Flink job* (RocksDB state, event-time watermarks with bounded out-of-orderness): dedup set of `event_id` with state TTL,
  spend per (account, day) and (account, category, month), balance. Merchants arrive as broadcast state from a compacted topic.
* *Lateness*: allowed lateness is 2 simulated hours. Later events go to the `late-events` topic and are counted, never dropped silently.
* *Exactly-once and idempotence*: checkpoints are `EXACTLY_ONCE` to S3. The job emits **absolute values** with a per-key version
  into a ClickHouse `ReplacingMergeTree`, so replaying after a failure overwrites rather than double counts.
* *Reprocessing (Y3)*: a new job version starts from offset 0 with a new consumer group and writes `kappa_v2_*`. When lag reaches ~0
  and the oracle check passes, the `kappa_current_*` views are repointed atomically and v1 is stopped. Rollback is the same switch.
* *Replay time*: `T ≈ H / (R_process − R_in)`, measured in B6 and extrapolated to 10 billion events in B10.

**Shared and independent pieces**

| Piece | Role |
|-------|------|
| Kafka (KRaft) | Ingest log; Kappa's source of truth. Production would use tiered storage to S3 |
| S3 object store + Iceberg REST catalog | Lambda master dataset, Flink checkpoints, results |
| ClickHouse | Serving store for both architectures |
| query-api (FastAPI) | `/lambda`, `/kappa`, `/truth`, `/trace/{arch}/{account}` (lists the events behind a number); every response carries as-of/lag metadata |
| Oracle (DuckDB) | Reads only `truth/` and applies the rule in SQL: a third independent implementation |
| Prometheus + Grafana | Ingest rate, consumer lag per group, checkpoint size, batch duration, mismatches vs oracle |

**Time and rules**: a simulated clock (`SIM_SPEEDUP`; the fast profile maps 1 simulated day to 5 real minutes) drives "nights",
the 2-hour POS lateness and TTLs. The spending rule lives in `shared/rules/spend_rules.yaml` with test vectors; Java, PySpark and
SQL must all pass them. Money is integer VND everywhere, never floats.

| | Lambda | Kappa |
|---|---|---|
| Codebases for the rule | 2 (PySpark + Java) + SQL oracle | 1 (Java) + SQL oracle |
| Services in the profile | 7 | 3 |
| Late data | exact at the next batch, any lateness | exact immediately within 2 h; beyond that surfaced in `late-events` |
| Rule change | re-run batch (fast) but two code changes, drift risk | replay from offset 0 (time grows with history) |
| Failure repair | batch overwrites speed-layer errors | checkpoint restore + idempotent sink |

### Quick start

Requirements: Docker Desktop (Compose v2; a 6 GiB VM is enough for the `fast` profile), host Python 3.10+ for unit tests only.

```
make env            # copy .env.example to .env (LK_PROFILE=fast selects the small dataset)
make reset          # wipe state, generate data, start Kafka/Iceberg/Kappa/Lambda from the same Parquet
make demo           # wallet UI :8080/ui · Grafana :3000 · Flink :8081
make test           # unit tests + rule vectors in Python, Java and PySpark
make test-e2e       # 2-3 minute end-to-end proof, then both verifications against the oracle
make bench-all RUN_ID=final   # all scenarios B1-B10 (about 1 hour on the fast profile)
make report RUN_ID=final      # results/final/report.html, slides/*.png
make down / make clean        # stop / stop and wipe volumes
```

Without GNU make, `python scripts/stack.py reset|start|status` and the commands in the runbook do the same.

### Main results (fast profile, `results/final`)

| Requirement | Lambda | Kappa |
|---|---|---|
| Y1 freshness p95 (API) | 0.071 s | 0.313 s |
| Y3 time to correct after rule change | 57.5 s | 471.7 s |
| Y3 rule implementations to change | 2 | 1 |
| Y3 drift if only Spark is updated | 192 account-days | not possible |
| Y4 late POS (≤ 2 h) until exact | next batch (643 s) | 0 s |
| Verification vs oracle | PASS (16/16) | PASS (15/15) |

Both reach 0 VND difference; the trade-off is time-to-correct, code duplication and operational weight (7 vs 3 services).
Absolute rates come from a small, starved VM, so only ratios and failure behaviour are meaningful. Full
results, design notes and threats to validity are in `docs/`.

### Repository layout

```
shared/  config, rule spec + test vectors, SQL rules, JSON schema      python/  generator, oracle, bench, query-api, demo-ui, tests
jvm/     rules-java, Flink jobs (one Maven reactor)                    spark/   PySpark archiver + batch + scheduler
infra/   Dockerfiles, Kafka/ClickHouse init, Flink, Grafana            scripts/ stack.py, bench_all.sh
docs/    documentation                                                 results/ data/ (generated)
```
