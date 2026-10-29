# Results (generated from results/final by `make report`; never edited by hand)

Profile `fast` · git `d278d6b8cb` · Docker 16 CPUs / 5.8 GiB · generated 2026-10-28 20:40 UTC

## Summary

| Requirement | Metric | Lambda | Kappa | Winner / note |
|---|---|---|---|---|
| Y1 fresh | end-to-end freshness p95 via API (s) | 0.071 | 0.313 | Lambda |
| Y1 fresh | freshness p95 reading the store directly (s) | 0.011 | 0.309 | Lambda |
| Y3 recompute | time to correct after a rule change (s) | 57.5 | 471.7 | Lambda |
| Y3 recompute | rule implementations to change | 2 (PySpark + Java) | 1 (Java) | Kappa |
| Y3 recompute | API failures during the switch | 0 | 0 | both zero-downtime |
| Y3 recompute | drift when only Spark has the new rule (mismatching account-days) | 192 | n/a (single code base) | Kappa |
| Y4 exact money | late POS (<= 2 h): time until exact (s) | 643 | 0 | Kappa |
| Y4 exact money | events later than the limit | absorbed at the next batch (exact: True) | 8 in late-events, not applied | Lambda recomputes everything; Kappa surfaces them |
| Y4 exact money | after duplicates + crash (TaskManager and archiver killed) | 0 accounts off by 0 VND until the next batch | exact (0 VND) | tie: both exact in this run (the speed layer can lose events in a crash window; the next batch repairs them) |
| Y1/Y4 correctness | lowest share of exactly-correct accounts over the run (%) | 99.941 | 97.892 | Lambda |
| scale | max sustained ingest on this laptop (events/s) | 1,200 | 400 | Lambda |
| cost | data-path lines of code | 662 | 523 | Kappa |
| cost | services in the profile | 7 | 3 | Kappa |

## Verification

**Kappa vs oracle: PASS** (15/15)

- PASS: kappa view active
- PASS: consumer caught up (committed offsets reach end offsets)
- PASS: oracle equality: balance (v2)
- PASS: oracle equality: spend_day (v2)
- PASS: oracle equality: spend_cat_month (v2)
- PASS: sum(balance) = oracle global total
- PASS: sum(spend over days) = sum(spend over categories) = oracle global total
- PASS: spend-contributing events = oracle
- PASS: events counted = logical events - late-events (distinct)
- PASS: trace table holds every accepted event exactly once
- PASS: Kafka: produced per partition = end offsets
- PASS: Kafka: committed offsets = end offsets
- PASS: dedup: injected duplicates = duplicates discarded by Kappa
- PASS: every stream event is accepted, discarded as duplicate or in late-events
- PASS: late data: every event routed to late-events is counted exactly once (none disappeared)

**Lambda vs oracle: PASS** (16/16)

- PASS: lambda batch view active
- PASS: speed layer caught up (consumer lag 0)
- PASS: batch view = oracle at t_k: balance (v2, t_k=2026-01-14T00:00:00.000Z)
- PASS: batch view = oracle at t_k: spend_day (v2, t_k=2026-01-14T00:00:00.000Z)
- PASS: batch view = oracle at t_k: spend_cat_month (v2, t_k=2026-01-14T00:00:00.000Z)
- PASS: batch input rows (deduped) = oracle events < t_k
- PASS: serving (batch + speed) = oracle now: balance
- PASS: serving (batch + speed) = oracle now: spend_day
- PASS: serving (batch + speed) = oracle now: spend_cat_month
- PASS: Kafka: produced per partition = end offsets
- PASS: master rows = every stream event (Parquet backfill + archived Kafka tail)
- PASS: master distinct event ids = oracle logical events
- PASS: master event-id checksum = oracle checksum
- PASS: dedup: duplicates injected = duplicate rows in master (the batch removes them)
- PASS: immutability: master snapshots are append-only
- PASS: immutability: rows only grow; per-day checksums of history unchanged since the previous record

- atomic swap: PASS
- replay determinism: PASS

## Charts and tables

### B1 Freshness CDF

![B1 Freshness CDF](../results/final/slides/b1_freshness_cdf.png)

| reader | p50 s | p95 s | p99 s | timeouts |
|---|---|---|---|---|
| kappa_api | 0.134 | 0.313 | 0.319 | 0 |
| lambda_api | 0.011 | 0.071 | 0.073 | 0 |
| kappa_direct | 0.128 | 0.309 | 0.314 | 0 |
| lambda_direct | 0.009 | 0.011 | 0.014 | 0 |

### B3 Correctness over time

![B3 Correctness over time](../results/final/slides/b3_correctness_over_time.png)

| t (s) | Kappa exact % | Lambda exact % | Lambda batch @t_k % | batch run |
|---|---|---|---|---|
| 0.0 | 100.0 | 100.0 | 100.0 | 20260101T000000_182205,manual_atomic1_1793211753,20260102T000000_183207 |
| 20.0 | 100.0 | 100.0 | 100.0 |  |
| 40.0 | 100.0 | 100.0 | 100.0 |  |
| 60.0 | 100.0 | 100.0 | 100.0 |  |
| 80.0 | 100.0 | 100.0 | 100.0 |  |
| 100.0 | 100.0 | 100.0 | 100.0 |  |
| 120.0 | 100.0 | 100.0 | 100.0 |  |
| 140.0 | 97.892 | 100.0 | 100.0 |  |
| 160.0 | 100.0 | 99.941 | 100.0 | 20260103T000000_184206 |
| 180.0 | 99.943 | 99.943 | 100.0 |  |
| 200.0 | 100.0 | 100.0 | 100.0 |  |
| 220.0 | 100.0 | 100.0 | 100.0 |  |
| 240.0 | 100.0 | 100.0 | 100.0 |  |
| 260.0 | 100.0 | 100.0 | 100.0 |  |
| 280.0 | 100.0 | 100.0 | 100.0 |  |
| 300.0 | 100.0 | 100.0 | 100.0 |  |
| 320.0 | 100.0 | 100.0 | 100.0 |  |
| 340.0 | 100.0 | 100.0 | 100.0 |  |
| 360.0 | 100.0 | 100.0 | 100.0 |  |
| 380.0 | 100.0 | 100.0 | 100.0 |  |
| 400.0 | 100.0 | 100.0 | 100.0 |  |
| 420.0 | 100.0 | 99.943 | 100.0 |  |
| 440.0 | 100.0 | 100.0 | 100.0 |  |
| 460.0 | 100.0 | 100.0 | 100.0 |  |
| 480.0 | 100.0 | 100.0 | 100.0 |  |
| 500.0 | 100.0 | 100.0 | 100.0 |  |
| 520.0 | 100.0 | 100.0 | 100.0 |  |
| 540.0 | 100.0 | 100.0 | 100.0 |  |
| 560.0 | 100.0 | 100.0 | 100.0 |  |
| 580.0 | 100.0 | 100.0 | 100.0 |  |
| 600.0 | 100.0 | 100.0 | 100.0 |  |

### B2 Throughput ramp

![B2 Throughput ramp](../results/final/slides/b2_throughput_ramp.png)

| rate/s | Kappa lag end | Kappa sustained | Lambda lag end | Lambda sustained |
|---|---|---|---|---|
| 100 | 3 | True | 0 | True |
| 200 | 40 | True | 0 | True |
| 400 | 40 | True | 0 | True |
| 800 | 2,483 | False | 0 | True |
| 1200 | 17,461 | False | 0 | True |

### B6 Replay lag curve, predicted vs measured T

![B6 Replay lag curve, predicted vs measured T](../results/final/slides/b6_replay_lag_curve.png)

| metric | value |
|---|---|
| history_events | 225,676 |
| job_started_s | 3.6 |
| catchup_s | 471.7 |
| measured_replay_rate_ev_s | 567 |
| predicted_T_s | 397.7 |
| measured_T_s | 471.7 |
| oracle_check_v2_passed | True |
| switched_views | True |
| rule_implementations_changed | 1 |
| implementation_languages | ['Java'] |

### B4 Late POS data: time until exact

![B4 Late POS data: time until exact](../results/final/slides/b4_late_pos.png)

| metric | value |
|---|---|
| in_limit_events | 40 |
| beyond_limit_events | 5 |
| batch_boundary | 2026-01-12T00:00:00+00:00 |
| kappa_exact_after_s | 0.0 |
| lambda_inexact_seconds | 0.0 |
| lambda_exact_after_s | 643.0 |
| lambda_exact_after_sim_minutes | 1543.2 |
| lambda_ever_inexact | True |
| lambda_beyond_limit_final_exact | True |
| kappa_beyond_limit_accounts_equal_truth | False |
| kappa_late_events_topic_distinct_ids | 8 |
| note | beyond-limit events are NOT applied by Kappa (they sit in late-events, never silently dropped); Lambda recomputes all history, so it absorbs arbitrarily late events once the next batch has run |

### B7 Drift between codebases

![B7 Drift between codebases](../results/final/slides/b7_drift.png)

| stage | mismatching account-days |
|---|---|
| baseline_v1 | 0 |
| spark_v2_flink_v1 | 192 |
| flink_fixed_before_next_batch | 192 |
| after_next_batch | 0 |

### B9 Resource usage per service

![B9 Resource usage per service](../results/final/slides/b9_resources.png)

| service | avg CPU % | peak CPU % | peak MB |
|---|---|---|---|
| bench | 0.5 | 0.88 | 148.6 |
| clickhouse | 75.2 | 131.77 | 1157.1 |
| flink-jobmanager | 11.2 | 69.74 | 313.0 |
| flink-taskmanager | 80.2 | 139.18 | 572.8 |
| generator | 1.6 | 1.87 | 138.9 |
| iceberg-rest | 1.2 | 7.65 | 201.7 |
| kafka | 3.1 | 3.6 | 508.4 |
| oracle | 3.1 | 23.55 | 31.9 |
| query-api | 1.6 | 11.51 | 57.7 |
| redis | 1.6 | 2.61 | 44.5 |
| seaweedfs | 1.6 | 5.54 | 430.7 |
| spark-archiver | 7.2 | 46.29 | 721.1 |
| spark-scheduler | 0.1 | 0.21 | 67.8 |

### B9 Lines of code and services

![B9 Lines of code and services](../results/final/slides/b9_loc_services.png)

| metric | Lambda | Kappa |
|---|---|---|
| lines of code | 662 | 523 |
| services (profile) | 7 | 3 |

### B10 Extrapolation to 10 billion events

![B10 Extrapolation to 10 billion events](../results/final/slides/b10_extrapolation.png)

| R_process ev/s | replay hours | replay days | cores at measured per-core rate |
|---|---|---|---|
| 60000 | 277.78 | 11.57 | 211.6 |
| 75000 | 111.11 | 4.63 | 264.6 |
| 100000 | 55.56 | 2.31 | 352.7 |
| 200000 | 18.52 | 0.77 | 705.5 |
| 500000 | 6.17 | 0.26 | 1763.7 |
| 1000000 | 2.92 | 0.12 | 3527.3 |
| 2000000 | 1.42 | 0.06 | 7054.7 |

See `results/<run>/report.html` for the self-contained report incl. threats to validity.