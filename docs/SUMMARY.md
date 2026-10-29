# Lambda vs Kappa e-wallet demo: full summary

One document covering what was built, how it was checked, what was measured, and where the results are weak.
Numbers come from `results/final/` (fast profile) and are also in `docs/RESULTS.md`, which is generated from the raw data.

## 1. Goal

Implement **one** e-wallet spending-statistics problem **twice on identical input**, as a Lambda and a Kappa architecture. Check both against an **independent SQL oracle**, and show four requirements:

| Id | Requirement |
|----|-------------|
| Y1 | Freshness: a new payment shows up in seconds |
| Y2 | A 3-year spending report per category |
| Y3 | A rule change (v1 → v2) is recomputed over all history |
| Y4 | Exact money (integer VND) despite late POS events, duplicates and crashes |

The project is for a university Big Data presentation. Nothing is faked or hard-coded. The report is regenerated from raw data only.

## 2. What was built

### Architectures
- **Lambda**
  - A Spark archiver writes Kafka → Iceberg, an append-only master on SeaweedFS (S3).
  - A nightly PySpark batch writes to ClickHouse. The result is swapped in with one atomic `EXCHANGE TABLES`, and the previous version is kept for rollback.
  - A Flink speed layer (Java) keeps per-day buckets in Redis and dedups with `SET NX`.
  - The query-api merges batch and speed results.
- **Kappa**
  - One Flink 1.20.2 job (Java 17, RocksDB state). It dedups first and sends late events to a side-output Kafka topic.
  - It writes absolute values with per-account versions to ClickHouse ReplacingMergeTree, so the writes are idempotent.
  - Checkpoints are EXACTLY_ONCE to S3.
  - A rule change is a replay from offset 0 into a new table version, followed by an atomic view switch.

### Shared pieces
- **Rule spec:** `shared/rules/spend_rules.yaml` has 4 vectors × v1/v2. It is implemented four times (Python, SQL/DuckDB, Java, PySpark) and all four pass.
- **Generator:** deterministic numpy. It injects late POS events, duplicates and out-of-order jitter, and writes a truth log before sending.
- **Time:** simulated time (fast profile 288×: 1 simulated day = 300 real seconds).
- **Oracle:** DuckDB over the truth Parquet. Equality is checked by order-independent row digests, plus conservation, dedup, Kafka offset, immutability and late-data accounting checks.
- **Extras:** wallet UI (`/ui`: Lambda | Kappa | Truth columns), Prometheus and Grafana dashboard "Lambda vs Kappa", and a bench harness with 10 scenarios.

### Stack
Kafka 3.9.1, SeaweedFS 3.97, Iceberg REST 1.10.0, ClickHouse 25.8, Redis 7.4, Spark 3.5.6, Flink 1.20.2, Python 3.11, Prometheus 2.55, Grafana 11.3. Everything runs in Docker Compose (profiles: core, kappa, lambda, lambda-tools, lambda-load, observability).
SeaweedFS replaced MinIO because MinIO has no pullable official image.

### Repo layout (monorepo)
`shared/` (config, rules, schemas) · `python/` (common, generator, oracle, query-api, bench, rules-python, tests) · `jvm/` (rules-java, flink-common, kappa-flink-job, lambda-flink-speed) · `spark/` · `infra/` · `scripts/stack.py` · `docker-compose.yml` · `Makefile` · `docs/` · `results/final/`.

## 3. Verification (final state)

| Check | Result |
|-------|--------|
| Kappa vs oracle | **PASS, 15/15** |
| Lambda vs oracle | **PASS, 16/16** |
| Atomic batch swap (reader sees only complete versions, rollback works) | PASS |
| Replay determinism (Kappa replayed twice gives identical output) | PASS |
| Rule vectors (Python, SQL, Java, PySpark) | PASS |
| Python unit tests | 15 passed, 1 skipped |

Both architectures reach **0 VND difference** from the oracle on money.

## 4. Metrics

Environment: 6 GiB Docker VM, Flink parallelism 1, 100k history events (about 350k in total with live traffic), 2,000 accounts, 100 tx/s live.

| Requirement | Metric | Lambda | Kappa | Better |
|---|---|---|---|---|
| Y1 | Freshness p95 via API | 0.071 s | 0.313 s | Lambda |
| Y1 | Freshness p95 direct from store | 0.011 s | 0.309 s | Lambda |
| Y3 | Time to correct after a rule change | 57.5 s | 471.7 s | Lambda |
| Y3 | Rule implementations to change | 2 (PySpark + Java) | 1 (Java) | Kappa |
| Y3 | API failures during the switch | 0 | 0 | Tie |
| Y3 | Drift when only Spark gets the new rule | 192 account-days | not possible | Kappa |
| Y4 | Late POS (≤ 2 h): time until exact | 643 s (next batch) | 0 s | Kappa |
| Y4 | Events later than the limit | absorbed at the next batch | 8 sit in `late-events`, not applied | Lambda recomputes; Kappa surfaces them |
| Y4 | After duplicates + crash (TaskManager and archiver killed) | exact | exact, 0 VND | Tie in this run |
| Y1/Y4 | Lowest share of exactly-correct accounts over the run | 99.941 % | 97.892 % | Lambda |
| Scale | Max sustained ingest | 1,200 ev/s | 400 ev/s | Lambda (caveat below) |
| Cost | Data-path lines of code | 662 | 523 | Kappa |
| Cost | Services in the profile | 7 | 3 | Kappa |

### Details worth quoting
- **B6 (rule change):** Kappa replayed 225,676 events at a measured 567 ev/s. The formula predicted 397.7 s and the measured time was 471.7 s. The v2 output passed the oracle check before the views were switched. Lambda took 57.5 s because it recomputes in Spark over the same data.
- **B7 (drift):** 0 → **192** → 192 → 0 mismatching account-days. Spark has v2 while the live Flink path still runs v1. Fixing Flink does not help until the next batch, because stale Redis buckets remain. This is the main argument for one codebase.
- **B4 (late POS):** Kappa was exact immediately. Lambda was inexact until the next nightly batch (643 s, about 1,543 simulated minutes). The `kappa_beyond_limit_accounts_equal_truth` value is **False**, by design: events beyond the lateness limit are routed to `late-events-v2` and never silently dropped.
- **B5 (crash):** Both jobs were back in about 30 s. Kappa restored from its S3 checkpoint with 0 VND difference, and injected duplicates equalled the duplicates discarded. The Lambda speed layer lost nothing in this run. The risk that it can lose events in a crash window is documented but **not observed**.
- **B3 (correctness over time):** both stay near 100 %. Kappa dipped to 97.9 % once and Lambda to 99.94 % around a batch.
- **B2 (throughput):** Kappa lagged from 800 ev/s upward, while Lambda kept up at 1,200 ev/s. Kappa runs one Flink slot here, and the generator and the starved VM may also be limits.
- **B10 (extrapolation):** replaying 10 billion events at 100k ev/s takes about 2.3 days, about 353 cores at the measured per-core rate.
- **B9 (resources):** ClickHouse and the Flink TaskManager are the heaviest services (about 75–80 % average CPU).

## 5. Conclusions

1. **Y1 freshness:** Lambda's Redis speed layer reads faster than Kappa's ClickHouse path. Both are well under 1 s.
2. **Y3 recompute:** Lambda is faster to correct in this setup, but needs **two** implementations that can drift (192 wrong account-days). Kappa needs **one** and cannot drift, but pays a replay time that grows with history (B10).
3. **Y4 exactness:** Kappa gives exact answers at once for late data inside the limit and surfaces anything beyond it. Lambda is exact only after the next batch, and is the only one that absorbs arbitrarily late data.
4. **Cost:** Kappa has 3 services and about 21 % less data-path code. Lambda needs 7 services.
5. **Money was exact in both architectures** once their repair mechanisms ran. The trade-off is time-to-correct, code duplication and operational weight, not final correctness.

## 6. Bugs found by the measurements (all fixed)

| # | Problem | Fix |
|---|---|---|
| 1 | Live generator reused event ids across runs | Seed mixes run id and restart count |
| 2 | Kafka log dir and Iceberg catalog were not persistent | `KAFKA_LOG_DIRS`, sqlite catalog volume, `register_master.py` recovery |
| 3 | `docker kill` did not trigger the restart policy | Scenarios call `docker start` |
| 4 | Flink could not restore any S3 checkpoint (S3A change detection vs SeaweedFS ETags) | `fs.s3a.change.detection.mode: none` |
| 5 | Nightly batch missed in-flight events (Lambda about 88 % exact) | Batch grace period of 2 simulated hours |
| 6 | Oracle slowed by thousands of per-second truth files | `compact_truth` |
| 7 | ClickHouse out of memory | Thread, cache and pool limits; filter before aggregating |
| 8 | Late events deleted by Kafka's 7-day retention (simulated timestamps are months old) | Per-version late topics with infinite retention; reader covers all partitions |
| 9 | Lag from committed offsets was stale | Processed-count lag with settled baselines |
| 10 | Windows CRLF line endings broke `sh` scripts | LF everywhere, `newline=""` |
| 11 | Kappa throughput 5.4k → 11.8k ev/s on the larger VM | No per-event timers, one state access for scalars |
| 12 | Iceberg write OOM, S3A ParentNotDirectory on the streaming checkpoint | Smaller row groups, snappy, `local[2]`; checkpoint on a local volume |

Full log: `docs/PROGRESS.md`. Design decisions D1–D24: `docs/DESIGN.md`.

## 7. Limitations (threats to validity)

- **Small environment:** one laptop, a 6 GiB VM, parallelism 1, 100k-event dataset. Absolute rates are low. An earlier 8 GiB run replayed Kappa at 11.8k ev/s, against 567 ev/s in the final run, so only **ratios and failure behaviour** are meaningful.
- **Run history:** B4, B5 and the verifications were rerun after bug fixes on the same stack, so the final numbers do not come from one uninterrupted sequence.
- **Repeats:** B1 and B8 ran 3 times, B2–B7 once. There are no confidence intervals for the single-run scenarios.
- **Kappa lateness:** it depends on watermark progress, so a replay can classify a borderline event differently from the live job. Hence the per-version late topics (D21).
- **UI:** the wallet UI and Grafana were checked through their endpoints, not clicked through in a real browser.
- **Grafana:** stale consumer groups of cancelled jobs still show old lag (cosmetic).
- **Not done:** a run from a fresh `git clone` (Phase 7), and `make` targets were run as their equivalent commands because `make` is not installed on the host.

## 8. Phase status

| Phase | Status |
|-------|--------|
| 1 Spec, design, data generator, oracle | Done |
| 2 Kappa pipeline | Done, 15/15 |
| 3 Lambda pipeline | Done, 16/16 |
| 4 Faults, reprocessing, drift | Done |
| 5 Benchmarks B1–B10 | Done, with caveats above |
| 6 Report, UI, Grafana, RUNBOOK | Done |
| 7 Clean-clone final run | Partly done (see section 7) |

## 9. How to run

```
python scripts/stack.py reset        # wipe and start everything (about 8 min, fast profile)
python scripts/stack.py resume       # restart without reloading data
python scripts/stack.py status
docker compose exec -T bench python -m lk_bench verify --arch kappa --rule v2 --run-id r1
docker compose exec -T bench python -m lk_bench verify --arch lambda --run-id r1
docker compose exec -T bench python -m lk_bench run e2e --run-id e2e     # 2-3 minute end-to-end proof
docker exec -d it4931-bench-1 sh -c "python -m lk_bench run-all --run-id final > /app/python/results/final_bench.log 2>&1"   # about 70 min
docker compose exec -T bench python -m lk_bench report --run-id final
docker compose --profile core --profile kappa --profile lambda --profile lambda-tools --profile lambda-load --profile observability stop
```

URLs: wallet UI http://localhost:8080/ui · API http://localhost:8080/docs · Oracle :8090 · Flink :8081 · ClickHouse :8123 · Grafana :3000 · Prometheus :9090.

## 10. Where things are

| What | Where |
|------|-------|
| Interactive report with charts, tables and threats | `results/final/report.html` |
| Slides (9 PNGs, 1920×1080) | `results/final/slides/` |
| Raw benchmark data | `results/final/` (CSV/JSON per scenario, `meta.json`) |
| Generated results | `docs/RESULTS.md` |
| Design decisions | `docs/DESIGN.md` |
| Commands, recovery, 12-minute presenter script | `docs/RUNBOOK.md` |
| Progress and bug log | `docs/PROGRESS.md` |

## 11. Housekeeping

- Nothing from this work is committed to git.
- `D:\tmp_lkdata` (a leftover from Phase 1) can be deleted by hand.
- `%UserProfile%\.wslconfig` was set to 6 GB memory, 4 GB swap and gradual memory reclaim.
- The `bench` container mounts the Docker socket. This is acceptable only for a demo (DESIGN D13).
- `.env` holds demo-only S3 keys. Do not publish it.
