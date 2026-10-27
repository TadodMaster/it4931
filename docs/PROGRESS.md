# Progress

## Phase 1 — design, skeleton, core compose, generator + truth + oracle, rule spec — DONE (2026-10-26/27)

What works (all verified in this session):

| Item | Evidence |
|------|----------|
| `docker compose --profile core up --wait` | all 7 services healthy (kafka, seaweedfs, iceberg-rest, clickhouse, oracle, generator, bench) + init containers exited 0; topics `transactions`(6p, retention -1), `merchants`(compact), `late-events`, `rules-control`; buckets `master/warehouse/checkpoints/results`; ClickHouse db `lk` |
| Rule spec + vectors | 4 vectors × 2 rules in `rules/spend_rules.yaml`; pass in Python (`rules-python`) and SQL (`rules-sql`); **Java pending (phase 2)** |
| Generator `backfill` | 20,000,000 logical events / 36 monthly Parquet files in 67 s; 20,200,175 stream events (200,175 duplicates, 173,181 late POS); deterministic (test) |
| Generator → Kafka | 20,200,175 events loaded at ~198k ev/s; per-partition end offsets (3,106,005 / 3,230,505 / 3,247,889 / 3,341,826 / 3,344,420 / 3,929,530) sum exactly to the stream count |
| Generator `live` | 2,000 tx/s for 20 s: 40,000 logical + 382 duplicates; Kafka total and truth rows match exactly; `injections.json` written |
| Marker API | `lk_generator marker` injected a 90-min-late POS payment on probe account A100001; Kafka offset +1, truth +1, oracle trace shows it |
| Oracle | 20,040,001 events after live; `id_checksum` of oracle (SQL) = generator manifest (numpy) = 15901213493562651063 on the backfill; point queries 0.6 s, full `totals` 21 s with checksum |
| Tests | `pytest` 12/12 on host (py3.10) and in container (py3.11): rule vectors (Python+SQL), generator determinism/counts/schema/late-POS, oracle ≡ pure-Python on full scan for v1+v2, Σcategories = Σdays, cutoff exclusivity |

Deviations / caveats (details in `docs/DESIGN.md` §2): SeaweedFS replaces MinIO (no pullable official MinIO image);
Kafka 3.9.1; Docker VM only 8 GB; `make` not installed on the Windows host (Makefile written, untested here);
`make test-e2e`, `bench-all`, `report`, `demo` are stubs that fail loudly until their phases.
Not yet exercised: Iceberg REST writes to SeaweedFS (first needed in phase 3 — flagged as the main risk of D1).
A stray smoke-test directory `D:\tmp_lkdata` (~tiny) was created at the drive root by an early test and could not be removed by the
tool (protected path); safe to delete by hand.

### Follow-up: restructure + bug fix (2026-10-27)
* Repo regrouped into `shared/ python/ jvm/ spark/ infra/` (DESIGN D10). After the move: 12/12 tests on host and in container, core stack rebuilt healthy, live mode and oracle verified.
* **Bug found and fixed:** two live runs with the same seed produced identical event_ids (truth had 20,050,001 rows but 20,040,001 distinct ids). Live RNG now mixes `run_id` and restart count. Kafka topic was recreated and the backfill reloaded so Kafka and truth agree; two fresh live runs give 20,020,000 events = 20,020,000 distinct ids.
* Phase 1 numbers above for the live run (40,000 events, marker) were from before this reload; the backfill numbers are unchanged.

## Phase 2 — Kappa path end to end — DONE (2026-10-27)

Verified on the demo-fast dataset (1,000,000 logical events, 1,010,038 stream events incl. 10,038 duplicates and 8,496 within-limit late POS),
`results/phase2/verification.json` and `results/phase2-live/verification.json` (after 20 s of live traffic + 2 marker events incl. a 90-min-late POS):
**all 15 checks PASS** — exact oracle equality (balance 10,000 rows; spend_day 527,600 rows; spend_cat_month 419,605 rows, equal digests),
sum(balance)/sum(spend) = oracle totals, Σdays = Σcategories, accepted+duplicates+late = Kafka stream total, injected duplicates (10,038) =
discarded (10,038), Kafka produced-per-partition = end offsets, committed = end, trace table holds each accepted event once.

What was built: `jvm/rules-java` (4 shared vectors pass in Java), `jvm/kappa-flink-job` (RocksDB state, dedup-first, lateness side output,
absolute-value upserts with per-account versions, sink flush on checkpoint, EXACTLY_ONCE checkpoints on SeaweedFS, recovery from checkpoint
observed after an induced crash), versioned ClickHouse tables + atomic `kappa_current_*` views, `query-api` (`/kappa/*`, `/trace/*`, `/truth/*`,
as_of), `bench verify`, `bench kappa-views`, `lk_bench.flinkapi`. Digest definition confirmed identical in DuckDB and ClickHouse.

Measured (informal, 8 GB VM): replay 5.4k ev/s (first version) → 11.8k ev/s (one state access for scalars, no per-event timers), parallelism 4;
20 M-event replay was started but abandoned at ~4.4 M after the VM ran out of memory (see "Environment incidents"). Demo-fast profile
(`LK_PROFILE=fast`, 1 M events) replays in ~90 s and is what phases 3-7 use unless stated.

Environment incidents (honest log): (1) SeaweedFS OOM-killed once during the 20 M replay (checkpoints failed, job recovered); (2) Docker engine
became unresponsive (HTTP 500) until restarted; (3) `.env` variables override profile overlays — HISTORY_EVENTS in `.env` silently produced a 20 M
dataset once; those variables are now commented out in `.env`/`.env.example`.

## Phase 3 - Lambda path end to end - DONE (verified)

Built: Iceberg master on S3 (`spark/load_master.py`, append-only, loaded from the SAME Parquet as Kafka), Spark archiver (Kafka -> Iceberg,
exactly-once via checkpoint), nightly PySpark batch with one atomic `EXCHANGE TABLES` swap, `lambda_batch_runs` bookkeeping, Flink speed layer
(Java, Redis day buckets, SET NX dedup), merge in the query-api (`/lambda/*`, `/trace/lambda`), `bench verify --arch lambda`.
Rule vectors pass in **Python, SQL, Java and PySpark** (4 vectors x 2 rules each).
Unit tests: 15 Python tests (host) + PySpark vectors (in the Spark image) + Java vectors and t_k boundary tests (Docker build).
Verified (`results/e2e/verification_lambda.json`, 16 checks, all PASS on the 300 k-event profile after live traffic + corrective batch):
batch view = oracle at its own t_k (3 metrics), serving (batch + speed) = oracle now (3 metrics), master rows = every Kafka event,
master distinct ids and id-checksum = oracle, injected duplicates = duplicate rows in master, snapshots append-only, history checksums unchanged.
Also done: swap atomicity under 6,117 concurrent reads + rollback (`results/final/proof_atomic_swap.json`, PASS).

## Phase 4 - fault injection, chaos, reprocessing, drift - CODE COMPLETE, NOT YET RUN

Implemented in `python/bench/lk_bench` (`bench run <name>`): `b5` (duplicates + kill TaskManager and Spark archiver, Kappa exactness,
Lambda speed-layer error and corrective batch), `b6` (rule change v1->v2: Lambda = Spark + Flink + full batch, Kappa = job v2 from offset 0,
lag curve, oracle check, view switch, zero-downtime pollers), `b7` (drift: Spark v2 only), `determinism` (Kappa replayed twice), `atomic` (done).
**No B5/B6/B7/determinism result exists yet.** A first determinism attempt on the 1 M dataset was cancelled (checkpoint durations of 7-12 s made the
replay far slower than the earlier 11.8 k events/s; default checkpoint interval is now 30 s).

## Phase 5 - benchmarks B1-B10 - CODE COMPLETE, NOT YET RUN

All ten scenarios exist (`b1`..`b10`, repetition via `--repeat`, `scripts/bench_all.sh` runs the whole sequence, about 1 hour on the fast profile).
No B1-B10 numbers have been produced; therefore `docs/RESULTS.md` still contains no measurements and none are claimed anywhere.

## Phase 6 - report, demo UI, Grafana, RUNBOOK, demo-fast - PARTLY DONE

* RUNBOOK rewritten (presenter script included). Demo UI served at `/ui` (HTTP 200 checked; its buttons and the Lambda/Kappa/Truth columns have NOT been exercised in a browser).
* Report generator `lk_bench/report.py` written (summary table, one chart per scenario, 1920x1080 slides, verification status, threats to validity) but **never executed**: expect small bugs on first run.
* Prometheus + Grafana (profile `observability`) and the `/metrics` endpoint are written but **never started**.
* `LK_PROFILE=fast` is now 300 k events, 5 k accounts, 300 tx/s, simulated day = 300 real seconds; `make demo-fast` exists but was not run as one command.

## Phase 7 - clean-clone final run - NOT DONE

`make clean && make reset && make bench-all && make report` has not been run from a clean clone. `make` itself is not installed on this Windows host (Makefile untested; the underlying commands were run by hand).

## State of the machine when work stopped

Stack is UP on the 300 k dataset (Kappa v1 caught up, Lambda batch scheduled). `results/final/` holds only `meta.json` and `proof_atomic_swap.json`.
A benchmark run was started and stopped at the user's request; its later steps failed only because the bench container was restarted. Run `make reset` before a real benchmark.
