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
(`LK_PROFILE=fast`, 1 M events at that time; later reduced to 100 k events, see Phase 5) was used for the first verifications.

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

## Phase 4 - fault injection, chaos, reprocessing, drift - DONE

Measured on the final run (`results/final/`, fast profile, 6 GiB VM, see RESULTS.md and the threats section of the report):
* **B5 duplicates + crash** (5 % duplicates, TaskManager killed, then the Spark archiver): both jobs back in about 30 s, Kappa restored from its S3
  checkpoint and passed all 15 verification checks with **0 VND difference** (dedup: injected = discarded). The Lambda speed layer lost nothing in this
  run (0 accounts off); the corrective batch left everything exact. The speed-layer undercount risk is documented but was NOT observed.
* **B6 rule change v1 -> v2**: Lambda 57.5 s (Spark change + Flink redeploy + full batch over 223 k rows, 2 implementations changed);
  Kappa 471.7 s replay from offset 0 (226 k events, measured 567 ev/s, predicted T = 398 s), views switched after the oracle check, **0 API failures** on
  both sides during the switch. The replay rate is low because the VM was starved (an earlier 8 GiB run replayed at 11.8 k ev/s).
* **B7 drift**: baseline 0 mismatching account-days, after the batch with Spark-only v2 0, after live traffic under the old Flink rule **192**, still 192
  after Flink was fixed (stale buckets in Redis), 0 after the next batch.
* **Replay determinism**: the Kappa job run twice from offset 0 gives identical output (metrics and counters). **Atomic swap**: concurrent reader saw only
  complete versions, rollback works.

## Phase 5 - benchmarks B1-B10 - DONE (see caveats)

All ten scenarios ran in one sequence (`bench run-all`, results in `results/final/`, raw CSV/JSON per scenario, `meta.json` with config, git commit,
image digests, host). Repetitions: B1 and B8 three times; the others once (state-changing scenarios cannot be repeated without a reset).
Headline numbers are in `docs/RESULTS.md` (generated). Caveats: single laptop, 6 GiB VM, parallelism 1, small data.

## Phase 6 - report, demo UI, Grafana, RUNBOOK, demo-fast - DONE

* `bench report` generates `report.html` (self-contained, light/dark page chrome, table view for every chart), 9 slide PNGs at 1920x1080 and `docs/RESULTS.md`;
  charts were inspected visually. Colors: validated categorical slots (blue = Lambda, orange = Kappa).
* Wallet UI `http://localhost:8080/ui` (Lambda | Kappa | Truth, red on mismatch, payment buttons, verification status): endpoints verified from a container
  (`/sim/now`, `/demo/marker`, `/verify/latest`, all APIs). **Not clicked through in a real browser.**
* Prometheus (:9090) and Grafana (:3000) start with `make up-observability`; targets up (Flink x2, query-api), dashboard "Lambda vs Kappa" provisioned.
  Stale consumer groups of cancelled jobs still show their old lag (cosmetic).
* RUNBOOK covers every command, recovery steps and the 12-minute presenter script. `make demo-fast` = `reset` + `test-e2e` (each part was run by hand).

## Phase 7 - clean-clone final run - PARTLY DONE

Done: `stack.py reset` (wipe + start from the shared Parquet) then `bench run-all` produced everything above from a clean state, and `bench verify` passes for both
architectures at the end (Kappa 15/15, Lambda 16/16). Several scenarios (B4, B5, verification) had to be rerun after fixing bugs; the final numbers come from
those reruns on the same stack, not from one uninterrupted sequence. **Not done:** a run from a fresh `git clone` (the repository has no commits of this work;
`make` is not installed on this host, the Makefile targets were exercised through the equivalent commands).

## Bugs found by the measurements (all fixed, kept here as the honest log)

1. Live generator reused event ids across runs (seeded by seed only) -> duplicates in the truth log.
2. Kafka log dir and Iceberg catalog were not persistent (defaults in /tmp and memory): an engine restart lost the topic and the table registration.
3. `docker kill` plus a restart policy does not restart the container: chaos scenarios start it explicitly.
4. Flink could not restore any S3 checkpoint (S3A change detection vs SeaweedFS ETags) -> `fs.s3a.change.detection.mode: none`.
5. The nightly batch started exactly at t_k and missed events still in the archiver pipeline (Lambda stuck near 88 % exact): added a grace period.
6. Oracle queries slowed to minutes by thousands of per-second truth files: compaction added.
7. ClickHouse out of memory (16 CPUs seen by default settings, big merges): thread, cache and pool limits plus more memory.
8. Per-version late-events topics were auto-created with 7-day retention while event timestamps are simulated (months old): Kafka deleted late events at once;
   the topic is now created with infinite retention and the verifier reads all partitions.
9. Lag measured from committed Kafka offsets is stale by a checkpoint interval; B2/B6 now use processed counts.
10. Windows line endings written by Python broke `sh` scripts; all text files are LF.

## State of the machine when work stopped

The stack is up on the fast profile (Kappa v2 active, rule v2, Lambda v2, Prometheus and Grafana running); `results/final/` is complete. Run `make reset` before any new benchmark.
WSL is capped at 6 GiB in `%UserProfile%\.wslconfig` (memory=6GB, swap=4GB, autoMemoryReclaim=gradual). The stray folder `D:\tmp_lkdata` from phase 1 is still there to delete by hand.
