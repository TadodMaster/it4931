# Runbook

Everything below was exercised on the **fast profile** (1 M events, 10 k accounts, 500 tx/s) on an 8 GiB Docker VM.
A full presentation takes about 12 minutes (section 8). `make` targets are shown; without GNU make run the command under each.

## 0. Prerequisites

* Docker Desktop (Compose v2), **8 GiB VM minimum**; Kappa + Lambda + core need about 7.3 GiB at peak. More memory
  (`%UserProfile%\.wslconfig` → `memory=12GB`, then `wsl --shutdown`) makes everything safer.
* `cp .env.example .env` (`make env`). Tunables live in `shared/config/demo.yaml` and `shared/config/profiles/fast.yaml`;
  `LK_PROFILE=fast` in `.env` selects the small dataset. **A variable set in `.env` overrides the YAML and the profile.**
* Host Python 3.10+ with `.venv` only for unit tests and `scripts/stack.py`; everything else runs in containers (Python 3.11).

| Goal | make | without make |
|------|------|--------------|
| Wipe everything and start all (Kafka ← Parquet, Iceberg master ← same Parquet, Kappa replay, Lambda jobs) | `make reset` | `python scripts/stack.py reset` |
| Start without wiping | `make start` | `python scripts/stack.py start` |
| Status | `make status` | `python scripts/stack.py status` |
| Unit tests (Python, Java vectors via Docker build, PySpark vectors) | `make test` | `docker build -f infra/docker/flink-job.Dockerfile -t lk-flink-job:dev .` + `.venv\Scripts\python -m pytest -c python/pytest.ini -q` |
| 2-3 minute end-to-end proof (live traffic → quiesce → batch → both verifications) | `make test-e2e` | `docker compose exec -T bench python -m lk_bench run e2e --run-id e2e` |
| Whole benchmark | `make bench-all RUN_ID=final` | the commands in the Makefile target |
| Report | `make report RUN_ID=final` | `docker compose exec -T bench python -m lk_bench report --run-id final` |
| Stop / wipe volumes | `make down` / `make clean` | `docker compose --profile core --profile kappa --profile lambda down [-v]` |

URLs: wallet UI http://localhost:8080/ui · API http://localhost:8080/docs · Oracle http://localhost:8090 · Flink http://localhost:8081
(its REST often resets connections from the Windows host; `lk_bench.flinkapi` talks to it from inside Docker) · ClickHouse :8123 ·
Grafana :3000 and Prometheus :9090 with `make up-observability`.

## 1. Data generation

```
lk_generator merchants                    # 10,000 merchants, fixed seed 42
lk_generator backfill --run-id seed       # monthly Parquet: stream (arrival order, duplicates, late POS) + truth (each event once)
lk_generator load-kafka                   # SAME stream files -> Kafka (+ merchants compacted topic)
spark-load-master                         # SAME stream files -> Iceberg master (make seed-master)
lk_generator live --run-id <id> --duration 60 --rate 500 [--duplicate-ratio 0.05 --late-pos-ratio 0.03 --beyond-limit-ratio 0.001]
lk_generator marker --account A010001 --type PAYMENT --amount 50000 --merchant M00001 [--channel POS --late-minutes 90]
```
Use a **new `--run-id` for every live run**. Truth is written before sending (write-ahead).
Oracle: `curl localhost:8090/truth/totals`, `/truth/balance/A010001`, `/truth/trace/A010001?day=2025-12-31&rule=v2`.

## 2. Kappa

One Flink job (`kappa-v1`): Kafka → keyBy(account) → dedup, lateness, rule, RocksDB state → ClickHouse `kappa_v1_*` (idempotent
absolute-value upserts), late events → topic `late-events`. Readers use the views `kappa_current_*`.
```
docker compose --profile core --profile kappa up -d --wait flink-jobmanager flink-taskmanager
KAPPA_START_FROM=earliest KAPPA_PARALLELISM=2 docker compose --profile core --profile kappa up kappa-submit   # idempotent
docker compose exec -T bench python -m lk_bench kappa-views --to v1        # rollback/switch = the same command with --to v2 / v1
docker compose exec -T bench python -m lk_bench verify --arch kappa --rule v1 --run-id r1   # exit 1 on any violation
docker compose exec -T bench python -m lk_bench.flinkapi                   # checkpoints, vertices, busy/backpressure
```
**The JobManager has no HA.** If it is recreated, running jobs are lost (checkpoints stay on S3). Resubmit: Kappa with
`KAPPA_START_FROM=earliest` (idempotent sink, safe), Lambda speed with `LAMBDA_START_FROM=committed`.

## 3. Lambda

Spark archiver (Kafka → Iceberg master, append-only) · nightly Spark batch (`spark-scheduler`, once per simulated night = 10 real
minutes) · Flink speed layer → Redis day buckets · merge in the query-api.
```
make seed-master                                 # once after seeding
make up-lambda                                   # redis, jobs, scheduler (first batch runs immediately)
docker compose exec -T bench python -m lk_bench run batch --run-id r1          # corrective batch now (t_k = sim now); quiet system only
docker compose exec -T bench python -m lk_bench run master-stats --run-id r1   # Iceberg snapshots / rows / checksums
docker compose exec -T bench python -m lk_bench verify --arch lambda --run-id r1
```
Batch swap = one `EXCHANGE TABLES lk.lambda_batch_cur AND lk.<new>` (atomic); the previous version is kept as `lambda_batch_prev_<id>`;
**rollback** = the same `EXCHANGE` with the previous table (`bench run atomic` demonstrates swap atomicity and rollback).
Manual batches inside a simulated day use `--purge-current-day` (also drops that day's speed-layer buckets): use them only when no live
traffic is flowing, otherwise events could be lost between snapshot and purge.

## 4. Recovering a consistent state

* Kafka lost or wrong: `python scripts/stack.py reset`.
* **Iceberg catalog lost** (it is now persisted in the `iceberg-data` volume): list `master/transactions/metadata/*.metadata.json` in the
  `warehouse` bucket, take the highest-numbered file and run
  `docker compose --profile core --profile lambda-tools run --rm spark-register-master /opt/spark/bin/spark-submit /app/spark/register_master.py s3://warehouse/master/transactions/metadata/<file>`.
* Redis lost: speed-layer buckets are gone; run one corrective batch while traffic is quiet.

## 5. Benchmarks (`bench run <name> --run-id <id> [--repeat N] [--arg key=value]`)

| name | scenario | notes |
|------|----------|-------|
| b1 | freshness (Y1) | markers on probe accounts, API and store polled; run with `--repeat 3` |
| b2 | max sustained throughput | `rates=500,1000,...` `step_s=30`; the single Python generator may be the limit |
| b3 | correctness over time | `minutes=25 interval=30`; needs ≥ 2 nightly batches to show the sawtooth |
| b4 | late POS | waits for a batch boundary (≤ 10 real min), then injects |
| b5 | duplicates + crash | kills TaskManager then archiver; corrective batch at the end |
| b6 | rule change v1 → v2 | Lambda: Spark + Flink + batch; Kappa: replay from offset 0, lag curve, predicted vs measured T |
| b7 | drift | Spark v2 only, then Flink fixed, then next batch |
| b8 | 3-year report | latency per account, batch duration, Kappa state size |
| b9 | cost & complexity | docker stats, storage, services per profile, lines of code |
| b10 | extrapolation | uses b6's measured replay rate |
| atomic / determinism | proofs 6 and 5 of `bench verify` | swap atomicity + rollback; Kappa replay twice → identical output |

Everything lands in `results/<run_id>/` with `meta.json` (config, git commit, image digests, host). Run **one scenario at a time**.
State changes: b6/b7 leave the rule at v2 and Kappa on `kappa_v2` (re-run `make reset` for a clean v1 start).

## 6. Chaos by hand

```
docker kill it4931-flink-taskmanager-1      # restart policy brings it back; both jobs resume from their last checkpoint
docker kill it4931-spark-archiver-1         # restarts; Structured Streaming checkpoint makes the append exactly-once
docker compose exec -T bench python -m lk_bench verify --arch kappa --rule v1 --run-id chaos   # Kappa stays exact (0 VND)
```

## 7. Observability

`make up-observability`, open Grafana (dashboard **Lambda vs Kappa**): ingest rate, consumer lag per group, Flink checkpoint duration
and size, Spark batch duration, Redis memory, ClickHouse latency, failed verification checks (mismatch vs oracle).

## 8. Presenter script (10-12 minutes)

Setup (before the audience arrives): `make reset` (≈ 8 min, fast profile), `make up-observability`, open
http://localhost:8080/ui (account **A010001**, auto refresh on) and Grafana.

| min | scene | do | point at |
|-----|-------|----|----------|
| 0-1 | Setup | show the three columns Lambda / Kappa / Truth and `docker compose ps` | one Flink job vs Spark + Flink + Redis + Iceberg |
| 1-3 | **Y1** fresh | UI button *Pay 50,000* | Kappa and Lambda (speed layer) update within seconds; `bench run b1` numbers from the report |
| 3-5 | **Y4** late POS | UI button *Inject 90-min-late POS payment* | Kappa exact at once; Lambda column red until the next nightly batch (≤ 10 min, see B3 chart: sawtooth); beyond-limit events in `late-events` |
| 5-7 | **Y4** crash | `docker kill it4931-flink-taskmanager-1`, then `docker kill it4931-spark-archiver-1`; then `bench verify --arch kappa` | green: 0 VND difference; Lambda speed layer may undercount until the next batch (B5) |
| 7-9 | **Y3** rule change | `bench run b6` (or show `results/final/b6_*`) | Kappa: lag curve to 0, views switched, API never failed; Lambda: Spark + Flink + full batch; B7 drift when only Spark changed |
| 9-10 | **Y2** report | UI 3-year report columns; B8 chart | batch duration vs state size |
| 10-12 | wrap-up | open `results/final/report.html` | requirement × architecture table, verification green/red, threats to validity |

Fallback: `results/final/slides/*.png` (1920×1080) and the report; every number there is regenerated from raw files by `make report`.
