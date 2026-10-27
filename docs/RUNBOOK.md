# Runbook

Status: **operations section is complete for phases 1–2**; the 10–12 minute presenter script (scenes Y1–Y4, §7 of the prompt)
is filled in during phase 6 and is only outlined here.

## 0. Prerequisites

* Docker Desktop with ≥ 8 GB for the VM (≥ 10–12 GB once Lambda and Kappa run together), Compose v2.
* `.env` created from `.env.example` (`make env`, or `Copy-Item .env.example .env`).
* GNU make (Git Bash / WSL). On plain PowerShell use the "no make" column.

| Goal | With make | No make (PowerShell, repo root) |
|------|-----------|----------------------------------|
| Start core | `make up` | `docker compose --profile core up -d --build --wait` |
| Generate history + load Kafka | `make seed` | `docker compose exec -T generator python -m lk_generator merchants` then `... backfill --run-id local` then `... load-kafka` |
| Live traffic | `make live` | `docker compose exec -T generator python -m lk_generator live --run-id local [--rate 2000] [--ramp 0:500,60:2000]` |
| Unit tests (Python) | `make test` | `.venv\Scripts\python -m pytest -c python/pytest.ini -q` or `docker compose exec -T generator python -m pytest -q` |
| Stop | `make down` | `docker compose --profile core --profile kappa --profile lambda down` |
| Wipe everything | `make clean` | add `-v`, delete `data/` |

## 1. Data generation (phase 1)

```
lk_generator merchants                     # 10,000 merchants, fixed seed 42 -> data/merchants.csv
lk_generator backfill --run-id <id>        # 20 M events default, 36 monthly Parquet files (stream + truth)
lk_generator load-kafka                    # same Parquet -> Kafka (+ merchants compacted topic)
lk_generator live --run-id <id> --duration 60 --rate 2000 [--late-pos-ratio 0.03 --duplicate-ratio 0.01]
lk_generator marker --account A100001 --type PAYMENT --amount 50000 --merchant M00001 [--channel POS --late-minutes 90]
```

Outputs: `data/stream/backfill`, `data/truth/{backfill,live}`, `data/backfill_manifest.json`,
`data/kafka_load_manifest.json`, `results/<run_id>/injections.json`, `data/clock.json` (shared simulated clock).
Use a **new `--run-id` for every live run**; truth files are per run id.

Oracle checks: `curl localhost:8090/truth/totals`, `/truth/balance/A000001`, `/truth/trace/A100001?day=2025-12-31&rule=v2`.

## 2. Recovering a consistent state

Kafka and truth must describe the same events. If you delete truth files or a run id, recreate the topic and reload:
```
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic transactions
docker compose up kafka-init            # recreates topics
docker compose exec -T generator python -m lk_generator load-kafka
```
(then re-run any live runs you still want, with new run ids).

## 3. Kappa (phase 2)

Components (profile `kappa`): `flink-jobmanager`, `flink-taskmanager` (4 slots, 2.5 GB), `kappa-submit` (one-shot job submit).
The whole architecture is ONE Flink job (`jvm/kappa-flink-job`): Kafka → keyBy(account) → dedup/lateness/rule/state (RocksDB)
→ ClickHouse upserts (`kappa_<ver>_{account,spend_day,spend_cat_month,trace,progress}`), late events → topic `late-events`.
Readers use the views `kappa_current_*` (repointed atomically with `CREATE OR REPLACE VIEW`).

```
# start cluster + job (first time: replay the whole topic from offset 0, parallelism 4)
$env:KAPPA_START_FROM='earliest'; $env:KAPPA_PARALLELISM='4'            # bash: KAPPA_START_FROM=earliest ...
docker compose --profile core --profile kappa up -d --build --wait flink-jobmanager flink-taskmanager
docker compose --profile core --profile kappa up kappa-submit            # idempotent: no-op if kappa-v1 already runs
docker compose exec -T bench python -m lk_bench kappa-views --to v1      # point kappa_current_* at v1

docker compose exec -T bench python -m lk_bench.flinkapi                 # checkpoints, vertices
docker compose exec -T bench python -m lk_bench verify --arch kappa --rule v1 --run-id phase2   # exit 1 on any violation
curl localhost:8080/kappa/balance/A000001 ; curl "localhost:8080/kappa/spend/A000001?day=2025-06-01"
curl "localhost:8080/kappa/report/A000001?from=2023-01&to=2025-12" ; curl "localhost:8080/trace/kappa/A100001?day=2025-12-31"
```
Flink UI: http://localhost:8081 (REST may reset connections from the Windows host; use `lk_bench.flinkapi` inside compose).
Resetting v1 (fresh replay): stop the Flink containers, `DROP TABLE lk.kappa_v1_* SYNC`, `DROP VIEW lk.kappa_current_*`, start again with `KAPPA_START_FROM=earliest`.
Always use `--start-from earliest` (not `committed`) for a deliberate replay; Flink restarts after a crash use its own checkpoint.

## 4. Presenter script (phase 6)

Scenes: Setup → Y1 freshness → Y4 late POS → Y4 crash → Y3 rule change → Y2 historical report → wrap-up. Commands and
expected output are written once the Lambda path and the bench scenarios exist.
