# Design — Lambda vs Kappa live demo & benchmark

One business problem (e-wallet customer spending statistics, requirements Y1–Y4) implemented twice on
**byte-identical input**, then measured. Source of requirements: `claude_code_prompt_lambda_kappa_demo.md`.

## 1. Architecture at a glance

```
generator ──► Parquet (stream, arrival-ordered, with duplicates/late)  ──► Kafka `transactions`  (Kappa source)
   │                                                                  └─► Iceberg master (Lambda, phase 3)
   └──► truth/ Parquet (every logical event exactly once) ──► oracle (DuckDB SQL) ── expected values

Lambda: Spark archiver → Iceberg master → Spark nightly batch → ClickHouse batch views ┐
        Flink speed layer (Java) → Redis (events ≥ t_k)                                 ├► query-api merge
Kappa : one Flink job (Java, RocksDB state) → ClickHouse ReplacingMergeTree upserts ────┘
```

Three independent implementations of the spending rule, all tested with the vectors in
`shared/rules/spend_rules.yaml`: Python/PySpark (`python/rules-python`), Java (`jvm/rules-java`, shared by both Flink jobs,
never by Spark), SQL (`shared/rules-sql`, oracle only).

## 2. Decisions and deviations from the spec

| # | Decision | Reason |
|---|----------|--------|
| D1 | **SeaweedFS 3.97 instead of MinIO** as the S3 store. Bucket names unchanged (`master`, `warehouse`, `checkpoints`, `results`). | Verified on 2026-10-26: `minio/minio` on Docker Hub is denied and no `quay.io/minio/*` tag could be resolved, so there is no pullable official MinIO image (and Bitnami is excluded by the spec). SeaweedFS is an official image with a compatible S3 API (path-style, static credentials). Risk: Spark/Flink/Iceberg S3A behaviour on SeaweedFS must be proven in phase 2/3 — fallback is building MinIO from source or Garage. |
| D2 | Kafka **3.9.1** (not 4.x). | Flink 1.20 / Spark 3.5 Kafka clients are validated against 3.x; KRaft single broker, same as spec. |
| D3 | Host dev uses Python 3.10 venv; all containers run Python **3.11.13**. | Host has only 3.10. Tests were run in both (12/12 pass). Code uses no 3.11-only features. |
| D4 | `generator` and `bench` are idle tool containers (`sleep infinity`), driven by `docker compose exec`. | They take CLI arguments per scenario; `oracle` is a real service. |
| D5 | Docker Desktop VM has 8 GB (host 16 GB). Core idles at ≈2.1 GB. | Lambda + Kappa together will not fit; cost benchmarks already run one profile at a time. Recommend `.wslconfig memory=12GB` before phase 3. |
| D6 | `make` is not installed on this Windows host; the Makefile targets work from Git Bash/WSL with GNU make. Equivalent commands are in `docs/RUNBOOK.md` (phase 6). | — |
| D7 | Spend-by-category adds the pseudo category `TRANSFER` for `TRANSFER_OUT` (no merchant). | Gives the invariant Σcategories = Σdays per month, used by `bench verify`. |
| D8 | Backfill arrival times are clamped to the end of their month. | A late/duplicate event can never cross a month file, so every file is self-contained; effect is ≤ 2 h on a handful of events at month ends. |
| D10 | **Monorepo layout** grouped by technology instead of the prompt's flat layout (map in section 7). | One PYTHONPATH/pytest root for Python, one Maven reactor under `jvm/` (shared `rules-java` used by both Flink jobs), infra files together. |
| D11 | Spark Structured Streaming **checkpoint of the archiver on a Docker volume**, not on S3. | Hadoop S3A fails on SeaweedFS (`ParentNotDirectoryException`: directory markers are reported as files). Iceberg (AWS SDK v2) and Flink (Hadoop FS plugin) work against SeaweedFS; Iceberg needs `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` because SeaweedFS stores `aws-chunked` trailer framing literally. The master data and metadata are on S3; only the stream offset checkpoint is local. |
| D9 | `produced_at` is `null` in backfill data (only live/marker events carry it). | Backfill has no wall-clock send time; freshness is measured on live/marker events only. The JSON schema allows null. |

## 3. Data & time model

* Event schema: `shared/schemas/transaction.schema.json` (JSON Schema 2020-12, `schema_version: 1`), Arrow schema in
  `python/common/lk_common/model.py`. Money is `int64` VND everywhere (generator asserts amount ≥ 1000, multiples of 1000).
* Simulated time: `time.sim_speedup` (default 144 → 1 sim day = 600 s). Conversion lives only in
  `lk_common.config` (`TimeConfig`, `SimClock`). The live clock anchor (`data/clock.json`) is shared by all containers.
  History = 2023-01-01 … 2026-01-01 (3 years); live time starts at the end of history.
* Allowed lateness = POS lateness limit (2 sim hours) + margin (10 sim min).
* Backfill layout: `data/stream/backfill/month=YYYY-MM/part-0.parquet` (arrival-ordered, includes duplicates),
  `data/truth/backfill/month=YYYY-MM/part-0.parquet` (logical events once, sorted by account), live truth in
  `data/truth/live/<run_id>/part-*.parquet`, written **before** the events are sent (write-ahead).
* Determinism: each month uses `SeedSequence([seed, 1, month_idx])`, merchants use fixed seed 42, event ids are derived from the RNG
  (RFC-4122 v4 shaped). Same seed ⇒ identical Parquet (tested). `id_checksum` = Σ first 8 bytes of each event_id mod 2⁶⁴, computed
  independently by the generator (numpy) and by the oracle (SQL) — they agree on the 20 M-event run.
* Fault injection: late POS (≤ 2 h, `pos_offline=true`), optional beyond-limit POS (`--beyond-limit-ratio`, B4), duplicates
  (same event_id, re-sent later; byte-identical in live mode), out-of-order jitter. Counts → `results/<run_id>/injections.json`
  and `data/backfill_manifest.json`.

## 4. Oracle

`python/oracle/lk_oracle`: DuckDB over `truth/**/*.parquet` + the static merchants CSV; rules as SQL macros in `shared/rules-sql/rules.sql`.
Event-time cutoff is exclusive. Never reads Kafka/ClickHouse/Redis. Endpoints `/truth/{balance,spend,report,trace,totals}`.

## 5. Pinned versions (verified 2026-10-26)

| Component | Version | Digest |
|-----------|---------|--------|
| Kafka | `apache/kafka:3.9.1` | `sha256:4ceccc57…622ed` |
| Object store | `chrislusf/seaweedfs:3.97` | `sha256:bb05d66d…89c6e` |
| Iceberg REST | `apache/iceberg-rest-fixture:1.10.0` | `sha256:df2e9dd4…ffc2e` |
| ClickHouse | `clickhouse/clickhouse-server:25.8.33.6` | `sha256:0152dd51…5ee77` |
| Python base | `python:3.11.13-slim-bookworm` | `sha256:86adf8db…2d2e1` |
| Python libs | `python/requirements.txt` (exact pins) | — |

Later phases add Spark 3.5.x, Flink 1.20.x, Redis 7.4, Prometheus/Grafana (to be pinned and recorded when introduced).
Full digests are recorded per run in `results/<run_id>/` by `bench` (phase 5).

## 6. Memory budget (Docker limits)

kafka 1.25 GiB · seaweedfs 0.75 · iceberg-rest 0.5 · clickhouse 2 · oracle 1 · generator 1 · bench 0.5 · init ≤ 0.25.
Measured idle after seeding 20 M events: kafka 0.8 GiB, clickhouse 0.5, oracle 0.6, seaweedfs 0.14, iceberg 0.1.

## 7. Repository layout (deviation D10)

```
docker-compose.yml  Makefile  .env.example
shared/   config/demo.yaml  rules/spend_rules.yaml  rules-sql/  schemas/      # language-neutral contracts
python/   common/ generator/ oracle/ bench/ query-api/ demo-ui/ rules-python/ tests/ requirements.txt pytest.ini
jvm/      rules-java/ lambda-flink-speed/ kappa-flink-job/                    # one Maven reactor (phase 2)
spark/    archiver/ batch/                                                    # PySpark jobs (phase 3)
infra/    docker/ kafka/ clickhouse/ grafana/
docs/  results/  scripts/  data/ (generated, git-ignored)
```
