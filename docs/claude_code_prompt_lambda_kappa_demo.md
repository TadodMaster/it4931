# Prompt for Claude Code — Lambda vs Kappa live demo & benchmark

Copy everything below the line into Claude Code, started in an empty directory.

---

You are building a reproducible, laptop-scale demo and benchmark that implements **one business problem twice** — once with the **Lambda architecture** and once with the **Kappa architecture** — on the **same input data**, then proves with measurements which one is better at what. This is for a university presentation (Big Data course, chapter "Lambda and Kappa architectures"). Correctness and honest measurement matter more than polish. Do not fake, hard-code or "simulate" any result that the system is supposed to compute.

Work in phases (listed at the end). Before writing code, write `docs/DESIGN.md` summarizing your plan, the pinned versions you chose and any deviation from this spec with a reason. Keep a `docs/PROGRESS.md` you update at the end of each phase.

## 1. The single business problem (used everywhere)

An e-wallet ("the wallet") receives transactions from its mobile app and from in-store POS terminals. The analytics system must serve **customer spending statistics** with four requirements:

| ID | Requirement | What "done" means in the demo |
|----|-------------|-------------------------------|
| **Y1** | Fresh: balance and spend appear in the app within seconds | End-to-end freshness p95 measured and reported |
| **Y2** | Historical: spend report over 3 years by merchant category (join with merchant dimension) | Report query served; build cost and query latency measured |
| **Y3** | Recompute: the rule "which transaction counts as spending" changes → re-apply to all history | Rule v1 → v2 switch executed live; time-to-correct measured |
| **Y4** | Exact money: POS terminals sync offline transactions up to 2 hours late; no loss, no duplicates | Late + duplicate + crash scenarios injected; zero-error proof against an oracle |

Production assumption from the slides: H ≈ 10 billion historical transactions, peak 50k tx/s. The demo is scaled down (configurable), and the report must extrapolate using the catch-up formula **T ≈ H / (R_process − R_in)** with measured rates.

### Data model

`transactions` event (JSON, schema versioned with a `schema_version` field; validate with a shared JSON Schema):

```
event_id        UUID string, globally unique (dedup key)
account_id      string, e.g. "A000123"  (Kafka key)
event_time      ISO-8601 UTC, simulated clock (when the customer paid)
produced_at     ISO-8601 UTC, wall clock when the generator sent it (for latency)
channel         APP | POS
type            TOPUP | PAYMENT | REFUND | TRANSFER_OUT | TRANSFER_IN | CASHBACK
amount          BIGINT, VND, always positive (sign comes from type)
merchant_id     nullable (PAYMENT/REFUND only)
is_self_transfer boolean (TRANSFER_OUT to the customer's own linked bank account)
pos_offline     boolean (true if this event was delayed by an offline POS)
```

Money is always integer VND. Never use floats for amounts anywhere (Spark, Flink, ClickHouse, Python).

Merchant dimension: `merchant_id, merchant_name, category` (FOOD, TRANSPORT, SHOPPING, BILLS, ENTERTAINMENT, HEALTH, EDUCATION, OTHER). ~10,000 merchants, static. Ship it as a CSV generated with a fixed seed, loaded to MinIO (Lambda) and to a compacted Kafka topic `merchants` (Kappa).

Derived metrics:
- `balance(A)` = Σ signed amounts (TOPUP, REFUND, TRANSFER_IN, CASHBACK positive; PAYMENT, TRANSFER_OUT negative). Independent of the spending rule.
- `spend(A, day)` and `spend(A, category, month)` according to the **spending rule**:
  - **Rule v1**: spend = PAYMENT + TRANSFER_OUT.
  - **Rule v2** (the Y3 change): spend = PAYMENT − REFUND (refund counted on its own event day, category of its merchant) + TRANSFER_OUT where `is_self_transfer = false`.

Rules live in **one specification file** `rules/spend_rules.yaml` (human-readable description + test vectors). Each engine implements the rule in its own language; all implementations must pass the same test vectors in CI. This is deliberate: Lambda needs two implementations (Spark + Flink) — that is the "two codebases" cost we want to show.

### Simulated time

Use a simulated clock with `SIM_SPEEDUP` (default 144 → 1 simulated day = 10 real minutes). All "nightly" schedules, the 2-hour POS lateness, allowed lateness, TTLs and windows are defined in simulated time and converted in one shared config module. Put every tunable in `.env` / `config/demo.yaml`.

## 2. Data generator and ground truth

`generator/` (Python 3.11):
- Deterministic with `--seed`. ~100,000 accounts with skewed activity (Zipf), daily seasonality, realistic amount distributions per type/category.
- Two modes:
  - `backfill`: generate history (default 3 simulated years, `HISTORY_EVENTS` default 20M, configurable up to 200M) into **Parquet files** first, then load the **same files** into Kafka (Kappa source) and into the Lambda master dataset. Both architectures must consume byte-identical input.
  - `live`: produce at a target rate (default 2,000 tx/s, configurable, supports ramp profiles) to Kafka topic `transactions`, keyed by `account_id`.
- Fault injection flags, all logged with counts to `results/<run_id>/injections.json`:
  - `--late-pos-ratio` (default 3%): POS events with `event_time` up to 2 simulated hours in the past.
  - `--duplicate-ratio` (default 1%): re-send the exact same event (same `event_id`) to simulate producer retries.
  - `--out-of-order-jitter`.
- **Ground-truth log**: every logical event (exactly once, before duplicate injection) is also appended to `truth/` as Parquet. This is the only input of the oracle.
- **Probe accounts**: reserve ~50 accounts used only by the benchmark to inject marker transactions and measure freshness.

## 3. Infrastructure (docker-compose)

One `docker-compose.yml` with **profiles**: `core` (always), `lambda`, `kappa`, `observability`, `demo-ui`. Target machine: 16 GB RAM, 8 vCPU; document memory limits per container. Use official images only and pin exact versions you verify (do **not** use Bitnami images; their free catalog changed in 2025). Healthchecks and `depends_on: condition: service_healthy` everywhere.

`core`:
- **Kafka** (official `apache/kafka`, KRaft, single broker). Topics created by an init container:
  - `transactions`: 6 partitions, key = `account_id`, `retention.ms=-1` (infinite — this is the Kappa source of truth; note in docs that production would use tiered storage to S3).
  - `merchants`: compacted.
  - `late-events` (Kappa side output), `rules-control` (optional).
- **MinIO** (S3): buckets `master` (Lambda immutable dataset), `warehouse` (Iceberg), `checkpoints` (Flink), `results`.
- **Iceberg REST catalog** (e.g. `apache/iceberg-rest-fixture` or `tabulario/iceberg-rest`) backed by MinIO.
- **ClickHouse** (serving for both architectures; mention Apache Doris as the slide's alternative).
- **Generator**, **oracle**, **bench** containers (Python).

`lambda`:
- **Raw archiver**: Spark Structured Streaming job Kafka → Iceberg table `master.transactions` (append-only, partitioned by `days(event_time)`), checkpointed in MinIO. Immutable: no updates/deletes ever; prove it (see §6).
- **Batch layer**: Spark 3.5 batch job (PySpark) run by a `batch-scheduler` container once per simulated night (configurable period P). It reads all of `master.transactions` with `event_time < t_k` (t_k = start of current simulated day), deduplicates by `event_id`, applies the rule (Spark implementation), joins merchants, and writes batch views:
  - `lambda_batch_spend_day(account_id, day, spend, tx_count)`
  - `lambda_batch_spend_cat_month(account_id, category, month, spend)`
  - `lambda_batch_balance(account_id, balance)`
  Load into ClickHouse **new versioned tables**, then switch atomically (`EXCHANGE TABLES` or a view repointed in one statement) and record `t_k`, run id, rule version, input snapshot id and duration in `lambda_batch_runs`. Never expose a half-loaded view. Keep the previous version for instant rollback.
- **Speed layer**: Flink 1.20 job (Java 17, Maven) reading `transactions` (latest offsets), dedup by `event_id` (Redis `SET NX` with TTL), applying the rule (Java implementation #1 for Lambda), incrementing per `(account, day)` and balance deltas in **Redis** for events with `event_time ≥ t_k`. Events older than `t_k` are ignored by the speed layer (classic Lambda: the next batch fixes them) — document this. After each batch swap, expire Redis buckets older than the new `t_k`.
- **Serving/merge**: part of `query-api` (below): `lambda_spend(A, day) = batch(≤ t_k) + realtime(> t_k)` with exactly matching `t_k` from `lambda_batch_runs`.

`kappa`:
- **One Flink 1.20 job** (Java 17; shared Maven module `rules-java` used by both Flink jobs — Lambda speed and Kappa — but NOT by Spark):
  - Kafka source with event-time watermarks (bounded out-of-orderness, configurable).
  - `keyBy(account_id)` → `KeyedProcessFunction` with RocksDB state: dedup set of `event_id` with state TTL ≥ allowed lateness + margin; per `(account, day)` spend, per `(account, category, month)` spend, balance. Merchant dimension via broadcast state from the `merchants` topic.
  - Allowed lateness = 2 simulated hours (+ configurable margin). Events later than that go to side output topic `late-events` and are counted — never silently dropped.
  - Emit an upsert on every state change (for freshness) with **absolute values**, not deltas.
  - Exactly-once: checkpointing `EXACTLY_ONCE` to MinIO, incremental RocksDB checkpoints. Sink to ClickHouse `ReplacingMergeTree(version)` keyed by the view key, where `version` makes upserts idempotent (e.g. event-time watermark + checkpoint id, or a monotonically increasing per-key sequence kept in state). Explain in docs why absolute-value upserts make the sink idempotent so replays after failure cannot double count.
  - Output tables are versioned by job version: `kappa_v1_*`, `kappa_v2_*`. Readers go through views `kappa_current_*` that are repointed atomically.
- **Reprocessing (Y3)**: script `bench reprocess --rule v2` that starts job v2 with a **new consumer group from offset 0**, writes `kappa_v2_*`, monitors consumer lag, switches `kappa_current_*` to v2 when lag ≈ 0 and the oracle check passes, keeps v1 running until switch, then stops v1. Rollback command switches back.

Shared services:
- **query-api** (FastAPI): 
  - `GET /lambda/balance/{account}`, `/lambda/spend/{account}?day=`, `/lambda/report/{account}?from=&to=` (Y2)
  - same under `/kappa/...`
  - `GET /truth/...` served by the oracle
  - `GET /trace/{arch}/{account}?day=`: returns the list of events that contributed and the computed total (requirement "debuggable: trace every transaction").
  - Every response includes `as_of`/watermark metadata (Lambda: `t_k` + speed lag; Kappa: current watermark + consumer lag).
- **oracle** (Python + DuckDB): reads only `truth/` Parquet, applies the rule in **SQL** (third independent implementation), returns exact expected values for any account/day/month at a given event-time cutoff and rule version. It never reads Kafka, ClickHouse or Redis.

`observability` (optional but recommended for the live demo):
- Prometheus + Grafana with provisioned dashboard: ingest rate, Kafka consumer lag per group (lambda-speed, kappa-v1, kappa-v2), Flink checkpoint duration/size, Spark batch duration, Redis memory, ClickHouse query latency, mismatch rate vs oracle.

`demo-ui` (Streamlit or a small React page):
- "Wallet app" screen: pick an account → three columns side by side: **Lambda | Kappa | Truth** for balance, today's spend, 3-year category report, with freshness and as-of info; red highlight on any mismatch.
- Buttons/panels mapped to the demo scenes in §7.

## 4. Repository layout and tooling

```
docker-compose.yml  .env.example  Makefile  config/demo.yaml
generator/  oracle/  query-api/  bench/  demo-ui/
lambda/spark-archiver/  lambda/spark-batch/  lambda/flink-speed/
kappa/flink-job/  rules/spend_rules.yaml  rules-java/  rules-python/(Spark)  rules-sql/(oracle)
clickhouse/init/*.sql  kafka/init/  grafana/  scripts/
docs/DESIGN.md  docs/RUNBOOK.md  docs/RESULTS.md  docs/PROGRESS.md
results/<run_id>/
```

Makefile targets (each idempotent, with clear logs): `make up`, `make up-lambda`, `make up-kappa`, `make seed` (backfill both from the same Parquet), `make live`, `make test` (unit + rule test vectors for all three implementations), `make bench-all`, `make bench-<scenario>`, `make report`, `make demo`, `make down`, `make clean`.

Every benchmark run gets a `run_id`, stores its full config, git commit hash, image digests, host info (CPU, RAM, Docker version) and raw measurements under `results/<run_id>/`.

## 5. Benchmark mechanism (`bench/`, Python CLI)

Run both architectures **concurrently on the same Kafka topic** for correctness/freshness scenarios, and **separately** (one profile at a time) for resource-cost scenarios so they do not compete. Repeat each timing scenario ≥ 3 times; report median and spread. Warm up before measuring.

| # | Scenario | Requirement | Method | Metrics |
|---|----------|-------------|--------|---------|
| B1 | Freshness | Y1 | Inject marker transactions on probe accounts at a steady rate; poll both APIs (and also subscribe to ClickHouse/Redis directly to separate API overhead) until the marker is reflected | p50/p95/p99 end-to-end latency per architecture; latency CDF |
| B2 | Max sustainable throughput | scale | Ramp ingest rate stepwise; a step is "sustained" if consumer lag stays bounded for N minutes | max R_in per architecture; CPU/mem at each step |
| B3 | Correctness over time | Y1/Y4 | Every K seconds compare a sample (and periodically ALL accounts) against the oracle at the architecture's own as-of cutoff and at wall-clock "now" | % exact matches, count of mismatched accounts, max abs diff (VND); plotted over time with batch-run markers (expect a Lambda sawtooth) |
| B4 | Late POS data | Y4 | Burst of offline POS events up to 2 sim hours late, plus a few beyond the limit | Lambda: when the number becomes correct (next batch); Kappa: correct immediately if within allowed lateness; count in `late-events` beyond it |
| B5 | Duplicates + crash | Y4 | Duplicate ratio on; `docker kill` the Flink TaskManager(s) and the Spark archiver mid-stream; restart | After recovery: total spend, balance and event counts match oracle exactly (0 VND difference) for Kappa; report honestly any Lambda speed-layer double counting (Redis dedup is not transactional) and show the next batch correcting it |
| B6 | Rule change | Y3 | Switch rule v1 → v2 | Lambda: wall time to update Spark + Flink and re-run full batch until views match oracle(v2); Kappa: replay catch-up time from offset 0, lag curve, measured R_process, and predicted vs measured T; zero-downtime check (API keeps answering during switch) |
| B7 | Drift between codebases | Lambda cost | Deploy rule v2 to Spark only (Flink still v1) | Show app number ≠ report for the same account/day; mismatch count until fixed |
| B8 | Historical report | Y2 | Run the 3-year category report for many accounts | Query latency p50/p95; Lambda batch duration; Kappa state size (RocksDB, checkpoint bytes) |
| B9 | Cost & complexity | operations | `docker stats` sampling per service; count services/containers per profile; `cloc` per path (Lambda: archiver + batch + speed + merge; Kappa: one job) ; number of rule implementations to change for Y3 | average/peak CPU and RAM, storage used (Kafka log, MinIO, ClickHouse, Redis, checkpoints), LOC |
| B10 | Extrapolation | slides | Use measured per-core throughput to compute replay time for H = 10 billion, R_in = 50k/s at several R_process values | table + chart matching the slide "Chi phí replay" |

## 6. Proving the data is correct (must be automated)

The demo must make claims only with evidence. Implement `bench verify` that produces `results/<run_id>/verification.json` and fails loudly on any violation:

1. **Oracle equality**: for every account (full scan at the end of each scenario) and each metric (balance, spend per day, spend per category-month), Lambda and Kappa vs oracle — exact integer equality at a defined cutoff. Report mismatches with drill-down links (`/trace/...`).
2. **Conservation invariants**:
   - Σ over all accounts of each metric = oracle global total.
   - Events counted by each architecture = logical events in truth log (duplicates excluded) − events reported in `late-events` (Kappa).
   - Kafka: produced count per partition (from generator logs) = end offsets; consumer committed offsets reach end offsets.
3. **Dedup proof**: number of duplicate sends injected (from `injections.json`) vs number of duplicates detected/discarded by each engine.
4. **Immutability proof (Lambda master dataset)**: Iceberg snapshot history shows only `append` operations; row count and a content checksum (e.g. sum of hashed `event_id`) only grow; per-partition checksums of history unchanged between runs.
5. **Replay determinism (Kappa)**: running job v1 twice from offset 0 produces identical output checksums.
6. **Batch view atomicity**: during a swap, a concurrent reader hammering the API never observes a partially loaded view (checksum of a fixed account set is either old or new, never mixed).
7. **Late-data accounting**: every injected late event is either reflected in the result or present in `late-events`; none disappears.
8. **Rule tests**: the same test vectors from `rules/spend_rules.yaml` pass in Java, PySpark and SQL.

Show a green/red verification summary in the demo UI and in the report.

## 7. Live demo runbook (`docs/RUNBOOK.md`, about 10–12 minutes)

Write a presenter script with exact commands, expected output and what to point at:
1. **Setup** (pre-recorded or already running): `make up && make seed && make live`; show Grafana and the wallet UI.
2. **Scene Y1**: make a payment for a probe account → Kappa and Lambda (speed) update in seconds; show freshness numbers.
3. **Scene Y4 — late POS**: inject a 2-hour-late POS batch → Kappa correct now; Lambda corrected after the next "night"; show B3 sawtooth chart.
4. **Scene Y4 — crash**: kill the Flink TaskManager → restart → totals still equal the oracle; show verification green.
5. **Scene Y3 — rule change**: Kappa replay with v2, lag curve going to 0, switch view; Lambda: two code changes + full batch; show B7 drift when only one side is changed.
6. **Scene Y2**: 3-year category report on both; show batch duration vs state size.
7. **Wrap-up**: results table Y1–Y4 × Lambda/Kappa from the report.

Also provide `make demo-fast` with smaller data so the whole demo works offline in under 15 minutes on the target laptop, and a fallback: screenshots/GIFs of each scene saved under `results/<run_id>/demo/`.

## 8. Report (`make report`)

Generate `results/<run_id>/report.html` (self-contained, charts with matplotlib or Plotly, no external CDN required) and `docs/RESULTS.md`:
- Environment, data size, config, run id.
- One chart per scenario: freshness CDF; correctness % over time with batch markers; throughput ramp; replay lag curve with predicted vs measured T; resource usage per service; LOC and service count; B10 extrapolation table.
- A summary table that maps directly to the slides:

| Requirement | Metric | Lambda | Kappa | Winner / note |
|---|---|---|---|---|

- An honest "threats to validity" section (scaled-down data, single-node Kafka, simulated time, laptop resources, image versions).
- Export the key charts as PNG at 1920×1080 into `results/<run_id>/slides/` so they can be pasted into PowerPoint.

## 9. Engineering rules

- Pin all versions (images, Maven, pip with lock files). Record image digests in the run metadata.
- No secrets in the repo; `.env.example` only.
- Unit tests for rules, generator determinism, oracle, merge logic (`t_k` boundary cases: event exactly at `t_k`, late events before `t_k`), and API. Integration test `make test-e2e` that runs a 2-minute scenario and asserts `bench verify` passes.
- Structured JSON logs with `run_id`. Every script prints what it is doing and where results went.
- If a component cannot run on the target laptop, say so in `docs/PROGRESS.md`, propose the smallest change, and continue with the rest — do not silently drop a requirement.
- Never edit benchmark numbers by hand; the report must be regenerated from raw data only.

## 10. Phases (stop and report after each phase with what works and how you verified it)

1. Design doc, repo skeleton, docker-compose `core`, generator + truth log + oracle, rule spec with test vectors.
2. Kappa path end to end (job, sink, views, query-api `/kappa`), `bench verify` against oracle.
3. Lambda path end to end (archiver, batch with atomic swap, speed layer, merge in query-api).
4. Fault injection, chaos, reprocessing (Kappa replay, Lambda re-batch), drift scenario.
5. Benchmark scenarios B1–B10 with repeat runs and raw data capture.
6. Report generator, demo UI, Grafana dashboard, RUNBOOK, `demo-fast`.
7. Final end-to-end run from a clean clone: `make clean && make up && make seed && make bench-all && make report`; attach the verification summary and the results table to `docs/RESULTS.md`.
