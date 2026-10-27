# Idempotent targets. Use GNU make from Git Bash / WSL. Without make, every target is one command shown in docs/RUNBOOK.md.
SHELL   := /bin/bash
PROFILES := --profile core --profile kappa --profile lambda --profile lambda-tools --profile lambda-load
DC      := docker compose $(PROFILES)
BENCH   := $(DC) exec -T bench python -m lk_bench
GEN     := $(DC) exec -T generator python -m lk_generator
PY      ?= $(if $(wildcard .venv/Scripts/python),.venv/Scripts/python,python3)
RUN_ID  ?= final
SCENARIOS := b1 b2 b3 b4 b5 b6 b7 b8 b9 b10 atomic determinism batch master-stats e2e

.PHONY: env up up-lambda up-kappa up-observability reset start status seed seed-files seed-kafka seed-master live test test-java test-spark \
        test-e2e bench-all report demo demo-fast verify down clean $(addprefix bench-,$(SCENARIOS))

env:
	@test -f .env || (cp .env.example .env && echo "[env] created .env from .env.example")

# ---- bring-up ----------------------------------------------------------------------------------------------------
up: env                         ## core only (Kafka, S3, Iceberg REST, ClickHouse, oracle, query-api, generator, bench)
	docker compose --profile core up -d --build --wait

up-kappa: up                    ## + Flink cluster and the Kappa job (replays Kafka from offset 0)
	docker compose --profile core --profile kappa up -d --build --wait flink-jobmanager flink-taskmanager
	KAPPA_START_FROM=earliest KAPPA_PARALLELISM=2 docker compose --profile core --profile kappa up kappa-submit
	$(BENCH) kappa-views --to v1

up-lambda: up                   ## + Redis, Spark archiver/scheduler, Flink speed layer (master must be loaded: make seed-master)
	docker compose --profile core --profile lambda up -d --build --wait redis flink-jobmanager flink-taskmanager
	docker compose --profile core --profile lambda up lambda-init
	LAMBDA_START_FROM=latest docker compose --profile core --profile lambda up lambda-speed-submit
	docker compose --profile core --profile lambda up -d spark-archiver spark-scheduler

up-observability:               ## Prometheus :9090 and Grafana :3000 (dashboard "Lambda vs Kappa")
	docker compose --profile core --profile observability up -d --build

reset: env                      ## wipe all state and start everything again from the shared Parquet (fast profile unless .env says otherwise)
	$(PY) scripts/stack.py reset

start: env                      ## start everything (assumes a wiped or fresh state)
	$(PY) scripts/stack.py start

status:
	$(PY) scripts/stack.py status

# ---- data ----------------------------------------------------------------------------------------------------------
seed-files: up                  ## generate merchants + backfill Parquet (stream with duplicates/late + truth log)
	$(GEN) merchants
	$(GEN) backfill --run-id seed

seed-kafka: up                  ## Parquet -> Kafka (Kappa source of truth)
	$(GEN) load-kafka

seed-master: up                 ## the SAME Parquet -> immutable Iceberg master (Lambda)
	$(DC) run --rm spark-load-master

seed: seed-files seed-kafka seed-master

live: up                        ## live traffic (RATE tx/s, default from config)
	$(GEN) live --run-id $${RUN_ID:-live}

# ---- tests ---------------------------------------------------------------------------------------------------------
test: test-java test-spark      ## Python unit tests + Java rule vectors (docker build) + PySpark rule vectors
	$(PY) -m pytest -c python/pytest.ini -q

test-java:                      ## builds both Flink jobs; maven runs the shared rule vectors in Java
	docker build -f infra/docker/flink-job.Dockerfile -t lk-flink-job:dev .

test-spark:
	docker compose --profile core --profile lambda-tools run --rm --no-deps -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/app/python/common:/app/python/rules-python:/app/python/tests --entrypoint python3 spark-master-stats -m pytest -q -c /app/python/pytest.ini /app/python/tests/test_spark_rules.py -p no:cacheprovider

test-e2e:                       ## 2-3 minute scenario on the running stack, then bench verify for both architectures
	$(BENCH) run e2e --run-id e2e

verify:                         ## exact comparison of both architectures with the oracle (needs a quiet system; see RUNBOOK)
	$(BENCH) run batch --run-id $(RUN_ID)
	$(BENCH) run master-stats --run-id $(RUN_ID)
	$(BENCH) verify --arch kappa --rule v1 --run-id $(RUN_ID)
	$(BENCH) verify --arch lambda --run-id $(RUN_ID)

# ---- benchmarks ----------------------------------------------------------------------------------------------------
bench-%:
	$(BENCH) run $* --run-id $(RUN_ID)

bench-all:                      ## the whole benchmark (about 1 hour on the fast profile): results/$(RUN_ID)/
	bash scripts/bench_all.sh $(RUN_ID)

report:                         ## results/$(RUN_ID)/report.html, slides/*.png (1920x1080), docs/RESULTS.md
	$(BENCH) report --run-id $(RUN_ID)

# ---- demo ----------------------------------------------------------------------------------------------------------
demo: up-observability          ## wallet UI http://localhost:8080/ui · Grafana http://localhost:3000 · Flink http://localhost:8081
	@echo "stack: make reset (or start) first; then open http://localhost:8080/ui and http://localhost:3000"
	$(GEN) live --run-id demo-live --duration 3600 --reset-clock

demo-fast:                      ## everything small (LK_PROFILE=fast): wipe, seed, start, 2-minute e2e proof
	@grep -q '^LK_PROFILE=fast' .env || echo 'LK_PROFILE=fast' >> .env
	$(MAKE) reset
	$(MAKE) test-e2e

down:
	$(DC) --profile observability down

clean:
	$(DC) --profile observability down -v
	rm -rf data results/* run
