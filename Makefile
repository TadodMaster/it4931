# Idempotent targets. On Windows run from Git Bash / WSL (GNU make), or call the docker/pytest commands directly.
SHELL := /bin/bash
DC      := docker compose
GEN     := $(DC) exec -T generator python -m lk_generator
PY      ?= $(if $(wildcard .venv/Scripts/python),.venv/Scripts/python,python3)

.PHONY: up up-lambda up-kappa env seed seed-files seed-kafka live test test-e2e bench-all report demo down clean

env:
	@test -f .env || (cp .env.example .env && echo "[env] created .env from .env.example")

up: env
	$(DC) --profile core up -d --build --wait
	@echo "[up] core is healthy: kafka:29092 s3:9000 iceberg:8181 clickhouse:8123 oracle:8090"

up-lambda: up
	$(DC) --profile core --profile lambda up -d --build --wait

up-kappa: up
	$(DC) --profile core --profile kappa up -d --build --wait

# Backfill: generate Parquet once (truth + arrival-ordered stream), then load the SAME files into Kafka.
# (Lambda master is loaded from the same Parquet in phase 3.)
seed-files: up
	$(GEN) merchants
	$(GEN) backfill --run-id $${RUN_ID:-local}

seed-kafka: up
	$(GEN) load-kafka

seed: seed-files seed-kafka

# Small dataset for rehearsals: LK_PROFILE=fast in .env (see shared/config/profiles/fast.yaml), then make seed.
seed-fast:
	@grep -q '^LK_PROFILE=fast' .env || echo 'LK_PROFILE=fast' >> .env
	$(DC) up -d --force-recreate generator oracle query-api bench
	$(MAKE) seed

live: up
	$(GEN) live --run-id $${RUN_ID:-local}

test:
	$(PY) -m pytest -c python/pytest.ini -q

test-e2e:
	@echo "phase 7: 2-minute scenario + bench verify (not implemented yet)"; exit 1

bench-all report demo:
	@echo "$@: implemented in a later phase"; exit 1

down:
	$(DC) --profile core --profile lambda --profile kappa --profile observability --profile demo-ui down

clean:
	$(DC) --profile core --profile lambda --profile kappa --profile observability --profile demo-ui down -v
	rm -rf data run
