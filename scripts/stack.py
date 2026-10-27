"""Stack lifecycle helper (host side, cross-platform): `python scripts/stack.py wipe | start | status`.

wipe   : stop jobs, delete Kafka topics, live truth, results, ClickHouse tables, Redis, Iceberg/S3 data, Spark checkpoints.
         (the generated backfill Parquet in data/stream + data/truth/backfill is kept: it is the shared input of both architectures)
start  : bring up core+kappa+lambda, load the SAME Parquet into Kafka (Kappa) and Iceberg (Lambda master), start every job.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ["--profile", "core", "--profile", "kappa", "--profile", "lambda", "--profile", "lambda-tools", "--profile", "lambda-load"]
PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", ROOT.name.lower().replace("-", "").replace("_", ""))


def sh(*args: str, check: bool = True, env: dict | None = None, quiet: bool = False) -> str:
    r = subprocess.run(list(args), cwd=ROOT, capture_output=True, text=True, env={**os.environ, **(env or {})})
    if check and r.returncode != 0:
        sys.exit(f"FAILED: {' '.join(args)}\n{r.stdout[-800:]}\n{r.stderr[-800:]}")
    if not quiet:
        print("$", " ".join(args), "->", (r.stdout.strip().splitlines() or [""])[-1][:120], flush=True)
    return r.stdout


def dc(*args: str, **kw) -> str:
    return sh("docker", "compose", *PROFILES, *args, **kw)


def gen(*args: str) -> str:
    return dc("exec", "-T", "generator", *args)


def wait_for(fn, what: str, timeout: float = 600, every: float = 5):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = fn()
            if v:
                return v
        except Exception:  # noqa: BLE001
            pass
        time.sleep(every)
    sys.exit(f"timeout waiting for {what}")


def wipe(regen: bool = False) -> None:
    print("== wipe", flush=True)
    dc("stop", "spark-archiver", "spark-scheduler", "flink-taskmanager", "flink-jobmanager", check=False)
    dc("up", "-d", "--wait", "kafka", "kafka-init", "clickhouse", "seaweedfs", "generator", check=False)
    for t in ("transactions", "late-events", "merchants"):
        dc("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--delete", "--topic", t, check=False)
    if regen:   # profile/size changed: the Parquet input must be generated again
        gen("sh", "-c", "rm -rf /data/stream /data/truth /data/backfill_manifest.json")
    gen("sh", "-c", "rm -rf /data/truth/live /data/clock.json /data/master_*.json* /data/kafka_load_manifest.json /app/python/results/*")
    ch = lambda q: sh("docker", "exec", f"{PROJECT}-clickhouse-1", "clickhouse-client", "-q", q, quiet=True)  # noqa: E731
    for name in ch("SELECT name FROM system.tables WHERE database='lk' AND (name LIKE 'kappa_%' OR name LIKE 'lambda_%' OR name LIKE 'batch_%' OR name LIKE 'trace_%')").split():
        ch(f"DROP {'VIEW' if 'current_' in name and name.startswith('kappa') else 'TABLE'} IF EXISTS lk.{name} SYNC")
    dc("exec", "-T", "redis", "redis-cli", "flushall", check=False)
    # Iceberg catalog + S3 data (checkpoints, warehouse)
    dc("stop", "iceberg-rest", check=False)
    sh("docker", "run", "--rm", "-v", f"{PROJECT}_iceberg-data:/c", "-v", f"{PROJECT}_spark-ckpt:/k", "python:3.11.13-slim-bookworm",
       "sh", "-c", "rm -rf /c/* /k/*")
    import boto3
    from botocore.config import Config
    env = dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines() if "=" in line and not line.startswith("#"))
    s3 = boto3.client("s3", endpoint_url="http://localhost:9000", region_name="us-east-1", aws_access_key_id=env["S3_ACCESS_KEY"],
                      aws_secret_access_key=env["S3_SECRET_KEY"], config=Config(s3={"addressing_style": "path"}))
    for b in ("warehouse", "checkpoints", "master", "results"):
        for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=b):
            objs = [{"Key": o["Key"]} for o in pg.get("Contents", [])]
            for i in range(0, len(objs), 500):
                s3.delete_objects(Bucket=b, Delete={"Objects": objs[i:i + 500]})
    print("== wiped", flush=True)


def start(profile_fast: bool = True) -> None:
    print("== start", flush=True)
    dc("up", "-d", "--build", "--wait", "kafka", "kafka-init", "seaweedfs", "bucket-init", "clickhouse", "iceberg-rest", "oracle",
       "query-api", "generator", "bench", "redis")
    dc("up", "kafka-init", check=False)
    man = ROOT / "data" / "backfill_manifest.json"
    if not man.exists():
        gen("python", "-m", "lk_generator", "merchants")
        gen("python", "-m", "lk_generator", "backfill", "--run-id", "seed")
    gen("python", "-m", "lk_generator", "load-kafka")                       # Kappa source
    dc("run", "--rm", "spark-load-master")                                  # Lambda master, from the SAME Parquet files
    dc("up", "-d", "--wait", "flink-jobmanager", "flink-taskmanager")
    dc("up", "lambda-init")
    dc("up", "lambda-speed-submit", env={"LAMBDA_START_FROM": "latest"})
    dc("up", "-d", "spark-archiver")
    dc("up", "kappa-submit", env={"KAPPA_START_FROM": "earliest", "KAPPA_PARALLELISM": "2"})
    dc("exec", "-T", "bench", "python", "-m", "lk_bench", "kappa-views", "--to", "v1")
    print("== waiting for Kappa replay ...", flush=True)

    def kappa_caught_up() -> bool:
        out = dc("exec", "-T", "bench", "python", "-c",
                 "from lk_common.kafka_lag import consumer_lag as c;print(c('kafka:9092','kappa-v1','transactions')['total'])", quiet=True)
        return out.strip() == "0"

    wait_for(kappa_caught_up, "kappa lag 0", 900, 10)
    dc("up", "-d", "spark-scheduler")                                       # first nightly batch starts here
    print("== stack is up (Kappa caught up, Lambda batch scheduled)", flush=True)


def status() -> None:
    print(sh("docker", "compose", *PROFILES, "ps", "--format", "{{.Name}} {{.Status}}", quiet=True))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["wipe", "start", "status", "reset"])
    ap.add_argument("--regen", action="store_true", help="also delete and regenerate the backfill Parquet")
    a = ap.parse_args()
    if a.cmd == "wipe":
        wipe(a.regen)
    elif a.cmd == "start":
        start()
    elif a.cmd == "reset":
        wipe(a.regen)
        start()
    else:
        status()
