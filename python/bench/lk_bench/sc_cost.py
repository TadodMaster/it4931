"""B9 cost & complexity, B10 extrapolation to the production numbers on the slides."""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import boto3
import redis
import yaml
from botocore.config import Config

from lk_common.ch import ClickHouse
from lk_common.config import load_config

from . import dockerctl
from .common import run_dir, save_csv, save_json

REPO = Path("/repo")

GROUPS = {
    "lambda": ["redis", "spark-archiver", "spark-scheduler", "lambda-speed-submit"],
    "kappa": ["kappa-submit"],
    "shared": ["flink-jobmanager", "flink-taskmanager", "kafka", "clickhouse", "seaweedfs", "iceberg-rest", "query-api", "oracle"],
}

LOC_PATHS = {
    "lambda": {"spark archiver + loader + batch + scheduler": ["spark/archiver.py", "spark/load_master.py", "spark/batch.py", "spark/scheduler.py",
                                                              "spark/lambda_init.py", "spark/master_stats.py", "spark/lk_spark"],
               "spark rule (PySpark)": ["python/rules-python/spark_rules.py"],
               "flink speed layer (Java)": ["jvm/lambda-flink-speed/src"],
               "serving merge (query-api)": ["python/query-api/lk_api/lambda_.py"]},
    "kappa": {"flink kappa job (Java)": ["jvm/kappa-flink-job/src"]},
    "shared": {"rules-java (used by both Flink jobs)": ["jvm/rules-java/src/main"], "flink-common": ["jvm/flink-common/src"]},
}


def _loc(paths: list[str]) -> int:
    n = 0
    for p in paths:
        base = REPO / p
        files = [base] if base.is_file() else [f for f in base.rglob("*") if f.is_file() and f.suffix in (".py", ".java", ".sql")]
        for f in files:
            n += sum(1 for line in f.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip() and not line.strip().startswith(("#", "//", "*", "/*")))
    return n


def b9_cost(run_id: str, seconds: int = 60, rate: int = 200, label: str = "mixed", start_generator: bool = True, **_) -> dict:
    """Sample docker stats while a steady load runs; add storage, service counts and lines of code."""
    from .sc_ops import start_generator as start_gen
    cfg = load_config()
    if start_generator:
        start_gen(f"{run_id}-gen", rate, seconds)
        time.sleep(5)
    rows, t0 = [], time.time()
    while time.time() - t0 < seconds:
        for svc, v in dockerctl.stats().items():
            rows.append({"t_s": round(time.time() - t0, 1), "service": svc, **v})
        time.sleep(3)
    save_csv(run_id, "b9_docker_stats.csv", rows)
    agg: dict[str, dict] = {}
    for svc in {r["service"] for r in rows}:
        s = [r for r in rows if r["service"] == svc]
        agg[svc] = {"cpu_avg_pct": round(sum(x["cpu_pct"] for x in s) / len(s), 1), "cpu_peak_pct": max(x["cpu_pct"] for x in s),
                    "mem_avg_mb": round(sum(x["mem_mb"] for x in s) / len(s), 1), "mem_peak_mb": max(x["mem_mb"] for x in s)}

    def group_sum(group: str) -> dict:
        svcs = [s for s in agg if s in GROUPS[group]]
        return {"services": svcs, "cpu_avg_pct": round(sum(agg[s]["cpu_avg_pct"] for s in svcs), 1),
                "mem_peak_mb": round(sum(agg[s]["mem_peak_mb"] for s in svcs), 1)}

    # storage
    ch = ClickHouse()
    s3 = boto3.client("s3", endpoint_url=cfg["s3"]["endpoint"], region_name="us-east-1", aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["S3_SECRET_KEY"], config=Config(s3={"addressing_style": "path"}))

    def bucket_bytes(b: str) -> int:
        tot = 0
        for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=b):
            tot += sum(o["Size"] for o in pg.get("Contents", []))
        return tot

    def du(svc: str, path: str) -> int | None:
        try:
            return int(dockerctl.exec_(svc, "du", "-sk", path).split()[0]) * 1024
        except Exception:  # noqa: BLE001
            return None

    storage = {"kafka_log_bytes": du("kafka", "/var/lib/kafka/data"),
               "s3_master_bucket_bytes": bucket_bytes("master"), "s3_warehouse_bucket_bytes (iceberg master)": bucket_bytes("warehouse"),
               "s3_checkpoints_bucket_bytes": bucket_bytes("checkpoints"),
               "clickhouse_kappa_bytes": int(ch.scalar("SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND table LIKE 'kappa_v%'") or 0),
               "clickhouse_lambda_bytes": int(ch.scalar("SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND table LIKE 'lambda_%' OR table LIKE 'batch_%'") or 0),
               "redis_used_memory_bytes": int(redis.Redis(host="redis").info("memory")["used_memory"])}
    # services per profile from the compose file
    comp = yaml.safe_load((REPO / "docker-compose.yml").read_text())
    prof: dict[str, list[str]] = {}
    for name, d in comp["services"].items():
        for p in d.get("profiles", []):
            prof.setdefault(p, []).append(name)
    loc = {arch: {k: _loc(v) for k, v in groups.items()} for arch, groups in LOC_PATHS.items()}
    out = {"label": label, "load": {"rate": rate, "seconds": seconds},
           "per_service": agg, "lambda_only_services": group_sum("lambda"), "kappa_only_services": group_sum("kappa"),
           "shared_services": group_sum("shared"), "storage": storage,
           "services_per_profile": {p: len(s) for p, s in prof.items()}, "service_names_per_profile": prof,
           "loc": loc,
           "loc_totals": {"lambda": sum(loc["lambda"].values()), "kappa": sum(loc["kappa"].values()), "shared": sum(loc["shared"].values())},
           "rule_implementations_to_change_for_Y3": {"lambda": 2, "kappa": 1,
                                                     "detail": "Lambda: PySpark (batch) + Java (speed layer); Kappa: Java only"},
           "containers_for_data_path": {"lambda": ["spark-archiver", "spark-scheduler(+batch job)", "flink (speed)", "redis", "clickhouse", "kafka", "iceberg-rest", "s3"],
                                        "kappa": ["flink (one job)", "clickhouse", "kafka", "s3 (checkpoints)"]}}
    save_json(run_id, "b9_cost.json", out)
    return out


def b10_extrapolation(run_id: str, from_runs: str = "", h: float = 10e9, r_in: float = 50_000, **_) -> dict:
    """Replay time for H = 10 billion events at R_in = 50k/s: T = H / (R_process - R_in), with measured per-core rates."""
    base = Path("results")
    kappa_rate, kappa_cores, batch_rate = None, None, None
    for d in ([base / x for x in from_runs.split(",") if x] or sorted(base.iterdir())):
        f = d / "b6_rule_change.json"
        if f.exists():
            k = json.loads(f.read_text())["kappa"]
            kappa_rate, kappa_cores = k["measured_replay_rate_ev_s"], 2
            lam = json.loads(f.read_text())["lambda"]
            if lam.get("batch_rows") and lam.get("batch_s"):
                batch_rate = lam["batch_rows"] / lam["batch_s"]
    assert kappa_rate, "run b6 first (needs the measured Kappa replay rate)"
    per_core = kappa_rate / kappa_cores
    table = []
    for rp in (60_000, 75_000, 100_000, 200_000, 500_000, 1_000_000, 2_000_000):
        t = h / (rp - r_in)
        table.append({"R_process_ev_s": rp, "replay_time_hours": round(t / 3600, 2), "replay_time_days": round(t / 86400, 2),
                      "cores_needed_at_measured_per_core_rate": round(rp / per_core, 1)})
    out = {"H": h, "R_in": r_in, "measured_kappa_replay_rate_ev_s": kappa_rate, "kappa_parallelism_cores": kappa_cores,
           "measured_per_core_ev_s": round(per_core), "measured_lambda_batch_rows_per_s": None if batch_rate is None else round(batch_rate),
           "table": table,
           "lambda_full_batch_hours_at_measured_rate": None if batch_rate is None else round(h / batch_rate / 3600, 2),
           "formula": "T = H / (R_process - R_in)",
           "note": "single laptop, 2 task slots, local Kafka; extrapolation assumes linear scaling with cores and is an order-of-magnitude "
                   "illustration, not a prediction"}
    save_json(run_id, "b10_extrapolation.json", out)
    return out
