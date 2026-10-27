"""Lambda batch layer: recompute ALL history < t_k from the immutable master, then swap the serving view atomically.

  1. pin the master snapshot, read events with event_time < t_k, dedup by event_id
  2. apply the rule (Spark implementation), join the merchant dimension
  3. load a NEW versioned ClickHouse table (never touch the live one)
  4. EXCHANGE TABLES (one atomic statement) -> readers see old or new, never a mix; keep the previous version for rollback
  5. record the run, publish t_k to Redis for the speed layer, expire speed-layer day buckets older than t_k
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import redis
from pyspark.sql import functions as F

from lk_common.config import load_config
from lk_spark.common import ClickHouse, MASTER_TABLE, insert_partition, load_merchants_df, spark_session
from spark_rules import signed_amount, spend_category, spend_delta

BATCH_DDL = ("(kind LowCardinality(String), account_id String, k1 String, k2 String, spend Int64, tx_count UInt64) "
             "ENGINE = MergeTree ORDER BY (kind, account_id, k1, k2)")
TRACE_DDL = ("(event_id String, account_id String, day Date, event_time DateTime64(3,'UTC'), type LowCardinality(String), "
             "amount Int64, merchant_id Nullable(String), is_self_transfer Bool, pos_offline Bool, "
             "spend_contribution Nullable(Int64)) ENGINE = MergeTree ORDER BY (account_id, day, event_id)")


def ensure_serving_tables(ch: ClickHouse) -> None:
    ch.execute("CREATE DATABASE IF NOT EXISTS lk")
    ch.execute(f"CREATE TABLE IF NOT EXISTS lk.lambda_batch_cur {BATCH_DDL}")
    ch.execute(f"CREATE TABLE IF NOT EXISTS lk.lambda_trace_cur {TRACE_DDL}")
    ch.execute("CREATE TABLE IF NOT EXISTS lk.lambda_batch_runs (run_id String, t_k DateTime64(3,'UTC'), "
               "rule_version LowCardinality(String), snapshot_id String, view_version String, "
               "started_at DateTime64(3,'UTC'), duration_ms UInt64, input_rows UInt64, status LowCardinality(String)) "
               "ENGINE = MergeTree ORDER BY started_at")


def swap(ch: ClickHouse, live: str, new: str, keep: int = 2) -> None:
    """Atomic exchange; the old content lands in `<live>_prev_<id>` (rollback = exchange back)."""
    ch.execute(f"EXCHANGE TABLES lk.{live}_cur AND lk.{new}")
    rid = new.split("_", 1)[1]
    ch.execute(f"RENAME TABLE lk.{new} TO lk.{live}_prev_{rid}")
    prev = [r for r in ch.execute(f"SELECT name FROM system.tables WHERE database='lk' AND name LIKE '{live}_prev_%' "
                                  f"ORDER BY name DESC").split() if r]
    for old in prev[keep:]:
        ch.execute(f"DROP TABLE IF EXISTS lk.{old} SYNC")


def run(t_k: datetime, rule: str, run_id: str, trace: bool = True, purge_current_day: bool = False) -> dict:
    cfg = load_config()
    base = f"http://{cfg['clickhouse']['host']}:{cfg['clickhouse']['http_port']}"
    ch = ClickHouse(base)
    ensure_serving_tables(ch)
    started = time.time()
    spark = spark_session("lk-batch", shuffle_partitions=8, driver_mem="1g")
    snap = spark.sql(f"SELECT snapshot_id FROM {MASTER_TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
    tk_lit = F.lit(t_k.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    ev = (spark.read.format("iceberg").option("snapshot-id", str(snap)).load(MASTER_TABLE)
          .where(F.col("event_time") < tk_lit).dropDuplicates(["event_id"])
          .join(F.broadcast(load_merchants_df(spark)), "merchant_id", "left").cache())
    input_rows = ev.count()
    t, a, s = F.col("type"), F.col("amount"), F.col("is_self_transfer")
    delta = spend_delta(rule, t, a, s)
    month = F.date_format("event_time", "yyyy-MM")
    day = F.date_format("event_time", "yyyy-MM-dd")

    bal = ev.groupBy("account_id").agg(F.sum(signed_amount(t, a)).alias("spend")) \
        .select(F.lit("balance").alias("kind"), "account_id", F.lit("").alias("k1"), F.lit("").alias("k2"), "spend",
                F.lit(0).cast("long").alias("tx_count"))
    dd = ev.where(delta.isNotNull()).groupBy("account_id", day.alias("d")).agg(
        F.sum(delta).alias("spend"), F.count(F.lit(1)).alias("tx_count")) \
        .select(F.lit("day").alias("kind"), "account_id", F.col("d").alias("k1"), F.lit("").alias("k2"), "spend", "tx_count")
    cm = ev.where(delta.isNotNull()).groupBy("account_id", spend_category(t, F.col("category")).alias("c"), month.alias("m")) \
        .agg(F.sum(delta).alias("spend")) \
        .select(F.lit("cat_month").alias("kind"), "account_id", F.col("c").alias("k1"), F.col("m").alias("k2"), "spend",
                F.lit(0).cast("long").alias("tx_count"))
    tmp = f"batch_{run_id}"
    ch.execute(f"CREATE TABLE lk.{tmp} {BATCH_DDL}")
    rows = bal.unionByName(dd).unionByName(cm).select(
        F.to_json(F.struct("kind", "account_id", "k1", "k2", "spend", "tx_count")).alias("j")).rdd.map(lambda r: r.j)
    rows.foreachPartition(lambda it: insert_partition(f"lk.{tmp}", base, it))
    meta = {"kind": "meta", "account_id": "", "k1": t_k.strftime("%Y-%m-%dT%H:%M:%S.000Z"), "k2": rule,
            "spend": 0, "tx_count": input_rows}
    ch.insert_json_rows(f"lk.{tmp}", [json.dumps(meta)])
    if trace:
        ttmp = f"trace_{run_id}"
        ch.execute(f"CREATE TABLE lk.{ttmp} {TRACE_DDL}")
        tr = ev.select(F.to_json(F.struct(
            "event_id", "account_id", day.alias("day"), F.date_format("event_time", "yyyy-MM-dd HH:mm:ss.SSS").alias("event_time"),
            "type", "amount", "merchant_id", "is_self_transfer", "pos_offline", delta.alias("spend_contribution"))).alias("j")) \
            .rdd.map(lambda r: r.j)
        tr.foreachPartition(lambda it: insert_partition(f"lk.{ttmp}", base, it))
        swap(ch, "lambda_trace", ttmp)
    swap(ch, "lambda_batch", tmp)          # <- the atomic switch
    dur = int((time.time() - started) * 1000)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000")
    ch.insert_json_rows("lk.lambda_batch_runs", [json.dumps({
        "run_id": run_id, "t_k": t_k.strftime("%Y-%m-%d %H:%M:%S.000"), "rule_version": rule, "snapshot_id": str(snap),
        "view_version": tmp, "started_at": datetime.fromtimestamp(started, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000"),
        "duration_ms": dur, "input_rows": input_rows, "status": "ok"})])
    publish_tk(cfg, t_k, purge_current_day)
    spark.stop()
    out = {"run_id": run_id, "t_k": t_k.isoformat(), "rule": rule, "snapshot_id": snap, "input_rows": input_rows,
           "duration_ms": dur, "finished_at": now}
    print("[batch] done " + json.dumps(out), flush=True)
    return out


def publish_tk(cfg: dict, t_k: datetime, purge_current_day: bool = False) -> None:
    """Tell the speed layer the new boundary, then drop its day buckets that the batch now covers."""
    import os
    r = redis.Redis(host=os.environ.get("REDIS_HOST", "redis"), decode_responses=True)
    r.set("lambda:tk", int(t_k.timestamp() * 1000))
    cutoff = t_k.strftime("%Y-%m-%d")
    for key in r.scan_iter("rt:*:*"):
        day = key.split(":")[2]
        # nightly batches (t_k = day boundary) keep the t_k day; manual intra-day batches (quiescent system) purge it too
        if day < cutoff or (purge_current_day and day == cutoff):
            r.delete(key)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--t-k", required=True, help="ISO UTC boundary, exclusive")
    ap.add_argument("--rule", default="v1")
    ap.add_argument("--run-id")
    ap.add_argument("--no-trace", action="store_true")
    ap.add_argument("--purge-current-day", action="store_true", help="manual batch at t_k inside a day: also drop that day's speed-layer buckets")
    a = ap.parse_args()
    tk = datetime.fromisoformat(a.t_k.replace("Z", "+00:00")).astimezone(timezone.utc)
    run(tk, a.rule, a.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"), trace=not a.no_trace, purge_current_day=a.purge_current_day)
    return 0


if __name__ == "__main__":
    sys.exit(main())
