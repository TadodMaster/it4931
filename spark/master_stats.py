"""Immutability evidence for the Lambda master dataset (Iceberg): snapshot operations, row count, content checksum.

Appends one record to data/master_stats.jsonl. `bench verify --arch lambda` compares the last two records:
operations must be append-only; rows and the event-id checksum may only grow; per-day partition checksums of the
history (days fully before the previous run's max day) must be unchanged.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from lk_common.config import load_config
from lk_spark.common import MASTER_TABLE, spark_session


def main() -> int:
    cfg = load_config()
    spark = spark_session("lk-master-stats", shuffle_partitions=4, driver_mem="768m")
    t = spark.table(MASTER_TABLE)
    h = F.conv(F.substring(F.regexp_replace("event_id", "-", ""), 1, 16), 16, 10).cast("decimal(38,0)")
    agg = t.agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("event_id").alias("distinct_ids"),
                F.sum(h).alias("checksum_sum_raw")).collect()[0]
    # event-id checksum over DISTINCT ids, same definition as generator/oracle (mod 2^64)
    d = t.select("event_id").distinct().select(h.alias("h")).agg(F.sum("h")).collect()[0][0]
    per_day = {r["d"]: [int(r["n"]), int(r["s"])] for r in t.groupBy(F.date_format("event_time", "yyyy-MM-dd").alias("d")).agg(
        F.count(F.lit(1)).alias("n"), F.sum(h).cast("decimal(38,0)").alias("s")).collect()}
    snaps = spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {MASTER_TABLE}.snapshots ORDER BY committed_at").collect()
    rec = {"at": time.time(), "rows": int(agg["rows"]), "distinct_event_ids": int(agg["distinct_ids"]),
           "id_checksum": int(d or 0) % (1 << 64),
           "snapshot_operations": sorted({r["operation"] for r in snaps}), "snapshots": len(snaps),
           "per_day": {k: [v[0], str(v[1])] for k, v in per_day.items()}}
    out = Path(cfg["data"]["paths"]["root"]) / "master_stats.jsonl"
    with open(out, "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(f"[master-stats] rows={rec['rows']:,} distinct={rec['distinct_event_ids']:,} ops={rec['snapshot_operations']} "
          f"snapshots={rec['snapshots']} -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
