"""Load the backfill stream Parquet (the very same files Kafka was loaded from) into the immutable Iceberg master.

Append-only. Safe to re-run only on an empty table: it refuses otherwise (immutability: no overwrite, no delete).
Writes data/master_load_manifest.json with the Kafka offsets the archiver must start from.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from lk_common.config import load_config
from lk_spark.common import MASTER_TABLE, create_master_table, spark_session


def main() -> int:
    cfg = load_config()
    paths = cfg["data"]["paths"]
    spark = spark_session("lk-load-master", shuffle_partitions=4, driver_mem="1536m")
    create_master_table(spark)
    if spark.table(MASTER_TABLE).limit(1).count() > 0:
        print("[load-master] master is not empty; refusing to load again (append-only dataset)")
        return 1
    t0 = time.time()
    df = (spark.read.parquet(paths["stream_backfill"])
          .withColumn("kafka_partition", F.lit(None).cast("int")).withColumn("kafka_offset", F.lit(None).cast("bigint"))
          .withColumn("ingested_at", F.current_timestamp())
          .select("schema_version", "event_id", "account_id", "event_time", "produced_at", "channel", "type", "amount",
                  "merchant_id", "is_self_transfer", "pos_offline", "kafka_partition", "kafka_offset", "ingested_at")
          .sortWithinPartitions("event_time"))
    df.writeTo(MASTER_TABLE).append()
    n = spark.table(MASTER_TABLE).count()
    km = json.loads((Path(paths["root"]) / "kafka_load_manifest.json").read_text())
    start = {str(p): int(c) for p, c in km["delivered"]["by_partition"].items()}
    out = {"rows": n, "seconds": round(time.time() - t0, 1), "archiver_start_offsets": start,
           "snapshot_id": spark.sql(f"SELECT snapshot_id FROM {MASTER_TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1")
           .collect()[0][0]}
    (Path(paths["root"]) / "master_load_manifest.json").write_text(json.dumps(out, indent=2))
    print(f"[load-master] {n:,} rows in {out['seconds']}s; archiver starts at {start}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
