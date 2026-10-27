"""Lambda raw archiver: Kafka `transactions` -> Iceberg master.transactions (append-only, exactly-once via checkpoint).

Starts from the offsets recorded by load_master.py (the backfill is loaded from the Parquet files, not through Kafka).
The master dataset is immutable: this job only ever appends.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType, StructField, StructType)

from lk_common.config import load_config
from lk_spark.common import MASTER_TABLE, create_master_table, spark_session

SCHEMA = StructType([
    StructField("schema_version", IntegerType()), StructField("event_id", StringType()),
    StructField("account_id", StringType()), StructField("event_time", StringType()),
    StructField("produced_at", StringType()), StructField("channel", StringType()), StructField("type", StringType()),
    StructField("amount", LongType()), StructField("merchant_id", StringType()),
    StructField("is_self_transfer", BooleanType()), StructField("pos_offline", BooleanType()),
])


def main() -> int:
    cfg = load_config()
    root = Path(cfg["data"]["paths"]["root"])
    spark = spark_session("lk-archiver", shuffle_partitions=2, driver_mem="768m")
    create_master_table(spark)
    mf = root / "master_load_manifest.json"
    topic = cfg["kafka"]["topic_transactions"]
    if mf.exists():
        starting = json.dumps({topic: json.loads(mf.read_text())["archiver_start_offsets"]})
    else:
        print("[archiver] no master_load_manifest.json: reading Kafka from earliest")
        starting = "earliest"
    ckpt = os.environ.get("ARCHIVER_CHECKPOINT", "s3a://checkpoints/spark-archiver")
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap"])
           .option("subscribe", topic).option("startingOffsets", starting).option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", 500_000).load())
    ev = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"),
                     F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"))
          .select("e.*", "kafka_partition", "kafka_offset")
          .withColumn("event_time", F.to_timestamp("event_time"))
          .withColumn("produced_at", F.to_timestamp("produced_at"))
          .withColumn("ingested_at", F.current_timestamp())
          .select("schema_version", "event_id", "account_id", "event_time", "produced_at", "channel", "type", "amount",
                  "merchant_id", "is_self_transfer", "pos_offline", "kafka_partition", "kafka_offset", "ingested_at"))
    q = (ev.writeStream.format("iceberg").outputMode("append").trigger(processingTime="10 seconds")
         .option("checkpointLocation", ckpt).option("fanout-enabled", "true").toTable(MASTER_TABLE))
    print(f"[archiver] running; start={starting if len(starting) < 80 else 'offsets'}; checkpoint={ckpt}", flush=True)
    q.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
