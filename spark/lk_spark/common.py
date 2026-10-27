"""Shared Spark helpers: session with the Iceberg REST catalog on the S3 store, ClickHouse HTTP writer, merchants."""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession

from lk_common.config import load_config
from lk_spark.ch import ClickHouse  # noqa: F401  (re-exported)

MASTER_TABLE = "lk.master.transactions"


def spark_session(app: str, shuffle_partitions: int = 8, driver_mem: str | None = None) -> SparkSession:
    cfg = load_config()
    b = (SparkSession.builder.appName(app).master(os.environ.get("SPARK_MASTER", "local[4]"))
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.lk", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.lk.type", "rest")
         .config("spark.sql.catalog.lk.uri", os.environ.get("ICEBERG_URI", "http://iceberg-rest:8181"))
         .config("spark.sql.catalog.lk.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config("spark.sql.catalog.lk.warehouse", "s3://warehouse/")
         .config("spark.sql.catalog.lk.s3.endpoint", cfg["s3"]["endpoint"])
         .config("spark.sql.catalog.lk.s3.path-style-access", "true")
         .config("spark.sql.catalog.lk.s3.access-key-id", os.environ["S3_ACCESS_KEY"])
         .config("spark.sql.catalog.lk.s3.secret-access-key", os.environ["S3_SECRET_KEY"])
         .config("spark.sql.catalog.lk.client.region", cfg["s3"]["region"])
         # Hadoop S3A for Structured Streaming checkpoints (s3a://checkpoints/...)
         .config("spark.hadoop.fs.s3a.endpoint", cfg["s3"]["endpoint"])
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
         .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"]))
    if driver_mem:
        b = b.config("spark.driver.memory", driver_mem)
    return b.getOrCreate()


def create_master_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lk.master")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {MASTER_TABLE} (
        schema_version int, event_id string, account_id string, event_time timestamp, produced_at timestamp,
        channel string, type string, amount bigint, merchant_id string, is_self_transfer boolean, pos_offline boolean,
        kafka_partition int, kafka_offset bigint, ingested_at timestamp)
        USING iceberg PARTITIONED BY (days(event_time))
        TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='67108864',
                       'write.distribution-mode'='none')""")
    spark.sql(f"""ALTER TABLE {MASTER_TABLE} SET TBLPROPERTIES ('write.distribution-mode'='none',
        'write.parquet.row-group-size-bytes'='8388608', 'write.parquet.page-size-bytes'='262144',
        'write.parquet.compression-codec'='snappy')""")


def insert_partition(table: str, base: str, rows) -> None:
    """foreachPartition body: batches of 20k JSON lines."""
    ch = ClickHouse(base)
    buf: list[str] = []
    for r in rows:
        buf.append(r)
        if len(buf) >= 20_000:
            ch.insert_json_rows(table, buf)
            buf = []
    if buf:
        ch.insert_json_rows(table, buf)


def load_merchants_df(spark: SparkSession):
    path = load_config()["data"]["paths"]["merchants_csv"]
    return spark.read.option("header", True).csv(path).select("merchant_id", "category")


def jdump(d: dict) -> str:
    return json.dumps(d, separators=(",", ":"))
