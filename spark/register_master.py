"""Recovery tool: re-register the master table in an (empty) Iceberg catalog from its newest metadata file on S3.

Needed only if the catalog database is lost (the catalog is now persisted in a volume). Data and metadata files on S3
are never touched; this only recreates the catalog pointer.
Usage: spark-submit register_master.py [s3://warehouse/master/transactions/metadata/NNNNN-uuid.metadata.json]
"""
from __future__ import annotations

import sys

from lk_common.config import load_config
from lk_spark.common import MASTER_TABLE, spark_session


def main() -> int:
    cfg = load_config()
    if len(sys.argv) < 2:
        sys.exit('usage: register_master.py s3://warehouse/master/transactions/metadata/NNNNN-uuid.metadata.json '
                 '(newest = highest NNNNN; list with: bench run catalog-metadata)')
    path = sys.argv[1]
    spark = spark_session("lk-register-master", shuffle_partitions=2, driver_mem="768m")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lk.master")
    spark.sql(f"CALL lk.system.register_table(table => '{MASTER_TABLE[3:]}', metadata_file => '{path}')")
    n = spark.table(MASTER_TABLE).count()
    print(f"[register-master] registered {MASTER_TABLE} from {path}: {n:,} rows", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
