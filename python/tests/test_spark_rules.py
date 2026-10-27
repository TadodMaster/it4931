"""The shared rule vectors through the PySpark column expressions (skipped where pyspark is not installed)."""
import pytest

pyspark = pytest.importorskip("pyspark")


def test_spark_vectors(spec):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    import spark_rules as sr
    spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    try:
        for v in spec["test_vectors"]:
            cats = v["merchants"]
            rows = [(e["account"], e["time"][:10], e["time"][:7], e["type"], int(e["amount"]),
                     bool(e["self"]), cats.get(e["merchant"]) if e["merchant"] else None) for e in v["events"]]
            df = spark.createDataFrame(rows, "a string, d string, m string, t string, amt long, self boolean, mc string")
            for rule in ("v1", "v2"):
                exp = v["expect"][rule]
                delta = sr.spend_delta(rule, F.col("t"), F.col("amt"), F.col("self"))
                bal = {r["a"]: r["s"] for r in df.groupBy("a").agg(F.sum(sr.signed_amount(F.col("t"), F.col("amt"))).alias("s")).collect()}
                assert bal == exp["balance"], (v["name"], rule)
                day = {f"{r['a']}|{r['d']}": {"spend": r["s"], "tx_count": r["c"]} for r in
                       df.where(delta.isNotNull()).groupBy("a", "d").agg(F.sum(delta).alias("s"), F.count(F.lit(1)).alias("c")).collect()}
                assert day == exp["spend_day"], (v["name"], rule)
                cat = {f"{r['a']}|{r['c']}|{r['m']}": r["s"] for r in
                       df.where(delta.isNotNull()).groupBy("a", sr.spend_category(F.col("t"), F.col("mc")).alias("c"), "m")
                       .agg(F.sum(delta).alias("s")).collect()}
                assert cat == exp["spend_cat_month"], (v["name"], rule)
    finally:
        spark.stop()
