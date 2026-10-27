"""PySpark column-expression form of the spending rule (rule implementation #1b, Spark batch layer).

Mirrors spend_rules_py.py / shared/rules/spend_rules.yaml; test_spark_rules.py runs the shared vectors through it.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

POSITIVE = ["TOPUP", "REFUND", "TRANSFER_IN", "CASHBACK"]
NEGATIVE = ["PAYMENT", "TRANSFER_OUT"]


def signed_amount(type_: Column, amount: Column) -> Column:
    return F.when(type_.isin(POSITIVE), amount).when(type_.isin(NEGATIVE), -amount)


def spend_delta(rule: str, type_: Column, amount: Column, is_self: Column) -> Column:
    """NULL when the rule does not touch the event."""
    if rule == "v1":
        return F.when(type_.isin("PAYMENT", "TRANSFER_OUT"), amount)
    if rule == "v2":
        return (F.when(type_ == "PAYMENT", amount).when(type_ == "REFUND", -amount)
                .when((type_ == "TRANSFER_OUT") & (~is_self), amount))
    raise ValueError(f"unknown rule {rule}")


def spend_category(type_: Column, merchant_category: Column) -> Column:
    return (F.when(type_.isin("PAYMENT", "REFUND"), merchant_category)
            .when(type_ == "TRANSFER_OUT", F.lit("TRANSFER")))
