"""Rule implementation #1 (Python/PySpark side). Pure functions; the Spark batch job wraps them."""
from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping

POSITIVE = {"TOPUP", "REFUND", "TRANSFER_IN", "CASHBACK"}
NEGATIVE = {"PAYMENT", "TRANSFER_OUT"}
MERCHANT_TYPES = {"PAYMENT", "REFUND"}
TRANSFER_CATEGORY = "TRANSFER"
RULES = ("v1", "v2")


def signed_amount(type_: str, amount: int) -> int:
    if type_ in POSITIVE:
        return amount
    if type_ in NEGATIVE:
        return -amount
    raise ValueError(f"unknown type {type_}")


def spend_delta(rule: str, type_: str, amount: int, is_self_transfer: bool) -> int | None:
    """Contribution to spend, or None when the rule does not touch this event."""
    if rule == "v1":
        return amount if type_ in ("PAYMENT", "TRANSFER_OUT") else None
    if rule == "v2":
        if type_ == "PAYMENT":
            return amount
        if type_ == "REFUND":
            return -amount
        if type_ == "TRANSFER_OUT" and not is_self_transfer:
            return amount
        return None
    raise ValueError(f"unknown rule {rule}")


def spend_category(type_: str, merchant_category: str | None) -> str | None:
    if type_ in MERCHANT_TYPES:
        return merchant_category
    if type_ == "TRANSFER_OUT":
        return TRANSFER_CATEGORY
    return None


def compute_metrics(rule: str, events: Iterable[Mapping], merchants: Mapping[str, str]) -> dict:
    """events: dicts with account,time(ISO),type,amount,merchant,self. Dedup is the caller's job."""
    balance: dict = defaultdict(int)
    day: dict = defaultdict(lambda: {"spend": 0, "tx_count": 0})
    cat: dict = defaultdict(int)
    for e in events:
        balance[e["account"]] += signed_amount(e["type"], e["amount"])
        d = spend_delta(rule, e["type"], e["amount"], bool(e["self"]))
        if d is None:
            continue
        a, t = e["account"], e["time"]
        day[f"{a}|{t[:10]}"]["spend"] += d
        day[f"{a}|{t[:10]}"]["tx_count"] += 1
        c = spend_category(e["type"], merchants.get(e["merchant"]) if e["merchant"] else None)
        cat[f"{a}|{c}|{t[:7]}"] += d
    return {"balance": dict(balance), "spend_day": {k: dict(v) for k, v in day.items()},
            "spend_cat_month": dict(cat)}
