"""Lambda serving layer: batch view (ClickHouse, events < t_k) + speed layer (Redis day buckets, events >= t_k).

merge(A, day) = batch(<= t_k) + realtime(> t_k), with the t_k that belongs to the batch table read in the SAME query.
"""
from __future__ import annotations

import json
import os

import redis

from lk_common.ch import ClickHouse

_r: redis.Redis | None = None


def rds() -> redis.Redis:
    global _r
    if _r is None:
        _r = redis.Redis(host=os.environ.get("REDIS_HOST", "redis"), decode_responses=True, socket_timeout=3)
    return _r


def rt_days() -> list[str]:
    """Days that currently have speed-layer buckets (a handful: everything since t_k)."""
    return sorted({k.split(":")[2] for k in rds().scan_iter("rt:bal:*")})


def batch_account(ch: ClickHouse, account: str) -> tuple[dict | None, list[dict]]:
    """One query -> meta row and the account's rows come from the same table version (atomic swap safe)."""
    rows = ch.query("SELECT kind, k1, k2, spend, tx_count FROM lk.lambda_batch_cur WHERE account_id = {a:String} OR kind = 'meta'",
                    a=account)
    meta = next((r for r in rows if r["kind"] == "meta"), None)
    return meta, [r for r in rows if r["kind"] != "meta"]


def meta_info(meta: dict | None) -> dict:
    if not meta:
        return {"t_k": None, "rule": None, "batch_input_rows": 0}
    return {"t_k": meta["k1"], "rule": meta["k2"], "batch_input_rows": int(meta["tx_count"])}


def balance(ch: ClickHouse, account: str) -> tuple[int, dict]:
    meta, rows = batch_account(ch, account)
    b = sum(int(r["spend"]) for r in rows if r["kind"] == "balance")
    rt = 0
    for d in rt_days():
        v = rds().hget(f"rt:bal:{d}", account)
        rt += int(v) if v else 0
    return b + rt, {**meta_info(meta), "batch_part": b, "realtime_part": rt}


def spend(ch: ClickHouse, account: str, day: str) -> tuple[dict, dict]:
    meta, rows = batch_account(ch, account)
    b = next((r for r in rows if r["kind"] == "day" and r["k1"] == day), None)
    s, c = (int(b["spend"]), int(b["tx_count"])) if b else (0, 0)
    rs = rds().hget(f"rt:spend:{day}", account)
    rc = rds().hget(f"rt:cnt:{day}", account)
    return ({"spend": s + int(rs or 0), "tx_count": c + int(rc or 0)},
            {**meta_info(meta), "batch_part": s, "realtime_part": int(rs or 0)})


def report(ch: ClickHouse, account: str, frm_month: str, to_month: str) -> tuple[list[dict], dict]:
    meta, rows = batch_account(ch, account)
    agg: dict[tuple[str, str], int] = {}
    for r in rows:
        if r["kind"] == "cat_month" and frm_month <= r["k2"] <= to_month:
            agg[(r["k2"], r["k1"])] = agg.get((r["k2"], r["k1"]), 0) + int(r["spend"])
    for d in rt_days():
        for field, v in rds().hscan_iter(f"rt:cat:{d}", match=f"{account}|*"):
            _, cat, month = field.split("|")
            if frm_month <= month <= to_month:
                agg[(month, cat)] = agg.get((month, cat), 0) + int(v)
    out = [{"month": m, "category": c, "spend": v} for (m, c), v in sorted(agg.items())]
    return out, meta_info(meta)


def trace(ch: ClickHouse, account: str, day: str) -> tuple[list[dict], dict]:
    meta = ch.query("SELECT k1, k2, tx_count FROM lk.lambda_batch_cur WHERE kind = 'meta'")
    rows = ch.query("SELECT event_id, formatDateTime(event_time, '%Y-%m-%dT%H:%i:%S.%f', 'UTC') AS event_time, type, amount, merchant_id, "
                    "is_self_transfer, pos_offline, spend_contribution FROM lk.lambda_trace_cur "
                    "WHERE account_id = {a:String} AND day = {d:Date} ORDER BY event_time, event_id", a=account, d=day)
    events = [{**r, "amount": int(r["amount"]), "source": "batch",
               "spend_contribution": None if r["spend_contribution"] is None else int(r["spend_contribution"])} for r in rows]
    for raw in rds().lrange(f"rt:tr:{day}:{account}", 0, -1):
        e = json.loads(raw)
        events.append({**e, "source": "speed"})
    events.sort(key=lambda e: (e["event_time"], e["event_id"]))
    m = {"t_k": meta[0]["k1"], "rule": meta[0]["k2"], "batch_input_rows": int(meta[0]["tx_count"])} if meta else meta_info(None)
    return events, m
