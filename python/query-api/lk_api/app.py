"""query-api: one HTTP surface for both architectures + the oracle. Every response carries as_of metadata."""
from __future__ import annotations

import os
import time

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response

from lk_common.ch import ClickHouse, current_kappa_version
from lk_common.config import load_config

from lk_common.kafka_lag import consumer_lag

from . import lambda_ as lam

cfg = load_config()
ch = ClickHouse()
ORACLE = os.environ.get("ORACLE_URL", cfg["oracle"]["url"])
app = FastAPI(title="lk-query-api")
_cache: dict = {}


def _cached(key: str, ttl: float, fn):
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = fn()
    _cache[key] = (now, val)
    return val


def kappa_as_of() -> dict:
    version = _cached("kv", 5, lambda: current_kappa_version(ch))
    if not version:
        raise HTTPException(503, "no kappa view is active yet")
    prog = ch.query("SELECT toUnixTimestamp64Milli(min(watermark)) AS wm, toUnixTimestamp64Milli(max(wall_time)) AS wall "
                    "FROM lk.kappa_current_progress")
    wm = prog[0]["wm"] if prog and prog[0]["wm"] else None
    lag = _cached("lag", 2, lambda: consumer_lag(cfg["kafka"]["bootstrap"], f"kappa-{version}", cfg["kafka"]["topic_transactions"]))
    return {"architecture": "kappa", "version": version, "watermark_ms": wm,
            "watermark_iso": None if wm is None else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(wm / 1000)),
            "last_progress_wall_ms": prog[0]["wall"] if prog else None, "consumer_lag": lag}


@app.get("/health")
def health():
    ch.scalar("SELECT 1")
    return {"ok": True}


@app.get("/kappa/balance/{account}")
def kappa_balance(account: str):
    rows = ch.query("SELECT balance, accepted, duplicates, late, ver AS version FROM lk.kappa_current_account "
                    "WHERE account_id = {a:String}", a=account)
    r = rows[0] if rows else {"balance": 0, "accepted": 0, "duplicates": 0, "late": 0, "version": 0}
    return {"account": account, "balance": int(r["balance"]), "events_counted": int(r["accepted"]),
            "duplicates_discarded": int(r["duplicates"]), "late_events": int(r["late"]), "as_of": kappa_as_of()}


@app.get("/kappa/spend/{account}")
def kappa_spend(account: str, day: str):
    rows = ch.query("SELECT spend, tx_count FROM lk.kappa_current_spend_day WHERE account_id = {a:String} AND day = {d:Date}",
                    a=account, d=day)
    r = rows[0] if rows else {"spend": 0, "tx_count": 0}
    return {"account": account, "day": day, "spend": int(r["spend"]), "tx_count": int(r["tx_count"]), "as_of": kappa_as_of()}


@app.get("/kappa/report/{account}")
def kappa_report(account: str, frm: str = Query(alias="from"), to: str = "9999-12"):
    """Spend by (month, category) for months frm[:7] .. to[:7] inclusive."""
    rows = ch.query("SELECT month, category, spend FROM lk.kappa_current_spend_cat_month WHERE account_id = {a:String} "
                    "AND month >= {f:String} AND month <= {t:String} ORDER BY month, category", a=account, f=frm[:7], t=to[:7])
    return {"account": account, "rows": [{"month": r["month"], "category": r["category"], "spend": int(r["spend"])} for r in rows],
            "as_of": kappa_as_of()}


@app.get("/trace/{arch}/{account}")
def trace(arch: str, account: str, day: str):
    if arch == "truth":
        return httpx.get(f"{ORACLE}/truth/trace/{account}", params={"day": day, "rule": "v1"}, timeout=60).json()
    if arch == "lambda":
        events, info = _lam(lam.trace, ch, account, day)
        return {"account": account, "day": day, "events": events,
                "spend": sum(e["spend_contribution"] or 0 for e in events),
                "tx_count": sum(e["spend_contribution"] is not None for e in events), "as_of": lambda_as_of(info)}
    if arch != "kappa":
        raise HTTPException(404, "unknown architecture")
    rows = ch.query("SELECT event_id, formatDateTime(event_time, '%Y-%m-%dT%H:%i:%S.%f', 'UTC') AS event_time, type, amount, merchant_id, "
                    "is_self_transfer, pos_offline, spend_contribution FROM lk.kappa_current_trace "
                    "WHERE account_id = {a:String} AND day = {d:Date} ORDER BY event_time, event_id", a=account, d=day)
    events = [{**r, "amount": int(r["amount"]), "spend_contribution": None if r["spend_contribution"] is None
               else int(r["spend_contribution"])} for r in rows]
    return {"account": account, "day": day, "events": events,
            "spend": sum(e["spend_contribution"] or 0 for e in events),
            "tx_count": sum(e["spend_contribution"] is not None for e in events), "as_of": kappa_as_of()}


def lambda_as_of(info: dict) -> dict:
    run = _cached("lrun", 3, lambda: ch.query("SELECT run_id, toString(t_k) AS t_k, rule_version, duration_ms, input_rows, "
                                              "toString(started_at) AS started_at FROM lk.lambda_batch_runs "
                                              "WHERE status = 'ok' ORDER BY started_at DESC LIMIT 1"))
    lag = _cached("llag", 2, lambda: consumer_lag(cfg["kafka"]["bootstrap"], "lambda-speed", cfg["kafka"]["topic_transactions"]))
    return {"architecture": "lambda", **info, "last_batch": run[0] if run else None, "speed_layer_consumer_lag": lag["total"]}


def _lam(fn, *a):
    try:
        return fn(*a)
    except Exception as e:  # noqa: BLE001 - redis/clickhouse down => lambda profile not running
        raise HTTPException(503, f"lambda serving unavailable: {e}")


@app.get("/lambda/balance/{account}")
def lambda_balance(account: str):
    bal, info = _lam(lam.balance, ch, account)
    return {"account": account, "balance": bal, "as_of": lambda_as_of(info)}


@app.get("/lambda/spend/{account}")
def lambda_spend(account: str, day: str):
    r, info = _lam(lam.spend, ch, account, day)
    return {"account": account, "day": day, **r, "as_of": lambda_as_of(info)}


@app.get("/lambda/report/{account}")
def lambda_report(account: str, frm: str = Query(alias="from"), to: str = "9999-12"):
    rows, info = _lam(lam.report, ch, account, frm[:7], to[:7])
    return {"account": account, "rows": rows, "as_of": lambda_as_of(info)}


# /truth/* is a thin proxy so clients see one host; the oracle itself stays independent.
@app.get("/truth/{path:path}")
def truth_proxy(path: str, request: Request):
    r = httpx.get(f"{ORACLE}/truth/{path}", params=dict(request.query_params), timeout=120)
    return Response(r.content, status_code=r.status_code, media_type="application/json")
