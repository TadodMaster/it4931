"""Oracle HTTP service (`/truth/...`). Also reachable through query-api (phase 2)."""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, Query

from lk_common.config import load_config

from .core import RULE_NUM, Oracle

cfg = load_config()
paths = cfg["data"]["paths"]
oracle = Oracle([paths["truth_backfill"], paths["truth_live"]], paths["merchants_csv"])
app = FastAPI(title="lk-oracle")


def _rule(rule: str) -> str:
    if rule not in RULE_NUM:
        raise HTTPException(400, f"rule must be one of {list(RULE_NUM)}")
    return rule


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/truth/balance/{account}")
def balance(account: str, cutoff: str | None = None):
    return {"account": account, "balance": oracle.balance(account, cutoff), "cutoff": cutoff}


@app.get("/truth/spend/{account}")
def spend(account: str, day: str, rule: str = "v1", cutoff: str | None = None):
    return {"account": account, "day": day, "rule": _rule(rule), "cutoff": cutoff,
            **oracle.spend_day(account, day, rule, cutoff)}


@app.get("/truth/report/{account}")
def report(account: str, frm: str = Query(alias="from"), to: str = "9999-12-31", rule: str = "v1",
           cutoff: str | None = None):
    return {"account": account, "rule": _rule(rule), "cutoff": cutoff,
            "rows": oracle.report(account, frm, to, rule, cutoff)}


@app.get("/truth/trace/{account}")
def trace(account: str, day: str, rule: str = "v1", cutoff: str | None = None):
    return {"account": account, "day": day, "rule": _rule(rule), **oracle.trace(account, day, rule, cutoff)}


@app.get("/truth/totals")
def totals(rule: str = "v1", cutoff: str | None = None, checksum: bool = False):
    """checksum=true adds Σ-hash of event ids (slow on 20M rows: ~20 s)."""
    extra = {"id_checksum": oracle.id_checksum(cutoff)} if checksum else {}
    return {"rule": _rule(rule), "cutoff": cutoff, **oracle.totals(rule, cutoff), **extra}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("ORACLE_PORT", "8090")))
