"""Shared helpers for bench scenarios: run directories, API/direct readers, probe accounts, timing."""
from __future__ import annotations

import csv
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

from lk_common.ch import ClickHouse
from lk_common.config import SimClock, TimeConfig, load_config
from lk_common.kafka_lag import consumer_lag
from lk_common.model import account_id

API = os.environ.get("QUERY_API", "http://query-api:8080")
_http = httpx.Client(timeout=30)


def run_dir(run_id: str) -> Path:
    p = Path("results") / run_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def save_json(run_id: str, name: str, obj) -> Path:
    p = run_dir(run_id) / name
    p.write_text(json.dumps(obj, indent=2, default=str))
    return p


def save_csv(run_id: str, name: str, rows: list[dict]) -> Path:
    p = run_dir(run_id) / name
    if rows:
        with open(p, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    return p


def pct(values: list[float], ps=(50, 95, 99)) -> dict:
    if not values:
        return {f"p{p}": None for p in ps} | {"n": 0}
    v = sorted(values)
    out = {f"p{p}": v[min(len(v) - 1, max(0, math.ceil(p / 100 * len(v)) - 1))] for p in ps}
    out.update(n=len(v), min=v[0], max=v[-1], mean=sum(v) / len(v))
    return out


def probe_accounts(cfg: dict, n: int | None = None) -> list[str]:
    d = cfg["data"]
    k = d["num_probe_accounts"] if n is None else n
    return [account_id(d["num_accounts"] + i) for i in range(k)]


def clock(cfg: dict) -> SimClock:
    tc = TimeConfig.from_config(cfg)
    return SimClock.load_or_create(tc, Path(cfg["data"]["paths"]["root"]) / "clock.json")


def now_sim(cfg: dict) -> datetime:
    return clock(cfg).sim_now()


# ---- readers -----------------------------------------------------------------------------------------------------
def api_get(path: str, **params) -> dict:
    """GET with a few retries: a 5xx while ClickHouse is busy merging is retried like any real client would (counted in API_RETRIES)."""
    last = None
    for attempt in range(4):
        r = _http.get(API + path, params=params)
        if r.status_code < 500:
            r.raise_for_status()
            return r.json()
        last = r
        API_RETRIES.append((path, r.status_code))
        time.sleep(0.5 * (attempt + 1))
    last.raise_for_status()
    return last.json()


API_RETRIES: list[tuple[str, int]] = []


def kappa_balance_api(a: str) -> int:
    return api_get(f"/kappa/balance/{a}")["balance"]


def lambda_balance_api(a: str) -> int:
    return api_get(f"/lambda/balance/{a}")["balance"]


def kappa_balance_direct(ch: ClickHouse, a: str) -> int:
    r = ch.query("SELECT balance FROM lk.kappa_current_account WHERE account_id = {a:String}", a=a)
    return int(r[0]["balance"]) if r else 0


def lambda_balance_direct(ch: ClickHouse, a: str) -> int:
    from lk_api import lambda_ as lam
    return lam.balance(ch, a)[0]


def lag_total(group: str, cfg: dict) -> int | None:
    try:
        return consumer_lag(cfg["kafka"]["bootstrap"], group, cfg["kafka"]["topic_transactions"])["total"]
    except Exception:  # noqa: BLE001 - group does not exist (that architecture is not running)
        return None


def wait_until(fn, timeout: float, interval: float = 0.05):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = fn()
        if v:
            return time.time() - t0, v
        time.sleep(interval)
    return None, None


def batch_runs(ch: ClickHouse) -> list[dict]:
    return ch.query("SELECT run_id, toString(t_k) AS t_k, rule_version, duration_ms, input_rows, "
                    "toUnixTimestamp64Milli(started_at) AS started_ms FROM lk.lambda_batch_runs WHERE status = 'ok' "
                    "ORDER BY started_at")


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


__all__ = ["timedelta"]


# ---- lag measured from PROCESSED counts (committed Kafka offsets only advance at Flink checkpoints) -----------------
def kafka_end_total(cfg: dict) -> int:
    from .verify import _end_offsets
    return sum(_end_offsets(cfg["kafka"]["bootstrap"], cfg["kafka"]["topic_transactions"]).values())


def kappa_processed(ch: ClickHouse, version: str) -> int | None:
    """Events the Kappa job version has handled (accepted + duplicates + late), read from its own output table."""
    try:
        r = ch.query(f"SELECT sum(n) AS s FROM (SELECT account_id, argMax(accepted + duplicates + late, version) AS n "
                     f"FROM lk.kappa_{version}_account GROUP BY account_id)")
        return int(r[0]["s"] or 0)
    except Exception:  # noqa: BLE001 - table does not exist yet
        return None


def speed_processed() -> int:
    import redis
    r = redis.Redis(host="redis", decode_responses=True)
    return sum(int(r.get(k) or 0) for k in ("stat:accepted", "stat:dup", "stat:old"))


def backfill_total(cfg: dict) -> int:
    p = Path(cfg["data"]["paths"]["root"]) / "kafka_load_manifest.json"
    return int(json.loads(p.read_text())["sent"])


def processed_count(group: str, ch: ClickHouse) -> int | None:
    if group.startswith("kappa-"):
        return kappa_processed(ch, group.split("-", 1)[1])
    if group == "lambda-speed":
        return speed_processed()
    return None


def settle(group: str, cfg: dict, ch: ClickHouse, quiet_s: float = 6.0, timeout: float = 300.0) -> tuple[int, int] | None:
    """Wait until `group` stops making progress (processed count and Kafka end offsets both stable) and return (end, processed).
    Used as the baseline for lag: lag(t) = (end(t) - end0) - (processed(t) - processed0)."""
    t0, last, since = time.time(), None, time.time()
    while time.time() - t0 < timeout:
        cur = (kafka_end_total(cfg), processed_count(group, ch))
        if cur[1] is None:
            return None
        if cur != last:
            last, since = cur, time.time()
        elif time.time() - since >= quiet_s:
            return cur
        time.sleep(1)
    return last


def processed_lag(group: str, cfg: dict, ch: ClickHouse, base: tuple[int, int] | None = None) -> int | None:
    """Messages still to be processed by `group` (kappa-<ver> or lambda-speed); None if that architecture is not running.
    Without a baseline the Kappa job is compared with the whole topic; the speed layer needs a settled baseline."""
    end = kafka_end_total(cfg)
    n = processed_count(group, ch)
    if n is None:
        return lag_total(group, cfg) if not group.startswith(("kappa-", "lambda")) else None
    if base is not None:
        return max(0, (end - base[0]) - (n - base[1]))
    if group.startswith("kappa-"):
        return max(0, end - n)
    return max(0, end - backfill_total(cfg) - n)
