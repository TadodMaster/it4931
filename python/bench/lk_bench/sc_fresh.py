"""B1 freshness (Y1), B3 correctness over time (Y1/Y4), B4 late POS data (Y4)."""
from __future__ import annotations

import json
import random
import threading
import time
from datetime import timedelta
from pathlib import Path

from lk_common.ch import ClickHouse
from lk_common.config import load_config
from lk_generator.emit import KafkaEmitter, TruthWriter, emit_marker
from lk_oracle.core import Oracle

from . import dockerctl
from .common import (api_get, batch_runs, clock, iso_utc, kappa_balance_api, kappa_balance_direct, lambda_balance_api,
                     lambda_balance_direct, now_sim, pct, probe_accounts, save_csv, save_json, wait_until)


def _emitter(cfg: dict, run_id: str):
    return (KafkaEmitter(cfg["kafka"]["bootstrap"], cfg["kafka"]["topic_transactions"], linger_ms=0),
            TruthWriter(cfg["data"]["paths"]["truth_live"], run_id))


def _log_markers(run_id: str, em: KafkaEmitter, last_id: str) -> None:
    """One line with CUMULATIVE per-partition delivery counts of this emitter (bench verify sums these)."""
    from .common import run_dir
    with open(run_dir(run_id) / "markers.jsonl", "a") as f:
        f.write(json.dumps({"event_id": last_id, "kafka": em.counts()}) + "\n")


# ======================================================================================================= B1
def b1_freshness(run_id: str, n: int = 60, probes: int = 20, timeout: float = 30.0, **_) -> dict:
    """Marker TOPUP on a probe account; poll API and the stores directly until the new balance shows up."""
    cfg = load_config()
    ch = ClickHouse()
    em, tw = _emitter(cfg, run_id)
    accts = probe_accounts(cfg, probes)
    expected = {a: kappa_balance_api(a) for a in accts}
    for a in accts:                                 # both architectures must agree before we start
        assert lambda_balance_api(a) == expected[a], f"baseline differs on {a}"
    readers = {"kappa_api": kappa_balance_api, "lambda_api": lambda_balance_api,
               "kappa_direct": lambda a: kappa_balance_direct(ch, a), "lambda_direct": lambda a: lambda_balance_direct(ch, a)}
    lat: dict[str, list[float]] = {k: [] for k in readers}
    timeouts = {k: 0 for k in readers}
    rows, last_id = [], ""
    for i in range(n):
        a = accts[i % len(accts)]
        amount = 1000 * (i + 1)
        expected[a] += amount
        row = emit_marker(cfg, tw, em, account=a, type_="TOPUP", amount=amount)
        last_id = row["event_id"]
        t_sent = time.time() - 0.0
        res: dict[str, float | None] = {}

        def poll(name: str, fn=None):
            fn = readers[name]
            dt, ok = wait_until(lambda: fn(a) == expected[a], timeout, 0.02)
            res[name] = None if dt is None else (time.time() - t_sent)

        ths = [threading.Thread(target=poll, args=(k,)) for k in readers]
        [t.start() for t in ths]
        [t.join() for t in ths]
        for k, v in res.items():
            if v is None:
                timeouts[k] += 1
            else:
                lat[k].append(v)
        rows.append({"i": i, "account": a, **{k: (None if v is None else round(v, 4)) for k, v in res.items()}})
        time.sleep(0.25)
    _log_markers(run_id, em, last_id)
    out = {"markers": n, "timeouts": timeouts, **{k: pct(v) for k, v in lat.items()}}
    save_csv(run_id, "b1_freshness_samples.csv", rows)
    save_json(run_id, "b1_freshness.json", out)
    return out


# ======================================================================================================= B3
def _lambda_all_balances(ch: ClickHouse) -> tuple[dict[str, int], dict[str, int], str | None]:
    """(merged now, batch-only, t_k) for every account."""
    from lk_api import lambda_ as lam
    r = lam.rds()
    meta = ch.query("SELECT k1 FROM lk.lambda_batch_cur WHERE kind = 'meta'")
    rows = ch.query("SELECT account_id, spend FROM lk.lambda_batch_cur WHERE kind = 'balance'")
    batch = {x["account_id"]: int(x["spend"]) for x in rows}
    merged = dict(batch)
    for d in lam.rt_days():
        for a, v in r.hscan_iter(f"rt:bal:{d}"):
            merged[a] = merged.get(a, 0) + int(v)
    return merged, batch, meta[0]["k1"] if meta else None


def _cmp(truth: dict[str, int], got: dict[str, int]) -> dict:
    accts = set(truth) | set(got)
    bad = [a for a in accts if truth.get(a, 0) != got.get(a, 0)]
    return {"accounts": len(accts), "mismatched": len(bad), "exact_pct": round(100 * (1 - len(bad) / max(1, len(accts))), 3),
            "max_abs_diff": max((abs(truth.get(a, 0) - got.get(a, 0)) for a in bad), default=0)}


def b3_correctness_over_time(run_id: str, minutes: float = 25, interval: float = 30, rate: int = 500,
                             start_generator: bool = True, **_) -> dict:
    """Every `interval` s compare ALL accounts (balance) of both architectures with the oracle, at wall-clock now and
    (Lambda) at the batch's own cutoff t_k. Expect a Lambda sawtooth that resets at each nightly batch."""
    cfg = load_config()
    ch = ClickHouse()
    paths = cfg["data"]["paths"]
    oracle = Oracle([paths["truth_backfill"], paths["truth_live"]], paths["merchants_csv"])
    if start_generator:
        dockerctl.exec_("generator", "python", "-m", "lk_generator", "live", "--run-id", f"{run_id}-gen", "--rate", str(rate),
                        "--duration", str(minutes * 60 + 5), detach=True)
        time.sleep(5)
    rows, t0 = [], time.time()
    seen_runs: set[str] = set()
    while time.time() - t0 < minutes * 60:
        ts = time.time()
        truth = oracle.all_balances()
        from .common import kappa_balance_direct  # noqa: F401
        kap = {r["account_id"]: int(r["balance"]) for r in ch.query("SELECT account_id, balance FROM lk.kappa_current_account")}
        merged, batch, tk = _lambda_all_balances(ch)
        asof = oracle.all_balances(cutoff=tk) if tk else {}
        k, lm, lb = _cmp(truth, kap), _cmp(truth, merged), (_cmp(asof, batch) if tk else {})
        runs = batch_runs(ch)
        new_batch = [r["run_id"] for r in runs if r["run_id"] not in seen_runs]
        seen_runs.update(new_batch)
        rows.append({"t_s": round(ts - t0, 1), "sim_time": iso_utc(now_sim(cfg)), "t_k": tk,
                     "kappa_exact_pct": k["exact_pct"], "kappa_mismatched": k["mismatched"], "kappa_max_abs_diff": k["max_abs_diff"],
                     "lambda_exact_pct": lm["exact_pct"], "lambda_mismatched": lm["mismatched"],
                     "lambda_max_abs_diff": lm["max_abs_diff"],
                     "lambda_batch_asof_exact_pct": lb.get("exact_pct"), "batch_run_started": ",".join(new_batch)})
        print(f"[b3] t={rows[-1]['t_s']:>6}s kappa {k['exact_pct']}%  lambda {lm['exact_pct']}%  (mismatch {lm['mismatched']})", flush=True)
        time.sleep(max(0.0, interval - (time.time() - ts)))
    save_csv(run_id, "b3_correctness_over_time.csv", rows)
    out = {"samples": len(rows), "batch_runs_seen": sorted(seen_runs),
           "kappa_min_exact_pct": min(r["kappa_exact_pct"] for r in rows),
           "lambda_min_exact_pct": min(r["lambda_exact_pct"] for r in rows),
           "lambda_samples_below_100": sum(r["lambda_exact_pct"] < 100 for r in rows),
           "kappa_samples_below_100": sum(r["kappa_exact_pct"] < 100 for r in rows)}
    save_json(run_id, "b3_correctness.json", out)
    return out


# ======================================================================================================= B4
def b4_late_pos(run_id: str, in_limit: int = 40, beyond: int = 5, wait_next_batch_s: float = 900, **_) -> dict:
    """Burst of offline-POS events (<= 2 sim hours late) right after a nightly batch, plus a few beyond the limit.
    Lambda corrects only at the next batch; Kappa is exact immediately (beyond-limit events go to late-events)."""
    cfg = load_config()
    ch = ClickHouse()
    tc_limit = timedelta(hours=cfg["time"]["pos_max_lateness_sim_hours"])
    em, tw = _emitter(cfg, run_id)
    probes = probe_accounts(cfg)
    ok_acc, bad_acc = probes[20:35], probes[35:40]
    # inject right after a batch completed so that event_time < t_k (outside the speed layer's responsibility)
    runs0 = batch_runs(ch)
    print("[b4] waiting for the next nightly batch boundary ...", flush=True)
    dt, _ = wait_until(lambda: len(batch_runs(ch)) > len(runs0), 1500, 2)
    assert dt is not None, "no batch ran within 25 minutes (is spark-scheduler up?)"
    last = batch_runs(ch)[-1]
    tk_dt = clock(cfg).sim_now().replace(hour=0, minute=0, second=0, microsecond=0)
    from lk_common.config import parse_ts
    tk = parse_ts(last["t_k"].replace(" ", "T") + "Z")
    base_k = {a: kappa_balance_api(a) for a in ok_acc + bad_acc}
    base_l = {a: lambda_balance_api(a) for a in ok_acc + bad_acc}
    exp_ok = dict(base_l)
    now = now_sim(cfg)
    t_inject = time.time()
    rnd = random.Random(7)
    last_id = ""
    for i in range(in_limit):
        a = ok_acc[i % len(ok_acc)]
        amt = 10_000 + 1000 * i
        et = tk - timedelta(minutes=rnd.randint(1, 55))
        assert now - et <= tc_limit, "event would exceed the POS limit; inject sooner after the batch"
        row = emit_marker(cfg, tw, em, account=a, type_="PAYMENT", amount=amt, merchant="M00001", channel="POS",
                          event_time_ms=int(et.timestamp() * 1000), pos_offline=True)
        last_id = row["event_id"]
        exp_ok[a] += -amt
    exp_bad = dict(base_l)
    for i in range(beyond):
        a = bad_acc[i % len(bad_acc)]
        et = now - timedelta(hours=3)
        emit_marker(cfg, tw, em, account=a, type_="PAYMENT", amount=20_000 + i, merchant="M00001", channel="POS",
                    event_time_ms=int(et.timestamp() * 1000), pos_offline=True)
        exp_bad[a] -= 20_000 + i
    _log_markers(run_id, em, last_id)
    state = lambda fn, exp: all(fn(a) == exp[a] for a in exp)  # noqa: E731
    k_dt, _ = wait_until(lambda: state(kappa_balance_api, exp_ok), 60, 0.1)
    l_ok = wait_until(lambda: state(lambda_balance_api, exp_ok), wait_next_batch_s, 2)
    l_bad = wait_until(lambda: state(lambda_balance_api, exp_bad), wait_next_batch_s, 2)
    kappa_bad_now = {a: kappa_balance_api(a) for a in bad_acc}
    from .verify import _late_event_ids
    late_ids = _late_event_ids(cfg)
    out = {"batch_t_k": last["t_k"], "in_limit_events": in_limit, "beyond_limit_events": beyond,
           "kappa_exact_after_s": None if k_dt is None else round(k_dt, 2),
           "lambda_exact_after_s": None if l_ok[0] is None else round(l_ok[0], 1),
           "lambda_exact_after_sim_minutes": None if l_ok[0] is None else round(l_ok[0] * cfg["time"]["sim_speedup"] / 60, 1),
           "lambda_beyond_limit_exact_after_s": None if l_bad[0] is None else round(l_bad[0], 1),
           "kappa_beyond_limit_accounts_equal_truth": all(kappa_bad_now[a] == exp_bad[a] for a in bad_acc),
           "kappa_late_events_topic_distinct_ids": len(late_ids),
           "note": "beyond-limit events are NOT applied by Kappa (they sit in late-events, never silently dropped); "
                   "Lambda recomputes all history, so it absorbs arbitrarily late events at the next batch"}
    save_json(run_id, "b4_late_pos.json", out)
    return out
