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


def _log_markers(run_id: str, em: KafkaEmitter, last_id: str, beyond: int = 0) -> None:
    """One line with CUMULATIVE per-partition delivery counts of this emitter (bench verify sums these)."""
    from .common import run_dir
    with open(run_dir(run_id) / "markers.jsonl", "a") as f:
        f.write(json.dumps({"event_id": last_id, "kafka": em.counts(), "beyond_limit": beyond}) + "\n")


# ======================================================================================================= B1
def b1_freshness(run_id: str, n: int = 40, probes: int = 20, timeout: float = 30.0, **_) -> dict:
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


def _cmp(truth: dict[str, int], got: dict[str, int], truth2: dict[str, int] | None = None) -> dict:
    """An account matches if the system equals the oracle read before OR after the system was read (events in flight while
    we measure are not errors; a missing late event matches neither)."""
    accts = set(truth) | set(got)
    t2 = truth2 or truth
    # accounts that changed between the two oracle reads have events in flight: they are not judged (both architectures alike)
    stable = [a for a in accts if truth.get(a, 0) == t2.get(a, 0)]
    bad = [a for a in stable if got.get(a, 0) != truth.get(a, 0)]
    accts = stable
    return {"accounts": len(accts), "mismatched": len(bad), "exact_pct": round(100 * (1 - len(bad) / max(1, len(accts))), 3),
            "max_abs_diff": max((abs(truth.get(a, 0) - got.get(a, 0)) for a in bad), default=0)}


def b3_correctness_over_time(run_id: str, minutes: float = 10, interval: float = 20, rate: int = 100,
                             start_generator: bool = True, **_) -> dict:
    """Every `interval` s compare ALL accounts (balance) of both architectures with the oracle, at wall-clock now and
    (Lambda) at the batch's own cutoff t_k. Expect a Lambda sawtooth that resets at each nightly batch."""
    cfg = load_config()
    ch = ClickHouse()
    paths = cfg["data"]["paths"]
    oracle = Oracle([paths["truth_backfill"], paths["truth_live"]], paths["merchants_csv"])
    if start_generator:
        from .sc_ops import start_generator as _sg
        _sg(f"{run_id}-gen", rate, minutes * 60 + 5)
        time.sleep(5)
    rows, t0 = [], time.time()
    seen_runs: set[str] = set()
    while time.time() - t0 < minutes * 60:
        ts = time.time()
        truth = oracle.all_balances()
        kap = {r["account_id"]: int(r["balance"]) for r in ch.query("SELECT account_id, balance FROM lk.kappa_current_account")}
        merged, batch, tk = _lambda_all_balances(ch)
        truth2 = oracle.all_balances()
        asof = oracle.all_balances(cutoff=tk) if tk else {}
        k, lm, lb = _cmp(truth, kap, truth2), _cmp(truth, merged, truth2), (_cmp(asof, batch) if tk else {})
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
    """Offline-POS burst (<= 2 simulated hours late) injected at the moment a nightly batch starts, plus a few events beyond the limit.
    The batch reads its snapshot at the start; the events arrive afterwards with event_time < t_k, so neither the batch nor the
    speed layer (which hands over at t_k) counts them until the NEXT batch. Kappa is exact at once."""
    cfg = load_config()
    ch = ClickHouse()
    tc_limit = timedelta(hours=cfg["time"]["pos_max_lateness_sim_hours"])
    em, tw = _emitter(cfg, run_id)
    probes = probe_accounts(cfg)
    ok_acc, bad_acc = probes[20:35], probes[35:40]
    clk = clock(cfg)
    boundary = clk.sim_now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    from .sc_ops import start_generator as _sg, stop_generator as _stop
    _sg(f"{run_id}-gen", 100, 1500)            # background traffic: Kappa's watermark only advances while events flow
    wait_real = (boundary - clk.sim_now()).total_seconds() / clk.tc.sim_speedup
    print(f"[b4] waiting {wait_real:.0f} s for the sim-day boundary {boundary.isoformat()} (the nightly batch starts then) ...", flush=True)
    grace_real = cfg["time"].get("batch_grace_sim_minutes", 0) * 60 / clk.tc.sim_speedup
    time.sleep(max(0.0, wait_real) + grace_real + 14)               # the batch starts at boundary + grace and reads its snapshot in its first seconds
    base_l = {a: lambda_balance_api(a) for a in ok_acc + bad_acc}
    base_k = {a: kappa_balance_api(a) for a in ok_acc + bad_acc}
    now = now_sim(cfg)
    rnd = random.Random(7)
    exp_ok = {a: base_l[a] for a in ok_acc}
    exp_bad = {a: base_l[a] for a in bad_acc}
    last_id = ""
    for i in range(in_limit):
        a = ok_acc[i % len(ok_acc)]
        amt = 10_000 + 1000 * i
        et = max(boundary - timedelta(minutes=rnd.randint(1, 55)), now - tc_limit + timedelta(minutes=5))
        row = emit_marker(cfg, tw, em, account=a, type_="PAYMENT", amount=amt, merchant="M00001", channel="POS",
                          event_time_ms=int(et.timestamp() * 1000), pos_offline=True)
        last_id = row["event_id"]
        exp_ok[a] -= amt
    for i in range(beyond):
        a = bad_acc[i % len(bad_acc)]
        et = now - timedelta(hours=3)
        emit_marker(cfg, tw, em, account=a, type_="PAYMENT", amount=20_000 + i, merchant="M00001", channel="POS",
                    event_time_ms=int(et.timestamp() * 1000), pos_offline=True)
        exp_bad[a] -= 20_000 + i
    _log_markers(run_id, em, last_id, beyond)
    t_inj = time.time()
    exact = lambda fn, exp: all(fn(a) == exp[a] for a in exp)      # noqa: E731
    timeline, k_dt = [], None
    while time.time() - t_inj < wait_next_batch_s:
        t = time.time() - t_inj
        k_ok = exact(kappa_balance_api, exp_ok)
        l_ok = exact(lambda_balance_api, exp_ok)
        if k_ok and k_dt is None:
            k_dt = t
        timeline.append({"t_s": round(t, 1), "kappa_exact": k_ok, "lambda_exact": l_ok,
                         "lambda_exact_beyond_limit": exact(lambda_balance_api, exp_bad)})
        # done once Lambda is exact again AND a batch that started after the injection has been published
        newer_batch = any(int(r["started_ms"]) > t_inj * 1000 for r in batch_runs(ch))
        if l_ok and newer_batch and len(timeline) > 1 and timeline[-2]["lambda_exact"]:
            break
        time.sleep(1.5)
    save_csv(run_id, "b4_timeline.csv", timeline)
    wrong = [x["t_s"] for x in timeline if not x["lambda_exact"]]
    final_exact = next((x["t_s"] for x in reversed(timeline) if not x["lambda_exact"]), None)
    from .verify import _late_event_ids
    from lk_common.ch import current_kappa_version
    late_ids = _late_event_ids(cfg, current_kappa_version(ch) or "v1")
    kappa_bad_now = {a: kappa_balance_api(a) for a in bad_acc}
    out = {"in_limit_events": in_limit, "beyond_limit_events": beyond, "batch_boundary": boundary.isoformat(),
           "kappa_exact_after_s": None if k_dt is None else round(k_dt, 2),
           "lambda_inexact_seconds": len(wrong) and round(max(wrong) - min(wrong), 1),
           "lambda_exact_after_s": None if final_exact is None else round(final_exact, 1),
           "lambda_exact_after_sim_minutes": None if final_exact is None else round(final_exact * cfg["time"]["sim_speedup"] / 60, 1),
           "lambda_ever_inexact": bool(wrong),
           "lambda_beyond_limit_final_exact": exact(lambda_balance_api, exp_bad),
           "kappa_beyond_limit_accounts_equal_truth": all(kappa_bad_now[a] == exp_bad[a] for a in bad_acc),
           "kappa_late_events_topic_distinct_ids": len(late_ids),
           "note": "beyond-limit events are NOT applied by Kappa (they sit in late-events, never silently dropped); Lambda recomputes "
                   "all history, so it absorbs arbitrarily late events once the next batch has run"}
    _stop()
    save_json(run_id, "b4_late_pos.json", out)
    return out
