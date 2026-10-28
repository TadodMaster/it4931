"""Verification items that need an experiment: batch-view atomicity + rollback, Kappa replay determinism."""
from __future__ import annotations

import threading
import time
from datetime import timedelta

from lk_common.ch import ClickHouse
from lk_common.config import load_config

from . import flinkapi
from .common import now_sim, save_json, wait_until
from .sc_ops import flink_cancel, flink_submit, kappa_digests, manual_batch, wait_lag_zero


def atomic_swap(run_id: str, accounts: int = 60, **_) -> dict:
    """A reader hammers the Lambda batch view while a new version is swapped in: it must only ever see a COMPLETE
    version (the old t_k with the old checksum, or the new t_k with the new checksum), never a mix. Then roll back."""
    cfg = load_config()
    ch = ClickHouse()
    wait_until(lambda: ch.query("SELECT 1 FROM lk.lambda_batch_cur WHERE kind = 'meta' LIMIT 1"), 600, 5)   # first nightly batch
    accts = [r["account_id"] for r in ch.query(f"SELECT DISTINCT account_id FROM lk.lambda_batch_cur WHERE kind = 'balance' LIMIT {accounts}")]
    in_list = ",".join(f"'{a}'" for a in accts)
    sql = ("SELECT anyIf(k1, kind = 'meta') AS tk, count() AS n, sum(cityHash64(kind, account_id, k1, k2, spend, tx_count)) AS cs "
           f"FROM lk.lambda_batch_cur WHERE kind = 'meta' OR (kind = 'day' AND account_id IN ({in_list}))")
    seen: dict[tuple, int] = {}
    stop = False

    def hammer():
        c = ClickHouse()
        while not stop:
            r = c.query(sql)[0]
            key = (r["tk"], int(r["n"]), int(r["cs"]))
            seen[key] = seen.get(key, 0) + 1

    th = threading.Thread(target=hammer, daemon=True)
    th.start()
    time.sleep(1)
    rule = ch.query("SELECT k2 FROM lk.lambda_batch_cur WHERE kind='meta'")[0]["k2"]
    mb = {}
    for attempt in range(1, 4):                      # the swap must happen while the reader is running: both versions have to be observed
        mb = manual_batch(rule, now_sim(cfg).replace(microsecond=0) - timedelta(minutes=attempt), f"atomic{attempt}")
        time.sleep(2)
        if len({k[0] for k in seen}) >= 2:
            break
    stop = True
    th.join()
    by_tk: dict[str, set] = {}
    for (tk, n, cs) in seen:
        by_tk.setdefault(tk, set()).add((n, cs))
    consistent = all(len(v) == 1 for v in by_tk.values())
    out = {"reads": sum(seen.values()), "distinct_versions_seen": len(by_tk), "versions": {k: list(v)[0] for k, v in by_tk.items()},
           "each_t_k_has_exactly_one_content": consistent, "batch_seconds": mb["seconds"]}
    # rollback = one more atomic exchange with the previous version
    prev = ch.query("SELECT name FROM system.tables WHERE database='lk' AND name LIKE 'lambda_batch_prev_%' ORDER BY name DESC LIMIT 1")
    cur_tk = ch.query("SELECT k1 FROM lk.lambda_batch_cur WHERE kind='meta'")[0]["k1"]
    if prev:
        ch.execute(f"EXCHANGE TABLES lk.lambda_batch_cur AND lk.{prev[0]['name']}")
        rolled = ch.query("SELECT k1 FROM lk.lambda_batch_cur WHERE kind='meta'")[0]["k1"]
        ch.execute(f"EXCHANGE TABLES lk.lambda_batch_cur AND lk.{prev[0]['name']}")      # and forward again
        out["rollback"] = {"t_k_before": cur_tk, "t_k_after_rollback": rolled, "worked": rolled != cur_tk}
    out["passed"] = consistent and 2 <= len(by_tk) <= 4 and out.get("rollback", {}).get("worked", False)
    save_json(run_id, "proof_atomic_swap.json", out)
    return out


def replay_determinism(run_id: str, **_) -> dict:
    """Run the Kappa job a second time from offset 0 into kappa_v1b_*; its output must be identical to kappa_v1_*."""
    cfg = load_config()
    ch = ClickHouse()
    assert flinkapi.running_job("kappa-v1"), "kappa-v1 must be running"
    wait_lag_zero("kappa-v1", cfg, 300)
    flink_submit("kappa", "--rule", "v1", "--version", "v1b", "--parallelism", "2", "--start-from", "earliest",
                 "--checkpoint-interval-ms", "30000")
    t0 = time.time()
    dt = wait_lag_zero("kappa-v1b", cfg, 1800)
    wait_lag_zero("kappa-v1", cfg, 300)
    time.sleep(12)   # one checkpoint interval: sink rows of both jobs are flushed
    a, b = kappa_digests(ch, "v1"), kappa_digests(ch, "v1b")
    acct = lambda v: ch.query(f"SELECT count() AS n, sum(cityHash64(account_id, balance, accepted, duplicates, late)) AS d FROM "  # noqa: E731
                              f"({__import__('lk_common.ch', fromlist=['kappa_view_sql']).kappa_view_sql(v)['account']})")[0]
    ca, cb = acct("v1"), acct("v1b")
    flink_cancel("kappa-v1b")
    out = {"replay_seconds": None if dt is None else round(dt, 1), "metrics_identical": a == b, "v1": a, "v1b": b,
           "counters_identical": ca == cb, "account_counters": {"v1": ca, "v1b": cb}}
    out["passed"] = out["metrics_identical"] and out["counters_identical"]
    save_json(run_id, "proof_replay_determinism.json", out)
    return out
