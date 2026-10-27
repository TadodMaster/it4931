"""bench verify: proves (or refutes) that an architecture's output equals the oracle. Fails loudly (exit 1).

Checks implemented in phase 2 (Kappa): oracle equality (exact, via order-independent row digests with a per-account
drill-down on mismatch), conservation invariants, Kafka offsets, dedup proof, late-data accounting.
Lambda-specific (immutability, atomic swap) and replay determinism arrive with phases 3/4.
"""
from __future__ import annotations

import glob
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from lk_common.ch import ClickHouse, current_kappa_version
from lk_common.config import load_config
from lk_common.kafka_lag import consumer_lag
from lk_oracle.core import Oracle

METRICS = ("balance", "spend_day", "spend_cat_month")
# same row-hash definition as Oracle._H: first 8 bytes of md5 as big-endian UInt64
CH_HASH = "reinterpretAsUInt64(reverse(substring(MD5(s), 1, 8)))"
CH_ROW = {
    "balance": ("SELECT account_id AS a, concat(account_id, '|', toString(balance)) AS s FROM lk.kappa_current_account"),
    "spend_day": ("SELECT account_id AS a, concat(account_id, '|', toString(day), '|', toString(spend), '|', toString(tx_count)) AS s "
                  "FROM lk.kappa_current_spend_day"),
    "spend_cat_month": ("SELECT account_id AS a, concat(account_id, '|', category, '|', month, '|', toString(spend)) AS s "
                        "FROM lk.kappa_current_spend_cat_month"),
}


class Report:
    def __init__(self):
        self.checks: list[dict] = []

    def add(self, name: str, ok: bool, **detail):
        self.checks.append({"name": name, "status": "pass" if ok else "fail", **detail})
        print(f"[verify] {'PASS' if ok else 'FAIL'}  {name}  {json.dumps(detail, default=str)[:300]}", flush=True)

    @property
    def ok(self) -> bool:
        return all(c["status"] == "pass" for c in self.checks)


def _end_offsets(bootstrap: str, topic: str) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "verify-probe", "enable.auto.commit": False})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    finally:
        c.close()


def _produced_by_partition(cfg: dict) -> tuple[dict[int, int], dict]:
    """Produced counts per partition from the generator logs: backfill load + every live run + markers."""
    by: dict[int, int] = {}
    src = {"backfill_load": 0, "live_runs": [], "markers": 0}

    def add(d: dict):
        for k, v in d.items():
            by[int(k)] = by.get(int(k), 0) + int(v)

    lm = Path(cfg["data"]["paths"]["root"]) / "kafka_load_manifest.json"
    if lm.exists():
        m = json.loads(lm.read_text())
        add(m["delivered"]["by_partition"])
        src["backfill_load"] = m["delivered"]["total"]
    for f in sorted(glob.glob("results/*/injections.json")):
        d = json.loads(Path(f).read_text()).get("live")
        if d:
            add(d["kafka"]["by_partition"])
            src["live_runs"].append({"run": Path(f).parent.name, "events": d["kafka"]["total"], "final": d["final"]})
    for f in sorted(glob.glob("results/*/markers.jsonl")):
        for line in Path(f).read_text().splitlines():
            add(json.loads(line)["kafka"]["by_partition"])
            src["markers"] += 1
    return by, src


def _injected(cfg: dict) -> dict:
    inj = {"duplicates": 0, "late_pos": 0, "beyond_limit": 0, "logical": 0}
    bm = Path(cfg["data"]["paths"]["manifest"])
    if bm.exists():
        m = json.loads(bm.read_text())
        inj["duplicates"] += m["duplicates"]
        inj["late_pos"] += m["late_pos"]
        inj["beyond_limit"] += m["beyond_limit"]
        inj["logical"] += m["logical_events"]
    for f in glob.glob("results/*/injections.json"):
        d = json.loads(Path(f).read_text()).get("live")
        if d:
            for k in ("duplicates", "late_pos", "beyond_limit", "logical"):
                inj[k] += d[k]
    return inj


def wait_for_lag_zero(cfg: dict, group: str, timeout: float) -> dict:
    t0 = time.time()
    while True:
        lag = consumer_lag(cfg["kafka"]["bootstrap"], group, cfg["kafka"]["topic_transactions"])
        if lag["total"] == 0 or time.time() - t0 > timeout:
            return {**lag, "waited_s": round(time.time() - t0, 1)}
        print(f"[verify] waiting for consumer lag of {group}: {lag['total']:,}", flush=True)
        time.sleep(5)


def verify_kappa(rule: str = "v1", run_id: str = "local", cutoff: str | None = None, lag_timeout: float = 600,
                 markers_extra_runs: bool = True) -> Report:
    cfg = load_config()
    paths = cfg["data"]["paths"]
    ch = ClickHouse()
    rep = Report()
    version = current_kappa_version(ch)
    rep.add("kappa view active", version is not None, version=version)
    if version is None:
        return rep
    group = f"kappa-{version}"
    lag = wait_for_lag_zero(cfg, group, lag_timeout)
    rep.add("consumer caught up (committed offsets reach end offsets)", lag["total"] == 0,
            lag=lag["total"], waited_s=lag["waited_s"])
    oracle = Oracle([paths["truth_backfill"], paths["truth_live"]], paths["merchants_csv"])

    # 1. oracle equality (exact integer equality of every row of every metric)
    for metric in METRICS:
        exp = oracle.digest(metric, rule, cutoff)
        got = ch.query(f"SELECT count() AS n, sum({CH_HASH}) AS d FROM ({CH_ROW[metric]})")[0]
        got = {"rows": int(got["n"]), "digest": int(got["d"])}
        ok = exp == got
        detail = {"oracle": exp, "kappa": got}
        if not ok:
            detail["drill_down"] = _drill_down(oracle, ch, metric, rule, cutoff)
        rep.add(f"oracle equality: {metric} ({rule})", ok, **detail)

    # 2. conservation invariants
    t = oracle.totals(rule, cutoff)
    kb = ch.query("SELECT sum(balance) AS b, sum(accepted) AS acc, sum(duplicates) AS dup, sum(late) AS late "
                  "FROM lk.kappa_current_account")[0]
    ks = ch.query("SELECT sum(spend) AS s, sum(tx_count) AS c FROM lk.kappa_current_spend_day")[0]
    kc = ch.query("SELECT sum(spend) AS s FROM lk.kappa_current_spend_cat_month")[0]
    rep.add("sum(balance) = oracle global total", int(kb["b"]) == t["balance_sum"], kappa=int(kb["b"]), oracle=t["balance_sum"])
    rep.add("sum(spend over days) = sum(spend over categories) = oracle global total",
            int(ks["s"]) == int(kc["s"]) == t["spend_sum"], days=int(ks["s"]), categories=int(kc["s"]), oracle=t["spend_sum"])
    rep.add("spend-contributing events = oracle", int(ks["c"]) == t["spend_events"], kappa=int(ks["c"]), oracle=t["spend_events"])
    late_ids = _late_event_ids(cfg) if int(kb["late"]) > 0 else set()
    rep.add("events counted = logical events - late-events (distinct)", int(kb["acc"]) == t["events"] - len(late_ids),
            kappa_accepted=int(kb["acc"]), oracle_events=t["events"], late_distinct=len(late_ids))
    trace_n = int(ch.scalar("SELECT uniqExact(event_id) FROM lk.kappa_current_trace"))
    rep.add("trace table holds every accepted event exactly once", trace_n == int(kb["acc"]), trace_distinct=trace_n)

    # 3. Kafka: produced (generator logs) = end offsets per partition; committed = end
    bootstrap = cfg["kafka"]["bootstrap"]
    ends = _end_offsets(bootstrap, cfg["kafka"]["topic_transactions"])
    produced, src = _produced_by_partition(cfg)
    rep.add("Kafka: produced per partition = end offsets", produced == ends, produced=produced, end_offsets=ends, sources=src)
    rep.add("Kafka: committed offsets = end offsets", all(v["committed"] == v["end"] for v in lag["partitions"].values()),
            lag=lag["total"])
    total_stream = sum(ends.values())

    # 4. dedup proof + late accounting
    inj = _injected(cfg)
    rep.add("dedup: injected duplicates = duplicates discarded by Kappa", inj["duplicates"] == int(kb["dup"]),
            injected=inj["duplicates"], discarded=int(kb["dup"]))
    rep.add("every stream event is accepted, discarded as duplicate or in late-events",
            int(kb["acc"]) + int(kb["dup"]) + int(kb["late"]) == total_stream,
            accepted=int(kb["acc"]), duplicates=int(kb["dup"]), late=int(kb["late"]), stream_total=total_stream)
    rep.add("late data: injected beyond-limit events all landed in late-events (none disappeared)",
            inj["beyond_limit"] == len(late_ids), injected=inj["beyond_limit"], late_events_distinct=len(late_ids),
            injected_late_pos_within_limit=inj["late_pos"], note="within-limit late POS are covered by oracle equality")

    out = Path("results") / run_id
    out.mkdir(parents=True, exist_ok=True)
    doc = {"run_id": run_id, "architecture": "kappa", "version": version, "rule": rule, "cutoff": cutoff,
           "verified_at": datetime.now(timezone.utc).isoformat(), "passed": rep.ok, "checks": rep.checks}
    (out / "verification.json").write_text(json.dumps(doc, indent=2, default=str))
    print(f"[verify] {'ALL CHECKS PASSED' if rep.ok else 'VERIFICATION FAILED'} -> {out / 'verification.json'}", flush=True)
    return rep


def _drill_down(oracle: Oracle, ch: ClickHouse, metric: str, rule: str, cutoff: str | None, limit: int = 20) -> dict:
    exp = oracle.account_digests(metric, rule, cutoff)
    got = {r["a"]: int(r["d"]) for r in ch.query(f"SELECT a, sum({CH_HASH}) AS d FROM ({CH_ROW[metric]}) GROUP BY a")}
    bad = sorted(a for a in set(exp) | set(got) if exp.get(a) != got.get(a))
    sample = bad[:limit]
    detail = []
    for a in sample[:5]:
        o = oracle.rows_for_accounts(metric, [a], rule, cutoff)
        k = sorted(r["s"] for r in ch.query(f"SELECT s FROM ({CH_ROW[metric]}) WHERE a = {{a:String}}", a=a))
        detail.append({"account": a, "only_oracle": sorted(set(o) - set(k))[:5], "only_kappa": sorted(set(k) - set(o))[:5],
                       "trace": f"/trace/kappa/{a}?day=<day> vs /truth/trace/{a}?day=<day>"})
    return {"mismatched_accounts": len(bad), "sample": sample, "detail": detail}


def _late_event_ids(cfg: dict) -> set[str]:
    c = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap"], "group.id": f"verify-late-{time.time_ns()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    topic = cfg["kafka"]["topic_late_events"]
    try:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
        c.assign([TopicPartition(topic, 0, lo)])
        ids, seen = set(), 0
        while seen < hi - lo:
            m = c.poll(5)
            if m is None:
                break
            if m.error():
                continue
            seen += 1
            ids.add(json.loads(m.value())["event_id"])
        return ids
    finally:
        c.close()
