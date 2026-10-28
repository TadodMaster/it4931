"""bench verify --arch lambda: batch view and serving layer (batch + speed) against the oracle, plus master-dataset proofs."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from lk_common.ch import ClickHouse
from lk_common.config import load_config
from lk_oracle.core import Oracle

from .verify import CH_HASH, METRICS, Report, _end_offsets, _injected, _produced_by_partition, wait_for_lag_zero

CH_ROW_L = {
    "balance": "SELECT account_id AS a, concat(account_id, '|', toString(spend)) AS s FROM lk.lambda_batch_cur WHERE kind = 'balance'",
    "spend_day": ("SELECT account_id AS a, concat(account_id, '|', k1, '|', toString(spend), '|', toString(tx_count)) AS s "
                  "FROM lk.lambda_batch_cur WHERE kind = 'day'"),
    "spend_cat_month": ("SELECT account_id AS a, concat(account_id, '|', k1, '|', k2, '|', toString(spend)) AS s "
                        "FROM lk.lambda_batch_cur WHERE kind = 'cat_month'"),
}


def _h(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode()).digest()[:8], "big")


def _digest(rows: list[str]) -> dict:
    return {"rows": len(rows), "digest": sum(_h(s) for s in rows) % (1 << 64)}


def lambda_meta(ch: ClickHouse) -> dict | None:
    r = ch.query("SELECT k1, k2, tx_count FROM lk.lambda_batch_cur WHERE kind = 'meta'")
    return {"t_k": r[0]["k1"], "rule": r[0]["k2"], "input_rows": int(r[0]["tx_count"])} if r else None


def merged_rows(ch: ClickHouse, min_day: str | None = None) -> dict[str, list[str]]:
    """Serving-layer rows (batch + Redis speed layer) as digest strings. min_day limits the day rows (used by B7)."""
    from lk_api import lambda_ as lam
    r = lam.rds()
    bal: dict[str, int] = {}
    day: dict[tuple[str, str], list[int]] = {}
    cat: dict[tuple[str, str, str], int] = {}
    where = " AND (kind != 'day' OR k1 >= '" + min_day + "')" if min_day else ""
    for row in ch.query("SELECT kind, account_id, k1, k2, spend, tx_count FROM lk.lambda_batch_cur WHERE kind != 'meta'" + where):
        k, a, k1, k2 = row["kind"], row["account_id"], row["k1"], row["k2"]
        sp, c = int(row["spend"]), int(row["tx_count"])
        if k == "balance":
            bal[a] = sp
        elif k == "day":
            day[(a, k1)] = [sp, c]
        else:
            cat[(a, k1, k2)] = sp
    for d in lam.rt_days():
        for a, v in r.hscan_iter(f"rt:bal:{d}"):
            bal[a] = bal.get(a, 0) + int(v)
        spend = dict(r.hscan_iter(f"rt:spend:{d}"))
        cnt = dict(r.hscan_iter(f"rt:cnt:{d}"))
        for a, v in spend.items():
            cur = day.setdefault((a, d), [0, 0])
            cur[0] += int(v)
            cur[1] += int(cnt.get(a, 0))
        for f, v in r.hscan_iter(f"rt:cat:{d}"):
            a, c, m = f.split("|")
            cat[(a, c, m)] = cat.get((a, c, m), 0) + int(v)
    return {"balance": [f"{a}|{v}" for a, v in bal.items()],
            "spend_day": [f"{a}|{d}|{v[0]}|{v[1]}" for (a, d), v in day.items()],
            "spend_cat_month": [f"{a}|{c}|{m}|{v}" for (a, c, m), v in cat.items()]}


RT_DDL = ("CREATE TABLE IF NOT EXISTS lk.lambda_rt_tmp (kind LowCardinality(String), account_id String, k1 String, k2 String, "
          "spend Int64, tx_count UInt64) ENGINE = Memory")


def _rt_rows() -> list[str]:
    """Speed-layer buckets from Redis as batch-shaped rows (kind, account, k1, k2, spend, tx_count)."""
    from lk_api import lambda_ as lam
    r = lam.rds()
    rows = []
    for d in lam.rt_days():
        for a, v in r.hscan_iter(f"rt:bal:{d}"):
            rows.append(json.dumps({"kind": "balance", "account_id": a, "k1": "", "k2": "", "spend": int(v), "tx_count": 0}))
        cnt = dict(r.hscan_iter(f"rt:cnt:{d}"))
        for a, v in r.hscan_iter(f"rt:spend:{d}"):
            rows.append(json.dumps({"kind": "day", "account_id": a, "k1": d, "k2": "", "spend": int(v), "tx_count": int(cnt.get(a, 0))}))
        for f, v in r.hscan_iter(f"rt:cat:{d}"):
            a, c, m = f.split("|")
            rows.append(json.dumps({"kind": "cat_month", "account_id": a, "k1": c, "k2": m, "spend": int(v), "tx_count": 0}))
    return rows


def serving_matches_oracle(ch: ClickHouse, oracle: Oracle, rule: str) -> dict:
    """Digest of what the serving layer returns for EVERY account (batch + speed layer) vs the oracle at wall-clock now.
    The merge runs inside ClickHouse (speed-layer rows are loaded into a temp table) so nothing big crosses the wire."""
    ch.execute(RT_DDL)
    ch.execute("TRUNCATE TABLE lk.lambda_rt_tmp")
    rt = _rt_rows()
    for i in range(0, len(rt), 50_000):
        ch.insert_json_rows("lk.lambda_rt_tmp", rt[i:i + 50_000])
    u = "(SELECT kind, account_id, k1, k2, spend, tx_count FROM lk.lambda_batch_cur WHERE kind != 'meta' UNION ALL SELECT * FROM lk.lambda_rt_tmp)"
    q = {
        "balance": f"SELECT concat(account_id, '|', toString(sp)) AS s FROM (SELECT account_id, sum(spend) AS sp FROM {u} WHERE kind = 'balance' GROUP BY account_id)",
        "spend_day": (f"SELECT concat(account_id, '|', k1, '|', toString(sp), '|', toString(c)) AS s FROM "
                      f"(SELECT account_id, k1, sum(spend) AS sp, sum(tx_count) AS c FROM {u} WHERE kind = 'day' GROUP BY account_id, k1)"),
        "spend_cat_month": (f"SELECT concat(account_id, '|', k1, '|', k2, '|', toString(sp)) AS s FROM "
                            f"(SELECT account_id, k1, k2, sum(spend) AS sp FROM {u} WHERE kind = 'cat_month' GROUP BY account_id, k1, k2)"),
    }
    out = {}
    for metric in METRICS:
        exp = oracle.digest(metric, rule, None)
        r = ch.query(f"SELECT count() AS n, sum({CH_HASH}) AS d FROM ({q[metric]})")[0]
        got = {"rows": int(r["n"]), "digest": int(r["d"])}
        out[metric] = {"ok": exp == got, "oracle": exp, "serving": got}
    ch.execute("TRUNCATE TABLE lk.lambda_rt_tmp")
    return out


def verify_lambda(run_id: str = "local", lag_timeout: float = 300, merged: bool = True) -> Report:
    cfg = load_config()
    paths = cfg["data"]["paths"]
    ch = ClickHouse()
    rep = Report()
    meta = lambda_meta(ch)
    rep.add("lambda batch view active", meta is not None, meta=meta)
    if meta is None:
        return rep
    rule = meta["rule"]
    lag = wait_for_lag_zero(cfg, "lambda-speed", lag_timeout)
    rep.add("speed layer caught up (consumer lag 0)", lag["total"] == 0, lag=lag["total"], waited_s=lag["waited_s"])
    from lk_oracle.core import compact_truth
    compact_truth(paths["truth_live"])
    oracle = Oracle([paths["truth_backfill"], paths["truth_live"]], paths["merchants_csv"])

    # 1a. batch view == oracle at the batch's own cutoff t_k
    for metric in METRICS:
        exp = oracle.digest(metric, rule, meta["t_k"])
        got = ch.query(f"SELECT count() AS n, sum({CH_HASH}) AS d FROM ({CH_ROW_L[metric]})")[0]
        got = {"rows": int(got["n"]), "digest": int(got["d"])}
        rep.add(f"batch view = oracle at t_k: {metric} ({rule}, t_k={meta['t_k']})", exp == got, oracle=exp, batch=got)
    t = oracle.totals(rule, meta["t_k"])
    rep.add("batch input rows (deduped) = oracle events < t_k", meta["input_rows"] == t["events"],
            batch=meta["input_rows"], oracle=t["events"])

    # 1b. serving layer (batch + speed) == oracle now, for every account
    if merged:
        for metric, r in serving_matches_oracle(ch, oracle, rule).items():
            rep.add(f"serving (batch + speed) = oracle now: {metric}", r["ok"], oracle=r["oracle"], serving=r["serving"])

    # 2. Kafka + master dataset conservation, dedup proof, immutability
    ends = _end_offsets(cfg["kafka"]["bootstrap"], cfg["kafka"]["topic_transactions"])
    produced, _ = _produced_by_partition(cfg)
    rep.add("Kafka: produced per partition = end offsets", produced == ends, produced=produced, end_offsets=ends)
    sf = Path(paths["root"], "master_stats.jsonl")
    stats = [json.loads(line) for line in sf.read_text().splitlines()] if sf.exists() else []
    inj = _injected(cfg)
    if stats:
        cur = stats[-1]
        full = oracle.totals(rule, None)
        rep.add("master rows = every stream event (Parquet backfill + archived Kafka tail)", cur["rows"] == sum(ends.values()),
                master_rows=cur["rows"], kafka_end_offsets=sum(ends.values()))
        rep.add("master distinct event ids = oracle logical events", cur["distinct_event_ids"] == full["events"],
                master=cur["distinct_event_ids"], oracle=full["events"])
        rep.add("master event-id checksum = oracle checksum", cur["id_checksum"] == oracle.id_checksum(None),
                master=cur["id_checksum"], oracle=oracle.id_checksum(None))
        rep.add("dedup: duplicates injected = duplicate rows in master (the batch removes them)",
                cur["rows"] - cur["distinct_event_ids"] == inj["duplicates"], injected=inj["duplicates"],
                master_duplicate_rows=cur["rows"] - cur["distinct_event_ids"])
        rep.add("immutability: master snapshots are append-only", cur["snapshot_operations"] == ["append"],
                operations=cur["snapshot_operations"], snapshots=cur["snapshots"])
        if len(stats) >= 2:
            prev = stats[-2]
            grown = cur["rows"] >= prev["rows"] and cur["snapshots"] >= prev["snapshots"]
            frozen = [d for d, v in prev["per_day"].items() if d < max(prev["per_day"]) and cur["per_day"].get(d) != v]
            rep.add("immutability: rows only grow; per-day checksums of history unchanged since the previous record",
                    grown and not frozen, rows=[prev["rows"], cur["rows"]], changed_days=frozen[:5])
    else:
        rep.add("master statistics available (run spark-master-stats first)", False)

    out = Path("results") / run_id
    out.mkdir(parents=True, exist_ok=True)
    doc = {"run_id": run_id, "architecture": "lambda", "rule": rule, "batch_t_k": meta["t_k"],
           "verified_at": datetime.now(timezone.utc).isoformat(), "passed": rep.ok, "checks": rep.checks}
    (out / "verification_lambda.json").write_text(json.dumps(doc, indent=2, default=str))
    print(f"[verify] {'ALL CHECKS PASSED' if rep.ok else 'VERIFICATION FAILED'} -> {out / 'verification_lambda.json'}", flush=True)
    return rep
