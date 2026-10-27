"""History generation: one Parquet file per simulated month, for both the stream and the truth log."""
from __future__ import annotations

import json
import time
from calendar import monthrange
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from lk_common.config import TimeConfig
from lk_common.model import generate_merchants, read_merchants_csv, write_merchants_csv

from .population import (DAY_MS, FaultConfig, Population, apply_faults, build_events, day_weights, id_checksum,
                         make_uuids, sample_time_in_days, to_arrow)


def month_starts(tc: TimeConfig) -> list[datetime]:
    out, d = [], tc.history_start
    while d < tc.history_end:
        out.append(d)
        d = d.replace(year=d.year + (d.month == 12), month=d.month % 12 + 1)
    return out


def _ms(d: datetime) -> int:
    return int(d.timestamp() * 1000)


def split_counts(weights: np.ndarray, total: int) -> np.ndarray:
    """Largest-remainder split: deterministic and sums exactly to total."""
    raw = weights / weights.sum() * total
    base = np.floor(raw).astype(np.int64)
    rem = total - base.sum()
    base[np.argsort(-(raw - base), kind="stable")[:rem]] += 1
    return base


def ensure_merchants(cfg: dict) -> list:
    p = Path(cfg["data"]["paths"]["merchants_csv"])
    if not p.exists():
        write_merchants_csv(p, generate_merchants(cfg["data"]["num_merchants"], 42))  # fixed seed by spec
    return read_merchants_csv(p)


def build_population(cfg: dict) -> Population:
    d = cfg["data"]
    return Population.build(cfg["seed"], d["num_accounts"], d["num_probe_accounts"], ensure_merchants(cfg),
                            d["zipf_exponent"])


def fault_config(cfg: dict, tc: TimeConfig) -> FaultConfig:
    g = cfg["generator"]
    lim = int(tc.pos_max_lateness_sim.total_seconds() * 1000)
    allowed = int(tc.allowed_lateness_sim.total_seconds() * 1000)
    return FaultConfig(late_pos_ratio=g["late_pos_ratio"], beyond_limit_ratio=g["beyond_limit_ratio"],
                       duplicate_ratio=g["duplicate_ratio"], jitter_ms=g["out_of_order_jitter_ms"],
                       pos_max_late_ms=lim, beyond_min_ms=allowed + 20 * 60_000, beyond_max_ms=allowed + 2 * 3_600_000)


def generate_month(cfg: dict, pop: Population, tc: TimeConfig, m_idx: int, month: datetime, n: int,
                   faults: FaultConfig) -> tuple[pa.Table, pa.Table, dict]:
    rng = np.random.default_rng(np.random.SeedSequence([cfg["seed"], 1, m_idx]))
    days = monthrange(month.year, month.month)[1]
    m0 = _ms(month)
    day_starts = m0 + np.arange(days, dtype=np.int64) * DAY_MS
    total_days = (tc.history_end - tc.history_start).days
    w = day_weights(day_starts, _ms(tc.history_start), total_days)
    et = sample_time_in_days(rng, n, day_starts, w)
    cols = build_events(rng, pop, n)
    f = apply_faults(rng, cols, faults)
    ids, heads = make_uuids(rng, n)

    truth = to_arrow(pop, cols, ids, et, f["pos_offline"])
    # stream = arrival-ordered: originals + duplicate re-sends. Arrival clamped to month end (documented).
    month_end = m0 + days * DAY_MS - 1
    arr = np.minimum(et + f["delay_ms"], month_end)
    di = f["dup_idx"]
    all_idx = np.concatenate([np.arange(n), di])
    all_arr = np.concatenate([arr, np.minimum(arr[di] + f["dup_delay_ms"], month_end)])
    order = np.argsort(all_arr, kind="stable")
    stream = truth.take(pa.array(all_idx[order]))
    truth = truth.take(pc.sort_indices(truth, sort_keys=[("account_id", "ascending"), ("event_time", "ascending")]))
    stats = {"month": month.strftime("%Y-%m"), "logical": int(n), "stream": int(len(all_idx)),
             "late_pos": int(f["late"].sum()), "beyond_limit": int(f["beyond"].sum()), "duplicates": int(len(di)),
             "id_checksum": id_checksum(heads)}
    return truth, stream, stats


def run_backfill(cfg: dict, run_id: str | None = None, months_limit: int | None = None) -> dict:
    tc = TimeConfig.from_config(cfg)
    pop = build_population(cfg)
    faults = fault_config(cfg, tc)
    paths = cfg["data"]["paths"]
    months = month_starts(tc)[:months_limit]
    total_days = (tc.history_end - tc.history_start).days
    mw = np.array([day_weights(_ms(m) + np.arange(monthrange(m.year, m.month)[1], dtype=np.int64) * DAY_MS,
                               _ms(tc.history_start), total_days).sum() for m in months])
    counts = split_counts(mw, cfg["data"]["history_events"])
    t0, per_month = time.time(), []
    for i, (m, n) in enumerate(zip(months, counts)):
        truth, stream, st = generate_month(cfg, pop, tc, i, m, int(n), faults)
        tag = f"month={st['month']}"
        for root, tbl in ((paths["truth_backfill"], truth), (paths["stream_backfill"], stream)):
            d = Path(root) / tag
            d.mkdir(parents=True, exist_ok=True)
            pq.write_table(tbl, d / "part-0.parquet", compression="zstd", row_group_size=250_000)
        per_month.append(st)
        print(f"[backfill] {st['month']}: {st['logical']:>9,} logical, {st['stream']:>9,} stream "
              f"({st['late_pos']:,} late, {st['duplicates']:,} dup)  {time.time() - t0:6.1f}s", flush=True)
    manifest = {
        "seed": cfg["seed"], "history_events": int(counts.sum()), "months": per_month,
        "logical_events": sum(s["logical"] for s in per_month), "stream_events": sum(s["stream"] for s in per_month),
        "late_pos": sum(s["late_pos"] for s in per_month), "duplicates": sum(s["duplicates"] for s in per_month),
        "beyond_limit": sum(s["beyond_limit"] for s in per_month),
        "id_checksum": sum(s["id_checksum"] for s in per_month) % (1 << 64),
        "faults": vars(faults), "history_start": tc.history_start.isoformat(), "history_end": tc.history_end.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(), "seconds": round(time.time() - t0, 1),
    }
    Path(paths["manifest"]).write_text(json.dumps(manifest, indent=2))
    if run_id:
        out = Path("results") / run_id
        out.mkdir(parents=True, exist_ok=True)
        (out / "injections.json").write_text(json.dumps({"backfill": {k: v for k, v in manifest.items()
                                                                     if k != "months"}}, indent=2))
    print(f"[backfill] done -> {paths['manifest']}", flush=True)
    return manifest
