"""Deterministic event synthesis: population, vectorised event columns, fault injection."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from lk_common.model import (CATEGORIES, CATEGORY_PARAMS, EVENT_SCHEMA, SCHEMA_VERSION, TYPES,
                             account_id, merchant_id)

TYPE_P = np.array([0.10, 0.60, 0.04, 0.09, 0.09, 0.08])
TOPUP_AMOUNTS = np.array([50_000, 100_000, 200_000, 500_000, 1_000_000, 2_000_000])
TOPUP_P = np.array([0.15, 0.25, 0.25, 0.20, 0.10, 0.05])
HOUR_W = np.array([0.2, 0.1, 0.1, 0.1, 0.2, 0.5, 1.0, 1.6, 1.8, 1.4, 1.3, 1.8,
                   2.2, 1.6, 1.3, 1.3, 1.5, 2.0, 2.4, 2.2, 1.6, 1.0, 0.6, 0.3])
DOW_W = np.array([1.0, 1.0, 1.0, 1.0, 1.1, 1.25, 1.2])  # Monday..Sunday
POS_SHARE = 0.45
SELF_TRANSFER_SHARE = 0.25
HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
T = {name: i for i, name in enumerate(TYPES)}


@dataclass
class Population:
    n_accounts: int
    n_probes: int
    account_ids: np.ndarray        # normal accounts, U7
    probe_ids: list[str]
    account_cdf: np.ndarray
    merchant_ids: np.ndarray
    merchant_cat: np.ndarray       # category index per merchant
    merchant_cdf: np.ndarray
    zipf: float

    @classmethod
    def build(cls, seed: int, n_accounts: int, n_probes: int, merchants: list, zipf: float) -> "Population":
        rng = np.random.default_rng(np.random.SeedSequence([seed, 3]))
        w = (np.arange(1, n_accounts + 1) ** -zipf)[rng.permutation(n_accounts)]
        mw = (np.arange(1, len(merchants) + 1) ** -0.9)[rng.permutation(len(merchants))]
        cat_idx = {c: i for i, c in enumerate(CATEGORIES)}
        return cls(
            n_accounts=n_accounts, n_probes=n_probes,
            account_ids=np.array([account_id(i) for i in range(n_accounts)]),
            probe_ids=[account_id(n_accounts + i) for i in range(n_probes)],
            account_cdf=np.cumsum(w / w.sum()),
            merchant_ids=np.array([m[0] for m in merchants]),
            merchant_cat=np.array([cat_idx[m[2]] for m in merchants]),
            merchant_cdf=np.cumsum(mw / mw.sum()), zipf=zipf)


def make_uuids(rng: np.random.Generator, n: int) -> tuple[pa.Array, np.ndarray]:
    """Deterministic RFC-4122 v4-shaped ids. Also returns a uint64 per id (checksum material)."""
    raw = np.frombuffer(rng.bytes(16 * n), dtype=np.uint8).reshape(n, 16).copy()
    raw[:, 6] = (raw[:, 6] & 0x0F) | 0x40
    raw[:, 8] = (raw[:, 8] & 0x3F) | 0x80
    hx = pa.array(np.frombuffer(raw.tobytes().hex().encode(), dtype="S32")).cast(pa.string())
    parts = [pc.utf8_slice_codeunits(hx, s, e) for s, e in ((0, 8), (8, 12), (12, 16), (16, 20), (20, 32))]
    head = np.ascontiguousarray(raw[:, :8]).view(">u8").reshape(n).astype(np.uint64)
    return pc.binary_join_element_wise(*parts, "-"), head


def id_checksum(heads: np.ndarray) -> int:
    """Σ first-8-bytes (big endian) of every event_id, mod 2^64. Same definition in SQL later."""
    return int(np.sum(heads, dtype=np.uint64))


def day_weights(day_starts_ms: np.ndarray, first_day_ms: int, total_days: int) -> np.ndarray:
    idx = (day_starts_ms - first_day_ms) // DAY_MS
    dow = ((day_starts_ms // DAY_MS) + 3) % 7          # 1970-01-01 was a Thursday -> index 3
    growth = 0.6 + 0.8 * idx / max(total_days - 1, 1)
    return DOW_W[dow] * growth


def sample_time_in_days(rng, n: int, day_starts_ms: np.ndarray, weights: np.ndarray) -> np.ndarray:
    d = rng.choice(len(day_starts_ms), size=n, p=weights / weights.sum())
    h = rng.choice(24, size=n, p=HOUR_W / HOUR_W.sum())
    return day_starts_ms[d] + h * HOUR_MS + rng.integers(0, HOUR_MS, size=n)


def build_events(rng: np.random.Generator, pop: Population, n: int) -> dict:
    """Everything except time and faults. Returns plain numpy columns."""
    acct = np.searchsorted(pop.account_cdf, rng.random(n)).clip(max=pop.n_accounts - 1)
    typ = rng.choice(len(TYPES), size=n, p=TYPE_P)
    has_m = (typ == T["PAYMENT"]) | (typ == T["REFUND"])
    channel_pos = has_m & (rng.random(n) < POS_SHARE)
    mi = np.searchsorted(pop.merchant_cdf, rng.random(n)).clip(max=len(pop.merchant_ids) - 1)
    cat = pop.merchant_cat[mi]
    mu = np.array([CATEGORY_PARAMS[c][1] for c in CATEGORIES])[cat]
    sg = np.array([CATEGORY_PARAMS[c][2] for c in CATEGORIES])[cat]
    z = rng.standard_normal(n)
    x = np.exp(mu + sg * z)
    x = np.where(typ == T["REFUND"], x * rng.uniform(0.3, 1.0, n), x)
    transfer = (typ == T["TRANSFER_OUT"]) | (typ == T["TRANSFER_IN"])
    x = np.where(transfer, np.exp(12.5 + 1.0 * z), x)
    x = np.where(typ == T["CASHBACK"], np.exp(9.2 + 0.6 * z), x)
    x = np.where(typ == T["TOPUP"], rng.choice(TOPUP_AMOUNTS, size=n, p=TOPUP_P), x)
    amount = np.clip(np.round(x / 1000) * 1000, 1000, 50_000_000).astype(np.int64)
    self_t = (typ == T["TRANSFER_OUT"]) & (rng.random(n) < SELF_TRANSFER_SHARE)
    return {"account_idx": acct, "type_idx": typ, "pos": channel_pos, "merchant_idx": np.where(has_m, mi, -1),
            "amount": amount, "self": self_t}


@dataclass
class FaultConfig:
    late_pos_ratio: float = 0.03
    beyond_limit_ratio: float = 0.0
    duplicate_ratio: float = 0.01
    jitter_ms: int = 2000
    pos_max_late_ms: int = 2 * HOUR_MS
    beyond_min_ms: int = 2 * HOUR_MS + 30 * 60_000   # > allowed lateness (limit + margin)
    beyond_max_ms: int = 4 * HOUR_MS


def apply_faults(rng: np.random.Generator, cols: dict, f: FaultConfig) -> dict:
    """delay_ms: arrival - event_time per logical event; dup_idx/dup_delay_ms: producer retries."""
    n = len(cols["amount"])
    pos = cols["pos"]
    delay = rng.integers(0, f.jitter_ms + 1, size=n) if f.jitter_ms > 0 else np.zeros(n, dtype=np.int64)
    late = pos & (rng.random(n) < f.late_pos_ratio)
    delay = np.where(late, rng.integers(60_000, f.pos_max_late_ms + 1, size=n), delay)
    beyond = pos & ~late & (rng.random(n) < f.beyond_limit_ratio)
    delay = np.where(beyond, rng.integers(f.beyond_min_ms, f.beyond_max_ms + 1, size=n), delay)
    dup_idx = np.flatnonzero(rng.random(n) < f.duplicate_ratio)
    dup_delay = rng.integers(1_000, 600_000, size=len(dup_idx))
    return {"delay_ms": delay.astype(np.int64), "pos_offline": late | beyond, "late": late, "beyond": beyond,
            "dup_idx": dup_idx, "dup_delay_ms": dup_delay.astype(np.int64)}


def to_arrow(pop: Population, cols: dict, event_ids: pa.Array, event_time_ms: np.ndarray,
             pos_offline: np.ndarray, produced_at_ms: np.ndarray | None = None) -> pa.Table:
    n = len(event_time_ms)
    typ = np.array(TYPES)[cols["type_idx"]]
    mi = cols["merchant_idx"]
    merchant = pa.array(np.where(mi >= 0, pop.merchant_ids[np.maximum(mi, 0)], ""), type=pa.string())
    merchant = pc.if_else(pa.array(mi >= 0), merchant, pa.scalar(None, pa.string()))
    ts = pa.timestamp("ms", tz="UTC")
    produced = (pa.nulls(n, ts) if produced_at_ms is None
                else pa.array(produced_at_ms, type=pa.int64()).cast(ts))
    return pa.Table.from_arrays([
        pa.array(np.full(n, SCHEMA_VERSION, dtype=np.int32)),
        event_ids,
        pa.array(pop.account_ids[cols["account_idx"]]),
        pa.array(event_time_ms, type=pa.int64()).cast(ts),
        produced,
        pa.array(np.where(cols["pos"], "POS", "APP")),
        pa.array(typ),
        pa.array(cols["amount"]),
        merchant,
        pa.array(cols["self"]),
        pa.array(pos_offline),
    ], schema=EVENT_SCHEMA)
