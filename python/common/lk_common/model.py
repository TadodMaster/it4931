"""Event model shared by generator, oracle, bench: Arrow schema, JSON encoding, merchant dimension."""
from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import pyarrow as pa

SCHEMA_VERSION = 1
TYPES = ["TOPUP", "PAYMENT", "REFUND", "TRANSFER_OUT", "TRANSFER_IN", "CASHBACK"]
CATEGORIES = ["FOOD", "TRANSPORT", "SHOPPING", "BILLS", "ENTERTAINMENT", "HEALTH", "EDUCATION", "OTHER"]

EVENT_SCHEMA = pa.schema([
    ("schema_version", pa.int32()),
    ("event_id", pa.string()),
    ("account_id", pa.string()),
    ("event_time", pa.timestamp("ms", tz="UTC")),
    ("produced_at", pa.timestamp("ms", tz="UTC")),
    ("channel", pa.string()),
    ("type", pa.string()),
    ("amount", pa.int64()),
    ("merchant_id", pa.string()),
    ("is_self_transfer", pa.bool_()),
    ("pos_offline", pa.bool_()),
])

# (category, share of merchants, lognormal mu, sigma) -> median payment e^mu VND
CATEGORY_PARAMS = {
    "FOOD": (0.26, 11.0, 0.7), "TRANSPORT": (0.12, 10.6, 0.6), "SHOPPING": (0.20, 12.0, 0.9),
    "BILLS": (0.10, 12.5, 0.8), "ENTERTAINMENT": (0.10, 11.8, 0.8), "HEALTH": (0.06, 12.2, 0.9),
    "EDUCATION": (0.04, 13.0, 0.8), "OTHER": (0.12, 11.5, 0.9),
}


def account_id(idx: int) -> str:
    return f"A{idx + 1:06d}"


def merchant_id(idx: int) -> str:
    return f"M{idx + 1:05d}"


def generate_merchants(n: int, seed: int) -> list[tuple[str, str, str]]:
    rng = np.random.default_rng(np.random.SeedSequence([seed, 7]))
    shares = np.array([CATEGORY_PARAMS[c][0] for c in CATEGORIES])
    cat = rng.choice(len(CATEGORIES), size=n, p=shares / shares.sum())
    return [(merchant_id(i), f"{CATEGORIES[c].title()} Shop {i + 1:05d}", CATEGORIES[c])
            for i, c in enumerate(cat)]


def write_merchants_csv(path: str | Path, merchants: list[tuple[str, str, str]]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["merchant_id", "merchant_name", "category"])
        w.writerows(merchants)


def read_merchants_csv(path: str | Path) -> list[tuple[str, str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        next(r)
        return [tuple(row) for row in r]


def ms_to_iso(ms: np.ndarray) -> np.ndarray:
    """int64 epoch millis -> array of 'YYYY-MM-DDTHH:MM:SS.mmmZ' strings."""
    s = ms.astype("datetime64[ms]").astype("U23")
    return np.char.add(s, "Z")


def table_to_json_rows(t: pa.Table) -> list[dict]:
    """Arrow table -> list of schema-conformant dicts (timestamps as ISO-8601 UTC strings)."""
    cols = {n: t.column(n).to_pylist() for n in t.column_names if n not in ("event_time", "produced_at")}
    cols["event_time"] = ms_to_iso(t.column("event_time").cast(pa.int64()).to_numpy()).tolist()
    pa_col = t.column("produced_at")
    iso = ms_to_iso(pa_col.fill_null(0).cast(pa.int64()).to_numpy()).tolist()
    valid = pa_col.is_valid().to_pylist()
    cols["produced_at"] = [v if ok else None for v, ok in zip(iso, valid)]
    names = list(EVENT_SCHEMA.names)
    return [dict(zip(names, vals)) for vals in zip(*(cols[n] for n in names))]
