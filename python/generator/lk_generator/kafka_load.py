"""Load the backfill stream Parquet into Kafka (Kappa source) and the merchants dimension into its compacted topic."""
from __future__ import annotations

import json
import time
from pathlib import Path

import orjson
import pyarrow.parquet as pq

from lk_common.model import read_merchants_csv, table_to_json_rows

from .emit import KafkaEmitter


def load_merchants(cfg: dict) -> int:
    em = KafkaEmitter(cfg["kafka"]["bootstrap"], cfg["kafka"]["topic_merchants"])
    rows = read_merchants_csv(cfg["data"]["paths"]["merchants_csv"])
    for mid, name, cat in rows:
        em.send(mid, orjson.dumps({"merchant_id": mid, "merchant_name": name, "category": cat}))
    em.flush()
    print(f"[load] merchants: {len(rows):,} -> {cfg['kafka']['topic_merchants']}", flush=True)
    return len(rows)


def load_stream(cfg: dict, months_limit: int | None = None) -> dict:
    em = KafkaEmitter(cfg["kafka"]["bootstrap"], cfg["kafka"]["topic_transactions"], linger_ms=20)
    root = Path(cfg["data"]["paths"]["stream_backfill"])
    files = sorted(root.glob("month=*/part-*.parquet"))[:months_limit]
    t0, sent = time.time(), 0
    for f in files:
        for batch in pq.ParquetFile(f).iter_batches(batch_size=100_000):
            import pyarrow as pa
            em.send_rows(table_to_json_rows(pa.Table.from_batches([batch])))
            sent += batch.num_rows
        print(f"[load] {f.parent.name}: cumulative {sent:,} events, {sent / (time.time() - t0):,.0f} ev/s", flush=True)
    em.flush(300)
    manifest = {"files": len(files), "sent": sent, "delivered": em.counts(), "seconds": round(time.time() - t0, 1),
                "topic": cfg["kafka"]["topic_transactions"]}
    assert manifest["delivered"]["total"] == sent, "delivery count mismatch"
    out = Path(cfg["data"]["paths"]["root"]) / "kafka_load_manifest.json"
    out.write_text(json.dumps(manifest, indent=2))
    print(f"[load] done: {sent:,} events -> {out}", flush=True)
    return manifest
