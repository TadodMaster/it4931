"""Write-ahead truth log + Kafka producer. Shared by live mode, backfill loader and the bench marker API."""
from __future__ import annotations

import os
import threading
import time
from collections import Counter
from pathlib import Path

import numpy as np
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Producer

from lk_common.model import EVENT_SCHEMA, table_to_json_rows


class TruthWriter:
    """Appends logical events (exactly once each) to truth/live/<run_id>/part-*.parquet.

    Files are written to a temp name and renamed, so the oracle never sees a partial file.
    """

    def __init__(self, root: str | Path, run_id: str):
        self.dir = Path(root) / run_id
        self.dir.mkdir(parents=True, exist_ok=True)
        self.seq = len(list(self.dir.glob("part-*.parquet")))
        self.rows = 0

    def append(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        name = f"part-{self.seq:06d}-{time.time_ns()}.parquet"
        tmp = self.dir / (name + ".tmp")
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, self.dir / name)
        self.seq += 1
        self.rows += table.num_rows


class KafkaEmitter:
    def __init__(self, bootstrap: str, topic: str, linger_ms: int = 5):
        self.topic = topic
        self.p = Producer({"bootstrap.servers": bootstrap, "linger.ms": linger_ms, "compression.type": "lz4",
                           "enable.idempotence": True, "queue.buffering.max.messages": 500_000})
        self.delivered: Counter = Counter()
        self.errors: list[str] = []
        self._lock = threading.Lock()

    def _cb(self, err, msg):
        with self._lock:
            if err:
                self.errors.append(str(err))
            else:
                self.delivered[msg.partition()] += 1

    def send(self, key: str, value: bytes) -> None:
        while True:
            try:
                self.p.produce(self.topic, key=key, value=value, on_delivery=self._cb)
                return
            except BufferError:
                self.p.poll(0.05)

    def send_rows(self, rows: list[dict]) -> list[bytes]:
        payloads = [orjson.dumps(r) for r in rows]
        for r, b in zip(rows, payloads):
            self.send(r["account_id"], b)
        self.p.poll(0)
        return payloads

    def flush(self, timeout: float = 60) -> None:
        left = self.p.flush(timeout)
        if left:
            raise RuntimeError(f"{left} messages not delivered")
        if self.errors:
            raise RuntimeError(f"delivery errors: {self.errors[:3]}")

    def counts(self) -> dict:
        with self._lock:
            return {"total": sum(self.delivered.values()),
                    "by_partition": {str(k): v for k, v in sorted(self.delivered.items())}}


def with_produced_at(table: pa.Table, produced_ms: int) -> pa.Table:
    col = pa.array(np.full(table.num_rows, produced_ms, dtype=np.int64)).cast(pa.timestamp("ms", tz="UTC"))
    return table.set_column(EVENT_SCHEMA.get_field_index("produced_at"), "produced_at", col)


def emit_marker(cfg: dict, truth: TruthWriter, emitter: KafkaEmitter, *, account: str, type_: str, amount: int,
                merchant: str | None = None, channel: str = "APP", is_self: bool = False,
                event_time_ms: int | None = None, pos_offline: bool = False) -> dict:
    """Inject one benchmark marker transaction (truth first, then Kafka). Returns the sent JSON row."""
    from datetime import datetime, timezone
    from uuid import uuid4

    from lk_common.config import SimClock, TimeConfig
    tc = TimeConfig.from_config(cfg)
    now = int(time.time() * 1000)
    if event_time_ms is None:
        clock = SimClock.load_or_create(tc, Path(cfg["data"]["paths"]["root"]) / "clock.json")
        event_time_ms = int(clock.sim_now().timestamp() * 1000)
    t = pa.table({
        "schema_version": pa.array([1], pa.int32()), "event_id": [str(uuid4())], "account_id": [account],
        "event_time": pa.array([event_time_ms], pa.int64()).cast(pa.timestamp("ms", tz="UTC")),
        "produced_at": pa.array([now], pa.int64()).cast(pa.timestamp("ms", tz="UTC")),
        "channel": [channel], "type": [type_], "amount": pa.array([amount], pa.int64()),
        "merchant_id": [merchant], "is_self_transfer": [is_self], "pos_offline": [pos_offline],
    }, schema=EVENT_SCHEMA)
    truth.append(t.set_column(EVENT_SCHEMA.get_field_index("produced_at"), "produced_at",
                              pa.nulls(1, pa.timestamp("ms", tz="UTC"))))
    row = table_to_json_rows(t)[0]
    emitter.send_rows([row])
    emitter.flush()
    return row
