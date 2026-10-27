"""Prometheus metrics for the demo dashboard (scraped from query-api): ingest, consumer lag, Redis, ClickHouse, batch, verification."""
from __future__ import annotations

import glob
import json
import time
from pathlib import Path

from fastapi import APIRouter, Response

from lk_common.ch import ClickHouse
from lk_common.config import load_config
from lk_common.kafka_lag import consumer_lag

router = APIRouter()
cfg = load_config()
_cache: dict = {"t": 0, "text": ""}


def _lines() -> list[str]:
    out: list[str] = []
    topic, boot = cfg["kafka"]["topic_transactions"], cfg["kafka"]["bootstrap"]
    ch = ClickHouse()
    ends = 0
    for g in ("kappa-v1", "kappa-v2", "lambda-speed"):
        try:
            lag = consumer_lag(boot, g, topic)
            out.append(f'lk_consumer_lag{{group="{g}"}} {lag["total"]}')
            ends = sum(p["end"] for p in lag["partitions"].values())
        except Exception:  # noqa: BLE001 - group not running
            pass
    out.append(f"lk_kafka_end_offset_total {ends}")           # prometheus rate() of this = ingest rate
    t = time.time()
    try:
        ch.scalar("SELECT count() FROM lk.kappa_current_account")
        out.append(f"lk_clickhouse_query_seconds {time.time() - t:.4f}")
    except Exception:  # noqa: BLE001
        pass
    try:
        import redis
        r = redis.Redis(host="redis", socket_timeout=2)
        out.append(f"lk_redis_used_memory_bytes {r.info('memory')['used_memory']}")
        for k in ("stat:accepted", "stat:dup", "stat:old"):
            out.append(f'lk_speed_layer_events_total{{kind="{k.split(":")[1]}"}} {int(r.get(k) or 0)}')
    except Exception:  # noqa: BLE001
        pass
    try:
        run = ch.query("SELECT duration_ms, input_rows FROM lk.lambda_batch_runs WHERE status='ok' ORDER BY started_at DESC LIMIT 1")
        if run:
            out.append(f"lk_lambda_batch_duration_seconds {int(run[0]['duration_ms']) / 1000}")
            out.append(f"lk_lambda_batch_input_rows {int(run[0]['input_rows'])}")
    except Exception:  # noqa: BLE001
        pass
    for arch, pat in (("kappa", "results/*/verification.json"), ("lambda", "results/*/verification_lambda.json")):
        files = sorted(glob.glob(pat), key=lambda p: Path(p).stat().st_mtime)
        if files:
            d = json.loads(Path(files[-1]).read_text())
            bad = sum(c["status"] == "fail" for c in d["checks"])
            out.append(f'lk_verify_failed_checks{{arch="{arch}"}} {bad}')
            out.append(f'lk_verify_checks_total{{arch="{arch}"}} {len(d["checks"])}')
    return out


@router.get("/metrics")
def metrics():
    if time.time() - _cache["t"] > 4:                # scrapes are cached: Kafka admin calls are not free
        _cache.update(t=time.time(), text="\n".join(_lines()) + "\n")
    return Response(_cache["text"], media_type="text/plain; version=0.0.4")
