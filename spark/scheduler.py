"""Runs the nightly batch once per simulated period P (default 24 sim hours = 10 real minutes at 144x)."""
from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import redis

from lk_common.config import SimClock, TimeConfig, load_config
from lk_spark.ch import ClickHouse


def boundary(clock: SimClock, period: timedelta) -> datetime:
    day0 = clock.sim_now().replace(hour=0, minute=0, second=0, microsecond=0)
    now = clock.sim_now()
    k = int((now - day0) / period)
    return day0 + k * period


def main() -> int:
    cfg = load_config()
    tc = TimeConfig.from_config(cfg)
    clock = SimClock.load_or_create(tc, Path(cfg["data"]["paths"]["root"]) / "clock.json")
    r = redis.Redis(host=os.environ.get("REDIS_HOST", "redis"), decode_responses=True)
    base = f"http://{cfg['clickhouse']['host']}:{cfg['clickhouse']['http_port']}"
    ch = ClickHouse(base)
    period = tc.batch_period_sim
    grace = timedelta(minutes=cfg["time"].get("batch_grace_sim_minutes", 0))
    print(f"[scheduler] period={period} (= {tc.sim_to_real_seconds(period):.0f} real s), grace={grace} (= {tc.sim_to_real_seconds(grace):.0f} real s)", flush=True)
    while True:
        try:
            tk = boundary(clock, period)
            if clock.sim_now() - tk < grace:          # let the archiver / late POS events settle before reading the master
                time.sleep(2)
                continue
            last = ch.scalar("SELECT toUnixTimestamp(max(t_k)) FROM lk.lambda_batch_runs WHERE status='ok'") \
                if ch.scalar("SELECT count() FROM system.tables WHERE database='lk' AND name='lambda_batch_runs'") != "0" else "0"
            last_ts = int(last or 0)
            if int(tk.timestamp()) > last_ts:
                rule = r.get("lambda:spark_rule") or "v1"
                rid = tk.strftime("%Y%m%dT%H%M%S") + "_" + datetime.now(timezone.utc).strftime("%H%M%S")
                print(f"[scheduler] sim boundary {tk.isoformat()} crossed -> batch run {rid} rule={rule}", flush=True)
                subprocess.run(["/opt/spark/bin/spark-submit", "--driver-memory", "768m", "/app/spark/batch.py",
                                "--t-k", tk.isoformat(), "--rule", rule, "--run-id", rid], check=False)
        except Exception as e:  # noqa: BLE001 - keep the scheduler alive
            print(f"[scheduler] error: {e}", flush=True)
        time.sleep(5)


if __name__ == "__main__":
    sys.exit(main())
