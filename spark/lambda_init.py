"""One-time Lambda bootstrap: merchant dimension + initial batch boundary t_k in Redis (idempotent)."""
from __future__ import annotations

import csv
import os
import sys

import redis

from lk_common.config import TimeConfig, load_config


def main() -> int:
    cfg = load_config()
    r = redis.Redis(host=os.environ.get("REDIS_HOST", "redis"), decode_responses=True)
    with open(cfg["data"]["paths"]["merchants_csv"], newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        next(rd)
        m = [tuple(row) for row in rd]
    r.hset("merchants", mapping={mid: cat for mid, _, cat in m})
    if r.get("lambda:tk") is None:
        tc = TimeConfig.from_config(cfg)
        r.set("lambda:tk", int(tc.history_end.timestamp() * 1000))   # until the first batch: everything after history is "realtime"
    r.setnx("lambda:spark_rule", "v1")
    print(f"[lambda-init] merchants={len(m):,} tk={r.get('lambda:tk')} spark_rule={r.get('lambda:spark_rule')}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
