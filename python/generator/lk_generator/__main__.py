"""CLI: python -m lk_generator {merchants,backfill,load-kafka,live,marker}"""
from __future__ import annotations

import argparse
import json
import os
import sys

from lk_common.config import load_config


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lk_generator")
    ap.add_argument("--seed", type=int)
    ap.add_argument("--late-pos-ratio", type=float)
    ap.add_argument("--beyond-limit-ratio", type=float)
    ap.add_argument("--duplicate-ratio", type=float)
    ap.add_argument("--out-of-order-jitter", type=int, help="ms")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("merchants")
    b = sub.add_parser("backfill")
    b.add_argument("--run-id")
    b.add_argument("--events", type=int)
    b.add_argument("--months-limit", type=int)
    sub.add_parser("load-kafka").add_argument("--months-limit", type=int)
    lv = sub.add_parser("live")
    lv.add_argument("--run-id", default=os.environ.get("RUN_ID", "local"))
    lv.add_argument("--rate", type=float)
    lv.add_argument("--ramp", help="step profile 'sec:rate,sec:rate' e.g. 0:500,60:2000,120:5000")
    lv.add_argument("--duration", type=float)
    lv.add_argument("--stream-id", type=int, default=0)
    lv.add_argument("--reset-clock", action="store_true")
    m = sub.add_parser("marker")
    m.add_argument("--run-id", default=os.environ.get("RUN_ID", "local"))
    m.add_argument("--account", required=True)
    m.add_argument("--type", default="PAYMENT")
    m.add_argument("--amount", type=int, required=True)
    m.add_argument("--merchant")
    m.add_argument("--channel", default="APP")
    m.add_argument("--self-transfer", action="store_true")
    m.add_argument("--late-minutes", type=float, default=0, help="sim minutes in the past (offline POS)")
    a = ap.parse_args(argv)

    cfg = load_config()
    if a.seed is not None:
        cfg["seed"] = a.seed
    g = cfg["generator"]
    for k, v in (("late_pos_ratio", a.late_pos_ratio), ("beyond_limit_ratio", a.beyond_limit_ratio),
                 ("duplicate_ratio", a.duplicate_ratio), ("out_of_order_jitter_ms", a.out_of_order_jitter)):
        if v is not None:
            g[k] = v

    if a.cmd == "merchants":
        from .backfill import ensure_merchants
        print(f"merchants: {len(ensure_merchants(cfg)):,} -> {cfg['data']['paths']['merchants_csv']}")
    elif a.cmd == "backfill":
        from .backfill import run_backfill
        if a.events:
            cfg["data"]["history_events"] = a.events
        run_backfill(cfg, a.run_id, a.months_limit)
    elif a.cmd == "load-kafka":
        from .kafka_load import load_merchants, load_stream
        load_merchants(cfg)
        load_stream(cfg, a.months_limit)
    elif a.cmd == "live":
        from .live import run_live
        if a.rate:
            g["live_rate"] = a.rate
        run_live(cfg, a.run_id, a.duration, a.ramp, a.stream_id, a.reset_clock)
    elif a.cmd == "marker":
        from pathlib import Path

        from lk_common.config import SimClock, TimeConfig

        from .emit import KafkaEmitter, TruthWriter, emit_marker
        em = KafkaEmitter(cfg["kafka"]["bootstrap"], cfg["kafka"]["topic_transactions"], linger_ms=0)
        tw = TruthWriter(cfg["data"]["paths"]["truth_live"], a.run_id)
        et = None
        if a.late_minutes:
            tc = TimeConfig.from_config(cfg)
            clock = SimClock.load_or_create(tc, Path(cfg["data"]["paths"]["root"]) / "clock.json")
            et = int(clock.sim_now().timestamp() * 1000) - int(a.late_minutes * 60_000)
        row = emit_marker(cfg, tw, em, account=a.account, type_=a.type, amount=a.amount, merchant=a.merchant,
                          channel=a.channel, is_self=a.self_transfer, event_time_ms=et,
                          pos_offline=bool(a.late_minutes))
        out = Path("results") / a.run_id
        out.mkdir(parents=True, exist_ok=True)
        with open(out / "markers.jsonl", "a") as f:       # delivery counts, used by bench verify (Kafka conservation)
            f.write(json.dumps({"event_id": row["event_id"], "kafka": em.counts()}) + "\n")
        print(json.dumps(row))
    return 0


if __name__ == "__main__":
    sys.exit(main())
