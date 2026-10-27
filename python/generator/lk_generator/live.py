"""Live mode: produce at a target rate (with optional step ramp) to Kafka, truth log written ahead of sending."""
from __future__ import annotations

import heapq
import json
import signal
import time
import zlib
from pathlib import Path

import numpy as np
import orjson

from lk_common.config import SimClock, TimeConfig
from lk_common.model import table_to_json_rows

from .backfill import build_population, fault_config
from .emit import KafkaEmitter, TruthWriter, with_produced_at
from .population import apply_faults, build_events, make_uuids, to_arrow

SLICES = 10  # a tick is sent in 10 sub-slices so arrivals are spread across the second


def parse_ramp(spec: str | None, default_rate: float) -> list[tuple[float, float]]:
    if not spec:
        return [(0.0, default_rate)]
    return sorted((float(a), float(b)) for a, b in (p.split(":") for p in spec.split(",")))


def rate_at(profile: list[tuple[float, float]], t: float) -> float:
    r = profile[0][1]
    for start, rate in profile:
        if t >= start:
            r = rate
    return r


def run_live(cfg: dict, run_id: str, duration: float | None = None, ramp: str | None = None,
             stream_id: int = 0, reset_clock: bool = False) -> dict:
    tc = TimeConfig.from_config(cfg)
    pop = build_population(cfg)
    faults = fault_config(cfg, tc)
    g = cfg["generator"]
    tick = float(g["live_tick_seconds"])
    profile = parse_ramp(ramp, float(g["live_rate"]))
    clock = SimClock.load_or_create(tc, Path(cfg["data"]["paths"]["root"]) / "clock.json", reset=reset_clock)
    truth = TruthWriter(cfg["data"]["paths"]["truth_live"], run_id)
    em = KafkaEmitter(cfg["kafka"]["bootstrap"], cfg["kafka"]["topic_transactions"])
    # distinct per run_id and per restart (truth.seq), so two live runs never reuse event_ids
    rng = np.random.default_rng(np.random.SeedSequence([cfg["seed"], 2, stream_id, zlib.crc32(run_id.encode()), truth.seq]))
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    inj = {"logical": 0, "late_pos": 0, "beyond_limit": 0, "duplicates": 0, "profile": profile}
    pending: list[tuple[float, int, str, bytes]] = []   # (due_wall, seq, key, payload) duplicate re-sends
    seq = 0
    carry = 0.0
    start = time.time()
    next_tick = start
    out = Path("results") / run_id
    out.mkdir(parents=True, exist_ok=True)

    def save(final: bool = False):
        doc = {"live": {**inj, "kafka": em.counts(), "truth_rows": truth.rows, "duration_s": round(time.time() - start, 1),
                        "final": final, "stream_id": stream_id}}
        (out / "injections.json").write_text(json.dumps(doc, indent=2, default=str))

    print(f"[live] run_id={run_id} profile={profile} sim_speedup={tc.sim_speedup}", flush=True)
    last_log = start
    while not stop["flag"] and (duration is None or time.time() - start < duration):
        rate = rate_at(profile, time.time() - start)
        carry += rate * tick
        n = int(carry)
        carry -= n
        if n:
            cols = build_events(rng, pop, n)
            f = apply_faults(rng, cols, faults)
            ids, _ = make_uuids(rng, n)
            base = int(clock.sim_now().timestamp() * 1000)
            et = base - f["delay_ms"]
            tbl = to_arrow(pop, cols, ids, et, f["pos_offline"])
            truth.append(tbl)                                   # write-ahead
            inj["logical"] += n
            inj["late_pos"] += int(f["late"].sum())
            inj["beyond_limit"] += int(f["beyond"].sum())
            dup = set(f["dup_idx"].tolist())
            step = max(1, -(-n // SLICES))
            for s in range(0, n, step):
                sl = tbl.slice(s, step)
                rows = table_to_json_rows(with_produced_at(sl, int(time.time() * 1000)))
                payloads = em.send_rows(rows)
                for i, (r, b) in enumerate(zip(rows, payloads)):
                    if s + i in dup:
                        seq += 1
                        heapq.heappush(pending, (time.time() + float(rng.uniform(0.5, 5.0)), seq, r["account_id"], b))
                time.sleep(max(0.0, tick / SLICES - 0.001))
        now = time.time()
        while pending and pending[0][0] <= now:
            _, _, key, payload = heapq.heappop(pending)
            em.send(key, payload)                               # byte-identical re-send (producer retry)
            inj["duplicates"] += 1
        em.p.poll(0)
        if now - last_log >= 10:
            save()
            print(f"[live] t={now - start:6.0f}s rate={rate:,.0f}/s logical={inj['logical']:,} dup={inj['duplicates']:,}"
                  f" late={inj['late_pos']:,}", flush=True)
            last_log = now
        next_tick += tick
        time.sleep(max(0.0, next_tick - time.time()))
    for _, _, key, payload in sorted(pending):
        em.send(key, payload)
        inj["duplicates"] += 1
    em.flush()
    save(final=True)
    print(f"[live] stopped: {inj['logical']:,} logical, {inj['duplicates']:,} duplicates -> {out / 'injections.json'}",
          flush=True)
    return inj
