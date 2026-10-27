"""Shared config + simulated-time conversion. The only place sim<->real time is converted."""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = REPO_ROOT / "shared" / "config" / "demo.yaml"

_ENV_OVERRIDES = {
    "RUN_ID": ("run_id",),
    "SEED": ("seed",),
    "SIM_SPEEDUP": ("time", "sim_speedup"),
    "HISTORY_EVENTS": ("data", "history_events"),
    "NUM_ACCOUNTS": ("data", "num_accounts"),
    "NUM_MERCHANTS": ("data", "num_merchants"),
    "LIVE_RATE": ("generator", "live_rate"),
    "LATE_POS_RATIO": ("generator", "late_pos_ratio"),
    "DUPLICATE_RATIO": ("generator", "duplicate_ratio"),
    "OUT_OF_ORDER_JITTER_MS": ("generator", "out_of_order_jitter_ms"),
    "KAFKA_BOOTSTRAP": ("kafka", "bootstrap"),
    "S3_ENDPOINT": ("s3", "endpoint"),
    "DATA_ROOT": ("data", "paths", "root"),
}


def _coerce(raw: str, like: Any) -> Any:
    if isinstance(like, bool):
        return raw.lower() in ("1", "true", "yes")
    if isinstance(like, int):
        return int(raw)
    if isinstance(like, float):
        return float(raw)
    return raw


def _merge(dst: dict, src: dict) -> None:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _merge(dst[k], v)
        else:
            dst[k] = v


def load_config(path: str | os.PathLike | None = None) -> dict:
    base = Path(path or os.environ.get("LK_CONFIG", DEFAULT_CONFIG))
    cfg = yaml.safe_load(base.read_text())
    profile = os.environ.get("LK_PROFILE")
    if profile:   # e.g. LK_PROFILE=fast merges <config dir>/profiles/fast.yaml over the base config
        _merge(cfg, yaml.safe_load((base.parent / "profiles" / f"{profile}.yaml").read_text()))
    for env, keys in _ENV_OVERRIDES.items():
        if env in os.environ:
            node = cfg
            for k in keys[:-1]:
                node = node[k]
            node[keys[-1]] = _coerce(os.environ[env], node[keys[-1]])
    root = Path(cfg["data"]["paths"]["root"])
    for k, v in cfg["data"]["paths"].items():
        if k != "root":
            # paths are written as data/...; re-root them when DATA_ROOT is overridden
            cfg["data"]["paths"][k] = str(root / Path(v).relative_to("data"))
    return cfg


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class TimeConfig:
    sim_speedup: float
    history_start: datetime
    history_end: datetime
    batch_period_sim: timedelta
    pos_max_lateness_sim: timedelta
    allowed_lateness_margin_sim: timedelta
    watermark_ooo_sim: timedelta

    @classmethod
    def from_config(cls, cfg: dict) -> "TimeConfig":
        t = cfg["time"]
        start = datetime.fromisoformat(t["history_start"]).replace(tzinfo=timezone.utc)
        end = start.replace(year=start.year + int(t["history_years"]))
        return cls(
            sim_speedup=float(t["sim_speedup"]),
            history_start=start,
            history_end=end,
            batch_period_sim=timedelta(hours=t["batch_period_sim_hours"]),
            pos_max_lateness_sim=timedelta(hours=t["pos_max_lateness_sim_hours"]),
            allowed_lateness_margin_sim=timedelta(minutes=t["allowed_lateness_margin_sim_minutes"]),
            watermark_ooo_sim=timedelta(seconds=t["watermark_out_of_orderness_sim_seconds"]),
        )

    @property
    def allowed_lateness_sim(self) -> timedelta:
        return self.pos_max_lateness_sim + self.allowed_lateness_margin_sim

    def sim_to_real_seconds(self, sim: timedelta) -> float:
        return sim.total_seconds() / self.sim_speedup

    def real_to_sim(self, real_seconds: float) -> timedelta:
        return timedelta(seconds=real_seconds * self.sim_speedup)


class SimClock:
    """Maps wall clock -> simulated clock. Anchor is persisted so every container agrees."""

    def __init__(self, tc: TimeConfig, sim_epoch: datetime, wall_epoch: float):
        self.tc, self.sim_epoch, self.wall_epoch = tc, sim_epoch, wall_epoch

    @classmethod
    def load_or_create(cls, tc: TimeConfig, path: str | os.PathLike, reset: bool = False) -> "SimClock":
        p = Path(path)
        if p.exists() and not reset:
            d = json.loads(p.read_text())
            if d["sim_speedup"] == tc.sim_speedup:
                return cls(tc, parse_ts(d["sim_epoch"]), d["wall_epoch"])
        clock = cls(tc, tc.history_end, time.time())
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"sim_epoch": to_iso(clock.sim_epoch), "wall_epoch": clock.wall_epoch,
                                 "sim_speedup": tc.sim_speedup}))
        return clock

    def sim_now(self, wall: float | None = None) -> datetime:
        w = time.time() if wall is None else wall
        return self.sim_epoch + self.tc.real_to_sim(w - self.wall_epoch)

    def sim_day_start(self, wall: float | None = None) -> datetime:
        n = self.sim_now(wall)
        return n.replace(hour=0, minute=0, second=0, microsecond=0)
