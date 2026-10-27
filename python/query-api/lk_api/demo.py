"""Demo helper endpoints used by the wallet UI: simulated clock, marker injection, latest verification, scene commands."""
from __future__ import annotations

import glob
import json
from datetime import timedelta
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from lk_common.config import SimClock, TimeConfig, load_config

router = APIRouter()
cfg = load_config()
STATIC = Path(__file__).parent / "static"


def _clock() -> SimClock:
    return SimClock.load_or_create(TimeConfig.from_config(cfg), Path(cfg["data"]["paths"]["root"]) / "clock.json")


@router.get("/ui")
def ui():
    return FileResponse(STATIC / "index.html")


@router.get("/sim/now")
def sim_now():
    c = _clock()
    n = c.sim_now()
    return {"sim_now": n.strftime("%Y-%m-%dT%H:%M:%SZ"), "sim_day": n.strftime("%Y-%m-%d"), "speedup": c.tc.sim_speedup,
            "probe_accounts": [f"A{cfg['data']['num_accounts'] + i + 1:06d}" for i in range(cfg["data"]["num_probe_accounts"])]}


@router.post("/demo/marker")
def marker(account: str, type: str = "PAYMENT", amount: int = 50000, merchant: str | None = "M00001", channel: str = "APP",
           late_minutes: float = 0):
    """Inject one transaction (truth log first, then Kafka) exactly like the generator's marker command."""
    from lk_generator.emit import KafkaEmitter, TruthWriter, emit_marker
    if type not in ("TOPUP", "PAYMENT", "REFUND", "TRANSFER_OUT", "TRANSFER_IN", "CASHBACK"):
        raise HTTPException(400, "bad type")
    em = KafkaEmitter(cfg["kafka"]["bootstrap"], cfg["kafka"]["topic_transactions"], linger_ms=0)
    tw = TruthWriter(cfg["data"]["paths"]["truth_live"], "demo")
    et = None
    if late_minutes:
        et = int((_clock().sim_now() - timedelta(minutes=late_minutes)).timestamp() * 1000)
    row = emit_marker(cfg, tw, em, account=account, type_=type, amount=amount, merchant=merchant if type in ("PAYMENT", "REFUND") else None,
                      channel=channel, event_time_ms=et, pos_offline=bool(late_minutes))
    d = Path("results") / "demo"
    d.mkdir(parents=True, exist_ok=True)
    with open(d / "markers.jsonl", "a") as f:       # delivery counts so `bench verify` can account for demo events
        f.write(json.dumps({"event_id": row["event_id"], "kafka": em.counts()}) + "\n")
    return row


@router.get("/verify/latest")
def verify_latest():
    out = {}
    for arch, pat in (("kappa", "results/*/verification.json"), ("lambda", "results/*/verification_lambda.json")):
        files = sorted(glob.glob(pat), key=lambda p: Path(p).stat().st_mtime)
        if files:
            d = json.loads(Path(files[-1]).read_text())
            out[arch] = {"passed": d["passed"], "checks": len(d["checks"]), "failed": [c["name"] for c in d["checks"] if c["status"] == "fail"],
                         "verified_at": d["verified_at"], "run": Path(files[-1]).parent.name}
    return out
