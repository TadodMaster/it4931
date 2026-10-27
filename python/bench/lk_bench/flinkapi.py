"""Flink REST helpers used by bench (job status, rates, backpressure, cancel). Talks to the jobmanager inside compose."""
from __future__ import annotations

import json
import os
import time
import urllib.request as u

BASE = os.environ.get("FLINK_REST", "http://flink-jobmanager:8081")


def _get(path: str):
    return json.load(u.urlopen(BASE + path, timeout=15))


def jobs() -> list[dict]:
    return _get("/jobs/overview")["jobs"]


def running_job(name_suffix: str | None = None) -> dict | None:
    for j in jobs():
        if j["state"] in ("RUNNING", "RESTARTING") and (name_suffix is None or j["name"].endswith(name_suffix)):
            return j
    return None


def cancel(jid: str) -> None:
    u.urlopen(u.Request(f"{BASE}/jobs/{jid}?mode=cancel", method="PATCH"), timeout=15)


def vertices(jid: str) -> list[dict]:
    return _get(f"/jobs/{jid}")["vertices"]


def vertex_metrics(jid: str, vid: str, names: list[str]) -> dict:
    data = _get(f"/jobs/{jid}/vertices/{vid}/subtasks/metrics?get={','.join(names)}")
    return {m["id"]: m for m in data}


def status(jid: str) -> dict:
    cp = _get(f"/jobs/{jid}/checkpoints")
    out = {"checkpoints": cp["counts"], "latest": (cp.get("latest") or {}).get("completed")}
    rows = []
    for v in vertices(jid):
        m = vertex_metrics(jid, v["id"], ["busyTimeMsPerSecond", "backPressuredTimeMsPerSecond", "numRecordsOutPerSecond"])
        rows.append({"vertex": v["name"][:36], "parallelism": v["parallelism"],
                     "busy_ms_per_s_avg": m.get("busyTimeMsPerSecond", {}).get("avg"),
                     "backpressured_ms_per_s_avg": m.get("backPressuredTimeMsPerSecond", {}).get("avg"),
                     "records_out_per_s_sum": m.get("numRecordsOutPerSecond", {}).get("sum")})
    out["vertices"] = rows
    return out


def source_records(jid: str) -> int:
    v = vertices(jid)[0]
    return int(v["metrics"]["write-records"])


def measure_rate(jid: str, seconds: float = 20) -> float:
    a = source_records(jid)
    t0 = time.time()
    time.sleep(seconds)
    return (source_records(jid) - a) / (time.time() - t0)


if __name__ == "__main__":
    j = running_job()
    print(json.dumps(status(j["jid"]), indent=2, default=str) if j else "no running job")
