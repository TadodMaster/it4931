"""Scenario registry + run metadata (config, git commit, image digests, host info)."""
from __future__ import annotations

import json
import os
import platform
import time
from pathlib import Path

import yaml

from lk_common.config import load_config

from . import dockerctl
from .common import run_dir, save_json


def collect_meta(run_id: str) -> dict:
    cfg = load_config()
    head = None
    git = Path("/repo/.git")
    try:
        ref = (git / "HEAD").read_text().strip()
        head = (git / ref.split(" ", 1)[1]).read_text().strip() if ref.startswith("ref:") else ref
    except Exception:  # noqa: BLE001
        pass
    digests = {}
    for line in dockerctl.run("ps", "--format", "{{.Names}} {{.Image}}").splitlines():
        n, img = line.split()
        if n.startswith(dockerctl.PROJECT + "-"):
            d = dockerctl.run("image", "inspect", "-f", "{{index .RepoDigests 0}}", img, check=False).strip()
            digests[n[len(dockerctl.PROJECT) + 1:]] = {"image": img, "digest": d or "(locally built)",
                                                       "id": dockerctl.run("image", "inspect", "-f", "{{.Id}}", img, check=False).strip()}
    info = json.loads(dockerctl.run("info", "--format", "{{json .}}"))
    meta = {"run_id": run_id, "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "git_commit": head,
            "lk_profile": os.environ.get("LK_PROFILE"), "config": cfg,
            "images": digests,
            "host": {"docker_server_version": info.get("ServerVersion"), "docker_cpus": info.get("NCPU"),
                     "docker_memory_bytes": info.get("MemTotal"), "docker_os": info.get("OperatingSystem"),
                     "docker_kernel": info.get("KernelVersion"), "bench_container_platform": platform.platform()}}
    save_json(run_id, "meta.json", meta)
    return meta


def registry():
    from . import sc_cost, sc_fresh, sc_ops, sc_proofs
    return {
        "b1": sc_fresh.b1_freshness, "b2": sc_ops.b2_throughput, "b3": sc_fresh.b3_correctness_over_time,
        "b4": sc_fresh.b4_late_pos, "b5": sc_ops.b5_duplicates_crash, "b6": sc_ops.b6_rule_change, "b7": sc_ops.b7_drift,
        "b8": sc_ops.b8_report, "b9": sc_cost.b9_cost, "b10": sc_cost.b10_extrapolation,
        "atomic": sc_proofs.atomic_swap, "determinism": sc_proofs.replay_determinism,
        "batch": sc_ops.corrective_batch, "master-stats": sc_ops.master_stats,
    }


def run(name: str, run_id: str, repeat: int = 1, **kwargs) -> dict:
    """Runs a scenario `repeat` times (timing scenarios: >= 3 in the final run); results go to results/<run_id>/."""
    fn = registry()[name]
    if not (run_dir(run_id) / "meta.json").exists():
        collect_meta(run_id)
    outs = []
    for i in range(repeat):
        rid = run_id if repeat == 1 else f"{run_id}/rep{i + 1}"
        run_dir(rid)
        print(f"[bench] {name} repetition {i + 1}/{repeat} -> results/{rid}", flush=True)
        outs.append(fn(rid, **kwargs))
    return outs[0] if repeat == 1 else {"repetitions": outs}
