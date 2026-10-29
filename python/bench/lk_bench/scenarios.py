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
        "e2e": sc_ops.e2e, "batch": sc_ops.corrective_batch, "master-stats": sc_ops.master_stats,
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


def run_all(run_id: str) -> dict:
    """The whole benchmark, in-process (runs inside the bench container, so it survives the caller). Every step is
    logged and failures do not stop the sequence; a status file is written after each step."""
    import traceback
    from .verify import verify_kappa
    from .verify_lambda import verify_lambda
    status: dict = {}
    steps = [("meta", None), ("atomic", {}), ("determinism", {}), ("b1", {"repeat": 3}), ("b8", {"repeat": 3}), ("b2", {}),
             ("b9", {}), ("b3", {}), ("b4", {}), ("b5", {}), ("batch", {}), ("master-stats", {}), ("verify-kappa", None),
             ("verify-lambda", None), ("b7", {}), ("b6", {}), ("b10", {}), ("report", None)]
    for name, kw in steps:
        t0 = time.time()
        print(f"\n=== [{time.strftime('%H:%M:%S')}] {name}", flush=True)
        try:
            if name == "meta":
                collect_meta(run_id)
            elif name == "verify-kappa":
                ok = verify_kappa("v1", run_id, None, 600).ok
                status[name] = {"passed": ok}
            elif name == "verify-lambda":
                status[name] = {"passed": verify_lambda(run_id, 600).ok}
            elif name == "report":
                from .report import build
                build(run_id)
            else:
                run(name, run_id, **(kw or {}))
            status.setdefault(name, {"ok": True})
        except BaseException as e:  # noqa: BLE001 - keep going; the status file says what failed
            traceback.print_exc()
            status[name] = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}
        status[name]["seconds"] = round(time.time() - t0, 1)
        save_json(run_id, "bench_all_status.json", status)
    print("\n=== bench-all finished", flush=True)
    return status
