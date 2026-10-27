"""Thin wrappers around the docker CLI (host daemon via the mounted socket): chaos, stats, exec."""
from __future__ import annotations

import json
import os
import subprocess

PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "it4931")


def name(service: str, idx: int = 1) -> str:
    return f"{PROJECT}-{service}-{idx}"


def run(*args: str, timeout: int = 300, check: bool = True) -> str:
    r = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {r.stderr.strip()[:300]}")
    return r.stdout


def kill(service: str) -> None:
    run("kill", name(service))


def restart(service: str) -> None:
    run("restart", name(service))


def start(service: str) -> None:
    run("start", name(service))


def stop(service: str) -> None:
    run("stop", name(service))


def status(service: str) -> str:
    return run("inspect", "-f", "{{.State.Status}}", name(service), check=False).strip()


def exec_(service: str, *cmd: str, detach: bool = False, timeout: int = 300) -> str:
    return run("exec", *(["-d"] if detach else []), name(service), *cmd, timeout=timeout)


def stats() -> dict[str, dict]:
    """One docker stats sample for every container of the project: {service: {cpu_pct, mem_mb}}."""
    out = {}
    for line in run("stats", "--no-stream", "--format", "{{json .}}", timeout=60).splitlines():
        d = json.loads(line)
        if not d["Name"].startswith(PROJECT + "-"):
            continue
        svc = d["Name"][len(PROJECT) + 1:].rsplit("-", 1)[0]
        mem = d["MemUsage"].split("/")[0].strip()
        num = float("".join(c for c in mem if c.isdigit() or c == "."))
        mb = num * (1024 if "GiB" in mem else 1 if "MiB" in mem else 1 / 1024)
        out[svc] = {"cpu_pct": float(d["CPUPerc"].rstrip("%")), "mem_mb": round(mb, 1)}
    return out


def running_services() -> list[str]:
    return sorted({n[len(PROJECT) + 1:].rsplit("-", 1)[0] for n in run("ps", "--format", "{{.Names}}").split()
                   if n.startswith(PROJECT + "-")})
