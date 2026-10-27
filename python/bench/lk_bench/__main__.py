"""CLI: python -m lk_bench {verify,kappa-views,run,meta}"""
from __future__ import annotations

import argparse
import os
import sys


def _kv(items: list[str]) -> dict:
    out = {}
    for it in items or []:
        k, v = it.split("=", 1)
        for cast in (int, float):
            try:
                v = cast(v)
                break
            except ValueError:
                pass
        out[k.replace("-", "_")] = v
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lk_bench")
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("verify", help="compare an architecture with the oracle; exit 1 on any violation")
    v.add_argument("--arch", default="kappa", choices=["kappa", "lambda"])
    v.add_argument("--rule", default="v1")
    v.add_argument("--run-id", default=os.environ.get("RUN_ID", "local"))
    v.add_argument("--cutoff", help="event-time cutoff (exclusive), ISO-8601")
    v.add_argument("--lag-timeout", type=float, default=600)
    kv = sub.add_parser("kappa-views", help="atomically point kappa_current_* at a job version")
    kv.add_argument("--to", required=True, help="e.g. v1")
    r = sub.add_parser("run", help="run a benchmark scenario: b1..b10, atomic, determinism")
    r.add_argument("scenario")
    r.add_argument("--run-id", default=os.environ.get("RUN_ID", "local"))
    r.add_argument("--repeat", type=int, default=1)
    r.add_argument("--arg", action="append", help="scenario parameter key=value (repeatable)")
    rp = sub.add_parser("report", help="build results/<run>/report.html, slides PNGs and docs/RESULTS.md from raw results")
    rp.add_argument("--run-id", default=os.environ.get("RUN_ID", "final"))
    m = sub.add_parser("meta", help="record config, git commit, image digests, host info")
    m.add_argument("--run-id", default=os.environ.get("RUN_ID", "local"))
    a = ap.parse_args(argv)

    if a.cmd == "verify":
        if a.arch == "lambda":
            from .verify_lambda import verify_lambda
            return 0 if verify_lambda(a.run_id, a.lag_timeout).ok else 1
        from .verify import verify_kappa
        return 0 if verify_kappa(a.rule, a.run_id, a.cutoff, a.lag_timeout).ok else 1
    if a.cmd == "kappa-views":
        from lk_common.ch import ClickHouse, repoint_kappa_views
        repoint_kappa_views(ClickHouse(), a.to)
        print(f"kappa_current_* -> kappa_{a.to}_*")
        return 0
    if a.cmd == "report":
        from .report import build
        build(a.run_id)
        return 0
    from . import scenarios
    if a.cmd == "meta":
        scenarios.collect_meta(a.run_id)
        print(f"meta -> results/{a.run_id}/meta.json")
        return 0
    res = scenarios.run(a.scenario, a.run_id, a.repeat, **_kv(a.arg))
    import json
    print(json.dumps(res, indent=2, default=str)[:4000])
    return 0


if __name__ == "__main__":
    sys.exit(main())
