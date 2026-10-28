"""B2 throughput, B5 duplicates+crash, B6 rule change (Y3), B7 drift between codebases, B8 historical report (Y2)."""
from __future__ import annotations

import json
from pathlib import Path
import random
import threading
import time
from datetime import timedelta

import httpx
import redis

from lk_common.ch import ClickHouse, kappa_view_sql, repoint_kappa_views
from lk_common.config import load_config
from lk_common.kafka_lag import consumer_lag
from lk_oracle.core import Oracle

from . import dockerctl, flinkapi
from .common import (api_get, batch_runs, iso_utc, lag_total, now_sim, pct, probe_accounts, processed_lag, save_csv, save_json,
                     wait_until)
from .verify import CH_HASH, CH_ROW, METRICS, verify_kappa
from .verify_lambda import _digest, merged_rows, serving_matches_oracle

NET = f"{dockerctl.PROJECT}_default"


# --------------------------------------------------------------------------------------------------- helpers
def oracle_for(cfg: dict) -> Oracle:
    p = cfg["data"]["paths"]
    from lk_oracle.core import compact_truth
    compact_truth(p["truth_live"])          # thousands of per-second files make every oracle query slow
    return Oracle([p["truth_backfill"], p["truth_live"]], p["merchants_csv"])


def start_generator(run_id: str, rate: int, seconds: float, *flags: str, ramp: str | None = None) -> str:
    """Starts a live run in the generator container. The run id gets a unique suffix so injections.json is never overwritten
    (bench verify sums every live run's produced counts)."""
    run_id = f"{run_id.replace('/', '_')}-{int(time.time())}"
    cmd = ["python", "-m", "lk_generator", *flags, "live", "--run-id", run_id, "--duration", str(seconds)]
    cmd += ["--ramp", ramp] if ramp else ["--rate", str(rate)]
    dockerctl.exec_("generator", *cmd, detach=True)
    return run_id


def generator_running() -> bool:
    out = dockerctl.exec_("generator", "python", "-c",
                          "import os;print(sum('lk_generator' in open('/proc/'+p+'/cmdline').read() and 'live' in open('/proc/'+p+'/cmdline').read() "
                          "for p in os.listdir('/proc') if p.isdigit()))")
    return int(out.strip() or 0) > 1      # the `python -c` itself matches once


def stop_generator() -> None:
    dockerctl.exec_("generator", "python", "-c",
                    "import os,signal\nfor p in os.listdir('/proc'):\n if p.isdigit():\n  c=open('/proc/'+p+'/cmdline').read()\n"
                    "  if 'lk_generator' in c and 'live' in c and str(os.getpid())!=p: os.kill(int(p),signal.SIGINT)")


def run_spark(script: str, *args: str, mem: str = "1g") -> str:
    """One-off Spark job in a throw-away container that reuses the archiver's mounts."""
    return dockerctl.run("run", "--rm", "--user", "root", "--network", NET, "--volumes-from", dockerctl.name("spark-archiver"),
                         "-e", "S3_ACCESS_KEY", "-e", "S3_SECRET_KEY", "-e", "REDIS_HOST=redis", "-e", "SPARK_MASTER=local[2]",
                         "-e", "AWS_REQUEST_CHECKSUM_CALCULATION=when_required", "-e", "AWS_RESPONSE_CHECKSUM_VALIDATION=when_required",
                         "-e", "LK_PROFILE=" + (__import__("os").environ.get("LK_PROFILE", "")), "--memory", "1700m",
                         "lk-spark:dev", "/opt/spark/bin/spark-submit", "--driver-memory", mem, f"/app/spark/{script}", *args, timeout=1800)


def manual_batch(rule: str, t_k, tag: str) -> dict:
    t0 = time.time()
    out = run_spark("batch.py", "--t-k", t_k.strftime("%Y-%m-%dT%H:%M:%S+00:00"), "--rule", rule, "--run-id", f"manual_{tag}_{int(t0)}", "--purge-current-day")
    line = next((x for x in out.splitlines() if "[batch] done" in x), "")
    return {"seconds": round(time.time() - t0, 1), "result": json.loads(line.split("done ", 1)[1]) if line else None}


def flink_submit(job: str, *args: str) -> str:
    jar = {"kappa": "/opt/jobs/kappa-flink-job.jar", "lambda": "/opt/jobs/lambda-flink-speed.jar"}[job]
    return dockerctl.exec_("flink-jobmanager", "flink", "run", "-d", "-m", "localhost:8081", jar, *args, timeout=300)


def flink_cancel(name_suffix: str) -> bool:
    j = flinkapi.running_job(name_suffix)
    if j:
        flinkapi.cancel(j["jid"])
        wait_until(lambda: flinkapi.running_job(name_suffix) is None, 60, 1)
    return bool(j)


def wait_lag_zero(group: str, cfg: dict, timeout: float = 900, sample: list | None = None, every: float = 2.0) -> float | None:
    """Wait until `group` has processed everything in Kafka (processed-count based; see common.processed_lag)."""
    t0 = time.time()
    ch = ClickHouse()
    while time.time() - t0 < timeout:
        lag = processed_lag(group, cfg, ch)
        if sample is not None and lag is not None:
            sample.append({"t_s": round(time.time() - t0, 1), "lag": lag})
        if lag == 0:
            return time.time() - t0
        time.sleep(every)
    return None


class Poller(threading.Thread):
    """Hits a URL continuously; counts failures and the longest gap between successes (zero-downtime check)."""

    def __init__(self, url: str, interval: float = 0.1):
        super().__init__(daemon=True)
        self.url, self.interval, self.stop_flag = url, interval, False
        self.ok = self.fail = 0
        self.max_gap = 0.0
        self.errors: list[str] = []

    def run(self) -> None:
        last = time.time()
        with httpx.Client(timeout=10) as c:
            while not self.stop_flag:
                try:
                    r = c.get(self.url)
                    r.raise_for_status()
                    self.ok += 1
                    self.max_gap = max(self.max_gap, time.time() - last)
                    last = time.time()
                except Exception as e:  # noqa: BLE001
                    self.fail += 1
                    self.errors.append(str(e)[:80])
                time.sleep(self.interval)

    def result(self) -> dict:
        self.stop_flag = True
        return {"ok": self.ok, "failed": self.fail, "max_gap_s": round(self.max_gap, 3), "sample_errors": self.errors[:3]}


def kappa_digests(ch: ClickHouse, version: str) -> dict:
    """Digest of every Kappa serving metric read straight from kappa_<version>_* (independent of the active view)."""
    v = kappa_view_sql(version)
    src = {"balance": ("lk.kappa_current_account", v["account"]), "spend_day": ("lk.kappa_current_spend_day", v["spend_day"]),
           "spend_cat_month": ("lk.kappa_current_spend_cat_month", v["spend_cat_month"])}
    out = {}
    for m in METRICS:
        q = CH_ROW[m].replace(src[m][0], "(" + src[m][1] + ")")
        r = ch.query(f"SELECT count() AS n, sum({CH_HASH}) AS d FROM ({q})")[0]
        out[m] = {"rows": int(r["n"]), "digest": int(r["d"])}
    return out


def oracle_digests(oracle: Oracle, rule: str) -> dict:
    return {m: oracle.digest(m, rule, None) for m in METRICS}


# ======================================================================================================= B2
def b2_throughput(run_id: str, rates: str = "200,400,800,1600,2400", step_s: int = 25, **_) -> dict:
    """Step the ingest rate; a step is sustained if every running architecture's consumer lag stays bounded."""
    cfg = load_config()
    rate_list = [int(x) for x in rates.split(",")]
    ramp = ",".join(f"{i * step_s}:{r}" for i, r in enumerate(rate_list))
    total = step_s * len(rate_list)
    chq = ClickHouse()
    groups = {g: processed_lag(g, cfg, chq) is not None for g in ("kappa-v1", "lambda-speed")}
    active = [g for g, ok in groups.items() if ok]
    for g in active:                                    # start from an idle system
        wait_lag_zero(g, cfg, 300)
    rid = f"{run_id}-gen"
    start_generator(rid, rate_list[0], total, ramp=ramp)
    samples, t0 = [], time.time()
    while time.time() - t0 < total + 5:
        row = {"t_s": round(time.time() - t0, 1)}
        for g in active:
            row[f"lag_{g}"] = processed_lag(g, cfg, chq)
        st = dockerctl.stats()
        for svc in ("flink-taskmanager", "spark-archiver", "redis", "clickhouse", "kafka"):
            if svc in st:
                row[f"cpu_{svc}"], row[f"mem_{svc}"] = st[svc]["cpu_pct"], st[svc]["mem_mb"]
        samples.append(row)
        time.sleep(1)
    save_csv(run_id, "b2_throughput_samples.csv", samples)
    steps = []
    for i, r in enumerate(rate_list):
        lo, hi = i * step_s, (i + 1) * step_s
        seg = [s for s in samples if lo <= s["t_s"] < hi]
        entry = {"rate": r}
        for g in active:
            lags = [s[f"lag_{g}"] for s in seg if s.get(f"lag_{g}") is not None]
            end, mid = lags[-1], lags[len(lags) // 2]
            slope = (end - mid) / max(1.0, len(lags) / 2)
            entry[g] = {"lag_end": end, "lag_growth_per_s": round(slope, 1), "sustained": end <= max(3 * r, 1500) and slope <= 0.05 * r}
        steps.append(entry)
    out = {"steps": steps, "step_seconds": step_s}
    for g in active:
        ok = [s["rate"] for s in steps if all(s[x]["sustained"] for x in [g])]
        out[f"max_sustained_{g}"] = max(ok) if ok else None
    out["note"] = ("generator is a single Python process; if achieved rate < target the limit is the generator, not the architecture "
                   "(see results/<run>-gen/injections.json)")
    save_json(run_id, "b2_throughput.json", out)
    return out


# ======================================================================================================= B5
def b5_duplicates_crash(run_id: str, seconds: int = 90, rate: int = 200, dup_ratio: float = 0.05, **_) -> dict:
    cfg = load_config()
    ch = ClickHouse()
    out: dict = {"duplicate_ratio": dup_ratio}
    start_generator(f"{run_id}-gen", rate, seconds, "--duplicate-ratio", str(dup_ratio))
    t0 = time.time()
    time.sleep(30)
    # `docker kill` counts as a manual stop for the restart policy, so we play the orchestrator: start it again after a pause
    dockerctl.kill("flink-taskmanager"); out["tm_killed_at_s"] = round(time.time() - t0, 1)
    time.sleep(8)
    dockerctl.start("flink-taskmanager")
    time.sleep(12)
    dockerctl.kill("spark-archiver"); out["archiver_killed_at_s"] = round(time.time() - t0, 1)
    time.sleep(8)
    dockerctl.start("spark-archiver")
    wait_until(lambda: dockerctl.status("flink-taskmanager") == "running", 120, 2)
    wait_until(lambda: dockerctl.status("spark-archiver") == "running", 120, 2)
    wait_until(lambda: flinkapi.running_job("kappa-v1") is not None and flinkapi.running_job("lambda-speed") is not None, 240, 2)
    out["jobs_back_at_s"] = round(time.time() - t0, 1)
    time.sleep(max(0, seconds + 10 - (time.time() - t0)))             # generator finishes
    out["kappa_catchup_s"] = wait_lag_zero("kappa-v1", cfg, 600)
    out["lambda_speed_catchup_s"] = wait_lag_zero("lambda-speed", cfg, 600)
    # Kappa: exact, 0 VND difference, no double counting
    rep = verify_kappa("v1", f"{run_id}-kappa", None, 300)
    out["kappa_verification_passed"] = rep.ok
    out["kappa_failed_checks"] = [c["name"] for c in rep.checks if c["status"] == "fail"]
    # Lambda speed layer: report honestly what the crash did, then show the next batch correcting it
    oracle = oracle_for(cfg)
    rule = "v1"
    before = serving_matches_oracle(ch, oracle, rule)
    out["lambda_serving_before_batch"] = {m: {"ok": r["ok"], "oracle_rows": r["oracle"]["rows"], "serving_rows": r["serving"]["rows"]}
                                          for m, r in before.items()}
    bal_o = oracle.all_balances()
    from .sc_fresh import _lambda_all_balances
    merged, _, _ = _lambda_all_balances(ch)
    diff = {a: merged.get(a, 0) - bal_o.get(a, 0) for a in set(bal_o) | set(merged) if merged.get(a, 0) != bal_o.get(a, 0)}
    out["lambda_balance_mismatched_accounts"] = len(diff)
    out["lambda_balance_total_error_vnd"] = sum(diff.values())
    out["lambda_overcounted_accounts"] = sum(1 for v in diff.values() if v > 0)
    out["lambda_undercounted_accounts"] = sum(1 for v in diff.values() if v < 0)
    out["stats"] = {k: redis.Redis(host="redis", decode_responses=True).get(k) for k in ("stat:accepted", "stat:dup", "stat:old")}
    t_batch = now_sim(cfg)
    time.sleep(2)
    mb = manual_batch(rule, now_sim(cfg).replace(microsecond=0), "b5")
    out["lambda_corrective_batch"] = mb
    after = serving_matches_oracle(ch, oracle, rule)
    out["lambda_serving_after_batch"] = {m: r["ok"] for m, r in after.items()}
    save_json(run_id, "b5_duplicates_crash.json", out)
    return out


# ======================================================================================================= B6
def b6_rule_change(run_id: str, live_rate: int = 0, **_) -> dict:
    """Y3: spending rule v1 -> v2. Lambda: change Spark + Flink code, re-run the batch. Kappa: replay from offset 0."""
    cfg = load_config()
    ch = ClickHouse()
    oracle = oracle_for(cfg)
    r = redis.Redis(host="redis", decode_responses=True)
    out: dict = {}
    pollers = {"kappa": Poller(f"{__import__('lk_bench.common', fromlist=['API']).API}/kappa/balance/{probe_accounts(cfg)[0]}"),
               "lambda": Poller(f"{__import__('lk_bench.common', fromlist=['API']).API}/lambda/balance/{probe_accounts(cfg)[0]}")}
    [p.start() for p in pollers.values()]

    # ---- Lambda: two code bases to change + full batch
    t0 = time.time()
    r.set("lambda:spark_rule", "v2")                                    # (1) Spark implementation deployed
    t_spark = time.time() - t0
    flink_cancel("lambda-speed")                                        # (2) Flink (Java) implementation deployed
    flink_submit("lambda", "--rule", "v2", "--parallelism", "2", "--start-from", "committed")
    wait_until(lambda: flinkapi.running_job("lambda-speed") is not None, 120, 1)
    t_flink = time.time() - t0
    wait_lag_zero("lambda-speed", cfg, 300)
    mb = manual_batch("v2", now_sim(cfg).replace(microsecond=0), "b6")  # (3) full batch over ALL history with the new rule
    t_batch = time.time() - t0
    ok_dt, _ = wait_until(lambda: all(x["ok"] for x in serving_matches_oracle(ch, oracle, "v2").values()), 120, 5)
    out["lambda"] = {"spark_change_s": round(t_spark, 2), "flink_redeploy_done_s": round(t_flink, 1),
                     "batch_s": mb["seconds"], "batch_rows": (mb["result"] or {}).get("input_rows"),
                     "total_time_to_correct_s": round(time.time() - t0, 1), "matches_oracle_v2": ok_dt is not None,
                     "rule_implementations_changed": 2, "implementation_languages": ["PySpark", "Java"]}
    out["lambda_api"] = pollers["lambda"].result()

    # ---- Kappa: start job v2 from offset 0 next to v1, switch when lag = 0 and the oracle check passes
    ends = consumer_lag(cfg["kafka"]["bootstrap"], "kappa-v1", cfg["kafka"]["topic_transactions"])["partitions"]
    history = sum(p["end"] for p in ends.values())
    r_in = live_rate
    lag_curve: list[dict] = []
    t0 = time.time()
    flink_submit("kappa", "--rule", "v2", "--version", "v2", "--parallelism", "2", "--start-from", "earliest",
                 "--checkpoint-interval-ms", "30000")
    t_started = time.time() - t0
    catch = wait_lag_zero("kappa-v2", cfg, 1800, lag_curve, 2.0)
    t_catch = time.time() - t0
    mids = [x for x in lag_curve if x["lag"] > 0]
    if len(mids) >= 3:
        a, b = mids[len(mids) // 4], mids[3 * len(mids) // 4]
        r_process = (a["lag"] - b["lag"]) / max(0.1, b["t_s"] - a["t_s"]) + r_in
    else:
        r_process = history / max(1.0, catch or t_catch)
    predicted = history / max(1.0, (r_process - r_in))
    dig_k, dig_o = kappa_digests(ch, "v2"), oracle_digests(oracle, "v2")
    check = dig_k == dig_o
    switched = False
    if check:
        repoint_kappa_views(ch, "v2")
        switched = True
        flink_cancel("kappa-v1")
    out["kappa"] = {"history_events": history, "job_started_s": round(t_started, 1), "catchup_s": None if catch is None else round(catch, 1),
                    "measured_replay_rate_ev_s": round(r_process), "predicted_T_s": round(predicted, 1),
                    "measured_T_s": None if catch is None else round(catch, 1), "oracle_check_v2_passed": check,
                    "switched_views": switched, "rule_implementations_changed": 1, "implementation_languages": ["Java"]}
    out["kappa_api"] = pollers["kappa"].result()
    save_csv(run_id, "b6_kappa_lag_curve.csv", lag_curve)
    save_json(run_id, "b6_rule_change.json", out)
    return out


# ======================================================================================================= B7
def b7_drift(run_id: str, live_seconds: int = 40, rate: int = 200, **_) -> dict:
    """Deploy the new rule to Spark only. After a batch the report follows v2, but everything the speed layer (Flink, still v1)
    computed since then disagrees with it; fixing Flink does not repair the stale buckets until the next batch replaces them."""
    cfg = load_config()
    ch = ClickHouse()
    oracle = oracle_for(cfg)
    rd = redis.Redis(host="redis", decode_responses=True)
    out: dict = {}
    min_day = "2026-01-01"                                              # drift only matters for the days of the live period

    def mismatches(rule: str) -> dict:
        mr = merged_rows(ch, min_day=min_day)
        got = set(mr["spend_day"])
        o = oracle.all_spend_day(rule)
        exp_rows = {f"{k}|{v['spend']}|{v['tx_count']}" for k, v in o.items() if k.split("|")[1] >= min_day}
        return {"mismatching_account_days": len({"|".join(x.split("|")[:2]) for x in got ^ exp_rows})}

    wait_lag_zero("lambda-speed", cfg, 120)
    out["baseline_v1"] = mismatches("v1")
    t0 = time.time()
    rd.set("lambda:spark_rule", "v2")                                      # Spark implementation changed, Flink not
    mb = manual_batch("v2", now_sim(cfg).replace(microsecond=0), "b7a")
    out["batch_v2_just_ran"] = {**mismatches("v2"), "batch_seconds": mb["seconds"], "note": "report = rule v2"}
    start_generator(f"{run_id}-gen", rate, live_seconds)                  # new traffic is handled by the speed layer (v1)
    time.sleep(live_seconds + 8)
    wait_lag_zero("lambda-speed", cfg, 120)
    out["spark_v2_flink_v1"] = {**mismatches("v2"), "elapsed_s": round(time.time() - t0, 1)}
    flink_cancel("lambda-speed")                                           # now fix Flink ...
    flink_submit("lambda", "--rule", "v2", "--parallelism", "2", "--start-from", "committed")
    wait_until(lambda: flinkapi.running_job("lambda-speed") is not None, 120, 1)
    wait_lag_zero("lambda-speed", cfg, 120)
    out["flink_fixed_before_next_batch"] = {**mismatches("v2"), "elapsed_s": round(time.time() - t0, 1),
                                           "note": "stale v1 buckets still in Redis"}
    mb2 = manual_batch("v2", now_sim(cfg).replace(microsecond=0), "b7b")   # ... and only the next batch repairs the numbers
    out["after_next_batch"] = {**mismatches("v2"), "batch_seconds": mb2["seconds"], "elapsed_s": round(time.time() - t0, 1)}
    save_json(run_id, "b7_drift.json", out)
    return out


# ======================================================================================================= B8
def b8_report(run_id: str, accounts: int = 150, **_) -> dict:
    """3-year spend report by category (join with merchant dimension) for many accounts, both architectures."""
    cfg = load_config()
    ch = ClickHouse()
    oracle = oracle_for(cfg)
    bal = oracle.all_balances()
    rnd = random.Random(11)
    sample = rnd.sample(sorted(bal), min(accounts, len(bal)))
    lat = {"kappa": [], "lambda": []}
    wrong = {"kappa": 0, "lambda": 0}
    rule = (ch.query("SELECT k2 FROM lk.lambda_batch_cur WHERE kind='meta'") or [{"k2": "v1"}])[0]["k2"]
    for a in sample:
        exp = oracle.report(a, "2023-01-01", "2026-12-31", rule)
        for arch in lat:
            t = time.time()
            rows = api_get(f"/{arch}/report/{a}", **{"from": "2023-01", "to": "2026-12"})["rows"]
            lat[arch].append((time.time() - t) * 1000)
            if rows != exp:
                wrong[arch] += 1
    runs = batch_runs(ch)
    cp = None
    j = flinkapi.running_job("kappa-v2") or flinkapi.running_job("kappa-v1")
    if j:
        cp = (flinkapi.status(j["jid"])["latest"] or {}).get("state_size")
    out = {"accounts": len(sample), "rule": rule, "latency_ms": {k: pct(v) for k, v in lat.items()},
           "reports_not_equal_to_oracle": wrong,
           "lambda_batch_duration_s": [round(int(r["duration_ms"]) / 1000, 1) for r in runs[-5:]],
           "lambda_batch_input_rows": [int(r["input_rows"]) for r in runs[-5:]],
           "kappa_checkpoint_state_bytes": cp,
           "clickhouse_bytes": {r["t"]: int(r["b"]) for r in ch.query(
               "SELECT table AS t, sum(bytes_on_disk) AS b FROM system.parts WHERE active AND database='lk' GROUP BY table")}}
    save_json(run_id, "b8_report.json", out)
    return out


# ======================================================================================================= helpers as scenarios
def corrective_batch(run_id: str, rule: str = "", **_) -> dict:
    """Run one full batch now (t_k = current simulated time). Used after quiescence so the batch includes every event."""
    cfg = load_config()
    rule = rule or redis.Redis(host="redis", decode_responses=True).get("lambda:spark_rule") or "v1"
    out = manual_batch(rule, now_sim(cfg).replace(microsecond=0), "corrective")
    save_json(run_id, "corrective_batch.json", out)
    return out


def master_stats(run_id: str, **_) -> dict:
    t0 = time.time()
    out = run_spark("master_stats.py", mem="768m")
    line = next((x for x in out.splitlines() if "[master-stats]" in x), "")
    res = {"seconds": round(time.time() - t0, 1), "line": line}
    save_json(run_id, "master_stats_run.json", res)
    return res


# ======================================================================================================= e2e
def e2e(run_id: str, seconds: int = 45, rate: int = 200, **_) -> dict:
    """make test-e2e: live traffic with duplicates and late POS, quiesce, corrective batch, then `bench verify` for both."""
    from .verify import _end_offsets
    from .verify_lambda import verify_lambda
    cfg = load_config()
    start_generator(f"{run_id}-gen", rate, seconds)
    time.sleep(seconds + 8)
    ends = sum(_end_offsets(cfg["kafka"]["bootstrap"], cfg["kafka"]["topic_transactions"]).values())
    wait_lag_zero("kappa-v1", cfg, 300)
    wait_lag_zero("lambda-speed", cfg, 300)
    rows = 0
    for _ in range(6):                                  # archiver is micro-batched: poll master row count until it equals Kafka
        master_stats(run_id)
        last = json.loads(Path("/data/master_stats.jsonl").read_text().splitlines()[-1])
        rows = last["rows"]
        if rows == ends:
            break
        time.sleep(10)
    out = {"kafka_end_offsets": ends, "master_rows": rows}
    corrective_batch(run_id)
    master_stats(run_id)
    rk = verify_kappa("v1", f"{run_id}", None, 300)
    rl = verify_lambda(run_id, 300)
    out.update(kappa_passed=rk.ok, lambda_passed=rl.ok, passed=rk.ok and rl.ok,
               failed=[c["name"] for c in rk.checks + rl.checks if c["status"] == "fail"])
    save_json(run_id, "e2e.json", out)
    if not out["passed"]:
        raise SystemExit(f"e2e FAILED: {out['failed']}")
    return out
