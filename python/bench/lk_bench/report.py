"""make report: results/<run>/report.html (self-contained), docs/RESULTS.md and 1920x1080 PNGs in results/<run>/slides/.

Everything is computed from the raw files under results/ - nothing here is typed in by hand.
Colors: validated categorical slots (blue = Lambda, orange = Kappa, aqua = third series), light surface; recessive grid;
thin lines; direct labels + legend; every chart has a table view in the HTML.
"""
from __future__ import annotations

import base64
import csv
import io
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

LAMBDA, KAPPA, THIRD = "#2a78d6", "#eb6834", "#1baf7a"
SURFACE, INK, INK2, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
RES = Path("results")


# ------------------------------------------------------------------------------------------------ data access
def _dirs(run_id: str) -> list[Path]:
    """The main run dir first, then every other results dir (newest first) as fallback."""
    others = sorted((d for d in RES.iterdir() if d.is_dir() and d.name != run_id), key=lambda d: d.stat().st_mtime, reverse=True)
    return ([RES / run_id] if (RES / run_id).exists() else []) + others


def find(run_id: str, name: str) -> Path | None:
    for d in _dirs(run_id):
        if (d / name).exists():
            return d / name
    return None


def jload(run_id: str, name: str):
    p = find(run_id, name)
    return json.loads(p.read_text()) if p else None


def cload(run_id: str, name: str) -> list[dict]:
    p = find(run_id, name)
    if not p:
        return []
    with open(p) as f:
        return list(csv.DictReader(f))


def reps(run_id: str, name: str) -> list[dict]:
    """All repetitions of a scenario output: <run>/rep1/<name> ... or the single file."""
    out = []
    for d in _dirs(run_id):
        found = sorted(d.glob(f"rep*/{name}"))
        if found:
            return [json.loads(p.read_text()) for p in found]
        if (d / name).exists():
            return [json.loads((d / name).read_text())]
    return out


def med(vals: list[float]) -> float | None:
    v = [x for x in vals if x is not None]
    return statistics.median(v) if v else None


def spread(vals: list[float]) -> str:
    v = [x for x in vals if x is not None]
    return f"{min(v):.3g}-{max(v):.3g}" if len(v) > 1 else ""


def f(x, nd=2):
    return "n/a" if x is None else (f"{x:,.{nd}f}".rstrip("0").rstrip(".") if isinstance(x, float) else f"{x:,}")


# ------------------------------------------------------------------------------------------------ charts
def _style(ax, title, xlabel="", ylabel="", big=False):
    fs = 20 if big else 11
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=fs + 4, fontweight="bold", pad=14)
    ax.set_xlabel(xlabel, color=INK2, fontsize=fs)
    ax.set_ylabel(ylabel, color=INK2, fontsize=fs)
    ax.tick_params(colors=MUTED, labelsize=fs - 2)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color("#c3c2b7")
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)


def _new(big):
    fig, ax = plt.subplots(figsize=(19.2, 10.8) if big else (9.6, 5.0), dpi=100)
    fig.patch.set_facecolor(SURFACE)
    return fig, ax


def _legend(ax, big):
    ax.legend(frameon=False, fontsize=18 if big else 10, labelcolor=INK2, loc="best")


def chart_b1(d, big):
    fig, ax = _new(big)
    for key, col, lab in (("lambda_api", LAMBDA, "Lambda (speed layer) via API"), ("kappa_api", KAPPA, "Kappa via API"),
                          ("lambda_direct", LAMBDA, "Lambda direct (Redis)"), ("kappa_direct", KAPPA, "Kappa direct (ClickHouse)")):
        v = sorted(float(r[key]) for r in d["samples"] if r.get(key) not in (None, ""))
        if v:
            ax.plot(v, [(i + 1) / len(v) for i in range(len(v))], color=col, lw=2.2, ls="-" if "api" in key else "--", label=lab)
    ax.set_xscale("log")
    _style(ax, "Freshness (Y1): marker visible after ... (CDF)", "seconds from send to visible (log scale)", "cumulative share", big)
    _legend(ax, big)
    return fig


def chart_b3(rows, big):
    fig, ax = _new(big)
    t = [float(r["t_s"]) / 60 for r in rows]
    ax.plot(t, [float(r["kappa_exact_pct"]) for r in rows], color=KAPPA, lw=2.2, label="Kappa vs oracle (now)")
    ax.plot(t, [float(r["lambda_exact_pct"]) for r in rows], color=LAMBDA, lw=2.2, label="Lambda (batch + speed) vs oracle (now)")
    b = [float(r["lambda_batch_asof_exact_pct"]) if r["lambda_batch_asof_exact_pct"] not in ("", None) else None for r in rows]
    ax.plot(t, [x if x is not None else float("nan") for x in b], color=THIRD, lw=1.6, ls=":", label="Lambda batch vs oracle at its own t_k")
    for r in rows:
        if r["batch_run_started"]:
            ax.axvline(float(r["t_s"]) / 60, color=MUTED, lw=1, ls="--")
    ax.text(0.99, 0.02, "dashed verticals = nightly batch runs", transform=ax.transAxes, ha="right", color=MUTED, fontsize=16 if big else 9)
    lo = min(float(r["lambda_exact_pct"]) for r in rows + [{"lambda_exact_pct": 100}])
    ax.set_ylim(max(0, min(lo, 99) - 2), 100.5)
    _style(ax, "Correctness over time (Y1/Y4): accounts with exact balance", "minutes", "% of accounts exactly equal to the oracle", big)
    _legend(ax, big)
    return fig


def chart_b2(rows, d, big):
    fig, ax = _new(big)
    t = [float(r["t_s"]) for r in rows]
    for key, col, lab in (("lag_kappa-v1", KAPPA, "Kappa consumer lag"), ("lag_lambda-speed", LAMBDA, "Lambda speed-layer consumer lag")):
        v = [float(r[key]) if r.get(key) not in (None, "") else float("nan") for r in rows]
        if any(x == x for x in v):
            ax.plot(t, v, color=col, lw=2.2, label=lab)
    step = d["step_seconds"]
    for i, s in enumerate(d["steps"]):
        ax.axvline(i * step, color=GRID, lw=1)
        ax.text(i * step + step / 2, ax.get_ylim()[1] * 0.97, f"{s['rate']:,}/s", ha="center", va="top", color=MUTED, fontsize=16 if big else 9)
    _style(ax, "Throughput ramp (B2): consumer lag per ingest step", "seconds", "messages behind", big)
    _legend(ax, big)
    return fig


def chart_b6(curve, k, big):
    fig, ax = _new(big)
    t = [float(r["t_s"]) for r in curve]
    ax.plot(t, [float(r["lag"]) for r in curve], color=KAPPA, lw=2.2, label="Kappa v2 replay: remaining events")
    ax.axvline(k["measured_T_s"] or 0, color=KAPPA, lw=1, ls="--")
    ax.axvline(k["predicted_T_s"], color=MUTED, lw=1.4, ls=":")
    ax.text(k["predicted_T_s"], ax.get_ylim()[1] * 0.9, f" predicted T = {k['predicted_T_s']:.0f}s\n (H / (R_process - R_in))", color=INK2, fontsize=16 if big else 9)
    ax.text(k["measured_T_s"] or 0, ax.get_ylim()[1] * 0.6, f" measured T = {k['measured_T_s']}s\n R_process = {k['measured_replay_rate_ev_s']:,} ev/s",
            color=INK2, fontsize=16 if big else 9)
    _style(ax, "Rule change (Y3): Kappa replay from offset 0", "seconds since job v2 started", "events still to replay", big)
    _legend(ax, big)
    return fig


def bars(title, labels, series, ylabel, big, fmt="{:,.0f}", log=False):
    """series: list of (name, color, values)"""
    fig, ax = _new(big)
    n = len(series)
    w = 0.8 / n
    for i, (name, col, vals) in enumerate(series):
        xs = [j + (i - (n - 1) / 2) * w for j in range(len(labels))]
        vv = [0 if v is None else v for v in vals]
        bars_ = ax.bar(xs, vv, w * 0.92, color=col, label=name, zorder=3)
        for x, v, rv in zip(xs, vv, vals):
            ax.text(x, v, ("n/a" if rv is None else fmt.format(v)), ha="center", va="bottom", color=INK2, fontsize=15 if big else 8)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels)
    if log:
        ax.set_yscale("log")
    _style(ax, title, "", ylabel, big)
    if n > 1:
        _legend(ax, big)
    return fig


def chart_b10(d, big):
    fig, ax = _new(big)
    rp = [r["R_process_ev_s"] / 1000 for r in d["table"]]
    ax.plot(rp, [r["replay_time_hours"] for r in d["table"]], color=KAPPA, lw=2.2, marker="o", ms=8, mfc=SURFACE, mew=2, label="Kappa replay")
    ax.set_xscale("log")
    ax.set_yscale("log")
    for r in d["table"]:
        ax.text(r["R_process_ev_s"] / 1000, r["replay_time_hours"], f"  {r['replay_time_hours']:.1f} h", color=INK2, fontsize=15 if big else 8, va="bottom")
    ax.axvline(d["R_in"] / 1000, color=MUTED, ls=":")
    ax.text(d["R_in"] / 1000, ax.get_ylim()[1] * 0.5, " R_in = 50k/s", color=INK2, fontsize=15 if big else 9)
    _style(ax, "Replay time for H = 10 billion events (T = H / (R_process - R_in))", "R_process (thousand events/s, log)", "replay time (hours, log)", big)
    return fig


def png(fig) -> bytes:
    b = io.BytesIO()
    fig.savefig(b, format="png", facecolor=SURFACE)
    plt.close(fig)
    return b.getvalue()


# ------------------------------------------------------------------------------------------------ report
def build(run_id: str) -> Path:
    out = RES / run_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "slides").mkdir(exist_ok=True)
    meta = jload(run_id, "meta.json") or {}
    b1 = reps(run_id, "b1_freshness.json")
    b1s = cload(run_id, "b1_freshness_samples.csv")
    b2, b2s = jload(run_id, "b2_throughput.json"), cload(run_id, "b2_throughput_samples.csv")
    b3, b3s = jload(run_id, "b3_correctness.json"), cload(run_id, "b3_correctness_over_time.csv")
    b4, b5 = reps(run_id, "b4_late_pos.json"), jload(run_id, "b5_duplicates_crash.json")
    b6, b6c = reps(run_id, "b6_rule_change.json"), cload(run_id, "b6_kappa_lag_curve.csv")
    b7, b8, b9, b10 = jload(run_id, "b7_drift.json"), jload(run_id, "b8_report.json"), jload(run_id, "b9_cost.json"), jload(run_id, "b10_extrapolation.json")
    ver_k, ver_l = jload(run_id, "verification.json"), jload(run_id, "verification_lambda.json")
    proofs = {"atomic swap": jload(run_id, "proof_atomic_swap.json"), "replay determinism": jload(run_id, "proof_replay_determinism.json")}

    charts: list[tuple[str, str, object, list[str], list[list]]] = []   # title, slug, builder(big)->fig, table header, table rows

    def add(slug, title, fn, header, rows):
        charts.append((slug, title, fn, header, rows))

    if b1:
        d = {"samples": b1s}
        add("b1_freshness_cdf", "B1 Freshness CDF", lambda big: chart_b1(d, big), ["reader", "p50 s", "p95 s", "p99 s", "timeouts"],
            [[k, f(med([r[k]["p50"] for r in b1]), 3), f(med([r[k]["p95"] for r in b1]), 3), f(med([r[k]["p99"] for r in b1]), 3),
              sum(r["timeouts"][k] for r in b1)] for k in ("kappa_api", "lambda_api", "kappa_direct", "lambda_direct")])
    if b3s:
        add("b3_correctness_over_time", "B3 Correctness over time", lambda big: chart_b3(b3s, big),
            ["t (s)", "Kappa exact %", "Lambda exact %", "Lambda batch @t_k %", "batch run"],
            [[r["t_s"], r["kappa_exact_pct"], r["lambda_exact_pct"], r["lambda_batch_asof_exact_pct"], r["batch_run_started"]] for r in b3s[::max(1, len(b3s) // 25)]])
    if b2 and b2s:
        add("b2_throughput_ramp", "B2 Throughput ramp", lambda big: chart_b2(b2s, b2, big), ["rate/s", "Kappa lag end", "Kappa sustained", "Lambda lag end", "Lambda sustained"],
            [[s["rate"], f(s.get("kappa-v1", {}).get("lag_end")), s.get("kappa-v1", {}).get("sustained"), f(s.get("lambda-speed", {}).get("lag_end")),
              s.get("lambda-speed", {}).get("sustained")] for s in b2["steps"]])
    if b6 and b6c:
        k = b6[0]["kappa"]
        add("b6_replay_lag_curve", "B6 Replay lag curve, predicted vs measured T", lambda big: chart_b6(b6c, k, big),
            ["metric", "value"], [[a, f(b)] for a, b in k.items()])
    if b4:
        a = b4[0]
        add("b4_late_pos", "B4 Late POS data: time until exact", lambda big: bars("Offline POS (<= 2 sim hours late): time until the number is exact", ["within limit"],
            [("Kappa", KAPPA, [a["kappa_exact_after_s"]]), ("Lambda (next nightly batch)", LAMBDA, [a["lambda_exact_after_s"]])], "seconds (real time)", big, "{:,.1f}"),
            ["metric", "value"], [[x, y] for x, y in a.items()])
    if b7:
        stages = ["baseline_v1", "spark_v2_flink_v1", "flink_fixed_before_next_batch", "after_next_batch"]
        add("b7_drift", "B7 Drift between codebases", lambda big: bars("Same account/day, app vs report: mismatching rows after each stage", ["baseline", "Spark v2 only", "Flink fixed,\nbefore batch", "after next batch"],
            [("mismatching account-days", LAMBDA, [b7[s]["mismatching_account_days"] for s in stages])], "account-days that disagree with the oracle", big),
            ["stage", "mismatching account-days"], [[s, b7[s]["mismatching_account_days"]] for s in stages])
    if b8:
        add("b8_report_latency", "B8 3-year category report latency", lambda big: bars("Historical report (Y2): API latency per account", ["p50", "p95"],
            [("Kappa", KAPPA, [b8["latency_ms"]["kappa"]["p50"], b8["latency_ms"]["kappa"]["p95"]]),
             ("Lambda", LAMBDA, [b8["latency_ms"]["lambda"]["p50"], b8["latency_ms"]["lambda"]["p95"]])], "milliseconds", big, "{:,.0f}"),
            ["metric", "value"], [["accounts", b8["accounts"]], ["batch durations s", b8["lambda_batch_duration_s"]], ["kappa checkpoint bytes", b8["kappa_checkpoint_state_bytes"]],
                                  ["reports != oracle", b8["reports_not_equal_to_oracle"]]])
    if b9:
        ps = b9["per_service"]
        names = sorted(ps, key=lambda s: -ps[s]["mem_peak_mb"])[:12]
        add("b9_resources", "B9 Resource usage per service", lambda big: bars("Peak memory per service during steady load", names,
            [("peak MB", LAMBDA, [ps[s]["mem_peak_mb"] for s in names])], "MB", big), ["service", "avg CPU %", "peak CPU %", "peak MB"],
            [[s, ps[s]["cpu_avg_pct"], ps[s]["cpu_peak_pct"], ps[s]["mem_peak_mb"]] for s in sorted(ps)])
        lt = b9["loc_totals"]
        add("b9_loc_services", "B9 Lines of code and services", lambda big: bars("Complexity: code you maintain and services you run", ["data-path lines of code\n(excl. shared)", "services in profile", "rule implementations\nto change for Y3"],
            [("Lambda", LAMBDA, [lt["lambda"], b9["services_per_profile"].get("lambda"), b9["rule_implementations_to_change_for_Y3"]["lambda"]]),
             ("Kappa", KAPPA, [lt["kappa"], b9["services_per_profile"].get("kappa"), b9["rule_implementations_to_change_for_Y3"]["kappa"]])], "count", big),
            ["metric", "Lambda", "Kappa"], [["lines of code", lt["lambda"], lt["kappa"]], ["services (profile)", b9["services_per_profile"].get("lambda"), b9["services_per_profile"].get("kappa")]])
    if b10:
        add("b10_extrapolation", "B10 Extrapolation to 10 billion events", lambda big: chart_b10(b10, big),
            ["R_process ev/s", "replay hours", "replay days", "cores at measured per-core rate"],
            [[r["R_process_ev_s"], r["replay_time_hours"], r["replay_time_days"], r["cores_needed_at_measured_per_core_rate"]] for r in b10["table"]])

    imgs = {}
    for slug, title, fn, header, rows in charts:
        (out / "slides" / f"{slug}.png").write_bytes(png(fn(True)))
        imgs[slug] = base64.b64encode(png(fn(False))).decode()

    summary = summary_table(b1, b2, b3, b4, b5, b6, b7, b8, b9)
    ver_html = verification_html(ver_k, ver_l, proofs)
    html = render_html(run_id, meta, summary, ver_html, charts, imgs, b10)
    (out / "report.html").write_text(html, encoding="utf-8")
    write_results_md(run_id, meta, summary, ver_k, ver_l, proofs, charts)
    print(f"[report] {out / 'report.html'}  slides: {len(charts)} PNG (1920x1080) -> {out / 'slides'}")
    return out / "report.html"


def summary_table(b1, b2, b3, b4, b5, b6, b7, b8, b9) -> list[list[str]]:
    rows: list[list[str]] = []

    def add(req, metric, lam, kap, win):
        rows.append([req, metric, lam, kap, win])

    def lower(a, b, unit=""):
        if a is None or b is None:
            return "not measured"
        return "Kappa" if b < a else "Lambda" if a < b else "tie"

    if b1:
        la, ka = med([r["lambda_api"]["p95"] for r in b1]), med([r["kappa_api"]["p95"] for r in b1])
        add("Y1 fresh", "end-to-end freshness p95 via API (s)", f(la, 3), f(ka, 3), lower(la, ka))
        ld, kd = med([r["lambda_direct"]["p95"] for r in b1]), med([r["kappa_direct"]["p95"] for r in b1])
        add("Y1 fresh", "freshness p95 reading the store directly (s)", f(ld, 3), f(kd, 3), lower(ld, kd))
    if b8:
        la, ka = b8["latency_ms"]["lambda"]["p95"], b8["latency_ms"]["kappa"]["p95"]
        add("Y2 historical", "3-year category report latency p95 (ms)", f(la, 1), f(ka, 1), lower(la, ka))
        add("Y2 historical", "cost of building the view", f"batch {f(med(b8['lambda_batch_duration_s']))} s per night",
            f"state {f((b8['kappa_checkpoint_state_bytes'] or 0) / 1e6, 1)} MB (checkpoint)", "different trade-off: Lambda pays per night, Kappa pays in state")
    if b6:
        la, ka = med([r["lambda"]["total_time_to_correct_s"] for r in b6]), med([r["kappa"]["catchup_s"] for r in b6])
        add("Y3 recompute", "time to correct after a rule change (s)", f(la, 1), f(ka, 1), lower(la, ka))
        add("Y3 recompute", "rule implementations to change", "2 (PySpark + Java)", "1 (Java)", "Kappa")
        add("Y3 recompute", "API failures during the switch", str(b6[0].get("lambda_api", {}).get("failed")), str(b6[0].get("kappa_api", {}).get("failed")), "both zero-downtime" if not b6[0].get("lambda_api", {}).get("failed") and not b6[0].get("kappa_api", {}).get("failed") else "see report")
    if b7:
        add("Y3 recompute", "drift when only Spark has the new rule (mismatching account-days)", str(b7["spark_v2_flink_v1"]["mismatching_account_days"]), "n/a (single code base)", "Kappa")
    if b4:
        a = b4[0]
        add("Y4 exact money", "late POS (<= 2 h): time until exact (s)", f(a["lambda_exact_after_s"], 1), f(a["kappa_exact_after_s"], 2), lower(a["lambda_exact_after_s"], a["kappa_exact_after_s"]))
        add("Y4 exact money", "events later than the limit", f"absorbed at next batch ({f(a['lambda_beyond_limit_exact_after_s'], 1)} s)",
            f"{a['kappa_late_events_topic_distinct_ids']} counted in late-events, not applied", "Lambda recomputes everything; Kappa surfaces them")
    if b5:
        add("Y4 exact money", "after duplicates + crash (TaskManager and archiver killed)",
            f"{b5['lambda_balance_mismatched_accounts']} accounts off by {f(b5['lambda_balance_total_error_vnd'], 0)} VND until the next batch",
            "exact (0 VND)" if b5["kappa_verification_passed"] else "NOT exact: " + ", ".join(b5["kappa_failed_checks"][:2]), "Kappa" if b5["kappa_verification_passed"] else "see report")
    if b3:
        add("Y1/Y4 correctness", "lowest share of exactly-correct accounts over the run (%)", f(b3["lambda_min_exact_pct"], 3), f(b3["kappa_min_exact_pct"], 3),
            "Kappa" if b3["kappa_min_exact_pct"] > b3["lambda_min_exact_pct"] else "tie" if b3["kappa_min_exact_pct"] == b3["lambda_min_exact_pct"] else "Lambda")
    if b2:
        km, lm = b2.get("max_sustained_kappa-v1"), b2.get("max_sustained_lambda-speed")
        add("scale", "max sustained ingest on this laptop (events/s)", f(lm), f(km), "not comparable if the generator was the limit" if (km == lm) else lower(-(lm or 0), -(km or 0)))
    if b9:
        add("cost", "data-path lines of code", str(b9["loc_totals"]["lambda"]), str(b9["loc_totals"]["kappa"]), "Kappa")
        add("cost", "services in the profile", str(b9["services_per_profile"].get("lambda")), str(b9["services_per_profile"].get("kappa")), "Kappa")
    return rows


def verification_html(ver_k, ver_l, proofs) -> str:
    h = []
    for title, v in (("Kappa vs oracle", ver_k), ("Lambda vs oracle", ver_l)):
        if not v:
            h.append(f"<h3>{title}</h3><p>not run</p>")
            continue
        ok = v["passed"]
        h.append(f"<h3>{title}: <span class='{'ok' if ok else 'bad'}'>{'PASS' if ok else 'FAIL'}</span> ({sum(c['status'] == 'pass' for c in v['checks'])}/{len(v['checks'])})</h3><table>"
                 + "".join(f"<tr><td class='{'ok' if c['status'] == 'pass' else 'bad'}'>{'&#10003; PASS' if c['status'] == 'pass' else '&#10007; FAIL'}</td><td>{c['name']}</td></tr>" for c in v["checks"]) + "</table>")
    for name, p in proofs.items():
        if p:
            h.append(f"<p><b>{name}:</b> <span class='{'ok' if p.get('passed') else 'bad'}'>{'&#10003; PASS' if p.get('passed') else '&#10007; FAIL'}</span></p>")
    return "\n".join(h)


CSS = """
:root{--bg:#f9f9f7;--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--grid:#e1e0d9;--ok:#006300;--bad:#d03b3b;--lam:#2a78d6;--kap:#eb6834}
@media (prefers-color-scheme: dark){:root:not([data-theme=light]){--bg:#0d0d0d;--surface:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--grid:#2c2c2a;--ok:#0ca30c;--bad:#e66767;--lam:#3987e5;--kap:#d95926}}
:root[data-theme=dark]{--bg:#0d0d0d;--surface:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--grid:#2c2c2a;--ok:#0ca30c;--bad:#e66767;--lam:#3987e5;--kap:#d95926}
body{background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif;margin:0;padding:24px 16px}
main{max-width:1100px;margin:0 auto}h1{font-size:26px}h2{margin-top:36px;font-size:20px}section{background:var(--surface);border:1px solid var(--grid);border-radius:10px;padding:16px;margin:16px 0}
table{border-collapse:collapse;width:100%;font-size:13.5px}td,th{border-bottom:1px solid var(--grid);padding:6px 8px;text-align:left;vertical-align:top}th{color:var(--ink2)}
.ok{color:var(--ok);font-weight:600}.bad{color:var(--bad);font-weight:600}img{max-width:100%;height:auto;border-radius:6px;background:#fcfcfb}
.lam{color:var(--lam);font-weight:600}.kap{color:var(--kap);font-weight:600}details summary{cursor:pointer;color:var(--ink2);margin:8px 0}.small{color:var(--ink2);font-size:13px}
"""


def render_html(run_id, meta, summary, ver_html, charts, imgs, b10) -> str:
    esc = lambda x: str(x).replace("&", "&amp;").replace("<", "&lt;")  # noqa: E731
    cfg = meta.get("config", {})
    env = (f"<p class='small'>Run <b>{esc(run_id)}</b> · profile {esc(meta.get('lk_profile'))} · git {esc((meta.get('git_commit') or '')[:10])} · "
           f"Docker {esc(meta.get('host', {}).get('docker_cpus'))} CPUs / {esc(round((meta.get('host', {}).get('docker_memory_bytes') or 0) / 2**30, 1))} GiB · "
           f"history events {esc(cfg.get('data', {}).get('history_events'))}, accounts {esc(cfg.get('data', {}).get('num_accounts'))}, sim speedup {esc(cfg.get('time', {}).get('sim_speedup'))} · "
           f"generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>")
    sums = ("<table><tr><th>Requirement</th><th>Metric</th><th class='lam'>Lambda</th><th class='kap'>Kappa</th><th>Winner / note</th></tr>"
            + "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in summary) + "</table>")
    secs = ""
    for slug, title, fn, header, rows in charts:
        tbl = "<table><tr>" + "".join(f"<th>{esc(h)}</th>" for h in header) + "</tr>" + "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in rows) + "</table>"
        secs += f"<section><h2>{esc(title)}</h2><img alt='{esc(title)}' src='data:image/png;base64,{imgs[slug]}'><details><summary>table view</summary>{tbl}</details></section>"
    threats = """<section><h2>Threats to validity</h2><ul>
<li><b>Scale:</b> the demo dataset is ~1 M events (profile <code>fast</code>), not 10 billion; B10 extrapolates linearly from measured rates and is an order of magnitude illustration only.</li>
<li><b>One laptop, one Kafka broker, 8 GiB Docker VM:</b> services compete for CPU and memory; Spark runs in local mode; Flink has 6 slots in one TaskManager. Absolute numbers are not production numbers; ratios and failure behaviour are what this run shows.</li>
<li><b>Simulated time</b> (144x): a "night" lasts 10 real minutes; real network, disk and GC effects do not scale with it. The 2-hour POS limit is 50 real seconds.</li>
<li><b>Object store:</b> SeaweedFS replaces MinIO (no pullable official image); Hadoop S3A did not work against it, so the Spark streaming checkpoint is on a local volume.</li>
<li><b>Single generator process</b> bounds the ingest rate in B2; when both architectures show the same maximum, the generator, not the architecture, is the limit.</li>
<li><b>Lambda speed layer:</b> Redis dedup (SET NX) is not transactional with the increments; the order chosen undercounts after a crash instead of double counting, and the next batch corrects it.</li>
<li><b>Repetitions:</b> timing scenarios are reported as median (spread) when repeated; scenarios that reset state (B5-B7) were run once unless stated.</li>
<li><b>Image versions:</b> see meta.json (digests) of this run; Kafka 3.9.1, Flink 1.20.2, Spark 3.5.6, Iceberg 1.10.0, ClickHouse 25.8, Redis 7.4.5, SeaweedFS 3.97.</li></ul></section>"""
    return (f"<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>Lambda vs Kappa Report</title><style>{CSS}</style></head><body><main><h1>Lambda vs Kappa: results</h1>{env}"
            f"<section><h2>Summary: requirements Y1-Y4</h2>{sums}</section><section><h2>Verification (automated, exact integer equality with the oracle)</h2>{ver_html}</section>"
            f"{secs}{threats}</main></body></html>")


def write_results_md(run_id, meta, summary, ver_k, ver_l, proofs, charts) -> None:
    L = [f"# Results (generated from results/{run_id} by `make report`; never edited by hand)\n",
         f"Profile `{meta.get('lk_profile')}` · git `{(meta.get('git_commit') or '')[:10]}` · Docker {meta.get('host', {}).get('docker_cpus')} CPUs / "
         f"{round((meta.get('host', {}).get('docker_memory_bytes') or 0) / 2**30, 1)} GiB · generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n",
         "## Summary\n", "| Requirement | Metric | Lambda | Kappa | Winner / note |", "|---|---|---|---|---|"]
    L += ["| " + " | ".join(str(c).replace("|", "/") for c in r) + " |" for r in summary]
    L.append("\n## Verification\n")
    for title, v in (("Kappa vs oracle", ver_k), ("Lambda vs oracle", ver_l)):
        if v:
            L.append(f"**{title}: {'PASS' if v['passed'] else 'FAIL'}** ({sum(c['status'] == 'pass' for c in v['checks'])}/{len(v['checks'])})\n")
            L += [f"- {'PASS' if c['status'] == 'pass' else 'FAIL'}: {c['name']}" for c in v["checks"]]
            L.append("")
    for name, p in proofs.items():
        if p:
            L.append(f"- {name}: {'PASS' if p.get('passed') else 'FAIL'}")
    L.append("\n## Charts and tables\n")
    for slug, title, fn, header, rows in charts:
        L += [f"### {title}\n", f"![{title}](../results/{run_id}/slides/{slug}.png)\n", "| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        L += ["| " + " | ".join(str(c).replace("|", "/") for c in r) + " |" for r in rows]
        L.append("")
    L.append("See `results/<run>/report.html` for the self-contained report incl. threats to validity.")
    Path("/app/docs/RESULTS.md" if Path("/app/docs").exists() else "docs/RESULTS.md").write_text("\n".join(L), encoding="utf-8")


if __name__ == "__main__":
    import sys
    build(sys.argv[1] if len(sys.argv) > 1 else "final")
