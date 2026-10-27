"""Oracle (SQL) must agree with the pure-Python rule implementation on generated data."""
import pyarrow.parquet as pq
import pytest

from lk_common.model import read_merchants_csv
from lk_generator.backfill import run_backfill
from lk_oracle.core import Oracle
from spend_rules_py import compute_metrics


@pytest.fixture(scope="module")
def setup(small_cfg):
    run = run_backfill(small_cfg)
    p = small_cfg["data"]["paths"]
    orc = Oracle([p["truth_backfill"]], p["merchants_csv"])
    truth = pq.read_table(p["truth_backfill"]).to_pylist()
    merchants = {m[0]: m[2] for m in read_merchants_csv(p["merchants_csv"])}
    events = [{"account": e["account_id"], "time": e["event_time"].strftime("%Y-%m-%dT%H:%M:%S.000Z"),
               "type": e["type"], "amount": e["amount"], "merchant": e["merchant_id"], "self": e["is_self_transfer"]}
              for e in truth]
    return run, orc, events, merchants


@pytest.mark.parametrize("rule", ["v1", "v2"])
def test_full_scan_equals_python(setup, rule):
    _, orc, events, merchants = setup
    py = compute_metrics(rule, events, merchants)
    assert orc.all_balances() == py["balance"]
    assert orc.all_spend_day(rule) == py["spend_day"]
    assert orc.all_spend_cat_month(rule) == py["spend_cat_month"]


def test_conservation_and_checksum(setup):
    run, orc, _, _ = setup
    for rule in ("v1", "v2"):
        days = orc.all_spend_day(rule)
        cats = orc.all_spend_cat_month(rule)
        assert sum(d["spend"] for d in days.values()) == sum(cats.values()) == orc.totals(rule)["spend_sum"]
    t = orc.totals("v1")
    assert t["events"] == t["distinct_event_ids"] == run["logical_events"]
    assert orc.id_checksum() == run["id_checksum"]


def test_point_queries_match_full_scan(setup):
    _, orc, _, _ = setup
    bal = orc.all_balances()
    acct = max(bal, key=lambda a: abs(bal[a]))
    assert orc.balance(acct) == bal[acct]
    days = orc.all_spend_day("v2")
    k = next(k for k in days if k.startswith(acct + "|"))
    _, day = k.split("|")
    assert orc.spend_day(acct, day, "v2") == days[k]
    tr = orc.trace(acct, day, "v2")
    assert tr["spend"] == days[k]["spend"] and tr["tx_count"] == days[k]["tx_count"]
    assert sum(r["spend"] for r in orc.report(acct, "2023-01-01", "2025-12-31", "v2")) == sum(
        v["spend"] for kk, v in days.items() if kk.startswith(acct + "|"))


def test_cutoff_is_exclusive(setup):
    _, orc, _, _ = setup
    total = orc.totals("v1")["events"]
    assert orc.totals("v1", "2023-01-01T00:00:00Z")["events"] == 0
    assert orc.totals("v1", "2026-01-01T00:00:00Z")["events"] == total
