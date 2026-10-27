"""The same test vectors must pass in every rule implementation (Python + SQL here, Java in phase 2)."""
from pathlib import Path

import duckdb

from spend_rules_py import compute_metrics

RULES_SQL = Path(__file__).resolve().parents[2] / "shared" / "rules-sql" / "rules.sql"


def test_python_vectors(spec):
    for v in spec["test_vectors"]:
        for rule in ("v1", "v2"):
            got = compute_metrics(rule, v["events"], v["merchants"])
            exp = v["expect"][rule]
            assert got["balance"] == exp["balance"], (v["name"], rule, "balance")
            assert got["spend_day"] == exp["spend_day"], (v["name"], rule, "spend_day")
            assert got["spend_cat_month"] == exp["spend_cat_month"], (v["name"], rule, "spend_cat_month")


def test_sql_vectors(spec):
    con = duckdb.connect()
    con.execute(RULES_SQL.read_text())
    for v in spec["test_vectors"]:
        con.execute("CREATE OR REPLACE TABLE m(merchant_id VARCHAR, category VARCHAR)")
        for k, c in v["merchants"].items():
            con.execute("INSERT INTO m VALUES (?, ?)", [k, c])
        con.execute("CREATE OR REPLACE TABLE t(account_id VARCHAR, ts TIMESTAMPTZ, type VARCHAR, amount BIGINT,"
                    " merchant_id VARCHAR, is_self BOOLEAN)")
        for e in v["events"]:
            con.execute("INSERT INTO t VALUES (?,?,?,?,?,?)",
                        [e["account"], e["time"], e["type"], e["amount"], e["merchant"], e["self"]])
        for rule, n in (("v1", 1), ("v2", 2)):
            exp = v["expect"][rule]
            bal = dict(con.execute("SELECT account_id, SUM(signed_amount(type, amount)) FROM t GROUP BY 1").fetchall())
            assert {k: int(x) for k, x in bal.items()} == exp["balance"], (v["name"], rule)
            day = {f"{a}|{d}": {"spend": int(s), "tx_count": int(c)} for a, d, s, c in con.execute(
                f"""SELECT account_id, strftime(ts AT TIME ZONE 'UTC', '%Y-%m-%d'),
                           SUM(spend_delta({n}, type, amount, is_self)),
                           COUNT(spend_delta({n}, type, amount, is_self)) FROM t
                    WHERE spend_delta({n}, type, amount, is_self) IS NOT NULL GROUP BY 1, 2""").fetchall()}
            assert day == exp["spend_day"], (v["name"], rule)
            cat = {f"{a}|{c}|{mo}": int(s) for a, c, mo, s in con.execute(
                f"""SELECT t.account_id, spend_category(t.type, m.category), strftime(ts AT TIME ZONE 'UTC', '%Y-%m'),
                           SUM(spend_delta({n}, t.type, t.amount, t.is_self)) FROM t LEFT JOIN m USING (merchant_id)
                    WHERE spend_delta({n}, t.type, t.amount, t.is_self) IS NOT NULL GROUP BY 1, 2, 3""").fetchall()}
            assert cat == exp["spend_cat_month"], (v["name"], rule)
