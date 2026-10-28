"""Oracle: exact expected values computed in SQL (DuckDB) from the truth Parquet log only.

Reads nothing but `truth/` plus the static merchants CSV. Never talks to Kafka, ClickHouse or Redis.
All results are integers (VND). `cutoff` is an event-time cutoff (exclusive); None = everything.
"""
from __future__ import annotations

import threading
from pathlib import Path

import duckdb

RULES_SQL = Path(__file__).resolve().parents[3] / "shared" / "rules-sql" / "rules.sql"
RULE_NUM = {"v1": 1, "v2": 2}


class Oracle:
    def __init__(self, truth_roots: list[str], merchants_csv: str):
        self.truth_roots = truth_roots
        self.con = duckdb.connect()
        self.lock = threading.Lock()
        self.merchants_csv = merchants_csv
        self._merchants_loaded = False
        self.excluded: set[str] = set()
        self.con.execute(RULES_SQL.read_text())

    # -- truth view, rebuilt per query so live-appended files are always seen ---------------------------------
    def _refresh(self) -> None:
        if not self._merchants_loaded:   # lazy: the service may start before `make seed` has produced the CSV
            self.con.execute(
                "CREATE OR REPLACE TABLE merchants AS SELECT merchant_id, category FROM read_csv(?, header=true, "
                "columns={'merchant_id':'VARCHAR','merchant_name':'VARCHAR','category':'VARCHAR'})",
                [self.merchants_csv])
            self._merchants_loaded = True
        globs = [f"{r}/**/*.parquet" for r in self.truth_roots if any(Path(r).rglob("*.parquet"))]
        if not globs:
            raise RuntimeError("no truth files found")
        self.con.execute("CREATE TABLE IF NOT EXISTS excluded (event_id VARCHAR)")
        lst = ", ".join("'" + g.replace("\\", "/").replace("'", "''") + "'" for g in globs)  # config paths, not user input
        self.con.execute(
            "CREATE OR REPLACE VIEW truth AS SELECT event_id, account_id, event_time, type, amount, merchant_id, "
            f"is_self_transfer, channel, pos_offline FROM read_parquet([{lst}], union_by_name=true) "
            "WHERE event_id NOT IN (SELECT event_id FROM excluded)")

    def _q(self, sql: str, params: list | None = None) -> list[tuple]:
        with self.lock:
            self._refresh()
            return self.con.execute(sql, params or []).fetchall()

    @staticmethod
    def _cut(cutoff: str | None) -> tuple[str, list]:
        return ("AND event_time < CAST(? AS TIMESTAMPTZ)", [cutoff]) if cutoff else ("", [])

    def exclude(self, event_ids: set[str]) -> None:
        """Events the architecture legitimately did not apply (Kappa: beyond the lateness limit, kept in late-events)."""
        with self.lock:
            self.con.execute("CREATE TABLE IF NOT EXISTS excluded (event_id VARCHAR)")
            self.con.execute("DELETE FROM excluded")
            if event_ids:
                self.con.executemany("INSERT INTO excluded VALUES (?)", [[e] for e in event_ids])
        self.excluded = set(event_ids)

    # -- scalar / per-account ----------------------------------------------------------------------------------
    def balance(self, account: str, cutoff: str | None = None) -> int:
        c, p = self._cut(cutoff)
        r = self._q(f"SELECT COALESCE(SUM(signed_amount(type, amount)), 0) FROM truth WHERE account_id=? {c}",
                    [account] + p)
        return int(r[0][0])

    def spend_day(self, account: str, day: str, rule: str = "v1", cutoff: str | None = None) -> dict:
        c, p = self._cut(cutoff)
        r = self._q(f"""SELECT COALESCE(SUM(spend_delta({RULE_NUM[rule]}, type, amount, is_self_transfer)), 0),
                               COUNT(spend_delta({RULE_NUM[rule]}, type, amount, is_self_transfer))
                        FROM truth WHERE account_id=? AND CAST(event_time AT TIME ZONE 'UTC' AS DATE)=CAST(? AS DATE) {c}""",
                    [account, day] + p)
        return {"spend": int(r[0][0]), "tx_count": int(r[0][1])}

    def report(self, account: str, frm: str, to: str, rule: str = "v1", cutoff: str | None = None) -> list[dict]:
        """Spend by (month, category) for [frm, to] inclusive days."""
        c, p = self._cut(cutoff)
        rows = self._q(f"""SELECT strftime(event_time AT TIME ZONE 'UTC', '%Y-%m') AS month,
                                  spend_category(t.type, m.category) AS category,
                                  SUM(spend_delta({RULE_NUM[rule]}, t.type, t.amount, t.is_self_transfer)) AS spend
                           FROM truth t LEFT JOIN merchants m USING (merchant_id)
                           WHERE account_id=? AND CAST(event_time AT TIME ZONE 'UTC' AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                             AND spend_delta({RULE_NUM[rule]}, t.type, t.amount, t.is_self_transfer) IS NOT NULL {c}
                           GROUP BY 1, 2 ORDER BY 1, 2""", [account, frm, to] + p)
        return [{"month": r[0], "category": r[1], "spend": int(r[2])} for r in rows]

    def trace(self, account: str, day: str, rule: str = "v1", cutoff: str | None = None) -> dict:
        c, p = self._cut(cutoff)
        rows = self._q(f"""SELECT t.event_id, strftime(event_time AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%gZ'), t.type,
                                  t.amount, t.merchant_id, t.is_self_transfer, t.pos_offline,
                                  spend_delta({RULE_NUM[rule]}, t.type, t.amount, t.is_self_transfer)
                           FROM truth t WHERE account_id=? AND CAST(event_time AT TIME ZONE 'UTC' AS DATE)=CAST(? AS DATE) {c}
                           ORDER BY event_time, event_id""", [account, day] + p)
        events = [{"event_id": r[0], "event_time": r[1], "type": r[2], "amount": int(r[3]), "merchant_id": r[4],
                   "is_self_transfer": r[5], "pos_offline": r[6],
                   "spend_contribution": None if r[7] is None else int(r[7])} for r in rows]
        return {"events": events, "spend": sum(e["spend_contribution"] or 0 for e in events),
                "tx_count": sum(e["spend_contribution"] is not None for e in events)}

    # -- full scans (verification) -----------------------------------------------------------------------------
    def all_balances(self, cutoff: str | None = None) -> dict[str, int]:
        c, p = self._cut(cutoff)
        return {a: int(b) for a, b in self._q(
            f"SELECT account_id, SUM(signed_amount(type, amount)) FROM truth WHERE TRUE {c} GROUP BY 1", p)}

    def all_spend_day(self, rule: str = "v1", cutoff: str | None = None) -> dict[str, dict]:
        c, p = self._cut(cutoff)
        n = RULE_NUM[rule]
        rows = self._q(f"""SELECT account_id, strftime(event_time AT TIME ZONE 'UTC', '%Y-%m-%d'),
                                  SUM(spend_delta({n}, type, amount, is_self_transfer)),
                                  COUNT(spend_delta({n}, type, amount, is_self_transfer))
                           FROM truth WHERE spend_delta({n}, type, amount, is_self_transfer) IS NOT NULL {c}
                           GROUP BY 1, 2""", p)
        return {f"{a}|{d}": {"spend": int(s), "tx_count": int(k)} for a, d, s, k in rows}

    def all_spend_cat_month(self, rule: str = "v1", cutoff: str | None = None) -> dict[str, int]:
        c, p = self._cut(cutoff)
        n = RULE_NUM[rule]
        rows = self._q(f"""SELECT t.account_id, spend_category(t.type, m.category),
                                  strftime(t.event_time AT TIME ZONE 'UTC', '%Y-%m'),
                                  SUM(spend_delta({n}, t.type, t.amount, t.is_self_transfer))
                           FROM truth t LEFT JOIN merchants m USING (merchant_id)
                           WHERE spend_delta({n}, t.type, t.amount, t.is_self_transfer) IS NOT NULL {c}
                           GROUP BY 1, 2, 3""", p)
        return {f"{a}|{cat}|{mo}": int(s) for a, cat, mo, s in rows}

    def totals(self, rule: str = "v1", cutoff: str | None = None) -> dict:
        c, p = self._cut(cutoff)
        n = RULE_NUM[rule]
        r = self._q(f"""SELECT COUNT(*), COUNT(DISTINCT event_id), COALESCE(SUM(signed_amount(type, amount)), 0),
                               COALESCE(SUM(spend_delta({n}, type, amount, is_self_transfer)), 0),
                               COUNT(spend_delta({n}, type, amount, is_self_transfer))
                        FROM truth WHERE TRUE {c}""", p)[0]
        return {"events": int(r[0]), "distinct_event_ids": int(r[1]), "balance_sum": int(r[2]),
                "spend_sum": int(r[3]), "spend_events": int(r[4])}

    def id_checksum(self, cutoff: str | None = None) -> int:
        """Σ first 8 bytes of event_id (big endian) mod 2^64 — same definition as the generator manifest."""
        c, p = self._cut(cutoff)
        r = self._q(f"""SELECT CAST(SUM(CAST(CAST(('0x' || substr(replace(event_id, '-', ''), 1, 16)) AS UBIGINT) AS HUGEINT)) AS HUGEINT)
                        FROM truth WHERE TRUE {c}""", p)
        return int(r[0][0] or 0) % (1 << 64)

    # -- order-independent digests: exact equality of whole result sets without shipping 10 M rows --------------------
    # row hash = first 8 bytes of md5(row string) as UBIGINT; digest = Σ row hash mod 2^64.
    # The same definition is computed inside ClickHouse by bench verify (reinterpretAsUInt64(reverse(substring(MD5(s),1,8)))).
    def _row_sql(self, metric: str, rule: str, cutoff: str | None) -> tuple[str, list]:
        c, p = self._cut(cutoff)
        n = RULE_NUM[rule]
        if metric == "balance":
            return (f"SELECT account_id AS a, account_id || '|' || CAST(SUM(signed_amount(type, amount)) AS VARCHAR) AS s "
                    f"FROM truth WHERE TRUE {c} GROUP BY account_id", p)
        if metric == "spend_day":
            return (f"""SELECT account_id AS a, account_id || '|' || strftime(event_time AT TIME ZONE 'UTC', '%Y-%m-%d') || '|'
                               || CAST(SUM(spend_delta({n}, type, amount, is_self_transfer)) AS VARCHAR) || '|'
                               || CAST(COUNT(spend_delta({n}, type, amount, is_self_transfer)) AS VARCHAR) AS s
                        FROM truth WHERE spend_delta({n}, type, amount, is_self_transfer) IS NOT NULL {c}
                        GROUP BY account_id, strftime(event_time AT TIME ZONE 'UTC', '%Y-%m-%d')""", p)
        if metric == "spend_cat_month":
            return (f"""SELECT t.account_id AS a, t.account_id || '|' || spend_category(t.type, m.category) || '|'
                               || strftime(t.event_time AT TIME ZONE 'UTC', '%Y-%m') || '|'
                               || CAST(SUM(spend_delta({n}, t.type, t.amount, t.is_self_transfer)) AS VARCHAR) AS s
                        FROM truth t LEFT JOIN merchants m USING (merchant_id)
                        WHERE spend_delta({n}, t.type, t.amount, t.is_self_transfer) IS NOT NULL {c}
                        GROUP BY t.account_id, spend_category(t.type, m.category), strftime(t.event_time AT TIME ZONE 'UTC', '%Y-%m')""", p)
        raise ValueError(metric)

    _H = "CAST(CAST('0x' || left(md5(s), 16) AS UBIGINT) AS HUGEINT)"

    def digest(self, metric: str, rule: str = "v1", cutoff: str | None = None) -> dict:
        sql, p = self._row_sql(metric, rule, cutoff)
        r = self._q(f"SELECT COUNT(*), COALESCE(SUM({self._H}), 0) FROM ({sql})", p)[0]
        return {"rows": int(r[0]), "digest": int(r[1]) % (1 << 64)}

    def account_digests(self, metric: str, rule: str = "v1", cutoff: str | None = None) -> dict[str, int]:
        sql, p = self._row_sql(metric, rule, cutoff)
        rows = self._q(f"SELECT a, SUM({self._H}) FROM ({sql}) GROUP BY a", p)
        return {a: int(d) % (1 << 64) for a, d in rows}

    def rows_for_accounts(self, metric: str, accounts: list[str], rule: str = "v1", cutoff: str | None = None) -> list[str]:
        sql, p = self._row_sql(metric, rule, cutoff)
        ph = ",".join("?" * len(accounts))
        return sorted(r[0] for r in self._q(f"SELECT s FROM ({sql}) WHERE a IN ({ph})", p + accounts))


def compact_truth(root: str, min_files: int = 40, min_age_s: float = 30.0) -> dict:
    """Merge the per-second live truth files of every run directory into one Parquet file (content unchanged).

    Only files older than `min_age_s` are touched, so a live run that is still writing is left alone. Run it when no oracle query is
    in flight (bench does it before scenarios that query the oracle hundreds of times)."""
    import time
    out = {"merged_files": 0, "dirs": 0}
    for d in sorted(Path(root).iterdir()) if Path(root).exists() else []:
        if not d.is_dir():
            continue
        files = [f for f in d.glob("part-*.parquet") if time.time() - f.stat().st_mtime > min_age_s]
        if len(files) < min_files:
            continue
        target = d / f"compact-{time.time_ns()}.parquet"
        tmp = d / (target.name + ".tmp")
        lst = ", ".join("'" + str(f).replace("\\", "/").replace("'", "''") + "'" for f in files)
        con = duckdb.connect()
        con.execute(f"COPY (SELECT * FROM read_parquet([{lst}], union_by_name=true)) TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.close()
        tmp.replace(target)
        for f in files:
            f.unlink()
        out["merged_files"] += len(files)
        out["dirs"] += 1
    return out
