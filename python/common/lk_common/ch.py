"""Tiny ClickHouse HTTP client (parameterised queries) + Kappa view management."""
from __future__ import annotations

import os

import httpx

from .config import load_config


class ClickHouse:
    def __init__(self, base: str | None = None, database: str = "lk"):
        cfg = load_config()["clickhouse"]
        self.base = base or os.environ.get("CLICKHOUSE_URL") or f"http://{cfg['host']}:{cfg['http_port']}"
        self.database = database or cfg["database"]
        self.http = httpx.Client(timeout=300)

    def query(self, sql: str, **params) -> list[dict]:
        r = self.http.post(self.base + "/", content=sql,
                           params={"default_format": "JSON", "database": self.database, "output_format_json_quote_64bit_integers": 0,
                                   **{f"param_{k}": v for k, v in params.items()}})
        if r.status_code != 200:
            raise RuntimeError(f"ClickHouse {r.status_code}: {r.text[:500]}")
        return r.json()["data"]

    def execute(self, sql: str) -> None:
        r = self.http.post(self.base + "/", content=sql, params={"database": self.database})
        if r.status_code != 200:
            raise RuntimeError(f"ClickHouse {r.status_code}: {r.text[:500]}")

    def scalar(self, sql: str, **params):
        rows = self.query(sql, **params)
        return next(iter(rows[0].values())) if rows else None


def kappa_view_sql(version: str) -> dict[str, str]:
    t = lambda s: f"lk.kappa_{version}_{s}"  # noqa: E731
    return {
        "account": f"SELECT account_id, argMax(balance, version) AS balance, argMax(accepted, version) AS accepted, "
                   f"argMax(duplicates, version) AS duplicates, argMax(late, version) AS late, max(version) AS ver "
                   f"FROM {t('account')} GROUP BY account_id",
        "spend_day": f"SELECT account_id, day, argMax(spend, version) AS spend, argMax(tx_count, version) AS tx_count "
                     f"FROM {t('spend_day')} GROUP BY account_id, day",
        "spend_cat_month": f"SELECT account_id, category, month, argMax(spend, version) AS spend "
                           f"FROM {t('spend_cat_month')} GROUP BY account_id, category, month",
        "trace": f"SELECT * FROM {t('trace')} FINAL",
        "progress": f"SELECT * FROM {t('progress')} FINAL",
    }


def repoint_kappa_views(ch: ClickHouse, version: str) -> None:
    """Atomic per view (CREATE OR REPLACE VIEW). Readers always go through kappa_current_*."""
    for name, select in kappa_view_sql(version).items():
        ch.execute(f"CREATE OR REPLACE VIEW lk.kappa_current_{name} AS {select}")


def current_kappa_version(ch: ClickHouse) -> str | None:
    import re
    q = ch.query("SELECT as_select FROM system.tables WHERE database='lk' AND name='kappa_current_account'")
    m = re.search(r"kappa_(\w+?)_account", q[0]["as_select"]) if q else None
    return m.group(1) if m else None
