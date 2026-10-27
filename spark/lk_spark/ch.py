"""Stdlib-only ClickHouse HTTP client (usable by the scheduler, which has no pyspark on its path, and by executors)."""
from __future__ import annotations

import os
import urllib.error
import urllib.parse
import urllib.request

from lk_common.config import load_config


class ClickHouse:
    """Minimal HTTP client usable inside Spark executors (stdlib only)."""

    def __init__(self, base: str | None = None):
        cfg = load_config()["clickhouse"]
        self.base = base or os.environ.get("CLICKHOUSE_URL") or f"http://{cfg['host']}:{cfg['http_port']}"

    def execute(self, sql: str) -> str:
        req = urllib.request.Request(self.base + "/", data=sql.encode(), method="POST")
        try:
            return urllib.request.urlopen(req, timeout=300).read().decode()
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"ClickHouse {e.code}: {e.read().decode()[:400]}") from None

    def insert_json_rows(self, table: str, rows: list[str]) -> None:
        q = urllib.parse.quote(f"INSERT INTO {table} FORMAT JSONEachRow")
        req = urllib.request.Request(f"{self.base}/?query={q}", data="\n".join(rows).encode(), method="POST")
        try:
            urllib.request.urlopen(req, timeout=300).read()
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"ClickHouse insert {e.code}: {e.read().decode()[:400]}") from None

    def scalar(self, sql: str) -> str:
        return self.execute(sql).strip()
