"""Lambda serving merge: batch(< t_k) + realtime(>= t_k), with fakes for ClickHouse and Redis."""
from lk_api import lambda_ as lam


class FakeCH:
    def __init__(self, rows):
        self.rows = rows

    def query(self, sql, **params):
        return self.rows


class FakeRedis:
    def __init__(self, hashes):
        self.h = hashes

    def scan_iter(self, pat):
        pre = pat.rstrip("*")
        return [k for k in self.h if k.startswith(pre)]

    def hget(self, key, f):
        return self.h.get(key, {}).get(f)

    def hscan_iter(self, key, match=None):
        pre = (match or "*").rstrip("*")
        return [(f, v) for f, v in self.h.get(key, {}).items() if f.startswith(pre)]

    def lrange(self, *a):
        return []


META = {"kind": "meta", "k1": "2026-01-02T00:00:00.000Z", "k2": "v1", "spend": 0, "tx_count": 10}


def setup(monkeypatch, rows, hashes):
    monkeypatch.setattr(lam, "rds", lambda: FakeRedis(hashes))
    return FakeCH([META] + rows)


def test_balance_is_batch_plus_realtime(monkeypatch):
    ch = setup(monkeypatch, [{"kind": "balance", "k1": "", "k2": "", "spend": 1000, "tx_count": 0}],
               {"rt:bal:2026-01-02": {"A1": "-300"}, "rt:bal:2026-01-03": {"A1": "50"}})
    bal, info = lam.balance(ch, "A1")
    assert bal == 750 and info["batch_part"] == 1000 and info["realtime_part"] == -250 and info["t_k"] == META["k1"]


def test_day_before_tk_comes_from_batch_day_at_tk_from_speed(monkeypatch):
    ch = setup(monkeypatch, [{"kind": "day", "k1": "2026-01-01", "k2": "", "spend": 500, "tx_count": 2}],
               {"rt:spend:2026-01-02": {"A1": "70"}, "rt:cnt:2026-01-02": {"A1": "1"}})
    assert lam.spend(ch, "A1", "2026-01-01")[0] == {"spend": 500, "tx_count": 2}      # t_k - 1 day: batch only
    assert lam.spend(ch, "A1", "2026-01-02")[0] == {"spend": 70, "tx_count": 1}       # the day starting exactly at t_k: speed only


def test_report_merges_month_straddling_tk(monkeypatch):
    ch = setup(monkeypatch, [{"kind": "cat_month", "k1": "FOOD", "k2": "2026-01", "spend": 100, "tx_count": 0}],
               {"rt:bal:2026-01-02": {"A1": "0"}, "rt:cat:2026-01-02": {"A1|FOOD|2026-01": "40", "A1|BILLS|2026-01": "5", "A2|FOOD|2026-01": "999"}})
    rows, _ = lam.report(ch, "A1", "2026-01", "2026-01")
    assert rows == [{"month": "2026-01", "category": "BILLS", "spend": 5}, {"month": "2026-01", "category": "FOOD", "spend": 140}]
