import json
from datetime import timedelta
from pathlib import Path

import jsonschema
import pyarrow.parquet as pq
import pytest

from lk_common.config import TimeConfig, load_config
from lk_common.model import table_to_json_rows
from lk_generator.backfill import run_backfill

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def run(small_cfg):
    return run_backfill(small_cfg)


def test_counts_and_manifest(run, small_cfg):
    assert run["logical_events"] == 30_000
    assert run["stream_events"] == run["logical_events"] + run["duplicates"]
    assert len(run["months"]) == 36
    truth = pq.read_table(small_cfg["data"]["paths"]["truth_backfill"])
    stream = pq.read_table(small_cfg["data"]["paths"]["stream_backfill"])
    assert truth.num_rows == 30_000 and len(set(truth.column("event_id").to_pylist())) == 30_000
    assert stream.num_rows == run["stream_events"]
    # every stream event is a logical event (duplicates share event_id); truth has no duplicates
    assert set(stream.column("event_id").to_pylist()) == set(truth.column("event_id").to_pylist())


def test_determinism(small_cfg, tmp_path):
    cfg2 = json.loads(json.dumps(small_cfg))
    old_root = Path(small_cfg["data"]["paths"]["root"])
    new_root = tmp_path / "again"
    cfg2["data"]["paths"] = {k: str(new_root / Path(v).relative_to(old_root)) for k, v in
                             small_cfg["data"]["paths"].items()}
    cfg2["data"]["paths"]["root"] = str(new_root)
    a = run_backfill(small_cfg)
    b = run_backfill(cfg2)
    for k in ("logical_events", "stream_events", "late_pos", "duplicates", "id_checksum"):
        assert a[k] == b[k]
    first = pq.read_table(old_root / "stream" / "backfill" / "month=2024-06")
    second = pq.read_table(new_root / "stream" / "backfill" / "month=2024-06")
    assert first.equals(second)


def test_schema_and_money_invariants(run, small_cfg):
    schema = json.loads((ROOT / "shared" / "schemas" / "transaction.schema.json").read_text())
    stream = pq.read_table(Path(small_cfg["data"]["paths"]["stream_backfill"]) / "month=2024-06")
    rows = table_to_json_rows(stream.slice(0, 2000))
    v = jsonschema.Draft202012Validator(schema)
    for r in rows:
        v.validate(r)
        assert isinstance(r["amount"], int) and r["amount"] >= 1000
    assert all(r["produced_at"] is None for r in rows)


def test_late_pos_are_pos_events(run, small_cfg):
    truth = pq.read_table(small_cfg["data"]["paths"]["truth_backfill"])
    late = truth.filter(truth.column("pos_offline"))
    assert late.num_rows == run["late_pos"] > 0
    assert set(late.column("channel").to_pylist()) == {"POS"}


def test_sim_clock_conversion():
    tc = TimeConfig.from_config(load_config())
    assert tc.sim_speedup == 144
    assert tc.sim_to_real_seconds(timedelta(days=1)) == 600
    assert tc.allowed_lateness_sim.total_seconds() == 2 * 3600 + 600
