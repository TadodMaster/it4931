from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def spec():
    return yaml.safe_load((ROOT / "shared" / "rules" / "spend_rules.yaml").read_text())


@pytest.fixture(scope="session")
def small_cfg(tmp_path_factory):
    """Tiny deterministic dataset in a temp dir: 36 months, 30k events, 500 accounts, 200 merchants."""
    from lk_common.config import load_config
    cfg = load_config()
    root = tmp_path_factory.mktemp("lkdata")
    cfg["data"].update(num_accounts=500, num_merchants=200, history_events=30_000)
    old = Path(cfg["data"]["paths"]["root"])
    cfg["data"]["paths"] = {"root": str(root), **{k: str(root / Path(v).relative_to(old))
                                                  for k, v in cfg["data"]["paths"].items() if k != "root"}}
    return cfg
