"""Verify your lab. Run inside the workspace: python -m pytest -q tests/test_environment.py"""

import json
import os
import py_compile
import sys
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
API_URL = os.environ.get("STOREFRONT_API_URL", "http://localhost:8000")


def test_python_version():
    assert sys.version_info[:2] == (3, 12), sys.version


def test_provided_files_are_here():
    for name in (
        "golden/july_daily_counts.csv",
        "golden/day27_files.csv",
        "incidents/DE-1841/scheduler-2026-08-19.log",
        "incidents/DE-1841/deploy.diff",
        "fernhill/pipeline/orders.py",
        "fernhill/pipeline/snapshots.py",
    ):
        assert (ROOT / name).is_file(), f"{name} is missing: download the starter again"
    broken = sorted((ROOT / "fixtures" / "broken_dags").glob("*.py"))
    assert len(broken) == 4, "fixtures/broken_dags should hold four DAG files"
    for dag_file in broken:  # they are valid Python: what is wrong with them is what DE-1803 is about
        py_compile.compile(str(dag_file), doraise=True)


def test_replay_data_matches_the_expected_results():
    from fernhill_source import data

    july = dict(data.golden()["july_daily_counts.csv"])
    lines = (ROOT / "golden" / "july_daily_counts.csv").read_text(encoding="utf-8").splitlines()[1:]
    assert len(lines) == 30
    assert {d: int(n) for d, n in (line.split(",") for line in lines)} == july


@pytest.mark.service
def test_practice_files_were_generated():
    data_dir = ROOT / "data"
    assert (data_dir / "3pl" / "fulfilment_20260727_b.csv").is_file(), (
        "data/3pl is incomplete. Restore it with: python -m fernhill_source generate --force"
    )
    assert len(list((data_dir / "customers").glob("customers_*.csv"))) == 62
    assert (data_dir / "incident" / "customer_snapshots_daily.csv").is_file()


@pytest.mark.service
def test_storefront_api_is_up_and_wants_a_token():
    with urllib.request.urlopen(f"{API_URL}/health", timeout=5) as response:
        assert json.load(response) == {"status": "ok"}
    with pytest.raises(urllib.error.HTTPError) as refused:
        urllib.request.urlopen(f"{API_URL}/v1/orders?date={date(2026, 8, 14)}", timeout=5)
    assert refused.value.code == 401


@pytest.mark.service
def test_warehouse_is_reachable():
    import psycopg

    with psycopg.connect(os.environ["WAREHOUSE_URL"], connect_timeout=5) as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
    assert version.startswith("16."), version
