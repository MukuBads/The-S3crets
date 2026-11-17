# Config checks that run in CI from minute one; P3/P5 add test_cleansing.py and test_kpis.py next to it.
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_state_region_covers_28_states_and_6_regions():
    rows = list(csv.DictReader(open(ROOT / "conf" / "state_region.csv")))
    assert len(rows) == 28
    assert {r["region"] for r in rows} == {"North", "South", "East", "West", "Central", "North-East"}


def test_festival_dates_are_in_2026_and_sorted():
    rows = list(csv.DictReader(open(ROOT / "conf" / "festivals.csv")))
    dates = [r["festival_date"] for r in rows]
    assert all(d.startswith("2026-") for d in dates) and dates == sorted(dates)


def test_bronze_defines_all_six_feeds():
    src = (ROOT / "src" / "pipeline" / "bronze.py").read_text()
    for feed in ["categories", "products", "customers", "orders", "order_items", "returns"]:
        assert f'"{feed}"' in src
