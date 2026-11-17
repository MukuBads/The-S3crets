# Config checks that run in CI; P3/P5 add test_cleansing.py and test_kpis.py next to it.
import ast
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_state_region_has_28_states_in_4_regions():
    rows = list(csv.DictReader(open(ROOT / "conf" / "state_region.csv")))
    assert len(rows) == 28
    assert {r["region"] for r in rows} == {"NORTH", "SOUTH", "EAST", "WEST"}


def test_state_region_matches_clean_reference():
    mapping = {r["state"]: r["region"] for r in csv.DictReader(open(ROOT / "conf" / "state_region.csv"))}
    for r in csv.DictReader(open(ROOT / "data" / "clean_reference" / "customers.csv")):
        assert mapping[r["state"]] == r["region"], r["customer_id"]


def test_bronze_columns_match_raw_csv_headers():
    tree = ast.parse((ROOT / "src" / "pipeline" / "bronze.py").read_text())
    columns = next(ast.literal_eval(n.value) for n in ast.walk(tree)
                   if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "COLUMNS")
    for feed, cols in columns.items():
        header = next(csv.reader(open(ROOT / "data" / "raw" / f"{feed}.csv")))
        assert header == cols, feed
