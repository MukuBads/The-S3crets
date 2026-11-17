# Databricks notebook source
# MAGIC %md
# MAGIC # Stream replayer (owner: P2)
# MAGIC Replays the 6 dirty source CSVs, **unchanged**, as a stream of small CSV files into
# MAGIC `/Volumes/<catalog>/landing/files/<feed>/`. The data already has its own dates, duplicates and
# MAGIC quality problems, so nothing is added or fixed here: every source row is written exactly once.
# MAGIC
# MAGIC - file 0: all reference data (categories, products, customers)
# MAGIC - then orders, order_items and returns in event-date order (items right after their order;
# MAGIC   rows whose date or parent can't be read are spread randomly, like real late/odd arrivals)
# MAGIC - writes `_manifest.json` (rows per feed) and `_state.json` (resume point)
# MAGIC
# MAGIC Run all. Re-running resumes from the last file; set `reset = yes` to start over
# MAGIC (also clear `landing/files` and full-refresh the pipeline if you want a truly clean restart).

# COMMAND ----------

import json, os, random, time
import pandas as pd

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("rows_per_file", "2000")
    dbutils.widgets.text("seconds_between_files", "5")
    dbutils.widgets.text("max_files", "0")           # 0 = run to the end
    dbutils.widgets.dropdown("reset", "no", ["no", "yes"])
    P = {k: dbutils.widgets.get(k) for k in ["catalog", "rows_per_file", "seconds_between_files", "max_files", "reset"]}
    SRC = f"/Volumes/{P['catalog']}/landing/source_csv"
    OUT = f"/Volumes/{P['catalog']}/landing/files"
except NameError:                                   # local test run
    P = {"rows_per_file": os.environ.get("ROWS", "2000"), "seconds_between_files": "0",
         "max_files": os.environ.get("MAX_FILES", "0"), "reset": "yes"}
    SRC, OUT = os.environ["SRC"], os.environ["OUT"]

ROWS, SLEEP, MAX_FILES = int(P["rows_per_file"]), float(P["seconds_between_files"]), int(P["max_files"])
FEEDS = ["categories", "products", "customers", "orders", "order_items", "returns"]
rng = random.Random(42)

# COMMAND ----------

# Read everything as text so dirty values ("two", "₹12,345.67", "Mar 05, 2026", "NULL") survive untouched
src = {f: pd.read_csv(f"{SRC}/{f}.csv", dtype=str, keep_default_na=False) for f in FEEDS}
COLS = {f: list(df.columns) for f, df in src.items()}


def sort_key(dates: pd.Series) -> pd.Series:
    """Best-effort date for ordering only (the pipeline does the real parsing).
    Unreadable dates get a random position inside the real date range."""
    d = pd.to_datetime(dates, errors="coerce", format="mixed", dayfirst=True)
    d = d.where((d >= "2023-01-01") & (d <= "2026-11-15"))
    lo, hi = pd.Timestamp("2023-01-01").value, pd.Timestamp("2026-11-15").value
    filler = pd.to_datetime([rng.randint(lo, hi) for _ in range(len(d))])
    return d.fillna(pd.Series(filler, index=d.index))


orders = src["orders"].assign(_t=lambda x: sort_key(x["order_date"]))
order_t = orders.drop_duplicates("order_id").set_index("order_id")["_t"]
items = src["order_items"].copy()
items["_t"] = items["order_id"].map(order_t)
missing = items["_t"].isna()
items.loc[missing, "_t"] = sort_key(pd.Series([""] * missing.sum(), index=items.index[missing]))
items["_t"] = pd.to_datetime(items["_t"]) + pd.Timedelta(seconds=1)   # right after their order
rets = src["returns"].assign(_t=lambda x: sort_key(x["return_date"]))

stream = pd.concat([orders.assign(_feed="orders"), items.assign(_feed="order_items"),
                    rets.assign(_feed="returns")], ignore_index=True)
stream = stream.sort_values("_t", kind="stable").reset_index(drop=True)

ref = pd.concat([src[f].assign(_feed=f) for f in ["categories", "products", "customers"]], ignore_index=True)
chunks = [ref] + [stream.iloc[i:i + ROWS] for i in range(0, len(stream), ROWS)]
manifest = {"rows_per_feed": {f: len(src[f]) for f in FEEDS}, "files": len(chunks), "rows_per_file": ROWS}

# COMMAND ----------

for f in FEEDS:
    os.makedirs(f"{OUT}/{f}", exist_ok=True)
state_path = f"{OUT}/_state.json"
start = 0
if P["reset"] == "no" and os.path.exists(state_path):
    start = json.load(open(state_path))["next_file"]
json.dump(manifest, open(f"{OUT}/_manifest.json", "w"), indent=2)
print("manifest:", manifest)

written = 0
for n in range(start, len(chunks)):
    if MAX_FILES and written >= MAX_FILES:
        break
    for feed, part in chunks[n].groupby("_feed"):
        part[COLS[feed]].to_csv(f"{OUT}/{feed}/part_{n:05d}.csv", index=False)   # same column order as the source
    json.dump({"next_file": n + 1}, open(state_path, "w"))
    written += 1
    print(f"file {n + 1}/{len(chunks)}: {len(chunks[n])} rows")
    if n + 1 < len(chunks):
        time.sleep(SLEEP)
print("done" if start + written >= len(chunks) else f"paused before file {start + written + 1}; run again to continue")
