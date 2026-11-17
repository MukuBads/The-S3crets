"""Step 3 - inject realistic data-quality issues into the clean expanded dataset.

Input : work/clean_expanded/*.csv
Output: output/*.csv                        (final, intentionally dirty datasets)
        output/clean_reference/*.csv        (ground-truth clean version)
        output/data_quality_issue_log.csv   (one line per injected issue)

Design
* Every table is read as text, so formatting corruption is stored exactly as written.
* Each issue type hits a small, fixed fraction of rows, so most rows stay clean.
* Customers / orders / returns must keep an exact row count, so duplicates *replace*
  "victim" rows instead of being appended. Victims are always newly generated rows
  (never original data) that have no child rows, so no foreign key breaks by accident.
* order_items has no fixed target: its duplicates are appended at random positions.
* Foreign keys are broken only on purpose, in a small, logged share of rows.
"""
from __future__ import annotations

import datetime as dt
import shutil

import numpy as np
import pandas as pd

from common import CLEAN_DIR, OUTPUT_DIR, SEED, UNCHANGED_FILES

rng = np.random.default_rng(SEED + 1)
LOG: list[dict] = []

ORIG_MAX = {"customers": 5000, "orders": 9000, "order_items": 15000, "returns": 1500}


# ----------------------------------------------------------------------------- helpers
class Table:
    def __init__(self, name: str, key: str):
        self.name, self.key = name, key
        self.df = pd.read_csv(CLEAN_DIR / f"{name}.csv", dtype=str, keep_default_na=False).astype(object)
        self.df["_rid"] = np.arange(len(self.df))
        self.touched: dict[str, set] = {}
        self.protected: set[int] = set()       # victims: excluded from cell corruption

    def pick(self, col: str, frac: float, mask: pd.Series | None = None) -> np.ndarray:
        ok = pd.Series(True, index=self.df.index) if mask is None else mask.copy()
        ok &= ~self.df.index.isin(list(self.protected))
        ok &= ~self.df.index.isin(list(self.touched.get(col, set())))
        cand = self.df.index[ok].to_numpy()
        n = min(len(cand), int(round(frac * len(self.df))))
        chosen = rng.choice(cand, n, replace=False) if n else np.array([], dtype=int)
        self.touched.setdefault(col, set()).update(chosen.tolist())
        return chosen

    def corrupt(self, col: str, frac: float, issue: str, fn, mask=None) -> None:
        for i in self.pick(col, frac, mask):
            old = self.df.at[i, col]
            new = fn(old, i)
            self.df.at[i, col] = new
            log(self, i, col, issue, old, new)


def log(t: Table, i: int, col: str, issue: str, old, new) -> None:
    LOG.append({"table": t.name, "_rid": int(t.df.at[i, "_rid"]), "key": t.df.at[i, t.key],
                "column": col, "issue_type": issue,
                "clean_value": "" if pd.isna(old) else str(old),
                "dirty_value": "" if pd.isna(new) else str(new)})


def null_value(*_):
    return np.nan if rng.random() < 0.75 else rng.choice(["NULL", "N/A", "null", "NaN", "none", "-"])


def date_variant(iso: str, *_):
    d = dt.date.fromisoformat(iso)
    fmt = rng.choice(["%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%b %d, %Y", "%Y%m%d",
                      "%m/%d/%Y", "%d %B %Y", "%Y-%m-%dT00:00:00", "%d.%m.%Y"])
    return d.strftime(fmt)


def shift(iso: str, lo: int, hi: int) -> str:
    return (dt.date.fromisoformat(iso) + dt.timedelta(days=int(rng.integers(lo, hi + 1)))).isoformat()


def future_date(*_):
    return (dt.date(2027, 1, 1) + dt.timedelta(days=int(rng.integers(0, 1500)))).isoformat()


def impossible_date(*_):
    return rng.choice(["2025-02-30", "2024-13-05", "2026-04-31", "0000-00-00", "2025-00-10", "2023-06-32"])


def placeholder_date(*_):
    return rng.choice(["1900-01-01", "1970-01-01", "1899-12-31"])


def case_noise(v: str, *_):
    k = rng.integers(0, 5)
    return [v.upper(), v.lower(), f"  {v}", f"{v}  ", v.replace(" ", "  ")][k]


def money_variant(v: str, *_):
    x = float(v)
    return rng.choice([f"₹{x:,.2f}", f"Rs. {x:.2f}", f"{x:,.2f}", f"{x:.2f} INR", f"INR {x:,.0f}"])


def victims(t: Table, n: int, eligible: pd.Series) -> np.ndarray:
    cand = t.df.index[eligible & ~t.df.index.isin(list(t.protected))].to_numpy()
    if len(cand) < n:
        raise RuntimeError(f"{t.name}: only {len(cand)} victim rows for {n} duplicates")
    v = rng.choice(cand, n, replace=False)
    t.protected.update(v.tolist())
    return v


def replace_with_duplicates(t: Table, vic: np.ndarray, mode: str, alter=None) -> None:
    """Overwrite each victim row with a copy of a random non-victim source row."""
    pool = t.df.index[~t.df.index.isin(list(t.protected))].to_numpy()
    for v in vic:
        s = rng.choice(pool)
        cols = [c for c in t.df.columns if c != "_rid"]
        old_key = t.df.at[v, t.key]
        t.df.loc[v, cols] = t.df.loc[s, cols].to_numpy()
        note = f"copy of {t.key}={t.df.at[s, t.key]}"
        if alter:
            note += "; " + alter(v)
        log(t, v, "*row*", mode, f"{t.key}={old_key}", note)


def near_duplicate(t: Table, vic: np.ndarray, cols: list[str]) -> None:
    """Same real-world entity, different primary key."""
    pool = t.df.index[~t.df.index.isin(list(t.protected))].to_numpy()
    for v in vic:
        s = rng.choice(pool)
        old = "|".join(str(t.df.at[v, c]) for c in cols)
        for c in cols:
            t.df.at[v, c] = t.df.at[s, c]
        log(t, v, "*row*", "near_duplicate_entity", old, f"same entity as {t.key}={t.df.at[s, t.key]}")


def is_new(t: Table) -> pd.Series:
    return t.df[t.key].astype(int) > ORIG_MAX[t.name]


# ----------------------------------------------------------------------------- customers
CITY_ALIAS = {"Bengaluru": "Bangalore", "Mumbai": "Bombay", "Gurugram": "Gurgaon", "Kolkata": "Calcutta",
              "Chennai": "Madras", "Mysuru": "Mysore", "Mangaluru": "Mangalore", "Vadodara": "Baroda",
              "Thiruvananthapuram": "Trivandrum", "Kochi": "Cochin", "Panaji": "Panjim", "Pune": "Poona"}
STATE_VARIANT = {"Maharashtra": ["Maharastra", "MH"], "Tamil Nadu": ["Tamilnadu", "TN"],
                 "Karnataka": ["KA", "Karnatka"], "Uttar Pradesh": ["UP", "U.P."],
                 "West Bengal": ["WB", "W. Bengal"], "Andhra Pradesh": ["AP", "Andhra"],
                 "Gujarat": ["Gujrat", "GJ"], "Rajasthan": ["Rajastan", "RJ"], "Kerala": ["KL", "Keral"],
                 "Telangana": ["TS", "Telengana"], "Odisha": ["Orissa", "OD"], "Uttarakhand": ["Uttaranchal", "UK"],
                 "Chhattisgarh": ["Chattisgarh", "CG"], "Punjab": ["PB", "Panjab"]}


def phone_variant(v: str, *_):
    d = v[-10:]
    return rng.choice([d, f"0{d}", f"+91 {d[:5]} {d[5:]}", f"91{d}", f"(+91) {d}", f"+91{d}",
                       f"{d[:3]}-{d[3:6]}-{d[6:]}"])


def phone_invalid(v: str, *_):
    d = v[-10:]
    return rng.choice([d[:9], d + "7", f"+91-{rng.integers(1, 6)}{d[1:]}", "0000000000",
                       "9999999999", f"+91-{d[:4]}X{d[5:]}", "12345"])


def email_invalid(v: str, *_):
    local, dom = v.split("@", 1)
    return rng.choice([local + dom, f"{local}@@{dom}", f"{local}@{dom.split('.')[0]}",
                       f"{local} @{dom}", f"{local}@{dom}.", f"@{dom}", f"{local}@", f"{local}#{dom}"])


def region_case(v: str, *_):
    return rng.choice([v.lower(), v.title(), f" {v}", v[0], f"{v} "])


def region_mismatch(v: str, *_):
    return rng.choice(sorted({"NORTH", "SOUTH", "EAST", "WEST"} - {v}))


def inject_customers(t: Table, orders_clean: pd.DataFrame) -> None:
    df = t.df
    has_orders = df["customer_id"].isin(set(orders_clean["customer_id"]))
    childless_new = is_new(t) & ~has_orders
    v_exact = victims(t, 300, childless_new)
    v_conf = victims(t, 90, childless_new)
    v_near = victims(t, 150, childless_new)

    t.corrupt("name", .004, "null", null_value)
    t.corrupt("name", .012, "inconsistent_format", case_noise)
    t.corrupt("email", .012, "null", null_value)
    t.corrupt("email", .01, "invalid_value", email_invalid)
    t.corrupt("email", .008, "inconsistent_format", lambda v, i: v.upper() if rng.random() < .5 else f" {v} ")
    t.corrupt("phone_number", .015, "null", null_value)
    t.corrupt("phone_number", .04, "inconsistent_format", phone_variant)
    t.corrupt("phone_number", .008, "invalid_value", phone_invalid)
    t.corrupt("signup_date", .006, "null", null_value)
    t.corrupt("signup_date", .015, "inconsistent_format", date_variant)
    t.corrupt("signup_date", .002, "incorrect_date_future", future_date)
    t.corrupt("signup_date", .0015, "incorrect_date_placeholder", placeholder_date)
    t.corrupt("signup_date", .0015, "incorrect_date_impossible", impossible_date)
    t.corrupt("city", .008, "null", null_value)
    t.corrupt("city", .008, "inconsistent_format", case_noise)
    t.corrupt("city", .008, "inconsistent_format_alias", lambda v, i: CITY_ALIAS[v],
              mask=df["city"].isin(CITY_ALIAS.keys()))
    t.corrupt("state", .006, "null", null_value)
    t.corrupt("state", .008, "inconsistent_format_alias", lambda v, i: rng.choice(STATE_VARIANT[v]),
              mask=df["state"].isin(STATE_VARIANT.keys()))
    t.corrupt("state", .005, "inconsistent_format", case_noise)
    t.corrupt("region", .01, "null", null_value)
    t.corrupt("region", .012, "inconsistent_format", region_case)
    t.corrupt("region", .004, "invalid_value",
              lambda v, i: rng.choice(["CENTRAL", "NORTH-EAST", "NE", "Unknown", "N.A.", "SOUTHWEST"]))
    t.corrupt("region", .005, "region_state_mismatch", region_mismatch)

    replace_with_duplicates(t, v_exact, "exact_duplicate")

    def alter_customer(v):
        c = rng.choice(["email", "phone_number", "city"])
        before = t.df.at[v, c]
        t.df.at[v, c] = (str(before).upper() if c == "email" else
                         phone_variant(str(before)) if c == "phone_number" else str(before).lower())
        return f"{c} altered"
    replace_with_duplicates(t, v_conf, "duplicate_key_conflicting_values", alter_customer)
    near_duplicate(t, v_near, ["name", "email", "phone_number", "city", "state", "region"])


# ----------------------------------------------------------------------------- orders
STATUS_VARIANT = {"Delivered": ["delivered", "DELIVERED", " Delivered", "Delivered ", "Deliverd"],
                  "In Transit": ["in transit", "IN TRANSIT", "In-Transit", "InTransit", "in_transit"],
                  "Canceled": ["Cancelled", "canceled", "CANCELED", "Cancel", " Canceled"]}
PAYMENT_VARIANT = {"Cash on Delivery": ["COD", "cash on delivery", "Cash On Delivery", "C.O.D"],
                   "Net Banking": ["Netbanking", "net banking", "NET BANKING", "Net-Banking"],
                   "UPI": ["upi", "Upi", "UPI ", " UPI"],
                   "Loyalty Points": ["loyalty points", "Loyalty Pts", "LOYALTY POINTS", "LoyaltyPoints"]}


def inject_orders(t: Table, items_clean: pd.DataFrame, cust_clean: pd.DataFrame) -> None:
    df = t.df
    childless_new = is_new(t) & ~df["order_id"].isin(set(items_clean["order_id"]))
    v_exact = victims(t, 1000, childless_new)
    v_conf = victims(t, 300, childless_new)

    signup = cust_clean.set_index("customer_id")["signup_date"]
    t.corrupt("customer_id", .005, "null", null_value)
    t.corrupt("customer_id", .008, "broken_foreign_key", lambda v, i: str(rng.integers(30001, 40000)))
    t.corrupt("customer_id", .002, "invalid_value", lambda v, i: f"CUST-{int(v):05d}")
    t.corrupt("order_date", .015, "null", null_value)
    t.corrupt("order_date", .04, "inconsistent_format", date_variant)
    t.corrupt("order_date", .003, "incorrect_date_future", future_date)
    t.corrupt("order_date", .008, "incorrect_date_before_signup",
              lambda v, i: shift(signup[df.at[i, "customer_id"]], -400, -1),
              mask=df["customer_id"].isin(signup.index))
    t.corrupt("order_date", .002, "incorrect_date_placeholder", placeholder_date)
    t.corrupt("order_date", .002, "incorrect_date_impossible", impossible_date)
    t.corrupt("order_status", .005, "null", null_value)
    t.corrupt("order_status", .03, "inconsistent_format", lambda v, i: rng.choice(STATUS_VARIANT[v]))
    t.corrupt("order_status", .004, "invalid_value",
              lambda v, i: rng.choice(["Unknown", "Pending??", "TBD", "0", "Shipped?", "ERROR"]))
    t.corrupt("payment_method", .01, "null", null_value)
    t.corrupt("payment_method", .03, "inconsistent_format", lambda v, i: rng.choice(PAYMENT_VARIANT[v]))
    t.corrupt("payment_method", .003, "invalid_value",
              lambda v, i: rng.choice(["Bitcoin", "XXXX", "123", "Cheque??", "unknown"]))

    replace_with_duplicates(t, v_exact, "exact_duplicate")

    def alter_order(v):
        c = rng.choice(["order_status", "payment_method", "order_date"])
        b = str(t.df.at[v, c])
        if c == "order_status":
            t.df.at[v, c] = rng.choice(sorted({"Delivered", "In Transit", "Canceled"} - {b}))
        elif c == "payment_method":
            t.df.at[v, c] = rng.choice(sorted({"Cash on Delivery", "Net Banking", "UPI", "Loyalty Points"} - {b}))
        else:
            try:
                t.df.at[v, c] = shift(b, 1, 3)
            except ValueError:
                t.df.at[v, c] = np.nan
        return f"{c} altered"
    replace_with_duplicates(t, v_conf, "duplicate_key_conflicting_values", alter_order)


# ----------------------------------------------------------------------------- order items
def sku_format(v: str, *_):
    return rng.choice([v.lower(), v.replace("-", ""), v.replace("-", "_"), f" {v}", v.replace("-", " "),
                       "SKU-" + v])


def inject_order_items(t: Table, sku_by_pid: dict) -> None:
    df = t.df
    t.corrupt("order_id", .003, "null", null_value)
    t.corrupt("order_id", .005, "broken_foreign_key", lambda v, i: str(rng.integers(100001, 130000)))
    t.corrupt("product_id", .003, "null", null_value)
    t.corrupt("product_id", .004, "broken_foreign_key", lambda v, i: str(rng.choice([0, 9999, *range(301, 451)])))
    t.corrupt("sku", .02, "null", null_value)
    t.corrupt("sku", .03, "inconsistent_format", sku_format)
    t.corrupt("sku", .005, "sku_product_mismatch",
              lambda v, i: sku_by_pid[str(rng.integers(1, 301))])
    t.corrupt("sku", .003, "invalid_value", lambda v, i: rng.choice(["SKU???", "000-000-0000", "TBD", "#N/A"]))
    t.corrupt("quantity", .005, "null", null_value)
    t.corrupt("quantity", .01, "invalid_value",
              lambda v, i: rng.choice(["0", "-1", "-3", "999", "1000", "two", "2.5"]))
    t.corrupt("unit_price", .01, "null", null_value)
    t.corrupt("unit_price", .003, "invalid_value_negative", lambda v, i: f"-{v}")
    t.corrupt("unit_price", .002, "invalid_value_zero", lambda v, i: "0")
    t.corrupt("unit_price", .01, "price_mismatch",
              lambda v, i: f"{float(v) * rng.choice([rng.uniform(.6, .95), rng.uniform(1.05, 1.4)]):.2f}")
    t.corrupt("unit_price", .01, "inconsistent_format", money_variant)

    # appended duplicates (table has no fixed row target)
    n = len(df)
    src_exact = rng.choice(df.index.to_numpy(), int(.01 * n), replace=False)
    src_conf = rng.choice(np.setdiff1d(df.index.to_numpy(), src_exact), int(.003 * n), replace=False)
    dup = df.loc[np.concatenate([src_exact, src_conf])].copy()
    dup["_rid"] = np.arange(n, n + len(dup))
    is_conf = np.r_[np.zeros(len(src_exact), bool), np.ones(len(src_conf), bool)]
    new_rows = []
    for (j, row), conf in zip(dup.iterrows(), is_conf):
        if conf:
            row["quantity"] = str(rng.integers(1, 6))
        new_rows.append(row)
    dup = pd.DataFrame(new_rows)
    # random insertion position: sort key = original position, dups get random fractional slot
    t.df["_pos"] = np.arange(n, dtype=float)
    dup["_pos"] = rng.uniform(0, n, len(dup))
    t.df = pd.concat([t.df, dup], ignore_index=True).sort_values("_pos", kind="stable") \
        .drop(columns="_pos").reset_index(drop=True)
    pos = {r: i for i, r in enumerate(t.df["_rid"])}
    for r, conf in zip(dup["_rid"], is_conf):
        i = pos[r]
        LOG.append({"table": t.name, "_rid": int(r), "key": t.df.at[i, t.key], "column": "*row*",
                    "issue_type": "duplicate_key_conflicting_values" if conf else "exact_duplicate",
                    "clean_value": "", "dirty_value": "appended copy" + ("; quantity altered" if conf else "")})


# ----------------------------------------------------------------------------- returns
def inject_returns(t: Table, items_clean: pd.DataFrame, orders_clean: pd.DataFrame) -> None:
    df = t.df
    v_exact = victims(t, 200, is_new(t))
    v_conf = victims(t, 60, is_new(t))
    v_twice = victims(t, 100, is_new(t))

    od = items_clean.merge(orders_clean, on="order_id").set_index("order_item_id")
    order_date = od["order_date"]
    not_delivered = od.index[od["order_status"] != "Delivered"].to_numpy()

    t.corrupt("order_item_id", .003, "null", null_value)
    t.corrupt("order_item_id", .006, "broken_foreign_key", lambda v, i: str(rng.integers(170001, 200000)))
    t.corrupt("order_item_id", .005, "return_on_undelivered_order", lambda v, i: str(rng.choice(not_delivered)))
    valid_item = df["order_item_id"].isin(order_date.index)
    t.corrupt("return_date", .02, "null", null_value)
    t.corrupt("return_date", .04, "inconsistent_format", date_variant)
    t.corrupt("return_date", .01, "incorrect_date_before_order",
              lambda v, i: shift(order_date[df.at[i, "order_item_id"]], -20, -1), mask=valid_item)
    t.corrupt("return_date", .008, "incorrect_date_outside_window",
              lambda v, i: shift(order_date[df.at[i, "order_item_id"]], 31, 120), mask=valid_item)
    t.corrupt("return_date", .003, "incorrect_date_future", future_date)
    t.corrupt("return_date", .002, "incorrect_date_impossible", impossible_date)
    t.corrupt("return_date", .001, "incorrect_date_placeholder", placeholder_date)
    t.corrupt("refund_amount", .01, "null", null_value)
    t.corrupt("refund_amount", .005, "invalid_value_negative", lambda v, i: f"-{v}")
    t.corrupt("refund_amount", .003, "invalid_value_zero", lambda v, i: "0.00")
    t.corrupt("refund_amount", .01, "refund_exceeds_item_value", lambda v, i: f"{float(v) * rng.uniform(1.2, 3):.2f}")
    t.corrupt("refund_amount", .008, "inconsistent_format", money_variant)

    replace_with_duplicates(t, v_exact, "exact_duplicate")

    def alter_return(v):
        b = str(t.df.at[v, "refund_amount"])
        try:
            t.df.at[v, "refund_amount"] = f"{float(b) * rng.uniform(.5, .95):.2f}"
        except ValueError:
            t.df.at[v, "refund_amount"] = np.nan
        return "refund_amount altered"
    replace_with_duplicates(t, v_conf, "duplicate_key_conflicting_values", alter_return)
    near_duplicate(t, v_twice, ["order_item_id", "return_date", "refund_amount"])


# ----------------------------------------------------------------------------- main
def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    ref_dir = OUTPUT_DIR / "clean_reference"
    ref_dir.mkdir(exist_ok=True)

    clean = {n: pd.read_csv(CLEAN_DIR / f"{n}.csv", dtype=str, keep_default_na=False)
             for n in ["customers", "orders", "order_items", "returns"]}
    products = pd.read_csv(CLEAN_DIR / "products.csv", dtype=str)
    items_tmp = clean["order_items"][["product_id", "sku"]].drop_duplicates()
    sku_by_pid = dict(zip(items_tmp["product_id"], items_tmp["sku"]))
    missing = set(products["product_id"]) - set(sku_by_pid)
    assert not missing, f"products without SKU in clean items: {sorted(missing)[:5]}"

    tables = {
        "customers": Table("customers", "customer_id"),
        "orders": Table("orders", "order_id"),
        "order_items": Table("order_items", "order_item_id"),
        "returns": Table("returns", "return_id"),
    }
    inject_customers(tables["customers"], clean["orders"])
    inject_orders(tables["orders"], clean["order_items"], clean["customers"])
    inject_order_items(tables["order_items"], sku_by_pid)
    inject_returns(tables["returns"], clean["order_items"], clean["orders"])

    log_df = pd.DataFrame(LOG)
    for name, t in tables.items():
        row_no = pd.Series(np.arange(1, len(t.df) + 1), index=t.df["_rid"].to_numpy())
        m = log_df["table"] == name
        log_df.loc[m, "row_number"] = log_df.loc[m, "_rid"].map(row_no).to_numpy()
        t.df.drop(columns="_rid").to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
        shutil.copyfile(CLEAN_DIR / f"{name}.csv", ref_dir / f"{name}.csv")
        touched = log_df.loc[m, "_rid"].nunique()
        print(f"{name:<12} rows={len(t.df):>8,}  issues={m.sum():>6,}  rows affected={touched:>6,} "
              f"({touched / len(t.df):.1%})")

    for f in UNCHANGED_FILES:
        shutil.copyfile(CLEAN_DIR / f, OUTPUT_DIR / f)
        shutil.copyfile(CLEAN_DIR / f, ref_dir / f)

    log_df["row_number"] = log_df["row_number"].astype(int)
    log_df = log_df.drop(columns="_rid")[["table", "row_number", "key", "column", "issue_type",
                                          "clean_value", "dirty_value"]]
    log_df.sort_values(["table", "row_number", "column"], kind="stable") \
        .to_csv(OUTPUT_DIR / "data_quality_issue_log.csv", index=False)
    print(f"issue log: {len(log_df):,} entries")
    print(log_df.groupby(["table", "issue_type"]).size().to_string())


if __name__ == "__main__":
    main()
