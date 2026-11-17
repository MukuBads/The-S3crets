"""Step 2 - expand the original dataset into a clean, referentially consistent dataset.

Input : work/extracted/*.csv (pristine copy unpacked from the input ZIP)
Output: work/clean_expanded/*.csv

Rules
* Every original row is preserved (values unchanged), only new columns are added.
* products.csv / categories.csv are copied byte-for-byte (never parsed and rewritten).
* New IDs continue after the original maximum IDs.
* Timeline: signup_date <= order_date <= SNAPSHOT, return_date = order_date + 1..30 days.
* Returns only reference items of Delivered orders, one return per order item.
"""
from __future__ import annotations

import re
import shutil

import numpy as np
import pandas as pd

from common import (CLEAN_DIR, EXTRACT_DIR, SEED, SNAPSHOT_DATE, STATE_REGION, TARGETS,
                    UNCHANGED_FILES, make_sku)

SNAPSHOT = pd.Timestamp(SNAPSHOT_DATE)     # "data extracted on" date
NEW_SIGNUP_START = pd.Timestamp("2023-01-01")
NEW_SIGNUP_END = pd.Timestamp("2026-10-31")

rng = np.random.default_rng(SEED)


def to_iso(s: pd.Series) -> pd.Series:
    return s.dt.strftime("%Y-%m-%d")


def rand_dates(low: pd.Series, high: pd.Series) -> pd.Series:
    """Uniform random date in [low, high] (inclusive), vectorised."""
    span = (high - low).dt.days.to_numpy()
    span = np.maximum(span, 0)
    off = np.floor(rng.random(len(low)) * (span + 1)).astype(int)
    return low + pd.to_timedelta(off, unit="D")


# --------------------------------------------------------------------------- customers
def expand_customers(cust: pd.DataFrame) -> pd.DataFrame:
    n_new = TARGETS["customers"] - len(cust)
    start_id = cust["customer_id"].max() + 1

    names = cust["name"].str.split()
    firsts = names.str[0].unique()
    lasts = names.str[-1].unique()
    state_cities = cust.groupby("state")["city"].unique().apply(sorted).to_dict()
    states = sorted(state_cities)

    first = rng.choice(firsts, n_new)
    last = rng.choice(lasts, n_new)
    state = rng.choice(states, n_new)                     # original generator: uniform state
    city = np.array([rng.choice(state_cities[s]) for s in state])

    used = set(cust["email"].str.lower())
    domains = ["example.com", "example.org", "example.net"]
    emails = []
    for f, l in zip(first, last):
        f2, l2 = re.sub(r"[^a-z]", "", f.lower()), re.sub(r"[^a-z]", "", l.lower())
        pat = rng.integers(0, 5)
        local = [f"{f2}{l2}", f"{f2}.{l2}", f"{f2[0]}{l2}", f"{f2}{rng.integers(1, 100):02d}",
                 f"{l2}{f2}{rng.integers(1, 1000)}"][pat]
        email = f"{local}@{rng.choice(domains)}"
        while email in used:
            email = f"{local}{rng.integers(1, 10000)}@{rng.choice(domains)}"
        used.add(email)
        emails.append(email)

    span = (NEW_SIGNUP_END - NEW_SIGNUP_START).days
    days = np.floor(rng.random(n_new) ** (1 / 1.5) * (span + 1)).astype(int)   # growth trend
    signup = NEW_SIGNUP_START + pd.to_timedelta(days, unit="D")

    new = pd.DataFrame({
        "customer_id": np.arange(start_id, start_id + n_new),
        "name": [f"{a} {b}" for a, b in zip(first, last)],
        "email": emails,
        "signup_date": to_iso(pd.Series(signup)),
        "city": city,
        "state": state,
    })
    out = pd.concat([cust, new], ignore_index=True)

    # phone numbers for every customer (originals had none)
    phones, seen = [], set()
    while len(phones) < len(out):
        p = f"{rng.choice([6, 7, 8, 9], p=[.15, .2, .25, .4])}{rng.integers(0, 10**9):09d}"
        if p not in seen:
            seen.add(p)
            phones.append(p)
    out["phone_number"] = ["+91-" + p for p in phones]
    out["region"] = out["state"].map(STATE_REGION)
    assert out["region"].notna().all(), "unmapped state"
    return out


# --------------------------------------------------------------------------- orders
def assign_order_dates(orders: pd.DataFrame, signup: pd.Series) -> pd.Series:
    sd = pd.to_datetime(orders["customer_id"].map(signup))
    low = sd.copy()
    high = pd.Series(SNAPSHOT - pd.Timedelta(days=1), index=orders.index)
    delivered = orders["order_status"].eq("Delivered")
    transit = orders["order_status"].eq("In Transit")
    high[delivered] = SNAPSHOT - pd.Timedelta(days=5)        # time to deliver + return window
    low[transit] = np.maximum(sd[transit], SNAPSHOT - pd.Timedelta(days=30))
    return rand_dates(low, high)


def expand_orders(orders: pd.DataFrame, cust: pd.DataFrame) -> pd.DataFrame:
    signup = cust.set_index("customer_id")["signup_date"]
    orders = orders.copy()
    orders["order_date"] = to_iso(assign_order_dates(orders, signup))

    n_new = TARGETS["orders"] - len(orders)
    sd = pd.to_datetime(cust["signup_date"])
    active = ((SNAPSHOT - sd).dt.days / (SNAPSHOT - NEW_SIGNUP_START).days).clip(0.02, 1)
    w = rng.lognormal(0, 0.9, len(cust)) * active.to_numpy()
    new = pd.DataFrame({
        "customer_id": rng.choice(cust["customer_id"].to_numpy(), n_new, p=w / w.sum()),
        "order_status": rng.choice(["Delivered", "Canceled", "In Transit"], n_new, p=[.80, .17, .03]),
        "payment_method": rng.choice(["Cash on Delivery", "Net Banking", "UPI", "Loyalty Points"], n_new),
    })
    new["order_date"] = assign_order_dates(new, signup)
    new = new.sort_values("order_date", kind="stable").reset_index(drop=True)  # ids follow time
    new["order_date"] = to_iso(new["order_date"])
    new.insert(0, "order_id", np.arange(orders["order_id"].max() + 1,
                                        orders["order_id"].max() + 1 + n_new))
    return pd.concat([orders, new[orders.columns]], ignore_index=True)


# --------------------------------------------------------------------------- order items
def expand_order_items(items: pd.DataFrame, orders: pd.DataFrame, products: pd.DataFrame,
                       n_orig_orders: int) -> pd.DataFrame:
    new_orders = orders.iloc[n_orig_orders:]
    k = rng.choice([1, 2, 3, 4], len(new_orders), p=[.55, .28, .12, .05])
    # some cancelled orders were cancelled before any line item was persisted
    canc = new_orders["order_status"].eq("Canceled").to_numpy()
    k[canc & (rng.random(len(new_orders)) < 0.15)] = 0
    order_ids = np.repeat(new_orders["order_id"].to_numpy(), k)

    pop = 1 / np.arange(1, len(products) + 1) ** 0.6           # mild best-seller skew
    pop = rng.permutation(pop)
    pid = rng.choice(products["product_id"].to_numpy(), len(order_ids), p=pop / pop.sum())
    qty = rng.choice([1, 2, 3, 4, 5], len(order_ids), p=[.5, .25, .12, .08, .05])
    price = products.set_index("product_id")["price"]

    new = pd.DataFrame({
        "order_item_id": np.arange(items["order_item_id"].max() + 1,
                                   items["order_item_id"].max() + 1 + len(order_ids)),
        "order_id": order_ids, "product_id": pid, "quantity": qty,
        "unit_price": price.reindex(pid).to_numpy(),
    })
    out = pd.concat([items, new], ignore_index=True)
    out["sku"] = out["product_id"].map(products.set_index("product_id")["sku"])
    return out


# --------------------------------------------------------------------------- returns
def return_lag(n: int, max_lag: np.ndarray) -> np.ndarray:
    lag = np.clip(np.round(rng.gamma(2.2, 4.0, n)), 1, 30).astype(int)
    return np.minimum(lag, max_lag)


def expand_returns(ret: pd.DataFrame, items: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    od = items[["order_item_id", "order_id", "quantity", "unit_price"]].merge(
        orders[["order_id", "order_status", "order_date"]], on="order_id")
    od["order_date"] = pd.to_datetime(od["order_date"])
    od = od.set_index("order_item_id")

    # fix the original return dates: order_date + 1..30 days (never after snapshot)
    ret = ret.copy()
    o_dates = od.loc[ret["order_item_id"], "order_date"].reset_index(drop=True)
    max_lag = (SNAPSHOT - o_dates).dt.days.to_numpy()
    ret["return_date"] = to_iso(o_dates + pd.to_timedelta(return_lag(len(ret), max_lag), unit="D"))

    n_new = TARGETS["returns"] - len(ret)
    cand = od[(od["order_status"] == "Delivered") & (~od.index.isin(ret["order_item_id"]))
              & (od["order_date"] <= SNAPSHOT - pd.Timedelta(days=1))]
    pick = cand.loc[rng.choice(cand.index.to_numpy(), n_new, replace=False)]
    max_lag = (SNAPSHOT - pick["order_date"]).dt.days.to_numpy()
    rdate = pick["order_date"].to_numpy() + pd.to_timedelta(return_lag(n_new, max_lag), unit="D")

    qty = pick["quantity"].to_numpy()
    ret_qty = qty.copy()
    partial = (qty > 1) & (rng.random(n_new) < 0.12)                  # partial-quantity returns
    ret_qty[partial] = [rng.integers(1, q) for q in qty[partial]]
    new = pd.DataFrame({
        "order_item_id": pick.index.to_numpy(),
        "return_date": pd.Series(rdate),
        "refund_amount": np.round(ret_qty * pick["unit_price"].to_numpy(), 2),
    }).sort_values(["return_date", "order_item_id"], kind="stable").reset_index(drop=True)
    new["return_date"] = to_iso(new["return_date"])
    new.insert(0, "return_id", np.arange(ret["return_id"].max() + 1, ret["return_id"].max() + 1 + n_new))
    return pd.concat([ret, new[ret.columns]], ignore_index=True)


def main() -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    src = {n: pd.read_csv(EXTRACT_DIR / f"{n}.csv")
           for n in ["customers", "orders", "order_items", "products", "returns"]}

    for f in UNCHANGED_FILES:                     # byte-for-byte copy, never re-serialised
        shutil.copyfile(EXTRACT_DIR / f, CLEAN_DIR / f)

    products = src["products"].copy()
    products["sku"] = make_sku(products)          # in-memory only; products.csv stays untouched

    cust = expand_customers(src["customers"])
    orders = expand_orders(src["orders"], cust)
    items = expand_order_items(src["order_items"], orders, products, len(src["orders"]))
    rets = expand_returns(src["returns"], items, orders)

    cust = cust[["customer_id", "name", "email", "phone_number", "signup_date", "city", "state", "region"]]
    orders = orders[["order_id", "customer_id", "order_date", "order_status", "payment_method"]]
    items = items[["order_item_id", "order_id", "product_id", "sku", "quantity", "unit_price"]]

    for name, df in {"customers": cust, "orders": orders, "order_items": items, "returns": rets}.items():
        df.to_csv(CLEAN_DIR / f"{name}.csv", index=False)
        print(f"{name:<12} {len(df):>8,} rows -> {CLEAN_DIR / (name + '.csv')}")

    # sanity: originals preserved
    for name, key in [("customers", "customer_id"), ("orders", "order_id"),
                      ("order_items", "order_item_id"), ("returns", "return_id")]:
        o = src[name]
        e = {"customers": cust, "orders": orders, "order_items": items, "returns": rets}[name]
        cols = [c for c in o.columns if not (name == "returns" and c == "return_date")]
        same = e.set_index(key).loc[o[key], [c for c in cols if c != key]].reset_index()
        assert same.equals(o[cols]), f"original {name} rows changed"
    print("original rows preserved (except corrected returns.return_date)")


if __name__ == "__main__":
    main()
