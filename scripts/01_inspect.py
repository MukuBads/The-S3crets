"""Step 1 - locate the dataset ZIP, extract it, and profile every file.

Writes work/inspection_report.txt and prints it.
"""
from __future__ import annotations

import io
import sys

import pandas as pd

from common import EXTRACT_DIR, WORK_DIR, extract_zip, find_or_build_input_zip, sha256


def profile(name: str, df: pd.DataFrame, buf: io.StringIO) -> None:
    w = buf.write
    w(f"\n=== {name}  rows={len(df):,}  cols={df.shape[1]} ===\n")
    for c in df.columns:
        s = df[c]
        w(f"  {c:<16} dtype={str(s.dtype):<10} nulls={s.isna().sum():<6} "
          f"unique={s.nunique():<7} sample={list(s.dropna().astype(str).head(3))}\n")
        if pd.api.types.is_numeric_dtype(s):
            w(f"  {'':<16} min={s.min()} max={s.max()}\n")
    w(f"  full-row duplicates: {df.duplicated().sum()}\n")


def main() -> None:
    zip_path = find_or_build_input_zip()
    files = extract_zip(zip_path)
    buf = io.StringIO()
    buf.write(f"ZIP: {zip_path}\nExtracted to: {EXTRACT_DIR}\n")
    dfs = {}
    for p in files:
        buf.write(f"  {p.name}  {p.stat().st_size:,} bytes  sha256={sha256(p)[:16]}\n")
        dfs[p.stem] = pd.read_csv(p) if p.suffix == ".csv" else pd.read_excel(p)
    for name, df in dfs.items():
        profile(name, df, buf)

    # Candidate key / FK checks
    buf.write("\n=== Key & relationship checks ===\n")
    pk = {"customers": "customer_id", "categories": "category_id", "products": "product_id",
          "orders": "order_id", "order_items": "order_item_id", "returns": "return_id"}
    for t, k in pk.items():
        if t in dfs:
            buf.write(f"  PK {t}.{k}: unique={dfs[t][k].is_unique} nulls={dfs[t][k].isna().sum()}\n")
    fks = [("products", "category_id", "categories", "category_id"),
           ("orders", "customer_id", "customers", "customer_id"),
           ("order_items", "order_id", "orders", "order_id"),
           ("order_items", "product_id", "products", "product_id"),
           ("returns", "order_item_id", "order_items", "order_item_id")]
    for ct, cc, pt, pc in fks:
        if ct in dfs and pt in dfs:
            orphan = (~dfs[ct][cc].isin(dfs[pt][pc])).sum()
            buf.write(f"  FK {ct}.{cc} -> {pt}.{pc}: orphans={orphan}\n")

    if {"order_items", "products"} <= dfs.keys():
        m = dfs["order_items"].merge(dfs["products"], on="product_id")
        buf.write(f"  order_items.unit_price == products.price: "
                  f"{(abs(m['unit_price'] - m['price']) < 0.005).mean():.1%}\n")
    if {"returns", "order_items", "orders"} <= dfs.keys():
        r = dfs["returns"].merge(dfs["order_items"], on="order_item_id").merge(dfs["orders"], on="order_id")
        buf.write(f"  returns on Delivered orders: {(r['order_status'] == 'Delivered').mean():.1%}\n")
        buf.write(f"  refund_amount == qty*unit_price: "
                  f"{(abs(r['refund_amount'] - r['quantity'] * r['unit_price']) < 0.01).mean():.1%}\n")
        buf.write(f"  returns per order_item: max={dfs['returns']['order_item_id'].value_counts().max()}\n")
    if "orders" in dfs:
        buf.write(f"  orders.order_status: {dfs['orders']['order_status'].value_counts().to_dict()}\n")
        buf.write(f"  orders.payment_method: {dfs['orders']['payment_method'].value_counts().to_dict()}\n")
    if "customers" in dfs:
        c = dfs["customers"]
        buf.write(f"  customers.signup_date range: {c['signup_date'].min()} .. {c['signup_date'].max()}\n")
        buf.write(f"  customers email unique: {c['email'].is_unique}\n")
    if "returns" in dfs:
        buf.write(f"  returns.return_date range: {dfs['returns']['return_date'].min()} .. "
                  f"{dfs['returns']['return_date'].max()}\n")

    WORK_DIR.mkdir(exist_ok=True)
    (WORK_DIR / "inspection_report.txt").write_text(buf.getvalue(), encoding="utf-8")
    sys.stdout.write(buf.getvalue())


if __name__ == "__main__":
    main()
