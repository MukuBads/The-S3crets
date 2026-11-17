"""Step 4 - validate the final datasets, write the data-quality report, build the final ZIP.

Detection is rule-based and independent of the injection log, so the report shows what a
data engineer would actually find. The same rules also run on output/clean_reference/ as a
control (expected: ~0 issues).

Hard checks (the script exits non-zero if any fails):
* customers == 30,000, orders == 100,000, returns == 20,000 rows
* products.csv / categories.csv byte-identical to the originals
* original source files unchanged (kaggle_dataset/ vs. the copies inside the input ZIP)
"""
from __future__ import annotations

import json
import re
import sys
import zipfile

import numpy as np
import pandas as pd

from common import (INPUT_DIR, NULL_TOKENS, ORIGINAL_DIR, OUTPUT_DIR, SNAPSHOT_DATE, STATE_REGION,
                    TARGETS, UNCHANGED_FILES, VALID_PAYMENT, VALID_REGIONS, VALID_STATUS, make_sku, sha256)

SNAPSHOT = pd.Timestamp(SNAPSHOT_DATE)
MIN_VALID_DATE = pd.Timestamp("2000-01-01")
ALT_DATE_FORMATS = ["%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%b %d, %Y", "%Y%m%d", "%d %B %Y",
                    "%Y-%m-%dT%H:%M:%S", "%d.%m.%Y", "%m/%d/%Y"]
STATUS_NORM = {"delivered": "Delivered", "deliverd": "Delivered", "intransit": "In Transit",
               "canceled": "Canceled", "cancelled": "Canceled", "cancel": "Canceled"}
PAYMENT_NORM = {"cashondelivery": "Cash on Delivery", "cod": "Cash on Delivery", "netbanking": "Net Banking",
                "upi": "UPI", "loyaltypoints": "Loyalty Points", "loyaltypts": "Loyalty Points"}
PK = {"customers": "customer_id", "orders": "order_id", "order_items": "order_item_id",
      "returns": "return_id", "products": "product_id", "categories": "category_id"}


# ----------------------------------------------------------------------------- helpers
def load(folder) -> dict[str, pd.DataFrame]:
    return {n: pd.read_csv(folder / f"{n}.csv", dtype=str, keep_default_na=False)
            for n in PK}


def is_null(s: pd.Series) -> pd.Series:
    return s.fillna("").str.strip().isin(NULL_TOKENS)


def letters(s: pd.Series) -> pd.Series:
    return s.fillna("").str.lower().str.replace(r"[^a-z]", "", regex=True)


def parse_dates(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return (parsed date, is_iso). Non-ISO strings are tried against common formats."""
    s = s.fillna("").str.strip()
    iso_shape = s.str.fullmatch(r"\d{4}-\d{2}-\d{2}")
    out = pd.to_datetime(s.where(iso_shape), format="%Y-%m-%d", errors="coerce")
    for fmt in ALT_DATE_FORMATS:
        todo = out.isna() & ~iso_shape & (s != "")
        if not todo.any():
            break
        out[todo] = pd.to_datetime(s[todo], format=fmt, errors="coerce")
    return out, iso_shape & out.notna()


def to_num(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return (numeric value, is_plain_number). Currency symbols / separators are stripped."""
    s = s.fillna("").str.strip()
    plain = s.str.fullmatch(r"-?\d+(\.\d+)?")
    cleaned = s.str.replace(r"(?i)(₹|rs\.?|inr|,|\s)", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce"), plain


def as_int(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.where(s.fillna("").str.fullmatch(r"\d+")), errors="coerce")


class Checks:
    def __init__(self):
        self.rows: list[dict] = []

    def add(self, table, column, check, category, mask, n_total):
        n = int(mask.sum())
        self.rows.append({"table": table, "column": column, "check": check, "category": category,
                          "violations": n, "pct": round(100 * n / n_total, 3) if n_total else 0})


# ----------------------------------------------------------------------------- validation
def validate(d: dict[str, pd.DataFrame], known_cities: set) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ck = Checks()
    c, o, i, r, p = d["customers"], d["orders"], d["order_items"], d["returns"], d["products"]

    # ---- row counts / duplicates / nulls
    summary, nulls = [], []
    for name, df in d.items():
        key = PK[name]
        summary.append({
            "table": name, "rows": len(df), "columns": df.shape[1],
            "target": TARGETS.get(name, ""),
            "exact_duplicate_rows": int(df.duplicated().sum()),
            "duplicate_pk_rows": int(df[key].duplicated(keep="first").sum()),
            "distinct_pk_values_duplicated": int(df[key][df[key].duplicated(keep=False)].nunique()),
            "null_pk": int(is_null(df[key]).sum()),
        })
        for col in df.columns:
            raw_empty = (df[col] == "").sum()
            null_like = is_null(df[col]).sum()
            nulls.append({"table": name, "column": col, "empty_cells": int(raw_empty),
                          "null_tokens": int(null_like - raw_empty),
                          "null_pct": round(100 * null_like / len(df), 3)})

    # ---- customers
    n = len(c)
    nn = ~is_null(c["email"])
    em = c["email"].str.strip().str.lower()
    ck.add("customers", "email", "invalid email syntax", "invalid_value",
           nn & ~em.str.fullmatch(r"[a-z0-9._%+-]+@[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}"), n)
    ck.add("customers", "email", "non-canonical (case/whitespace)", "inconsistent_format",
           nn & (c["email"] != em) & em.str.fullmatch(r"[^@\s]+@[^@\s]+\.[a-z]{2,}"), n)
    ck.add("customers", "email", "same email on different customer_id", "duplicate_entity",
           nn & em.duplicated(keep=False) & ~c.duplicated(["customer_id", "email"], keep=False), n)

    nn = ~is_null(c["phone_number"])
    canon = c["phone_number"].str.fullmatch(r"\+91-[6-9]\d{9}")
    digits = c["phone_number"].str.replace(r"\D", "", regex=True)
    core = digits.str.replace(r"^(91|0)(?=\d{10}$)", "", regex=True)
    fixable = core.str.fullmatch(r"[6-9]\d{9}") & ~c["phone_number"].str.contains(r"[A-Za-z]")
    ck.add("customers", "phone_number", "non-canonical but valid number", "inconsistent_format", nn & ~canon & fixable, n)
    ck.add("customers", "phone_number", "invalid phone number", "invalid_value", nn & ~canon & ~fixable, n)
    ck.add("customers", "phone_number", "same phone on different customer_id", "duplicate_entity",
           nn & fixable & core.duplicated(keep=False) & ~c.duplicated(["customer_id", "phone_number"], keep=False), n)

    nn = ~is_null(c["name"])
    ck.add("customers", "name", "case/whitespace anomaly", "inconsistent_format",
           nn & ((c["name"] != c["name"].str.strip()) | c["name"].str.contains("  ")
                 | (c["name"] == c["name"].str.upper()) | (c["name"] == c["name"].str.lower())), n)

    states = set(STATE_REGION)
    for col, domain, label in [("state", states, "unknown state / abbreviation / misspelling"),
                               ("city", known_cities, "unknown city / legacy alias")]:
        nn = ~is_null(c[col])
        canon = {x.lower(): x for x in domain}
        fixed = c[col].str.strip().str.lower().map(canon)
        ck.add("customers", col, "case/whitespace variant", "inconsistent_format",
               nn & ~c[col].isin(domain) & fixed.notna(), n)
        ck.add("customers", col, label, "invalid_value", nn & fixed.isna(), n)

    nn = ~is_null(c["region"])
    rg = c["region"].str.strip().str.upper().replace({"N": "NORTH", "S": "SOUTH", "E": "EAST", "W": "WEST"})
    ck.add("customers", "region", "case/whitespace/abbrev variant", "inconsistent_format",
           nn & ~c["region"].isin(VALID_REGIONS) & rg.isin(VALID_REGIONS), n)
    ck.add("customers", "region", "outside {NORTH,SOUTH,EAST,WEST}", "invalid_value", nn & ~rg.isin(VALID_REGIONS), n)
    ck.add("customers", "region", "region does not match state", "inconsistent_value",
           rg.isin(VALID_REGIONS) & c["state"].isin(states) & (rg != c["state"].map(STATE_REGION)), n)

    sd, sd_iso = parse_dates(c["signup_date"])
    date_checks(ck, "customers", "signup_date", c["signup_date"], sd, sd_iso, n)

    # ---- orders
    n = len(o)
    cust_ids = set(as_int(c["customer_id"]).dropna())
    cid = as_int(o["customer_id"])
    nn = ~is_null(o["customer_id"])
    ck.add("orders", "customer_id", "non-numeric id", "invalid_value", nn & cid.isna(), n)
    ck.add("orders", "customer_id", "FK -> customers missing", "referential_integrity", cid.notna() & ~cid.isin(cust_ids), n)

    od, od_iso = parse_dates(o["order_date"])
    date_checks(ck, "orders", "order_date", o["order_date"], od, od_iso, n)
    signup_by_c = pd.Series(sd.to_numpy(), index=as_int(c["customer_id"])).groupby(level=0).min()
    s_for_o = cid.map(signup_by_c)
    ck.add("orders", "order_date", "order before customer signup", "incorrect_date",
           od.notna() & s_for_o.notna() & (od < s_for_o) & (od >= MIN_VALID_DATE), n)

    for col, valid, norm in [("order_status", VALID_STATUS, STATUS_NORM), ("payment_method", VALID_PAYMENT, PAYMENT_NORM)]:
        nn = ~is_null(o[col])
        fixed = letters(o[col]).map(norm)
        ck.add("orders", col, "non-canonical spelling/case", "inconsistent_format", nn & ~o[col].isin(valid) & fixed.notna(), n)
        ck.add("orders", col, "value outside allowed domain", "invalid_value", nn & fixed.isna(), n)

    # ---- order items
    n = len(i)
    order_ids = set(as_int(o["order_id"]).dropna())
    prod_ids = set(as_int(p["product_id"]).dropna())
    oid, pid = as_int(i["order_id"]), as_int(i["product_id"])
    ck.add("order_items", "order_id", "FK -> orders missing", "referential_integrity", oid.notna() & ~oid.isin(order_ids), n)
    ck.add("order_items", "product_id", "FK -> products missing", "referential_integrity", pid.notna() & ~pid.isin(prod_ids), n)

    pp = p.copy()
    pp["sku"] = make_sku(pp)
    exp_sku = pid.map(pd.Series(pp["sku"].to_numpy(), index=as_int(pp["product_id"])))
    nn = ~is_null(i["sku"])
    canon = i["sku"].str.fullmatch(r"[A-Z]{3}-[A-Z]{3}-\d{4}")
    norm_sku = i["sku"].str.upper().str.replace(r"[^A-Z0-9]", "", regex=True).str.replace(r"^SKU", "", regex=True)
    exp_norm = exp_sku.fillna("").str.replace("-", "")
    ck.add("order_items", "sku", "non-canonical SKU format", "inconsistent_format",
           nn & ~canon & (norm_sku == exp_norm), n)
    ck.add("order_items", "sku", "malformed SKU", "invalid_value",
           nn & ~canon & (norm_sku != exp_norm) & ~norm_sku.str.fullmatch(r"[A-Z]{6}\d{4}"), n)
    ck.add("order_items", "sku", "SKU does not match product_id", "inconsistent_value",
           nn & exp_sku.notna() & (norm_sku != exp_norm) & norm_sku.str.fullmatch(r"[A-Z]{6}\d{4}"), n)

    q = as_int(i["quantity"])
    nn = ~is_null(i["quantity"])
    ck.add("order_items", "quantity", "non-integer quantity", "invalid_value", nn & q.isna(), n)
    ck.add("order_items", "quantity", "quantity <= 0 or > 100", "invalid_value",
           nn & i["quantity"].str.fullmatch(r"-?\d+") & ~pd.to_numeric(i["quantity"], errors="coerce").between(1, 100), n)

    up, up_plain = to_num(i["unit_price"])
    nn = ~is_null(i["unit_price"])
    ck.add("order_items", "unit_price", "currency symbol / separators", "inconsistent_format", nn & ~up_plain & up.notna(), n)
    ck.add("order_items", "unit_price", "non-numeric", "invalid_value", nn & up.isna(), n)
    ck.add("order_items", "unit_price", "price <= 0", "invalid_value", up.notna() & (up <= 0), n)
    list_price = pid.map(pd.Series(pd.to_numeric(p["price"]).to_numpy(), index=as_int(p["product_id"])))
    ck.add("order_items", "unit_price", "differs from products.price", "inconsistent_value",
           up.notna() & (up > 0) & list_price.notna() & ((up - list_price).abs() > 0.01), n)

    # ---- returns
    n = len(r)
    item_ids = as_int(i["order_item_id"])
    rid_item = as_int(r["order_item_id"])
    ck.add("returns", "order_item_id", "FK -> order_items missing", "referential_integrity",
           rid_item.notna() & ~rid_item.isin(set(item_ids.dropna())), n)
    ck.add("returns", "order_item_id", "item returned under >1 return_id", "duplicate_entity",
           r.groupby(rid_item)["return_id"].transform("nunique").fillna(0) > 1, n)

    # item -> order lookups (first occurrence wins when keys are duplicated)
    it = pd.DataFrame({"item": item_ids, "oid": oid, "q": q, "up": up}).dropna(subset=["item"]) \
        .drop_duplicates("item").set_index("item")
    ordf = pd.DataFrame({"oid": as_int(o["order_id"]), "od": od,
                         "status": letters(o["order_status"]).map(STATUS_NORM)}).dropna(subset=["oid"]) \
        .drop_duplicates("oid").set_index("oid")
    r_oid = rid_item.map(it["oid"])
    r_status = r_oid.map(ordf["status"])
    r_odate = r_oid.map(ordf["od"])
    ck.add("returns", "order_item_id", "return on non-Delivered order", "inconsistent_value",
           r_status.notna() & (r_status != "Delivered"), n)

    rd, rd_iso = parse_dates(r["return_date"])
    date_checks(ck, "returns", "return_date", r["return_date"], rd, rd_iso, n)
    lag = (rd - r_odate).dt.days
    valid_both = rd.notna() & r_odate.notna() & (rd >= MIN_VALID_DATE) & (rd <= SNAPSHOT)
    ck.add("returns", "return_date", "return before order date", "incorrect_date", valid_both & (lag < 0), n)
    ck.add("returns", "return_date", "same day as order (lag 0)", "incorrect_date", valid_both & (lag == 0), n)
    ck.add("returns", "return_date", "more than 30 days after order", "incorrect_date", valid_both & (lag > 30), n)

    ra, ra_plain = to_num(r["refund_amount"])
    nn = ~is_null(r["refund_amount"])
    ck.add("returns", "refund_amount", "currency symbol / separators", "inconsistent_format", nn & ~ra_plain & ra.notna(), n)
    ck.add("returns", "refund_amount", "non-numeric", "invalid_value", nn & ra.isna(), n)
    ck.add("returns", "refund_amount", "refund <= 0", "invalid_value", ra.notna() & (ra <= 0), n)
    item_value = rid_item.map(it["q"] * it["up"])
    ck.add("returns", "refund_amount", "refund > quantity * unit_price", "inconsistent_value",
           ra.notna() & (ra > 0) & item_value.notna() & (ra > item_value + 0.01), n)

    # ---- unchanged tables (should be clean)
    ck.add("products", "category_id", "FK -> categories missing", "referential_integrity",
           ~as_int(p["category_id"]).isin(set(as_int(d["categories"]["category_id"]))), len(p))

    return pd.DataFrame(summary), pd.DataFrame(nulls), pd.DataFrame(ck.rows)


def date_checks(ck, table, col, raw, parsed, iso, n):
    nn = ~is_null(raw)
    ck.add(table, col, "parseable but not ISO yyyy-mm-dd", "inconsistent_format", nn & parsed.notna() & ~iso, n)
    ck.add(table, col, "unparseable / impossible date", "invalid_value", nn & parsed.isna(), n)
    ck.add(table, col, f"in the future (> {SNAPSHOT.date()})", "incorrect_date", parsed > SNAPSHOT, n)
    ck.add(table, col, "placeholder / before 2000-01-01", "incorrect_date", parsed < MIN_VALID_DATE, n)


# ----------------------------------------------------------------------------- integrity
def file_integrity() -> tuple[list[dict], bool]:
    rows, ok = [], True
    zips = sorted(INPUT_DIR.glob("*.zip"))
    zf = zipfile.ZipFile(zips[0]) if zips else None
    zmap = {n.split("/")[-1]: n for n in zf.namelist()} if zf else {}
    import hashlib
    for orig in sorted(ORIGINAL_DIR.glob("*.csv")):
        h_orig = sha256(orig)
        h_zip = hashlib.sha256(zf.read(zmap[orig.name])).hexdigest() if orig.name in zmap else None
        row = {"file": orig.name, "original_sha256": h_orig, "input_zip_match": h_zip == h_orig}
        if orig.name in UNCHANGED_FILES:
            for label, path in [("output", OUTPUT_DIR / orig.name),
                                ("clean_reference", OUTPUT_DIR / "clean_reference" / orig.name)]:
                row[f"{label}_match"] = sha256(path) == h_orig
                row[f"{label}_bytes_equal"] = path.read_bytes() == orig.read_bytes()
            ok &= all(v for k, v in row.items() if k.endswith(("_match", "_equal")))
        ok &= row["input_zip_match"]
        rows.append(row)
    return rows, ok


# ----------------------------------------------------------------------------- report
def md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(str(v) for v in row.tolist()) + " |")
    return "\n".join(lines)


def main() -> None:
    dirty = load(OUTPUT_DIR)
    clean = load(OUTPUT_DIR / "clean_reference")
    # city domain = cities present in the ORIGINAL customers file
    ref_cities = set(pd.read_csv(ORIGINAL_DIR / "customers.csv", dtype=str)["city"])
    summary, nulls, checks = validate(dirty, ref_cities)
    _, _, c_checks = validate(clean, ref_cities)

    integrity, files_ok = file_integrity()
    counts_ok = all(len(dirty[t]) == n for t, n in TARGETS.items())

    log = pd.read_csv(OUTPUT_DIR / "data_quality_issue_log.csv", dtype=str, keep_default_na=False)
    injected = log.groupby(["table", "issue_type"]).size().rename("injected").reset_index()
    affected = log.groupby("table")["row_number"].nunique()

    checks = checks.merge(c_checks[["table", "column", "check", "violations"]]
                          .rename(columns={"violations": "clean_ref_violations"}),
                          on=["table", "column", "check"], how="left")
    checks_nonzero = checks[(checks["violations"] > 0) | (checks["clean_ref_violations"] > 0)]
    by_cat = checks.groupby(["table", "category"])["violations"].sum().unstack(fill_value=0)
    ri = checks[checks["category"] == "referential_integrity"][["table", "column", "check", "violations", "pct"]]

    summary["rows_with_injected_issue"] = summary["table"].map(affected).fillna(0).astype(int)
    summary["pct_rows_dirty"] = (100 * summary["rows_with_injected_issue"] / summary["rows"]).round(2)
    summary["clean_rows_pct"] = (100 - summary["pct_rows_dirty"]).round(2)
    summary["target_met"] = summary.apply(lambda x: "" if x["target"] == "" else x["rows"] == x["target"], axis=1)

    md = [f"# Data Quality Report\n",
          f"Snapshot date assumed: **{SNAPSHOT.date()}**. Generated by `scripts/04_validate_report.py`. "
          f"Detection is rule-based and does not read the injection log; the *clean_ref* column runs "
          f"the same rule on `clean_reference/` as a control.\n",
          "## 1. Hard checks\n",
          f"- Row-count targets met (customers 30,000 / orders 100,000 / returns 20,000): **{counts_ok}**",
          f"- products.csv & categories.csv byte-for-byte identical to originals: **{files_ok}**",
          f"- Original source files unchanged (match copies in input ZIP): "
          f"**{all(x['input_zip_match'] for x in integrity)}**\n",
          "## 2. Row counts, duplicates and dirty-row share\n",
          md_table(summary),
          "\n`exact_duplicate_rows` = full-row duplicates; `duplicate_pk_rows` = extra rows sharing a primary key "
          "(includes the exact duplicates). `pct_rows_dirty` = rows touched by at least one injected issue.\n",
          "## 3. Issues by category (detected)\n",
          md_table(by_cat.reset_index()),
          "\n## 4. Referential integrity violations\n",
          md_table(ri),
          "\nNULL foreign keys are counted under nulls (section 6), not as RI violations.\n",
          "## 5. Detailed rule results (non-zero only)\n",
          md_table(checks_nonzero[["table", "column", "check", "category", "violations", "pct", "clean_ref_violations"]]),
          "\n## 6. Null percentages\n",
          "Null-like = empty cell or one of the tokens " + ", ".join(f"`{t}`" for t in sorted(NULL_TOKENS) if t) + ".\n",
          md_table(nulls[nulls["table"].isin(["customers", "orders", "order_items", "returns"])]),
          "\n## 7. Injected issues (ground truth from `data_quality_issue_log.csv`)\n",
          md_table(injected),
          "\n## 8. File integrity (SHA-256)\n",
          md_table(pd.DataFrame(integrity).assign(original_sha256=lambda x: x["original_sha256"].str[:16] + "…")),
          "\n## 9. Clean reference control\n",
          f"Total rule violations in `clean_reference/`: **{int(c_checks['violations'].sum())}** "
          f"(non-zero rules: {', '.join(c_checks[c_checks.violations > 0]['check']) or 'none'}).\n",
          ]
    (OUTPUT_DIR / "data_quality_report.md").write_text("\n".join(md), encoding="utf-8")
    json.dump({"snapshot": str(SNAPSHOT.date()), "row_counts_ok": counts_ok, "unchanged_files_ok": files_ok,
               "summary": summary.to_dict("records"), "checks": checks.to_dict("records"),
               "nulls": nulls.to_dict("records"), "integrity": integrity,
               "clean_reference_total_violations": int(c_checks["violations"].sum())},
              open(OUTPUT_DIR / "data_quality_report.json", "w", encoding="utf-8"), indent=2, default=str)

    # ---- final ZIP
    zpath = OUTPUT_DIR / "ecommerce_dataset_final.zip"
    members = sorted(p for p in OUTPUT_DIR.rglob("*") if p.is_file() and p != zpath)
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as zf:
        for pth in members:
            zf.write(pth, arcname=pth.relative_to(OUTPUT_DIR).as_posix())
    with zipfile.ZipFile(zpath) as zf:
        zip_ok = all(zf.read(f).__eq__((ORIGINAL_DIR / f).read_bytes()) for f in UNCHANGED_FILES)
        bad = zf.testzip()

    print(summary[["table", "rows", "target", "target_met", "exact_duplicate_rows", "duplicate_pk_rows",
                   "pct_rows_dirty"]].to_string(index=False))
    print("\nIssues by category:\n" + by_cat.to_string())
    print("\nReferential integrity:\n" + ri.to_string(index=False))
    print(f"\nclean_reference total violations: {int(c_checks['violations'].sum())}")
    print(c_checks[c_checks.violations > 0].to_string(index=False))
    print(f"\nrow counts ok: {counts_ok} | products/categories byte-identical: {files_ok} | "
          f"zip copies identical: {zip_ok} | zip CRC ok: {bad is None}")
    print(f"ZIP: {zpath} ({len(members)} files, {zpath.stat().st_size / 1e6:.1f} MB)")
    if not (counts_ok and files_ok and zip_ok and bad is None):
        sys.exit(1)


if __name__ == "__main__":
    main()
