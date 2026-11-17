"""Shared paths, constants and helpers for the dataset pipeline."""
from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT / "input"
ORIGINAL_DIR = ROOT / "kaggle_dataset"          # where the raw CSVs were found
WORK_DIR = ROOT / "work"
EXTRACT_DIR = WORK_DIR / "extracted"            # pristine copy unpacked from the ZIP
CLEAN_DIR = WORK_DIR / "clean_expanded"         # expanded, still-clean data
OUTPUT_DIR = ROOT / "output"

SEED = 20261117

TARGETS = {"customers": 30_000, "orders": 100_000, "returns": 20_000}
SNAPSHOT_DATE = "2026-11-15"   # assumed data-extraction date; no dates after this are valid

UNCHANGED_FILES = ["products.csv", "categories.csv"]

# State -> region mapping (Indian geography; Central states assigned to nearest of the 4)
STATE_REGION = {
    "Haryana": "NORTH", "Himachal Pradesh": "NORTH", "Punjab": "NORTH",
    "Uttar Pradesh": "NORTH", "Uttarakhand": "NORTH", "Rajasthan": "NORTH",
    "Delhi": "NORTH", "Jammu and Kashmir": "NORTH",
    "Andhra Pradesh": "SOUTH", "Karnataka": "SOUTH", "Kerala": "SOUTH",
    "Tamil Nadu": "SOUTH", "Telangana": "SOUTH",
    "Arunachal Pradesh": "EAST", "Assam": "EAST", "Bihar": "EAST",
    "Jharkhand": "EAST", "Manipur": "EAST", "Meghalaya": "EAST",
    "Mizoram": "EAST", "Nagaland": "EAST", "Odisha": "EAST", "Sikkim": "EAST",
    "Tripura": "EAST", "West Bengal": "EAST", "Chhattisgarh": "EAST",
    "Goa": "WEST", "Gujarat": "WEST", "Maharashtra": "WEST",
    "Madhya Pradesh": "WEST",
}

VALID_REGIONS = {"NORTH", "SOUTH", "EAST", "WEST"}
VALID_STATUS = {"Delivered", "In Transit", "Canceled"}
VALID_PAYMENT = {"Cash on Delivery", "Net Banking", "UPI", "Loyalty Points"}
NULL_TOKENS = {"", "NULL", "null", "N/A", "NaN", "none", "-"}

CATEGORY_SKU_CODE = {
    1: "APP",  # Apparel/Fashion
    2: "ELE",  # Electronics
    3: "BEA",  # Beauty/Cosmetics
    4: "HKF",  # Home & Kitchen Furniture
    5: "FNB",  # Food & Beverages
    6: "TOY",  # Toys
}


PRODUCT_NAME_CODE = {
    "Smartphone": "SPH", "Laptop": "LAP", "Wireless earbuds": "WEB", "Bluetooth speaker": "BTS",
    "Smartwatch": "SWT", "Running shoes": "RSH", "T-shirt": "TSH", "Backpack": "BPK",
    "Perfume": "PRF", "Coffee maker": "CFM", "Office chair": "OFC", "Bedsheets": "BDS",
    "Gaming mouse": "GMS", "Printer": "PRT", "Water bottle": "WBT",
}


def make_sku(products):
    """SKU = <CATEGORY CODE>-<PRODUCT NAME CODE>-<4-digit product_id>, e.g. ELE-SPH-0004."""
    import re

    def name_code(n: str) -> str:
        if n in PRODUCT_NAME_CODE:
            return PRODUCT_NAME_CODE[n]
        return (re.sub(r"[^A-Za-z]", "", n).upper() + "XXX")[:3]
    pid = products["product_id"].astype(int)
    cat = products["category_id"].astype(int).map(CATEGORY_SKU_CODE).fillna("GEN")
    return cat + "-" + products["product_name"].map(name_code) + "-" + pid.map(lambda i: f"{i:04d}")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def find_or_build_input_zip() -> Path:
    """Return the dataset ZIP in input/. If none exists, package the raw CSVs
    from kaggle_dataset/ into input/original_dataset.zip (originals are only read)."""
    INPUT_DIR.mkdir(exist_ok=True)
    zips = sorted(INPUT_DIR.glob("*.zip"))
    if zips:
        return zips[0]
    target = INPUT_DIR / "original_dataset.zip"
    csvs = sorted(ORIGINAL_DIR.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError("No ZIP in input/ and no CSVs in kaggle_dataset/")
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in csvs:
            zf.write(p, arcname=p.name)
    return target


def extract_zip(zip_path: Path) -> list[Path]:
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    out = []
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            name = Path(info.filename).name
            if info.is_dir() or not name.lower().endswith((".csv", ".xlsx", ".xls")):
                continue
            dest = EXTRACT_DIR / name
            with zf.open(info) as src, open(dest, "wb") as dst:
                dst.write(src.read())
            out.append(dest)
    return out
