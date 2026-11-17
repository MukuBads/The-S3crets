"""
Silver layer - Retail KPI medallion pipeline
(Databricks Delta Live Tables / Lakeflow Declarative Pipelines, Python `dlt` API)

Input  : {CAT}.bronze.<feed>  (6 append-only Auto Loader streaming tables, every column STRING)
Output : {CAT}.silver.<feed>                         current accepted state (VALID + WARNING rows)
         {CAT}.silver.quarantine_<feed>              rejected rows (INVALID), one row per distinct payload
         {CAT}.silver.referential_integrity_exceptions   unresolved / orphaned foreign keys (re-evaluated every update)
         {CAT}.silver.quarantine_all                 consolidated quarantine + RI failures, with resolution status
         {CAT}.silver.dq_status_summary / dq_rule_summary / dq_bronze_duplicate_keys   monitoring

Per feed:
  bronze.<feed> --readStream--> exact-duplicate suppression (dropDuplicatesWithinWatermark on content hash)
      --> clean + safe type conversion --> rule tagging (all failing rules kept)
      |--> quality_status IN (VALID, WARNING) --> view silver_<feed>_accepted   --AUTO CDC (key = PK)----------> silver.<feed>
      |--> quality_status = INVALID           --> view silver_<feed>_rejected   --AUTO CDC (key = quarantine_id)--> silver.quarantine_<feed>

Runtime requirements: Spark >= 3.5 (serverless or DBR 14.3 LTS+) for try_to_timestamp, try_element_at,
dropDuplicatesWithinWatermark. Checkpoints and state are managed by the pipeline; no manual checkpoint paths.

Pipeline configuration keys (Settings > Configuration):
  retail.catalog                         required, e.g. workspace
  retail.bronze.schema                   default "bronze"
  retail.silver.schema                   default "silver"
  retail.silver.scd_type                 1 (current state, default) or 2 (keep every accepted version)
  retail.silver.dedup_window             default "2 hours"   - state retention for exact-duplicate suppression
  retail.silver.fk_grace_minutes         default 360        - wait time for a late parent before ORPHANED
  retail.silver.min_valid_date           default 2000-01-01 - older dates are treated as placeholders
  retail.silver.max_quantity             default 100        - larger order quantities are rejected
  retail.silver.enable_bronze_duplicate_audit   default true - builds dq_bronze_duplicate_keys (full Bronze scan)
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import chain

import dlt
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

# =====================================================================================================
# 1. IMPORTS AND CONFIGURATION
# =====================================================================================================

spark = SparkSession.getActiveSession()


def _conf(key: str, default: str) -> str:
    return spark.conf.get(key, default)


CAT = spark.conf.get("retail.catalog")  # no default: a missing catalog must fail the update, not guess
BRONZE_SCHEMA = _conf("retail.bronze.schema", "bronze")
SILVER_SCHEMA = _conf("retail.silver.schema", "silver")
SCD_TYPE = int(_conf("retail.silver.scd_type", "1"))
DEDUP_WINDOW = _conf("retail.silver.dedup_window", "2 hours")
FK_GRACE_MINUTES = int(_conf("retail.silver.fk_grace_minutes", "360"))
MIN_VALID_DATE = _conf("retail.silver.min_valid_date", "2000-01-01")
MAX_QUANTITY = int(_conf("retail.silver.max_quantity", "100"))
ENABLE_BRONZE_DUPLICATE_AUDIT = _conf("retail.silver.enable_bronze_duplicate_audit", "true").strip().lower() == "true"

if SCD_TYPE not in (1, 2):
    raise ValueError(f"retail.silver.scd_type must be 1 or 2, got {SCD_TYPE}")

# With the default EXCEPTION policy, values such as '2025-02-30' can raise SparkUpgradeException instead of
# returning NULL, which would fail the whole micro-batch on one bad row. Require CORRECTED (strict parser,
# NULL on failure) to be set in the pipeline configuration rather than changing it silently here.
_TIME_PARSER_POLICY = spark.conf.get("spark.sql.legacy.timeParserPolicy", "EXCEPTION").upper()
if _TIME_PARSER_POLICY != "CORRECTED":
    raise ValueError("Set pipeline configuration spark.sql.legacy.timeParserPolicy = CORRECTED "
                     f"(current: {_TIME_PARSER_POLICY}); Silver relies on NULL-on-failure date parsing.")

# Auto CDC entry point: `create_auto_cdc_flow` is the current name; `apply_changes` is the older name of
# the same API with the same arguments. Only one of them is ever called.
_auto_cdc_flow = getattr(dlt, "create_auto_cdc_flow", None) or dlt.apply_changes

# ---- Source contract (must match the Bronze explicit schema) -----------------------------------------
COLUMNS = {
    "categories":  ["category_id", "category_name"],
    "products":    ["product_id", "product_name", "category_id", "price", "cost_price"],
    "customers":   ["customer_id", "name", "email", "phone_number", "signup_date", "city", "state", "region"],
    "orders":      ["order_id", "customer_id", "order_date", "order_status", "payment_method"],
    "order_items": ["order_item_id", "order_id", "product_id", "sku", "quantity", "unit_price"],
    "returns":     ["return_id", "order_item_id", "return_date", "refund_amount"],
}
PRIMARY_KEYS = {
    "categories": "category_id", "products": "product_id", "customers": "customer_id",
    "orders": "order_id", "order_items": "order_item_id", "returns": "return_id",
}
# (child feed, FK column, parent feed) - verified against SCHEMA_AND_RELATIONSHIPS.md
FOREIGN_KEYS = [
    ("products", "category_id", "categories"),
    ("orders", "customer_id", "customers"),
    ("order_items", "order_id", "orders"),
    ("order_items", "product_id", "products"),
    ("returns", "order_item_id", "order_items"),
]
METADATA_COLUMNS = ["_ingested_at", "_source_file", "_row_hash"]
RESCUED_COLUMN = "_rescued_data"

# ---- Quality vocabulary ---------------------------------------------------------------------------------
VALID, WARNING, INVALID = "VALID", "WARNING", "INVALID"
CAT_KEY = "KEY"                      # primary / foreign key structurally unusable
CAT_COMPLETENESS = "COMPLETENESS"    # value missing
CAT_TYPE = "TYPE_CONVERSION"         # value cannot be converted to the target type
CAT_DOMAIN = "DOMAIN"                # value outside the documented set of allowed values
CAT_FORMAT = "FORMAT"                # value usable after documented standardisation (warning only)
CAT_BUSINESS = "BUSINESS_RULE"       # value converts but breaks a range rule
CAT_SCHEMA = "SCHEMA_DRIFT"          # data outside the declared source schema
CAT_RI = "REFERENTIAL_INTEGRITY"

RI_PENDING = "UNRESOLVED_PENDING"            # parent not seen yet, still inside the grace period
RI_ORPHANED = "ORPHANED"                     # parent still missing after the grace period
RI_PARENT_QUARANTINED = "PARENT_QUARANTINED" # parent arrived but was rejected (INVALID)

# ---- Reference values established from the dataset and SCHEMA_AND_RELATIONSHIPS.md -------------------
NULL_TOKENS = ["", "null", "n/a", "nan", "none", "-"]           # compared after trim + lower()
ID_REGEX = r"^[0-9]+$"                                          # all keys in the dataset are positive integers
DECIMAL_TYPE = "decimal(12,2)"                                  # max observed value 309,559.35, 2 decimals
PLAIN_DECIMAL_REGEX = r"^-?[0-9]{1,10}(\.[0-9]{1,2})?$"          # guarantees the cast cannot overflow or round
CURRENCY_NOISE_REGEX = r"(?i)(₹|rs\.?|inr|,|\s)"                # ₹12,345.67 / Rs. 123.00 / 123.00 INR / INR 1,234
INTEGER_REGEX = r"^-?[0-9]{1,6}$"
EMAIL_REGEX = r"^[a-z0-9._%+-]+@[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}$"
PHONE_CORE_REGEX = r"^[6-9][0-9]{9}$"                           # Indian mobile, canonical form +91-XXXXXXXXXX
PHONE_PLACEHOLDER_REGEX = r"^([0-9])\1{9}$"                     # 9999999999, 0000000000 ...
SKU_REGEX = r"^[A-Z]{3}-[A-Z]{3}-[0-9]{4}$"                     # <CAT>-<NAME>-<4-digit product_id>

# Date formats observed in the feeds. Order matters only for readability: the regexes are mutually exclusive.
DATE_FORMATS = [
    ("iso", r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$", "yyyy-MM-dd"),
    ("iso_datetime", r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$", "yyyy-MM-dd'T'HH:mm:ss"),
    ("yyyy/MM/dd", r"^[0-9]{4}/[0-9]{2}/[0-9]{2}$", "yyyy/MM/dd"),
    ("yyyyMMdd", r"^[0-9]{8}$", "yyyyMMdd"),
    ("dd-MM-yyyy", r"^[0-9]{2}-[0-9]{2}-[0-9]{4}$", "dd-MM-yyyy"),
    ("dd.MM.yyyy", r"^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$", "dd.MM.yyyy"),
    ("MMM dd, yyyy", r"^[A-Za-z]{3} [0-9]{2}, [0-9]{4}$", "MMM dd, yyyy"),
    ("dd MMMM yyyy", r"^[0-9]{2} [A-Za-z]+ [0-9]{4}$", "dd MMMM yyyy"),
]
# nn/nn/yyyy appears both day-first and month-first in the feeds -> resolved only when one reading is impossible
SLASH_DATE_REGEX = r"^[0-9]{2}/[0-9]{2}/[0-9]{4}$"

# Categorical domains. Keys are lower-case letters only, so case, spaces, '-' and '_' variants map to the
# canonical value. Only spellings documented as variants of an allowed value are listed; anything else
# (e.g. 'Deliverd', 'Cancel', 'Loyalty Pts') is NOT guessed and fails the domain rule.
ORDER_STATUS_MAP = {"delivered": "Delivered", "intransit": "In Transit",
                    "canceled": "Canceled", "cancelled": "Canceled"}
PAYMENT_METHOD_MAP = {"cashondelivery": "Cash on Delivery", "cod": "Cash on Delivery",
                      "netbanking": "Net Banking", "upi": "UPI", "loyaltypoints": "Loyalty Points"}
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
STATE_REGION = {
    "Haryana": "NORTH", "Himachal Pradesh": "NORTH", "Punjab": "NORTH", "Rajasthan": "NORTH",
    "Uttar Pradesh": "NORTH", "Uttarakhand": "NORTH",
    "Andhra Pradesh": "SOUTH", "Karnataka": "SOUTH", "Kerala": "SOUTH", "Tamil Nadu": "SOUTH", "Telangana": "SOUTH",
    "Arunachal Pradesh": "EAST", "Assam": "EAST", "Bihar": "EAST", "Chhattisgarh": "EAST", "Jharkhand": "EAST",
    "Manipur": "EAST", "Meghalaya": "EAST", "Mizoram": "EAST", "Nagaland": "EAST", "Odisha": "EAST",
    "Sikkim": "EAST", "Tripura": "EAST", "West Bengal": "EAST",
    "Goa": "WEST", "Gujarat": "WEST", "Maharashtra": "WEST", "Madhya Pradesh": "WEST",
}
KNOWN_CITIES = [
    "Agartala", "Ahmedabad", "Aizawl", "Amritsar", "Bengaluru", "Bhagalpur", "Bhilai", "Bhopal", "Bhubaneswar",
    "Bilaspur", "Bishnupur", "Chennai", "Coimbatore", "Cuttack", "Dehradun", "Dhanbad", "Dharamshala",
    "Dharmanagar", "Dibrugarh", "Dimapur", "Durgapur", "Faridabad", "Gangtok", "Gaya", "Guntur", "Gurugram",
    "Guwahati", "Gyalshing", "Haldwani", "Haridwar", "Howrah", "Hyderabad", "Imphal", "Indore", "Itanagar",
    "Jabalpur", "Jaipur", "Jalandhar", "Jamshedpur", "Jodhpur", "Jowai", "Kanpur", "Kochi", "Kohima", "Kolkata",
    "Kozhikode", "Lucknow", "Ludhiana", "Lunglei", "Madurai", "Mangaluru", "Margao", "Mokokchung", "Mumbai",
    "Mysuru", "Nagpur", "Naharlagun", "Namchi", "Nizamabad", "Noida", "Panaji", "Panipat", "Pasighat", "Patna",
    "Pune", "Raipur", "Ranchi", "Rourkela", "Saiha", "Shillong", "Shimla", "Silchar", "Solan", "Surat",
    "Thiruvananthapuram", "Thoubal", "Tura", "Udaipur", "Vadodara", "Vasco da Gama", "Vijayawada",
    "Visakhapatnam", "Warangal",
]


def bronze_table(feed: str) -> str:
    return f"{CAT}.{BRONZE_SCHEMA}.{feed}"


def silver_table(name: str) -> str:
    return f"{CAT}.{SILVER_SCHEMA}.{name}"


# =====================================================================================================
# 2. BRONZE STREAMING SOURCE DEFINITIONS
# =====================================================================================================

def _validate_bronze_schema(feed: str, columns: list[str]) -> list[str]:
    """Fail the update on an incompatible Bronze schema; return extra (unmapped) Bronze business columns.

    Bronze uses an explicit schema, so a missing column means the Bronze code changed incompatibly. That is
    a deployment error, not a data error, so it must stop the pipeline instead of being quarantined.
    Additional non-metadata columns are an additive change: they are carried in `_raw` and flagged.
    """
    expected = COLUMNS[feed] + METADATA_COLUMNS
    missing = [c for c in expected if c not in columns]
    if missing:
        raise ValueError(f"{bronze_table(feed)} is missing expected columns {missing}; "
                         f"Silver cannot process this feed until Bronze and Silver schemas are aligned.")
    return [c for c in columns if c not in expected and not c.startswith("_")]


def raw_payload(feed: str, unmapped: list[str]) -> Column:
    """Every source value exactly as stored in Bronze (strings), incl. unmapped columns and rescued data."""
    cols = COLUMNS[feed] + unmapped + [RESCUED_COLUMN]
    return F.struct(*[F.col(c).cast("string").alias(c) for c in cols])


def read_bronze(feed: str) -> DataFrame:
    """Incremental read of one Bronze streaming table, with source contract checks and exact-duplicate
    suppression. Progress (Delta table version / offsets) is stored in the flow's managed checkpoint."""
    df = spark.readStream.table(bronze_table(feed))
    unmapped = _validate_bronze_schema(feed, df.columns)
    if RESCUED_COLUMN not in df.columns:  # Bronze always sets it; tolerate its absence without failing
        df = df.withColumn(RESCUED_COLUMN, F.lit(None).cast("string"))

    df = (df
          .withColumn("_ingested_at", F.col("_ingested_at").cast("timestamp"))
          .withColumn("_raw", raw_payload(feed, unmapped))
          # Deterministic identity of the payload. Independent of how Bronze computes _row_hash.
          .withColumn("_content_hash", F.sha2(F.to_json(F.col("_raw")), 256))
          .withColumn("_unmapped_populated",
                      F.coalesce(*[F.col(c).cast("string") for c in unmapped]).isNotNull()
                      if unmapped else F.lit(False)))

    # Exact re-deliveries (same payload, any file) inside DEDUP_WINDOW are dropped here; state is kept in the
    # checkpoint and evicted after the window. The watermark is on processing time, which only moves
    # forward, so no row is ever discarded as "late" by this operator. Copies arriving after the window
    # reach the AUTO CDC flows, which are idempotent for identical payloads (see section 6).
    return (df
            .withColumn("_silver_seen_at", F.current_timestamp())
            .withWatermark("_silver_seen_at", DEDUP_WINDOW)
            .dropDuplicatesWithinWatermark(["_content_hash"])
            .drop("_silver_seen_at"))


# =====================================================================================================
# 3. REUSABLE CLEANING FUNCTIONS
# =====================================================================================================

def clean_string(c: Column) -> Column:
    """Trim all leading/trailing whitespace; turn empty strings and null tokens into NULL. Case unchanged."""
    trimmed = F.regexp_replace(c, r"^\s+|\s+$", "")
    return F.when(F.lower(trimmed).isin(NULL_TOKENS), F.lit(None).cast("string")).otherwise(trimmed)


def s(col: str) -> Column:
    """Cleaned string version of a source column."""
    return F.col(f"_s_{col}")


def add_clean_strings(df: DataFrame, feed: str) -> DataFrame:
    for c in COLUMNS[feed]:
        df = df.withColumn(f"_s_{c}", clean_string(F.col(c)))
    return df


def letters_key(c: Column) -> Column:
    """Lower-case letters only - used to match documented categorical variants."""
    return F.regexp_replace(F.lower(c), "[^a-z]", "")


def literal_map(mapping: dict) -> Column:
    return F.create_map(*[F.lit(x) for x in chain.from_iterable(mapping.items())])


def lookup(mapping: dict, key: Column) -> Column:
    """NULL when the key is missing (never raises, also under ANSI mode)."""
    return F.try_element_at(literal_map(mapping), key)


# =====================================================================================================
# 4. TYPE CONVERSION AND INVALID-CAST DETECTION
#    Every converter returns (typed value, outcome label). The label records how the value was read, so a
#    failed conversion is visible as 'unparseable' instead of silently becoming NULL.
# =====================================================================================================

def _try_date(c: Column, pattern: str) -> Column:
    return F.try_to_timestamp(c, F.lit(pattern)).cast("date")


def parse_date(c: Column) -> tuple[Column, Column]:
    """Labels: NULL (no value) | iso | <other format> | ambiguous | unparseable."""
    value = F.when(c.isNull(), F.lit(None).cast("date"))
    label = F.when(c.isNull(), F.lit(None).cast("string"))
    for name, regex, pattern in DATE_FORMATS:
        parsed = _try_date(c, pattern)
        value = value.when(c.rlike(regex), parsed)
        label = label.when(c.rlike(regex), F.when(parsed.isNotNull(), F.lit(name)).otherwise(F.lit("unparseable")))

    day_first, month_first = _try_date(c, "dd/MM/yyyy"), _try_date(c, "MM/dd/yyyy")
    ambiguous = day_first.isNotNull() & month_first.isNotNull() & (day_first != month_first)
    slash = c.rlike(SLASH_DATE_REGEX)
    value = value.when(slash & ambiguous, F.lit(None).cast("date")).when(slash, F.coalesce(day_first, month_first))
    label = (label.when(slash & ambiguous, F.lit("ambiguous"))
                  .when(slash & F.coalesce(day_first, month_first).isNotNull(), F.lit("nn/nn/yyyy"))
                  .when(slash, F.lit("unparseable")))
    return value.otherwise(F.lit(None).cast("date")), label.otherwise(F.lit("unparseable"))


def parse_decimal(c: Column) -> tuple[Column, Column]:
    """Labels: NULL | plain | reformatted (currency symbol / separators removed) | unparseable.
    The regex guard bounds digits and scale, so the cast can neither overflow nor round."""
    stripped = F.regexp_replace(c, CURRENCY_NOISE_REGEX, "")
    value = (F.when(c.rlike(PLAIN_DECIMAL_REGEX), c.cast(DECIMAL_TYPE))
              .when(stripped.rlike(PLAIN_DECIMAL_REGEX), stripped.cast(DECIMAL_TYPE)))
    label = (F.when(c.isNull(), F.lit(None).cast("string"))
              .when(c.rlike(PLAIN_DECIMAL_REGEX), F.lit("plain"))
              .when(stripped.rlike(PLAIN_DECIMAL_REGEX), F.lit("reformatted"))
              .otherwise(F.lit("unparseable")))
    return value, label


def parse_int(c: Column) -> tuple[Column, Column]:
    """Labels: NULL | plain | unparseable ('2.5', 'two')."""
    value = F.when(c.rlike(INTEGER_REGEX), c.cast("int"))
    label = (F.when(c.isNull(), F.lit(None).cast("string"))
              .when(c.rlike(INTEGER_REGEX), F.lit("plain")).otherwise(F.lit("unparseable")))
    return value, label


def add_date(df: DataFrame, col: str) -> DataFrame:
    value, label = parse_date(s(col))
    return df.withColumn(f"_d_{col}", value).withColumn(f"_f_{col}", label)


def add_decimal(df: DataFrame, col: str) -> DataFrame:
    value, label = parse_decimal(s(col))
    return df.withColumn(f"_n_{col}", value).withColumn(f"_f_{col}", label)


def add_int(df: DataFrame, col: str) -> DataFrame:
    value, label = parse_int(s(col))
    return df.withColumn(f"_i_{col}", value).withColumn(f"_f_{col}", label)


# =====================================================================================================
# 5. DATA QUALITY RULES
# =====================================================================================================

@dataclass(frozen=True)
class Rule:
    rule: str          # stable identifier, e.g. order_date_unparseable
    column: str        # column the rule is about
    severity: str      # WARNING or INVALID
    category: str      # one of the CAT_* values
    condition: Column  # TRUE when the record breaks the rule (NULL is treated as "not broken")


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Evaluate every rule (no short-circuit) and derive the quality columns."""
    issues = F.array(*[
        F.when(r.condition, F.struct(F.lit(r.rule).alias("rule"), F.lit(r.column).alias("column"),
                                     F.lit(r.severity).alias("severity"), F.lit(r.category).alias("category")))
        for r in rules
    ])
    df = df.withColumn("quality_issues", F.filter(issues, lambda x: x.isNotNull()))
    invalid_issues = F.filter("quality_issues", lambda x: x["severity"] == F.lit(INVALID))
    return (df
            .withColumn("quality_status",
                        F.when(F.size(invalid_issues) > 0, F.lit(INVALID))
                         .when(F.size("quality_issues") > 0, F.lit(WARNING))
                         .otherwise(F.lit(VALID)))
            .withColumn("is_valid", F.col("quality_status") != F.lit(INVALID))
            .withColumn("quality_issue_summary",
                        F.array_join(F.transform("quality_issues",
                                                 lambda x: F.concat(x["severity"], F.lit(":"), x["rule"])), "; "))
            # rules are declared in priority order, so the first INVALID issue is the primary category
            .withColumn("error_category", F.get(invalid_issues, 0)["category"])
            .withColumn("validation_timestamp", F.current_timestamp()))


# ---- reusable rule builders ------------------------------------------------------------------------

def key_rules(col: str) -> list[Rule]:
    """Primary/foreign key present and well formed (positive integer string)."""
    return [
        Rule(f"{col}_missing", col, INVALID, CAT_KEY, s(col).isNull()),
        Rule(f"{col}_malformed", col, INVALID, CAT_KEY, s(col).isNotNull() & ~s(col).rlike(ID_REGEX)),
    ]


def date_rules(col: str, severity: str, mandatory: bool) -> list[Rule]:
    d, f = F.col(f"_d_{col}"), F.col(f"_f_{col}")
    return [
        Rule(f"{col}_missing", col, INVALID if mandatory else WARNING, CAT_COMPLETENESS, s(col).isNull()),
        Rule(f"{col}_unparseable", col, severity, CAT_TYPE, f == "unparseable"),
        Rule(f"{col}_ambiguous_format", col, severity, CAT_TYPE, f == "ambiguous"),
        Rule(f"{col}_non_iso_format", col, WARNING, CAT_FORMAT,
             f.isNotNull() & ~f.isin("iso", "unparseable", "ambiguous")),
        Rule(f"{col}_before_min_valid_date", col, severity, CAT_BUSINESS, d < F.lit(MIN_VALID_DATE).cast("date")),
        # +1 day tolerance: source dates are local (IST) calendar dates, _ingested_at is UTC
        Rule(f"{col}_after_ingestion", col, severity, CAT_BUSINESS,
             d > F.date_add(F.to_date("_ingested_at"), 1)),
    ]


def date_is_usable(col: str) -> Column:
    d = F.col(f"_d_{col}")
    return (d >= F.lit(MIN_VALID_DATE).cast("date")) & (d <= F.date_add(F.to_date("_ingested_at"), 1))


def amount_rules(col: str, mandatory: bool) -> list[Rule]:
    n, f = F.col(f"_n_{col}"), F.col(f"_f_{col}")
    sev = INVALID if mandatory else WARNING
    return [
        Rule(f"{col}_missing", col, sev, CAT_COMPLETENESS, s(col).isNull()),
        Rule(f"{col}_unparseable", col, sev, CAT_TYPE, f == "unparseable"),
        Rule(f"{col}_reformatted", col, WARNING, CAT_FORMAT, f == "reformatted"),
        Rule(f"{col}_not_positive", col, sev, CAT_BUSINESS, n <= 0),
    ]


def schema_rules() -> list[Rule]:
    return [
        Rule("rescued_data_present", RESCUED_COLUMN, WARNING, CAT_SCHEMA, F.col(RESCUED_COLUMN).isNotNull()),
        Rule("unmapped_bronze_column_populated", "_raw", WARNING, CAT_SCHEMA, F.col("_unmapped_populated")),
    ]


def changed(col: str, standardized: Column) -> Column:
    """TRUE when the standardised value differs from what arrived in Bronze."""
    return standardized.isNotNull() & (F.col(col) != standardized)


# ---- feed specifications: standardisation (typed `_t_<col>` columns) + rules ---------------------------
# Typed columns only ever hold values that passed their checks. A value that fails a WARNING-level check is
# set to NULL in the typed column; the original text is always kept in `_raw`.

def standardize_categories(df: DataFrame) -> tuple[DataFrame, list[Rule]]:
    df = (df.withColumn("_t_category_id", s("category_id"))
            .withColumn("_t_category_name", s("category_name")))
    rules = key_rules("category_id") + [
        Rule("category_name_missing", "category_name", WARNING, CAT_COMPLETENESS, s("category_name").isNull()),
    ]
    return df, rules


def standardize_products(df: DataFrame) -> tuple[DataFrame, list[Rule]]:
    df = add_decimal(add_decimal(df, "price"), "cost_price")
    df = (df.withColumn("_t_product_id", s("product_id"))
            .withColumn("_t_product_name", s("product_name"))
            .withColumn("_t_category_id", s("category_id"))
            .withColumn("_t_price", F.when(F.col("_n_price") > 0, F.col("_n_price")))
            .withColumn("_t_cost_price", F.when(F.col("_n_cost_price") > 0, F.col("_n_cost_price"))))
    rules = (key_rules("product_id") + key_rules("category_id")
             + amount_rules("price", mandatory=True) + amount_rules("cost_price", mandatory=True)
             + [Rule("product_name_missing", "product_name", WARNING, CAT_COMPLETENESS, s("product_name").isNull())])
    return df, rules


def standardize_customers(df: DataFrame) -> tuple[DataFrame, list[Rule]]:
    """Customer is master data: only key problems reject the record. Attribute defects are WARNINGs, so the
    customer stays available as an FK parent for its orders."""
    df = add_date(df, "signup_date")

    email = F.lower(s("email"))
    email_ok = email.rlike(EMAIL_REGEX)

    phone_digits = F.regexp_replace(s("phone_number"), r"[^0-9]", "")
    phone_core = (F.when((F.length(phone_digits) == 12) & phone_digits.startswith("91"), F.substring(phone_digits, 3, 10))
                   .when((F.length(phone_digits) == 11) & phone_digits.startswith("0"), F.substring(phone_digits, 2, 10))
                   .when(F.length(phone_digits) == 10, phone_digits))
    # coalesce: a wrong digit count gives a NULL core, which must count as "not ok" (not as "unknown")
    phone_ok = F.coalesce(~s("phone_number").rlike("[A-Za-z]") & phone_core.rlike(PHONE_CORE_REGEX), F.lit(False))
    phone_placeholder = phone_ok & phone_core.rlike(PHONE_PLACEHOLDER_REGEX)
    phone_canonical = F.concat(F.lit("+91-"), phone_core)

    city_canonical = lookup({c.lower(): c for c in KNOWN_CITIES}, F.lower(s("city")))
    state_canonical = lookup({st.lower(): st for st in STATE_REGION}, F.lower(s("state")))
    region_upper = F.upper(s("region"))
    region_ok = region_upper.isin(REGIONS)
    expected_region = lookup(STATE_REGION, state_canonical)
    name = s("name")
    has_letters = name.rlike("[A-Za-z]")

    df = (df.withColumn("_t_customer_id", s("customer_id"))
            .withColumn("_t_name", name)
            .withColumn("_t_email", F.when(email_ok, email))
            .withColumn("_t_phone_number", F.when(phone_ok & ~phone_placeholder, phone_canonical))
            .withColumn("_t_signup_date", F.when(date_is_usable("signup_date"), F.col("_d_signup_date")))
            .withColumn("_t_city", F.coalesce(city_canonical, s("city")))     # unknown cities kept, flagged
            .withColumn("_t_state", F.coalesce(state_canonical, s("state")))  # unknown states kept, flagged
            .withColumn("_t_region", F.when(region_ok, region_upper)))        # closed 4-value domain

    rules = key_rules("customer_id") + [
        Rule("name_missing", "name", WARNING, CAT_COMPLETENESS, name.isNull()),
        Rule("name_nonstandard_format", "name", WARNING, CAT_FORMAT,
             changed("name", name) | name.contains("  ")
             | (has_letters & ((name == F.upper(name)) | (name == F.lower(name))))),
        Rule("email_missing", "email", WARNING, CAT_COMPLETENESS, s("email").isNull()),
        Rule("email_invalid", "email", WARNING, CAT_DOMAIN, s("email").isNotNull() & ~email_ok),
        Rule("email_normalized", "email", WARNING, CAT_FORMAT, email_ok & changed("email", email)),
        Rule("phone_number_missing", "phone_number", WARNING, CAT_COMPLETENESS, s("phone_number").isNull()),
        Rule("phone_number_invalid", "phone_number", WARNING, CAT_DOMAIN, s("phone_number").isNotNull() & ~phone_ok),
        Rule("phone_number_placeholder", "phone_number", WARNING, CAT_DOMAIN, phone_placeholder),
        Rule("phone_number_normalized", "phone_number", WARNING, CAT_FORMAT,
             phone_ok & ~phone_placeholder & changed("phone_number", phone_canonical)),
    ] + date_rules("signup_date", WARNING, mandatory=False) + [
        Rule("city_missing", "city", WARNING, CAT_COMPLETENESS, s("city").isNull()),
        Rule("city_case_normalized", "city", WARNING, CAT_FORMAT, changed("city", city_canonical)),
        Rule("city_not_in_reference_list", "city", WARNING, CAT_DOMAIN, s("city").isNotNull() & city_canonical.isNull()),
        Rule("state_missing", "state", WARNING, CAT_COMPLETENESS, s("state").isNull()),
        Rule("state_case_normalized", "state", WARNING, CAT_FORMAT, changed("state", state_canonical)),
        Rule("state_not_in_reference_list", "state", WARNING, CAT_DOMAIN,
             s("state").isNotNull() & state_canonical.isNull()),
        Rule("region_missing", "region", WARNING, CAT_COMPLETENESS, s("region").isNull()),
        Rule("region_normalized", "region", WARNING, CAT_FORMAT, region_ok & changed("region", region_upper)),
        Rule("region_not_allowed", "region", WARNING, CAT_DOMAIN, s("region").isNotNull() & ~region_ok),
        Rule("region_state_mismatch", "region", WARNING, CAT_BUSINESS,
             region_ok & expected_region.isNotNull() & (region_upper != expected_region)),
    ]
    return df, rules


def standardize_orders(df: DataFrame) -> tuple[DataFrame, list[Rule]]:
    df = add_date(df, "order_date")
    status = lookup(ORDER_STATUS_MAP, letters_key(s("order_status")))
    payment = lookup(PAYMENT_METHOD_MAP, letters_key(s("payment_method")))
    df = (df.withColumn("_t_order_id", s("order_id"))
            .withColumn("_t_customer_id", s("customer_id"))
            .withColumn("_t_order_date", F.when(date_is_usable("order_date"), F.col("_d_order_date")))
            .withColumn("_t_order_status", status)
            .withColumn("_t_payment_method", payment))
    rules = key_rules("order_id") + key_rules("customer_id") + date_rules("order_date", INVALID, mandatory=True) + [
        Rule("order_status_missing", "order_status", INVALID, CAT_COMPLETENESS, s("order_status").isNull()),
        Rule("order_status_not_allowed", "order_status", INVALID, CAT_DOMAIN,
             s("order_status").isNotNull() & status.isNull()),
        Rule("order_status_normalized", "order_status", WARNING, CAT_FORMAT, changed("order_status", status)),
        Rule("payment_method_missing", "payment_method", WARNING, CAT_COMPLETENESS, s("payment_method").isNull()),
        Rule("payment_method_not_allowed", "payment_method", WARNING, CAT_DOMAIN,
             s("payment_method").isNotNull() & payment.isNull()),
        Rule("payment_method_normalized", "payment_method", WARNING, CAT_FORMAT, changed("payment_method", payment)),
    ]
    return df, rules


def standardize_order_items(df: DataFrame) -> tuple[DataFrame, list[Rule]]:
    df = add_decimal(add_int(df, "quantity"), "unit_price")
    sku_compact = F.regexp_replace(F.regexp_replace(F.upper(s("sku")), "[^A-Z0-9]", ""), "^SKU", "")
    sku_ok = sku_compact.rlike(r"^[A-Z]{6}[0-9]{4}$")
    sku_canonical = F.concat_ws("-", F.substring(sku_compact, 1, 3), F.substring(sku_compact, 4, 3),
                                F.substring(sku_compact, 7, 4))
    pid = s("product_id")
    sku_mismatch = F.coalesce(sku_ok & pid.rlike(ID_REGEX) & (F.length(pid) <= 4)
                              & (F.substring(sku_compact, 7, 4) != F.lpad(pid, 4, "0")), F.lit(False))
    qty = F.col("_i_quantity")

    df = (df.withColumn("_t_order_item_id", s("order_item_id"))
            .withColumn("_t_order_id", s("order_id"))
            .withColumn("_t_product_id", pid)
            .withColumn("_t_sku", F.when(sku_ok & ~sku_mismatch, sku_canonical))
            .withColumn("_t_quantity", F.when((qty >= 1) & (qty <= MAX_QUANTITY), qty))
            .withColumn("_t_unit_price", F.when(F.col("_n_unit_price") > 0, F.col("_n_unit_price"))))
    rules = key_rules("order_item_id") + key_rules("order_id") + key_rules("product_id") + [
        Rule("quantity_missing", "quantity", INVALID, CAT_COMPLETENESS, s("quantity").isNull()),
        Rule("quantity_not_integer", "quantity", INVALID, CAT_TYPE, F.col("_f_quantity") == "unparseable"),
        Rule("quantity_not_positive", "quantity", INVALID, CAT_BUSINESS, qty < 1),
        Rule("quantity_above_maximum", "quantity", INVALID, CAT_BUSINESS, qty > MAX_QUANTITY),
    ] + amount_rules("unit_price", mandatory=True) + [
        Rule("sku_missing", "sku", WARNING, CAT_COMPLETENESS, s("sku").isNull()),
        Rule("sku_invalid", "sku", WARNING, CAT_DOMAIN, s("sku").isNotNull() & ~sku_ok),
        Rule("sku_normalized", "sku", WARNING, CAT_FORMAT, sku_ok & ~sku_mismatch & changed("sku", sku_canonical)),
        Rule("sku_product_mismatch", "sku", WARNING, CAT_BUSINESS, sku_mismatch),
    ]
    return df, rules


def standardize_returns(df: DataFrame) -> tuple[DataFrame, list[Rule]]:
    df = add_decimal(add_date(df, "return_date"), "refund_amount")
    df = (df.withColumn("_t_return_id", s("return_id"))
            .withColumn("_t_order_item_id", s("order_item_id"))
            .withColumn("_t_return_date", F.when(date_is_usable("return_date"), F.col("_d_return_date")))
            .withColumn("_t_refund_amount", F.when(F.col("_n_refund_amount") > 0, F.col("_n_refund_amount"))))
    rules = (key_rules("return_id") + key_rules("order_item_id")
             + date_rules("return_date", INVALID, mandatory=True) + amount_rules("refund_amount", mandatory=True))
    return df, rules


STANDARDIZERS = {
    "categories": standardize_categories, "products": standardize_products, "customers": standardize_customers,
    "orders": standardize_orders, "order_items": standardize_order_items, "returns": standardize_returns,
}

TRACE_COLUMNS = ["_raw", RESCUED_COLUMN, "_source_file", "_ingested_at", "_row_hash", "_content_hash", "_sequence"]
QUALITY_COLUMNS = ["quality_status", "is_valid", "quality_issues", "quality_issue_summary", "validation_timestamp"]


def tag_feed(feed: str) -> DataFrame:
    """Bronze stream -> cleaned, typed and quality-tagged stream (helper columns removed)."""
    df = add_clean_strings(read_bronze(feed), feed)
    df, rules = STANDARDIZERS[feed](df)
    df = apply_rules(df, rules + schema_rules())
    # Ordering used by AUTO CDC when one key has several versions (see section 6).
    df = df.withColumn("_sequence", F.struct("_ingested_at", "_source_file", "_content_hash"))
    typed = [F.col(f"_t_{c}").alias(c) for c in COLUMNS[feed]]
    return df.select(*typed, *QUALITY_COLUMNS, "error_category", *TRACE_COLUMNS)


# =====================================================================================================
# 6. DUPLICATE AND UPDATE HANDLING
#
#  Layer 1 - exact re-delivery: dropDuplicatesWithinWatermark(_content_hash) in read_bronze().
#  Layer 2 - AUTO CDC keyed on the business PK (Silver) or on quarantine_id (quarantine):
#     * same PK + same payload again (after the window, or replayed after a failure) -> upsert of
#       identical values: no new row.
#     * same PK + different payload -> treated as a new version of the entity. The version with the
#       highest _sequence = (_ingested_at, _source_file, _content_hash) wins, whatever the order in which
#       versions are processed, so out-of-order and replayed batches converge to the same result.
#       The source has no business "updated_at", so arrival order is the only available version order.
#     * SCD type 2 (retail.silver.scd_type=2) keeps every accepted version with __START_AT / __END_AT.
#       History is tracked on business + quality columns only, so re-delivering an identical payload
#       never creates a new version.
#  Every delivery, including superseded ones, remains in append-only Bronze; dq_bronze_duplicate_keys
#  reports keys that arrived more than once and whether the payloads differ.
# =====================================================================================================

def silver_history_columns(feed: str) -> list[str]:
    return COLUMNS[feed] + ["quality_status", "quality_issues", "_content_hash"]


def to_quarantine(df: DataFrame, feed: str) -> DataFrame:
    pk = PRIMARY_KEYS[feed]
    return df.select(
        F.lit(feed).alias("feed"),
        # Same payload of the same key -> same quarantine_id -> one row, however often it is re-delivered.
        # A new payload for that key is a different quarantine row (a different version).
        F.sha2(F.concat_ws("||", F.lit(feed), F.coalesce(F.col(pk), F.lit("<null>")), F.col("_content_hash")),
               256).alias("quarantine_id"),
        F.col(pk).alias("record_pk"),
        "quality_status", "is_valid", "quality_issues", "quality_issue_summary", "error_category",
        F.lit("QUARANTINED").alias("processing_status"),
        F.col("validation_timestamp").alias("quarantined_at"),
        *TRACE_COLUMNS,
    )


# =====================================================================================================
# 7. REFERENTIAL INTEGRITY HANDLING
#
#  FK checks are NOT done inside the streaming flows. A stream-static join would judge a child against
#  whatever parents happen to exist at that micro-batch and could never revisit the decision; a
#  stream-stream join would need unbounded state because parents can arrive at any time.
#  Instead, structurally valid children are accepted into Silver, and `referential_integrity_exceptions`
#  (a materialized view, recomputed on every pipeline update) compares the current Silver children with the
#  current Silver parents and the parent quarantine. When a late parent arrives, the next update no longer
#  lists the child - no manual reprocessing is needed. Consumers (Gold) must anti-join this view, or map
#  unresolved keys to an unknown member, before using child rows.
# =====================================================================================================

def current_silver(feed: str) -> DataFrame:
    df = spark.read.table(silver_table(feed))
    return df.filter(F.col("__END_AT").isNull()) if SCD_TYPE == 2 else df


def referential_exceptions() -> DataFrame:
    grace_cutoff = F.current_timestamp() - F.expr(f"INTERVAL {FK_GRACE_MINUTES} MINUTES")
    parts = []
    for child, fk, parent in FOREIGN_KEYS:
        children = current_silver(child).select(
            F.lit(child).alias("feed"), F.col(PRIMARY_KEYS[child]).alias("record_pk"),
            F.lit(fk).alias("fk_column"), F.col(fk).alias("fk_value"), F.lit(parent).alias("parent_feed"),
            "_ingested_at", "_source_file", "_row_hash")
        parent_keys = current_silver(parent).select(F.col(PRIMARY_KEYS[parent]).alias("fk_value"))
        quarantined_parents = (spark.read.table(silver_table(f"quarantine_{parent}"))
                               .filter(F.col("record_pk").isNotNull())
                               .select(F.col("record_pk").alias("fk_value")).distinct()
                               .withColumn("_parent_quarantined", F.lit(True)))
        unresolved = (children.join(parent_keys, "fk_value", "left_anti")
                              .join(quarantined_parents, "fk_value", "left"))
        parts.append(unresolved.withColumn(
            "ri_status",
            F.when(F.col("_parent_quarantined"), F.lit(RI_PARENT_QUARANTINED))
             .when(F.col("_ingested_at") < grace_cutoff, F.lit(RI_ORPHANED))
             .otherwise(F.lit(RI_PENDING))).drop("_parent_quarantined"))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.withColumn("evaluated_at", F.current_timestamp())


# =====================================================================================================
# 8. SILVER STREAMING TABLE DEFINITIONS
# 9. QUARANTINE TABLE DEFINITIONS
#    (defined together per feed so the accepted/rejected split of one stream stays in one place)
# =====================================================================================================

def register_feed(feed: str) -> None:
    pk = PRIMARY_KEYS[feed]
    accepted_view, rejected_view = f"silver_{feed}_accepted", f"silver_{feed}_rejected"

    # Guard expectations: they only fail if this code routes a row to the wrong side (a programming error),
    # which must stop the update instead of writing bad data. The WARNING expectation is metrics-only and
    # shows the share of accepted rows with warnings in the pipeline event log.
    @dlt.view(name=accepted_view, comment=f"{feed}: VALID and WARNING rows from Bronze, cleaned and typed")
    @dlt.expect_all_or_fail({"accepted_rows_are_not_invalid": "quality_status IN ('VALID', 'WARNING')"})
    @dlt.expect_all({"accepted_row_has_no_warning": "quality_status = 'VALID'"})
    def accepted() -> DataFrame:
        return tag_feed(feed).filter(F.col("quality_status") != INVALID).drop("error_category")

    @dlt.view(name=rejected_view, comment=f"{feed}: INVALID rows from Bronze with all rejection reasons")
    @dlt.expect_all_or_fail({"rejected_rows_are_invalid": "quality_status = 'INVALID'"})
    def rejected() -> DataFrame:
        return to_quarantine(tag_feed(feed).filter(F.col("quality_status") == INVALID), feed)

    dlt.create_streaming_table(
        name=silver_table(feed),
        comment=(f"Silver {feed}: one row per {pk} (latest accepted version"
                 f"{'' if SCD_TYPE == 1 else ', SCD2 history'}); quality_status VALID or WARNING."),
        table_properties={"quality": "silver"},
    )
    cdc_kwargs = {"track_history_column_list": silver_history_columns(feed)} if SCD_TYPE == 2 else {}
    _auto_cdc_flow(
        target=silver_table(feed),
        source=accepted_view,
        keys=[pk],
        sequence_by=F.col("_sequence"),
        stored_as_scd_type=SCD_TYPE,
        **cdc_kwargs,
    )

    dlt.create_streaming_table(
        name=silver_table(f"quarantine_{feed}"),
        comment=f"Quarantine for {feed}: one row per distinct invalid payload, with all failure reasons.",
        table_properties={"quality": "quarantine"},
    )
    _auto_cdc_flow(
        target=silver_table(f"quarantine_{feed}"),
        source=rejected_view,
        keys=["quarantine_id"],
        sequence_by=F.col("_sequence"),
        stored_as_scd_type=1,
    )


for _feed in COLUMNS:
    register_feed(_feed)


@dlt.table(
    name=silver_table("referential_integrity_exceptions"),
    comment=("Child rows in Silver whose parent key is not (yet) in Silver. Recomputed every update: rows "
             "disappear automatically when the parent arrives. ri_status: UNRESOLVED_PENDING (inside grace "
             "period), ORPHANED (parent still missing after grace period), PARENT_QUARANTINED."),
    table_properties={"quality": "silver"},
)
def referential_integrity_exceptions() -> DataFrame:
    return referential_exceptions()


@dlt.table(
    name=silver_table("quarantine_all"),
    comment=("All rejected records: row-level quarantine of the six feeds plus ORPHANED / PARENT_QUARANTINED "
             "foreign keys. resolution_status shows whether a newer accepted version has superseded it."),
    table_properties={"quality": "quarantine"},
)
def quarantine_all() -> DataFrame:
    parts = []
    for feed, pk in PRIMARY_KEYS.items():
        accepted = current_silver(feed).select(F.col(pk).alias("record_pk"),
                                               F.col("_ingested_at").alias("_accepted_ingested_at"))
        q = spark.read.table(silver_table(f"quarantine_{feed}"))
        parts.append(
            q.join(accepted, "record_pk", "left").select(
                "feed", "quarantine_id", "record_pk", F.lit(None).cast("string").alias("fk_column"),
                F.lit(None).cast("string").alias("fk_value"), "error_category", "quality_issues",
                "quality_issue_summary", F.to_json("_raw").alias("raw_record"), "_source_file", "_ingested_at",
                "_row_hash", "quarantined_at",
                F.when(F.col("_accepted_ingested_at") > F.col("_ingested_at"), F.lit("SUPERSEDED_BY_ACCEPTED_VERSION"))
                 .when(F.col("_accepted_ingested_at").isNotNull(), F.lit("OLDER_ACCEPTED_VERSION_IN_SILVER"))
                 .otherwise(F.lit("OPEN")).alias("resolution_status")))

    ri = (spark.read.table(silver_table("referential_integrity_exceptions"))
          .filter(F.col("ri_status").isin(RI_ORPHANED, RI_PARENT_QUARANTINED)))
    rule = F.when(F.col("ri_status") == RI_ORPHANED, F.lit("fk_orphaned")).otherwise(F.lit("fk_parent_quarantined"))
    parts.append(ri.select(
        "feed",
        F.sha2(F.concat_ws("||", "feed", "record_pk", "fk_column", "fk_value"), 256).alias("quarantine_id"),
        "record_pk", "fk_column", "fk_value", F.lit(CAT_RI).alias("error_category"),
        F.array(F.struct(rule.alias("rule"), F.col("fk_column").alias("column"), F.lit(INVALID).alias("severity"),
                         F.lit(CAT_RI).alias("category"))).alias("quality_issues"),
        F.concat(F.lit("INVALID:"), rule, F.lit(" -> "), F.col("parent_feed"), F.lit("."), F.col("fk_column"),
                 F.lit("="), F.col("fk_value")).alias("quality_issue_summary"),
        F.lit(None).cast("string").alias("raw_record"), "_source_file", "_ingested_at", "_row_hash",
        F.col("evaluated_at").alias("quarantined_at"), F.lit("OPEN").alias("resolution_status")))

    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


# =====================================================================================================
# 10. MONITORING AND VALIDATION
#     Pipeline-level metrics (throughput, backlog, expectation results, failures, retries) come from the
#     pipeline event log - see silver_monitoring.sql. The views below give data-level counts.
# =====================================================================================================

@dlt.table(
    name=silver_table("dq_status_summary"),
    comment="Record counts per feed and outcome: Silver VALID/WARNING, quarantined INVALID, RI statuses.",
)
def dq_status_summary() -> DataFrame:
    parts = []
    for feed in COLUMNS:
        parts.append(current_silver(feed).groupBy(F.col("quality_status").alias("outcome")).count()
                     .select(F.lit(feed).alias("feed"), F.lit("silver").alias("layer"), "outcome", "count"))
        parts.append(spark.read.table(silver_table(f"quarantine_{feed}")).groupBy().count()
                     .select(F.lit(feed).alias("feed"), F.lit("quarantine").alias("layer"),
                             F.lit(INVALID).alias("outcome"), "count"))
    parts.append(spark.read.table(silver_table("referential_integrity_exceptions"))
                 .groupBy("feed", F.col("ri_status").alias("outcome")).count()
                 .select("feed", F.lit("referential_integrity").alias("layer"), "outcome", "count"))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.withColumnRenamed("count", "records").withColumn("measured_at", F.current_timestamp())


@dlt.table(
    name=silver_table("dq_rule_summary"),
    comment="How many current Silver and quarantine rows break each rule (all reasons, not only the first).",
)
def dq_rule_summary() -> DataFrame:
    parts = []
    for feed in COLUMNS:
        for layer, df in [("silver", current_silver(feed)),
                          ("quarantine", spark.read.table(silver_table(f"quarantine_{feed}")))]:
            parts.append(df.select(F.lit(feed).alias("feed"), F.lit(layer).alias("layer"),
                                   F.explode("quality_issues").alias("issue")))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return (out.groupBy("feed", "layer", F.col("issue.rule").alias("rule"), F.col("issue.column").alias("column"),
                        F.col("issue.severity").alias("severity"), F.col("issue.category").alias("category"))
               .count().withColumnRenamed("count", "records"))


if ENABLE_BRONZE_DUPLICATE_AUDIT:
    @dlt.table(
        name=silver_table("dq_bronze_duplicate_keys"),
        comment=("Primary keys delivered more than once to Bronze. EXACT_REDELIVERY = identical payloads; "
                 "CONFLICTING_VERSIONS = same key, different payloads (Silver keeps the latest accepted one)."),
    )
    def dq_bronze_duplicate_keys() -> DataFrame:
        parts = []
        for feed, pk in PRIMARY_KEYS.items():
            b = spark.read.table(bronze_table(feed))
            unmapped = _validate_bronze_schema(feed, b.columns)
            if RESCUED_COLUMN not in b.columns:
                b = b.withColumn(RESCUED_COLUMN, F.lit(None).cast("string"))
            b = b.select(clean_string(F.col(pk)).alias("record_pk"),
                         F.sha2(F.to_json(raw_payload(feed, unmapped)), 256).alias("_content_hash"),
                         "_source_file", F.col("_ingested_at").cast("timestamp").alias("_ingested_at"))
            parts.append(
                b.filter(F.col("record_pk").isNotNull()).groupBy("record_pk")
                 .agg(F.count(F.lit(1)).alias("deliveries"),
                      F.countDistinct("_content_hash").alias("distinct_payloads"),
                      F.countDistinct("_source_file").alias("source_files"),
                      F.min("_ingested_at").alias("first_ingested_at"),
                      F.max("_ingested_at").alias("last_ingested_at"))
                 .filter(F.col("deliveries") > 1)
                 .withColumn("feed", F.lit(feed)))
        out = parts[0]
        for p in parts[1:]:
            out = out.unionByName(p)
        return out.withColumn(
            "duplicate_type",
            F.when(F.col("distinct_payloads") == 1, F.lit("EXACT_REDELIVERY")).otherwise(F.lit("CONFLICTING_VERSIONS")))
