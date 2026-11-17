"""
Gold layer - Retail KPI medallion pipeline
(Databricks Delta Live Tables / Lakeflow Declarative Pipelines, Python `dlt` API - same API as silver.py)

Input  : {CAT}.silver.<feed>  (current accepted state, VALID + WARNING rows, AUTO CDC targets)
         {CAT}.silver.referential_integrity_exceptions   (only to label excluded rows)
Output : {CAT}.gold.*  - every object is a materialized view (MV), see GOLD_SCHEMA_DOCUMENTATION.md

Why materialized views and not streaming tables:
  The Silver tables are AUTO CDC targets, i.e. they receive updates (late versions, corrections). A streaming
  read of such a table either fails on the update commits or (with skipChangeCommits) silently ignores them.
  MVs read the current Silver state on every update, so late orders, late returns, late parents and corrected
  records always land in the right period. On serverless pipelines MVs are refreshed incrementally when the
  query allows it; all Gold queries are deterministic (no current_timestamp / random) to keep that possible.

Pipeline configuration keys (in addition to the Silver keys, which are reused):
  retail.catalog                     required (shared with Bronze/Silver)
  retail.silver.schema               default "silver"  (shared)
  retail.silver.scd_type             1 or 2            (shared; Gold reads the current version when 2)
  retail.gold.schema                 default "gold"
  retail.gold.date_dim_start         default = retail.silver.min_valid_date (2000-01-01)
  retail.gold.date_dim_end           default 2035-12-31
  retail.gold.digital_payment_methods   default "UPI,Net Banking"   (comma separated, Silver canonical values)
  retail.gold.return_window_days     default 30   (returns later than this are flagged, not removed)
  retail.gold.cohort_matrix_months   default 24   (Month 0 .. Month N columns in the retention matrix)
"""

from __future__ import annotations

from functools import reduce

import dlt
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# =====================================================================================================
# 1. CONFIGURATION
# =====================================================================================================

spark = SparkSession.getActiveSession()


def _conf(key: str, default: str) -> str:
    return spark.conf.get(key, default)


CAT = spark.conf.get("retail.catalog")
SILVER_SCHEMA = _conf("retail.silver.schema", "silver")
GOLD_SCHEMA = _conf("retail.gold.schema", "gold")
SCD_TYPE = int(_conf("retail.silver.scd_type", "1"))
DATE_DIM_START = _conf("retail.gold.date_dim_start", _conf("retail.silver.min_valid_date", "2000-01-01"))
DATE_DIM_END = _conf("retail.gold.date_dim_end", "2035-12-31")
DIGITAL_PAYMENT_METHODS = [m.strip() for m in _conf("retail.gold.digital_payment_methods", "UPI,Net Banking").split(",")
                           if m.strip()]
RETURN_WINDOW_DAYS = int(_conf("retail.gold.return_window_days", "30"))
COHORT_MATRIX_MONTHS = int(_conf("retail.gold.cohort_matrix_months", "24"))
TOP_N = 5

UNKNOWN_KEY = -1
UNKNOWN_LABEL = "UNKNOWN"
MONEY = "decimal(18,2)"        # line-level amounts (max line value ~310k)
MONEY_TOTAL = "decimal(28,2)"  # aggregated amounts

# Canonical payment methods produced by Silver (silver.PAYMENT_METHOD_MAP values). Silver turns every other
# value into NULL (WARNING payment_method_not_allowed / _missing), which Gold maps to the UNKNOWN member.
# Keys are fixed so they never change between refreshes.
PAYMENT_METHODS = [
    (1, "Cash on Delivery", "CASH_ON_DELIVERY"),
    (2, "Net Banking", "NET_BANKING"),
    (3, "UPI", "UPI"),
    (4, "Loyalty Points", "LOYALTY_POINTS_REDEMPTION"),
]
# Canonical order statuses produced by Silver (silver.ORDER_STATUS_MAP values; order_status is mandatory there).
# counts_toward_sales follows the project architecture: "fact_sales ... excludes Canceled".
ORDER_STATUSES = [
    # key, order_status, status_category, counts_toward_sales, is_delivered
    (1, "Delivered", "FULFILLED", True, True),
    (2, "In Transit", "IN_PROGRESS", True, False),
    (3, "Canceled", "CANCELED", False, False),
]
# Documented state -> region mapping (data/README.md and conf/state_region.csv; identical to silver.STATE_REGION).
STATE_REGION = {
    "Haryana": "NORTH", "Himachal Pradesh": "NORTH", "Punjab": "NORTH", "Rajasthan": "NORTH",
    "Uttar Pradesh": "NORTH", "Uttarakhand": "NORTH",
    "Andhra Pradesh": "SOUTH", "Karnataka": "SOUTH", "Kerala": "SOUTH", "Tamil Nadu": "SOUTH", "Telangana": "SOUTH",
    "Arunachal Pradesh": "EAST", "Assam": "EAST", "Bihar": "EAST", "Chhattisgarh": "EAST", "Jharkhand": "EAST",
    "Manipur": "EAST", "Meghalaya": "EAST", "Mizoram": "EAST", "Nagaland": "EAST", "Odisha": "EAST",
    "Sikkim": "EAST", "Tripura": "EAST", "West Bengal": "EAST",
    "Goa": "WEST", "Gujarat": "WEST", "Maharashtra": "WEST", "Madhya Pradesh": "WEST",
}

if SCD_TYPE not in (1, 2):
    raise ValueError(f"retail.silver.scd_type must be 1 or 2, got {SCD_TYPE}")
_unknown_digital = sorted(set(DIGITAL_PAYMENT_METHODS) - {m for _, m, _ in PAYMENT_METHODS})
if _unknown_digital:
    raise ValueError(f"retail.gold.digital_payment_methods contains values Silver never produces: {_unknown_digital}")


# =====================================================================================================
# 2. SHARED HELPERS
# =====================================================================================================

def silver(feed: str) -> DataFrame:
    """Current accepted Silver state. With SCD2 only the open version (__END_AT IS NULL) is used: Silver's
    versions are sequenced by arrival, not by business-effective dates, so a point-in-time join on
    order_date would be wrong (see GOLD_SCHEMA_DOCUMENTATION.md, SCD handling)."""
    df = spark.read.table(f"{CAT}.{SILVER_SCHEMA}.{feed}")
    return df.filter(F.col("__END_AT").isNull()) if SCD_TYPE == 2 else df


def gold_name(name: str) -> str:
    return f"{CAT}.{GOLD_SCHEMA}.{name}"


def gold(name: str) -> DataFrame:
    return spark.read.table(gold_name(name))


def surrogate_key(entity: str, business_key: str) -> Column:
    """Deterministic, persistent surrogate key: 64-bit hash of entity name + business key. Identical across
    refreshes and full refreshes; collisions are checked in GOLD_VALIDATION.sql (never -1)."""
    return F.xxhash64(F.lit(entity), F.col(business_key))


def date_key(c: Column) -> Column:
    """YYYYMMDD integer."""
    return (F.year(c) * 10000 + F.month(c) * 100 + F.dayofmonth(c)).cast("int")


def numeric_id(col: str) -> Column:
    """Numeric value of an all-digit business key, for deterministic tie-breaking (NULL if not numeric)."""
    return F.expr(f"try_cast(`{col}` AS BIGINT)")


def safe_pct(num: Column, den: Column) -> Column:
    """num / den * 100, rounded to 2 decimals; NULL when the denominator is NULL or 0 (never divides by 0)."""
    return F.when(den.isNotNull() & (den != 0), F.round(num.cast("double") * 100.0 / den.cast("double"), 2))


def safe_div_money(num: Column, den: Column) -> Column:
    return F.when(den.isNotNull() & (den != 0), F.round(num / den, 2).cast(MONEY_TOTAL))


def lookup(mapping: dict, key: Column) -> Column:
    pairs = [F.lit(x) for kv in mapping.items() for x in kv]
    return F.try_element_at(F.create_map(*pairs), key)


def union_all(dfs: list[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


def zero_if_null(*cols: str) -> dict:
    return {c: 0 for c in cols}


def quarter_label(d: Column) -> Column:
    return F.concat(F.year(d).cast("string"), F.lit("-Q"), F.quarter(d).cast("string"))


# =====================================================================================================
# 3. DIMENSIONS
# =====================================================================================================

@dlt.table(
    name=gold_name("dim_date"),
    comment="Calendar dimension, one row per day (date_key = YYYYMMDD) plus Unknown member -1.",
    table_properties={"quality": "gold"},
)
def dim_date() -> DataFrame:
    days = spark.range(1).select(F.explode(F.sequence(F.to_date(F.lit(DATE_DIM_START)),
                                                      F.to_date(F.lit(DATE_DIM_END)),
                                                      F.expr("INTERVAL 1 DAY"))).alias("full_date"))
    d = F.col("full_date")
    iso_dow = ((F.dayofweek(d) + 5) % 7) + 1          # 1 = Monday ... 7 = Sunday
    known = days.select(
        date_key(d).alias("date_key"),
        d.alias("full_date"),
        F.dayofmonth(d).alias("day"),
        F.date_format(d, "EEEE").alias("day_name"),
        iso_dow.alias("day_of_week"),
        F.weekofyear(d).alias("week"),
        F.month(d).alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.date_format(d, "MMM yyyy").alias("month_year"),
        F.quarter(d).alias("quarter"),
        F.concat(F.lit("Q"), F.quarter(d).cast("string")).alias("quarter_name"),
        F.year(d).alias("year"),
        quarter_label(d).alias("year_quarter"),
        F.date_format(d, "yyyy-MM").alias("year_month"),
        iso_dow.isin(6, 7).alias("is_weekend"),
        F.trunc(d, "month").alias("month_start"),
        F.trunc(d, "quarter").alias("quarter_start"),
        (F.year(d) * 100 + F.month(d)).cast("int").alias("month_key"),
        (F.year(d) * 10 + F.quarter(d)).cast("int").alias("quarter_key"),
    )
    unknown = spark.range(1).select(
        F.lit(UNKNOWN_KEY).cast("int").alias("date_key"), F.lit(None).cast("date").alias("full_date"),
        F.lit(None).cast("int").alias("day"), F.lit(UNKNOWN_LABEL).alias("day_name"),
        F.lit(None).cast("int").alias("day_of_week"), F.lit(None).cast("int").alias("week"),
        F.lit(None).cast("int").alias("month"), F.lit(UNKNOWN_LABEL).alias("month_name"),
        F.lit(UNKNOWN_LABEL).alias("month_year"), F.lit(None).cast("int").alias("quarter"),
        F.lit(UNKNOWN_LABEL).alias("quarter_name"), F.lit(None).cast("int").alias("year"),
        F.lit(UNKNOWN_LABEL).alias("year_quarter"), F.lit(UNKNOWN_LABEL).alias("year_month"),
        F.lit(None).cast("boolean").alias("is_weekend"), F.lit(None).cast("date").alias("month_start"),
        F.lit(None).cast("date").alias("quarter_start"), F.lit(None).cast("int").alias("month_key"),
        F.lit(None).cast("int").alias("quarter_key"))
    return known.unionByName(unknown)


@dlt.table(
    name=gold_name("dim_customer"),
    comment=("Customer dimension (type 1, current Silver state). customer_key = xxhash64('customer', customer_id); "
             "-1 = Unknown member. region_reporting = state->region mapping, else source region, else UNKNOWN."),
    table_properties={"quality": "gold"},
)
@dlt.expect_all_or_fail({"customer_id_present": "customer_id IS NOT NULL",
                         "customer_key_not_reserved": "customer_key <> -1 OR customer_id = 'UNKNOWN'"})
def dim_customer() -> DataFrame:
    c = silver("customers")
    region_from_state = lookup(STATE_REGION, F.col("state"))
    known = c.select(
        surrogate_key("customer", "customer_id").alias("customer_key"),
        "customer_id",
        F.col("name").alias("customer_name"),
        "email", "phone_number", "signup_date",
        F.coalesce(date_key(F.col("signup_date")), F.lit(UNKNOWN_KEY)).alias("signup_date_key"),
        "city", "state",
        F.col("region").alias("region_source"),
        F.coalesce(region_from_state, F.col("region"), F.lit(UNKNOWN_LABEL)).alias("region"),
        F.when(region_from_state.isNotNull(), F.lit("STATE_MAPPING"))
         .when(F.col("region").isNotNull(), F.lit("SOURCE_REGION"))
         .otherwise(F.lit(UNKNOWN_LABEL)).alias("region_derivation"),
        F.col("quality_status").alias("source_quality_status"),
        F.col("quality_issue_summary").alias("source_quality_issues"),
        F.col("_row_hash").alias("source_row_hash"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("source_ingested_at"),
    )
    unknown = spark.range(1).select(
        F.lit(UNKNOWN_KEY).cast("bigint").alias("customer_key"), F.lit(UNKNOWN_LABEL).alias("customer_id"),
        F.lit("Unknown customer").alias("customer_name"), F.lit(UNKNOWN_KEY).alias("signup_date_key"),
        F.lit(UNKNOWN_LABEL).alias("region"), F.lit(UNKNOWN_LABEL).alias("region_derivation"))
    return known.unionByName(unknown, allowMissingColumns=True)


@dlt.table(
    name=gold_name("dim_product"),
    comment=("Product dimension (type 1) = Silver products + categories. product_key = xxhash64('product', "
             "product_id); -1 = Unknown member. sku = most frequent canonical SKU observed for the product in "
             "Silver order_items (products carry no SKU). current_price / cost_price are CURRENT values only."),
    table_properties={"quality": "gold"},
)
@dlt.expect_all_or_fail({"product_id_present": "product_id IS NOT NULL"})
@dlt.expect_all({"category_resolved": "is_category_resolved OR product_id = 'UNKNOWN'",
                 "single_sku_per_product": "sku_distinct_count <= 1 OR sku_distinct_count IS NULL"})
def dim_product() -> DataFrame:
    p = silver("products")
    cats = silver("categories").select("category_id", "category_name")
    sku_counts = silver("order_items").filter(F.col("sku").isNotNull()).groupBy("product_id", "sku").count()
    sku_mode = (sku_counts
                .withColumn("_rn", F.row_number().over(
                    Window.partitionBy("product_id").orderBy(F.desc("count"), F.asc("sku"))))
                .withColumn("sku_distinct_count", F.count(F.lit(1)).over(Window.partitionBy("product_id")))
                .filter(F.col("_rn") == 1)
                .select("product_id", "sku", "sku_distinct_count"))
    known = (p.join(cats, "category_id", "left").join(sku_mode, "product_id", "left").select(
        surrogate_key("product", "product_id").alias("product_key"),
        "product_id", "product_name", "sku", "sku_distinct_count", "category_id",
        F.coalesce(F.col("category_name"), F.lit("Unknown category")).alias("category_name"),
        F.col("category_name").isNotNull().alias("is_category_resolved"),
        F.col("price").alias("current_price"),
        "cost_price",
        F.col("quality_status").alias("source_quality_status"),
        F.col("_row_hash").alias("source_row_hash"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("source_ingested_at"),
    ))
    unknown = spark.range(1).select(
        F.lit(UNKNOWN_KEY).cast("bigint").alias("product_key"), F.lit(UNKNOWN_LABEL).alias("product_id"),
        F.lit("Unknown product").alias("product_name"), F.lit(UNKNOWN_LABEL).alias("category_id"),
        F.lit("Unknown category").alias("category_name"), F.lit(False).alias("is_category_resolved"))
    return known.unionByName(unknown, allowMissingColumns=True)


@dlt.table(
    name=gold_name("dim_payment_method"),
    comment=("Payment methods produced by Silver plus Unknown (-1, Silver NULL). payment_category: DIGITAL if the "
             "method is in retail.gold.digital_payment_methods, NON_DIGITAL otherwise, UNKNOWN for -1."),
    table_properties={"quality": "gold"},
)
def dim_payment_method() -> DataFrame:
    rows = [(k, m, t, "DIGITAL" if m in DIGITAL_PAYMENT_METHODS else "NON_DIGITAL", m in DIGITAL_PAYMENT_METHODS)
            for k, m, t in PAYMENT_METHODS]
    rows.append((UNKNOWN_KEY, None, UNKNOWN_LABEL, UNKNOWN_LABEL, None))
    return spark.createDataFrame(rows, "payment_method_key BIGINT, payment_method STRING, payment_type STRING, "
                                       "payment_category STRING, is_digital_payment BOOLEAN")


@dlt.table(
    name=gold_name("dim_order_status"),
    comment=("Order statuses produced by Silver plus Unknown (-1). status_category is a 1:1 relabel of the three "
             "documented values; counts_toward_sales excludes Canceled (project architecture)."),
    table_properties={"quality": "gold"},
)
def dim_order_status() -> DataFrame:
    rows = list(ORDER_STATUSES) + [(UNKNOWN_KEY, None, UNKNOWN_LABEL, False, False)]
    return spark.createDataFrame(rows, "order_status_key BIGINT, order_status STRING, status_category STRING, "
                                       "counts_toward_sales BOOLEAN, is_delivered BOOLEAN")


# =====================================================================================================
# 4. FACTS
# =====================================================================================================

def order_header() -> DataFrame:
    """One row per Silver order with all dimension keys resolved (Unknown member -1 when unresolved).
    Shared by fact_sales and fact_orders so both use identical key logic."""
    o = silver("orders").select(
        "order_id", "customer_id", "order_date", "order_status", "payment_method",
        F.col("quality_status").alias("order_quality_status"),
        F.col("_row_hash").alias("order_row_hash"),
        F.col("_source_file").alias("order_source_file"),
        F.col("_ingested_at").alias("order_ingested_at"))
    cust = gold("dim_customer").filter(F.col("customer_key") != UNKNOWN_KEY).select(
        "customer_id", "customer_key", "signup_date")
    pay = gold("dim_payment_method").filter(F.col("payment_method_key") != UNKNOWN_KEY).select(
        "payment_method", "payment_method_key")
    status = gold("dim_order_status").filter(F.col("order_status_key") != UNKNOWN_KEY).select(
        "order_status", "order_status_key", "counts_toward_sales", "is_delivered")
    h = (o.join(cust, "customer_id", "left")
          .join(pay, "payment_method", "left")
          .join(status, "order_status", "left"))
    return h.select(
        "order_id", "customer_id",
        F.coalesce(F.col("customer_key"), F.lit(UNKNOWN_KEY).cast("bigint")).alias("customer_key"),
        "order_date",
        date_key(F.col("order_date")).alias("order_date_key"),
        F.coalesce(F.col("payment_method_key"), F.lit(UNKNOWN_KEY).cast("bigint")).alias("payment_method_key"),
        F.coalesce(F.col("order_status_key"), F.lit(UNKNOWN_KEY).cast("bigint")).alias("order_status_key"),
        F.coalesce(F.col("counts_toward_sales"), F.lit(False)).alias("is_revenue_eligible"),
        F.coalesce(F.col("is_delivered"), F.lit(False)).alias("is_delivered"),
        F.col("customer_key").isNotNull().alias("is_customer_resolved"),
        F.coalesce(F.col("order_date") < F.col("signup_date"), F.lit(False)).alias("is_order_before_signup"),
        "order_quality_status", "order_row_hash", "order_source_file", "order_ingested_at")


@dlt.table(
    name=gold_name("fact_sales"),
    comment=("Grain: one row per order_item_id (Silver order_items whose order exists in Silver). gross_sales = "
             "quantity * unit_price (transaction price). total_cost uses the CURRENT product cost_price; NULL when "
             "the product is unresolved. All statuses are kept; KPIs filter is_revenue_eligible (not Canceled)."),
    table_properties={"quality": "gold"},
)
@dlt.expect_all_or_fail({"grain_key_present": "order_item_id IS NOT NULL AND order_id IS NOT NULL",
                         "positive_quantity": "quantity >= 1",
                         "positive_unit_price": "unit_price > 0",
                         "order_date_known": "order_date_key IS NOT NULL"})
@dlt.expect_all({"product_resolved": "product_key <> -1",
                 "customer_resolved": "customer_key <> -1",
                 "unit_price_equals_list_price": "NOT unit_price_differs_from_list_price"})
def fact_sales() -> DataFrame:
    items = silver("order_items").select(
        "order_item_id", "order_id", "product_id", "sku", "quantity", "unit_price",
        F.col("quality_status").alias("item_quality_status"),
        F.col("_row_hash").alias("item_row_hash"),
        F.col("_source_file").alias("item_source_file"),
        F.col("_ingested_at").alias("item_ingested_at"))
    prod = gold("dim_product").filter(F.col("product_key") != UNKNOWN_KEY).select(
        "product_id", "product_key", F.col("cost_price").alias("unit_cost"), F.col("current_price").alias("list_price"))
    f = items.join(order_header(), "order_id", "inner").join(prod, "product_id", "left")

    gross = (F.col("quantity") * F.col("unit_price")).cast(MONEY)
    cost = F.when(F.col("unit_cost").isNotNull(), (F.col("quantity") * F.col("unit_cost")).cast(MONEY))
    return f.select(
        "order_item_id", "order_id", "customer_id", "product_id",
        "customer_key",
        F.coalesce(F.col("product_key"), F.lit(UNKNOWN_KEY).cast("bigint")).alias("product_key"),
        "order_date", "order_date_key", "payment_method_key", "order_status_key",
        "sku", "quantity", "unit_price",
        gross.alias("gross_sales"),
        "unit_cost",
        cost.alias("total_cost"),
        (gross - cost).cast(MONEY).alias("gross_profit"),
        "is_revenue_eligible", "is_delivered", "is_customer_resolved",
        F.col("product_key").isNotNull().alias("is_product_resolved"),
        F.col("unit_cost").isNotNull().alias("has_unit_cost"),
        F.col("list_price").alias("current_list_price"),
        F.coalesce(F.col("unit_price") != F.col("list_price"), F.lit(False)).alias("unit_price_differs_from_list_price"),
        "is_order_before_signup",
        "item_quality_status", "order_quality_status",
        "item_row_hash", "item_source_file", "item_ingested_at",
        "order_row_hash", "order_source_file", "order_ingested_at")


@dlt.table(
    name=gold_name("fact_orders"),
    comment=("Supporting order-header fact. Grain: one row per Silver order_id, including orders without items. "
             "Measures are aggregated from fact_sales. Needed to count each order exactly once (AOV, payment mix, "
             "orders by status, cohorts)."),
    table_properties={"quality": "gold"},
)
@dlt.expect_all({"order_has_items": "has_items"})
def fact_orders() -> DataFrame:
    lines = gold("fact_sales").groupBy("order_id").agg(
        F.count(F.lit(1)).alias("order_item_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("gross_sales").alias("gross_sales"),
        F.sum("total_cost").alias("_sum_cost"),
        F.sum(F.when(F.col("total_cost").isNull(), 1).otherwise(0)).alias("items_without_cost"))
    o = order_header().join(lines, "order_id", "left")
    has_items = F.coalesce(F.col("order_item_count"), F.lit(0)) > 0
    total_cost = F.when(has_items & (F.col("items_without_cost") == 0), F.col("_sum_cost"))
    return o.select(
        "order_id", "customer_id", "customer_key", "order_date", "order_date_key", "payment_method_key",
        "order_status_key",
        F.coalesce(F.col("order_item_count"), F.lit(0)).cast("bigint").alias("order_item_count"),
        F.coalesce(F.col("total_quantity"), F.lit(0)).cast("bigint").alias("total_quantity"),
        F.coalesce(F.col("gross_sales"), F.lit(0)).cast(MONEY_TOTAL).alias("gross_sales"),
        total_cost.cast(MONEY_TOTAL).alias("total_cost"),
        (F.coalesce(F.col("gross_sales"), F.lit(0)) - total_cost).cast(MONEY_TOTAL).alias("gross_profit"),
        F.coalesce(F.col("items_without_cost"), F.lit(0)).cast("bigint").alias("items_without_cost"),
        has_items.alias("has_items"),
        "is_revenue_eligible",
        (F.col("is_revenue_eligible") & has_items).alias("is_sales_order"),
        "is_delivered", "is_customer_resolved", "is_order_before_signup",
        "order_quality_status", "order_row_hash", "order_source_file", "order_ingested_at")


@dlt.table(
    name=gold_name("fact_returns"),
    comment=("Grain: one row per return_id whose order_item_id exists in fact_sales. No returned quantity exists "
             "in the source: measures are return count and refund_amount. Cross-feed anomalies are flagged, not "
             "removed (see dq_exceptions)."),
    table_properties={"quality": "gold"},
)
@dlt.expect_all_or_fail({"grain_key_present": "return_id IS NOT NULL AND order_item_id IS NOT NULL",
                         "positive_refund": "refund_amount > 0"})
@dlt.expect_all({"return_not_before_order": "NOT is_return_before_order",
                 "refund_within_item_value": "NOT is_refund_above_item_value",
                 "return_on_delivered_order": "is_order_delivered"})
def fact_returns() -> DataFrame:
    r = silver("returns").select(
        "return_id", "order_item_id", "return_date", "refund_amount",
        F.col("quality_status").alias("return_quality_status"),
        F.col("_row_hash").alias("return_row_hash"),
        F.col("_source_file").alias("return_source_file"),
        F.col("_ingested_at").alias("return_ingested_at"))
    sales = gold("fact_sales").select(
        "order_item_id", "order_id", "customer_key", "product_key", "order_date", "order_date_key",
        F.col("quantity").alias("item_quantity"), F.col("gross_sales").alias("item_gross_sales"),
        F.col("is_revenue_eligible").alias("is_sale_revenue_eligible"),
        F.col("is_delivered").alias("is_order_delivered"))
    f = r.join(sales, "order_item_id", "inner")
    days = F.datediff(F.col("return_date"), F.col("order_date"))
    seq = F.row_number().over(Window.partitionBy("order_item_id")
                              .orderBy("return_date", numeric_id("return_id"), "return_id"))
    return f.select(
        "return_id", "order_item_id", "order_id", "customer_key", "product_key",
        date_key(F.col("return_date")).alias("return_date_key"),
        F.col("order_date_key").alias("original_order_date_key"),
        "return_date", "order_date", "refund_amount", "item_quantity", "item_gross_sales",
        days.alias("days_to_return"),
        (days < 0).alias("is_return_before_order"),
        (days > RETURN_WINDOW_DAYS).alias("is_outside_return_window"),
        (F.col("refund_amount") > F.col("item_gross_sales")).alias("is_refund_above_item_value"),
        "is_order_delivered", "is_sale_revenue_eligible",
        seq.alias("return_sequence_for_item"),
        (seq > 1).alias("is_repeat_return_for_item"),
        "return_quality_status", "return_row_hash", "return_source_file", "return_ingested_at")


# =====================================================================================================
# 5. DATA QUALITY: EXCLUSIONS, CROSS-FEED EXCEPTIONS AND RECONCILIATION
# =====================================================================================================

def _ri_status(feed: str, fk_column: str) -> DataFrame:
    return (spark.read.table(f"{CAT}.{SILVER_SCHEMA}.referential_integrity_exceptions")
            .filter((F.col("feed") == feed) & (F.col("fk_column") == fk_column))
            .select(F.col("record_pk").alias("record_key"), "ri_status").dropDuplicates(["record_key"]))


def _flag(df: DataFrame, source_table: str, key: str, rule: str, cond: Column, amount: Column,
          detail: Column, lineage: tuple[str, str]) -> DataFrame:
    return df.filter(cond).select(
        F.lit(source_table).alias("source_table"), F.col(key).alias("record_key"), F.lit(rule).alias("rule"),
        F.lit("INCLUDED_WITH_FLAG").alias("treatment"), detail.alias("detail"),
        amount.cast(MONEY_TOTAL).alias("amount"), F.lit(None).cast("string").alias("ri_status"),
        F.col(lineage[0]).alias("source_row_hash"), F.col(lineage[1]).alias("source_file"))


@dlt.table(
    name=gold_name("dq_exceptions"),
    comment=("Every Silver row excluded from a Gold fact (treatment EXCLUDED_FROM_FACT, with the Silver RI status) "
             "and every included row with a cross-feed anomaly (INCLUDED_WITH_FLAG). Nothing is dropped silently."),
    table_properties={"quality": "gold"},
)
def dq_exceptions() -> DataFrame:
    fs, fo, fr = gold("fact_sales"), gold("fact_orders"), gold("fact_returns")

    items = silver("order_items")
    excluded_items = (items.join(fs.select("order_item_id"), "order_item_id", "left_anti")
                      .withColumnRenamed("order_item_id", "record_key")
                      .join(_ri_status("order_items", "order_id"), "record_key", "left")
                      .select(F.lit("order_items").alias("source_table"), "record_key",
                              F.lit("ORDER_NOT_RESOLVED").alias("rule"),
                              F.lit("EXCLUDED_FROM_FACT").alias("treatment"),
                              F.concat(F.lit("order_id="), F.col("order_id")).alias("detail"),
                              (F.col("quantity") * F.col("unit_price")).cast(MONEY_TOTAL).alias("amount"),
                              F.coalesce(F.col("ri_status"), F.lit("NOT_LISTED")).alias("ri_status"),
                              F.col("_row_hash").alias("source_row_hash"), F.col("_source_file").alias("source_file")))

    item_ids = items.select("order_item_id", F.lit(True).alias("_item_in_silver"))
    excluded_returns = (silver("returns").join(fr.select("return_id"), "return_id", "left_anti")
                        .join(item_ids, "order_item_id", "left")
                        .withColumnRenamed("return_id", "record_key")
                        .join(_ri_status("returns", "order_item_id"), "record_key", "left")
                        .select(F.lit("returns").alias("source_table"), "record_key",
                                F.when(F.col("_item_in_silver"), F.lit("ORDER_ITEM_EXCLUDED_FROM_FACT_SALES"))
                                 .otherwise(F.lit("ORDER_ITEM_NOT_RESOLVED")).alias("rule"),
                                F.lit("EXCLUDED_FROM_FACT").alias("treatment"),
                                F.concat(F.lit("order_item_id="), F.col("order_item_id")).alias("detail"),
                                F.col("refund_amount").cast(MONEY_TOTAL).alias("amount"),
                                F.coalesce(F.col("ri_status"), F.lit("NOT_LISTED")).alias("ri_status"),
                                F.col("_row_hash").alias("source_row_hash"),
                                F.col("_source_file").alias("source_file")))

    s_lin, o_lin, r_lin = ("item_row_hash", "item_source_file"), ("order_row_hash", "order_source_file"), \
        ("return_row_hash", "return_source_file")
    flagged = [
        _flag(fs, "fact_sales", "order_item_id", "PRODUCT_NOT_RESOLVED", ~F.col("is_product_resolved"),
              F.col("gross_sales"), F.concat(F.lit("product_id="), F.col("product_id")), s_lin),
        _flag(fs, "fact_sales", "order_item_id", "UNIT_PRICE_DIFFERS_FROM_LIST_PRICE",
              F.col("unit_price_differs_from_list_price"), F.col("gross_sales"),
              F.concat(F.lit("unit_price="), F.col("unit_price").cast("string"), F.lit(" list_price="),
                       F.col("current_list_price").cast("string")), s_lin),
        _flag(fo, "fact_orders", "order_id", "CUSTOMER_NOT_RESOLVED", ~F.col("is_customer_resolved"),
              F.col("gross_sales"), F.concat(F.lit("customer_id="), F.col("customer_id")), o_lin),
        _flag(fo, "fact_orders", "order_id", "ORDER_BEFORE_CUSTOMER_SIGNUP", F.col("is_order_before_signup"),
              F.col("gross_sales"), F.concat(F.lit("order_date="), F.col("order_date").cast("string")), o_lin),
        _flag(fo, "fact_orders", "order_id", "ORDER_WITHOUT_ITEMS", ~F.col("has_items"),
              F.lit(0), F.concat(F.lit("order_status_key="), F.col("order_status_key").cast("string")), o_lin),
        _flag(fr, "fact_returns", "return_id", "RETURN_BEFORE_ORDER", F.col("is_return_before_order"),
              F.col("refund_amount"), F.concat(F.lit("days_to_return="), F.col("days_to_return").cast("string")), r_lin),
        _flag(fr, "fact_returns", "return_id", "RETURN_OUTSIDE_RETURN_WINDOW", F.col("is_outside_return_window"),
              F.col("refund_amount"), F.concat(F.lit("days_to_return="), F.col("days_to_return").cast("string")), r_lin),
        _flag(fr, "fact_returns", "return_id", "REFUND_ABOVE_ITEM_VALUE", F.col("is_refund_above_item_value"),
              F.col("refund_amount"), F.concat(F.lit("item_gross_sales="), F.col("item_gross_sales").cast("string")),
              r_lin),
        _flag(fr, "fact_returns", "return_id", "RETURN_ON_NON_DELIVERED_ORDER", ~F.col("is_order_delivered"),
              F.col("refund_amount"), F.concat(F.lit("order_id="), F.col("order_id")), r_lin),
        _flag(fr, "fact_returns", "return_id", "REPEAT_RETURN_FOR_ORDER_ITEM", F.col("is_repeat_return_for_item"),
              F.col("refund_amount"), F.concat(F.lit("order_item_id="), F.col("order_item_id")), r_lin),
    ]
    return union_all([excluded_items, excluded_returns, *flagged])


def _scalar(df: DataFrame, expr: Column, name: str) -> DataFrame:
    return df.agg(expr.cast(MONEY_TOTAL).alias(name))


def _check(name: str, expected: DataFrame, actual: DataFrame, excluded: DataFrame | None = None) -> DataFrame:
    exc = excluded if excluded is not None else spark.range(1).select(F.lit(0).cast(MONEY_TOTAL).alias("excluded"))
    row = expected.crossJoin(actual).crossJoin(exc)
    diff = F.col("expected") - F.col("actual") - F.coalesce(F.col("excluded"), F.lit(0))
    return row.select(F.lit(name).alias("check_name"), "expected", "actual",
                      F.coalesce(F.col("excluded"), F.lit(0)).cast(MONEY_TOTAL).alias("excluded"),
                      diff.cast(MONEY_TOTAL).alias("difference"),
                      F.when(diff == 0, F.lit("PASS")).otherwise(F.lit("FAIL")).alias("status"))


@dlt.table(
    name=gold_name("dq_reconciliation"),
    comment=("Silver-to-Gold reconciliation: expected (Silver) = actual (Gold) + excluded (dq_exceptions). "
             "status PASS/FAIL per check. Also checks fact grains and KPI totals against facts."),
    table_properties={"quality": "gold"},
)
def dq_reconciliation() -> DataFrame:
    si, so, sr = silver("order_items"), silver("orders"), silver("returns")
    fs, fo, fr = gold("fact_sales"), gold("fact_orders"), gold("fact_returns")
    exc = gold("dq_exceptions").filter(F.col("treatment") == "EXCLUDED_FROM_FACT")
    exc_items, exc_returns = exc.filter(F.col("source_table") == "order_items"), exc.filter(F.col("source_table") == "returns")
    eligible = fs.filter("is_revenue_eligible")
    total_kpi = gold("kpi_sales_summary").filter(F.col("grain") == "TOTAL")

    def e(df, x):
        return _scalar(df, x, "expected")

    def a(df, x):
        return _scalar(df, x, "actual")

    def x(df, v):
        return _scalar(df, v, "excluded")

    return union_all([
        _check("order_items_rows", e(si, F.count(F.lit(1))), a(fs, F.count(F.lit(1))), x(exc_items, F.count(F.lit(1)))),
        _check("order_items_value", e(si, F.sum(F.col("quantity") * F.col("unit_price"))), a(fs, F.sum("gross_sales")),
               x(exc_items, F.sum("amount"))),
        _check("returns_rows", e(sr, F.count(F.lit(1))), a(fr, F.count(F.lit(1))), x(exc_returns, F.count(F.lit(1)))),
        _check("returns_refund_amount", e(sr, F.sum("refund_amount")), a(fr, F.sum("refund_amount")),
               x(exc_returns, F.sum("amount"))),
        _check("orders_rows", e(so, F.count(F.lit(1))), a(fo, F.count(F.lit(1)))),
        _check("customers_rows", e(silver("customers"), F.count(F.lit(1))),
               a(gold("dim_customer").filter(F.col("customer_key") != UNKNOWN_KEY), F.count(F.lit(1)))),
        _check("products_rows", e(silver("products"), F.count(F.lit(1))),
               a(gold("dim_product").filter(F.col("product_key") != UNKNOWN_KEY), F.count(F.lit(1)))),
        _check("fact_sales_grain_unique", e(fs, F.count(F.lit(1))), a(fs, F.countDistinct("order_item_id"))),
        _check("fact_returns_grain_unique", e(fr, F.count(F.lit(1))), a(fr, F.countDistinct("return_id"))),
        _check("fact_orders_grain_unique", e(fo, F.count(F.lit(1))), a(fo, F.countDistinct("order_id"))),
        _check("fact_orders_sales_equal_fact_sales", e(fs, F.sum("gross_sales")), a(fo, F.sum("gross_sales"))),
        _check("kpi_total_sales_equal_fact_sales", e(eligible, F.sum("gross_sales")), a(total_kpi, F.sum("gross_sales"))),
    ])


# =====================================================================================================
# 6. REUSABLE KPI BUILDING BLOCKS
# =====================================================================================================

def eligible_sales() -> DataFrame:
    """fact_sales rows that count toward sales (not Canceled), with reporting region and category."""
    cust = gold("dim_customer").select("customer_key", "region")
    prod = gold("dim_product").select("product_key", "category_id", "category_name")
    return (gold("fact_sales").filter(F.col("is_revenue_eligible"))
            .join(cust, "customer_key", "left").join(prod, "product_key", "left"))


def sales_orders() -> DataFrame:
    """fact_orders rows that are sales orders (not Canceled, at least one item), with region."""
    cust = gold("dim_customer").select("customer_key", "region")
    return gold("fact_orders").filter(F.col("is_sales_order")).join(cust, "customer_key", "left")


def returns_measures(group_cols: list[str], returns: DataFrame) -> DataFrame:
    """Return measures. returned_order_items counts distinct items; returned_eligible_items is the return-rate
    numerator (items whose sale is revenue-eligible, i.e. inside the denominator)."""
    return returns.groupBy(*group_cols).agg(
        F.count(F.lit(1)).alias("return_count"),
        F.countDistinct("order_item_id").alias("returned_order_items"),
        F.countDistinct(F.when(F.col("is_sale_revenue_eligible"), F.col("order_item_id"))).alias("returned_eligible_items"),
        F.sum("refund_amount").cast(MONEY_TOTAL).alias("refund_amount"))


def sales_measures(group_cols: list[str], sales: DataFrame) -> DataFrame:
    return sales.groupBy(*group_cols).agg(
        F.sum("gross_sales").cast(MONEY_TOTAL).alias("gross_sales"),
        F.sum("quantity").cast("bigint").alias("quantity_sold"),
        F.countDistinct("order_id").alias("distinct_orders"),
        F.count(F.lit(1)).alias("order_items"),
        F.sum("total_cost").cast(MONEY_TOTAL).alias("_cost_sum"),
        F.sum(F.when(F.col("total_cost").isNull(), 1).otherwise(0)).alias("items_without_cost"))


def with_profit(df: DataFrame) -> DataFrame:
    """Cost/profit/margin only when every contributing line has a cost: missing cost is never treated as 0."""
    cost = F.when(F.col("order_items") == 0, F.lit(0).cast(MONEY_TOTAL)) \
            .when(F.col("items_without_cost") == 0, F.col("_cost_sum"))
    return (df.withColumn("total_cost", cost.cast(MONEY_TOTAL))
              .withColumn("gross_profit", (F.col("gross_sales") - F.col("total_cost")).cast(MONEY_TOTAL))
              .withColumn("gross_margin_pct", safe_pct(F.col("gross_profit"), F.col("gross_sales")))
              .drop("_cost_sum"))


def rank_desc(metric: str, tie_col: str, partition: list[str] | None = None) -> Column:
    w = Window.partitionBy(*(partition or [])).orderBy(F.desc_nulls_last(metric), numeric_id(tie_col).asc_nulls_last(),
                                                        F.asc(tie_col))
    return F.row_number().over(w)


# =====================================================================================================
# 7. STANDARD RETAIL KPIs
# =====================================================================================================

def _grain_values(grain: str, d: Column, region: Column) -> tuple[Column, Column]:
    """(grain_value, period_start) for a date column; labels match dim_date formats."""
    if grain == "TOTAL":
        return F.lit("ALL"), F.lit(None).cast("date")
    if grain == "DAY":
        return F.date_format(d, "yyyy-MM-dd"), d
    if grain == "MONTH":
        return F.date_format(d, "yyyy-MM"), F.trunc(d, "month")
    if grain == "QUARTER":
        return quarter_label(d), F.trunc(d, "quarter")
    if grain == "YEAR":
        return F.year(d).cast("string"), F.trunc(d, "year")
    if grain == "REGION":
        return region, F.lit(None).cast("date")
    raise ValueError(grain)


@dlt.table(
    name=gold_name("kpi_sales_summary"),
    comment=("Sales KPIs at grains TOTAL, DAY, MONTH, QUARTER, YEAR, REGION (grain + grain_value). Revenue-eligible "
             "sales only. AOV = gross_sales / distinct_orders; ASP = gross_sales / quantity_sold; repeat_customers = "
             ">1 distinct order inside the grain; first_time_buyers = customers whose first-ever order is in the grain."),
    table_properties={"quality": "gold"},
)
def kpi_sales_summary() -> DataFrame:
    base = eligible_sales()
    known = base.filter(F.col("customer_key") != UNKNOWN_KEY)
    firsts = known.groupBy("customer_key", "region").agg(F.min("order_date").alias("first_order_date"))
    out = []
    for grain in ["TOTAL", "DAY", "MONTH", "QUARTER", "YEAR", "REGION"]:
        gv, ps = _grain_values(grain, F.col("order_date"), F.col("region"))
        b = base.withColumn("grain_value", gv).withColumn("period_start", ps)
        m = b.groupBy("grain_value", "period_start").agg(
            F.sum("gross_sales").cast(MONEY_TOTAL).alias("gross_sales"),
            F.sum("quantity").cast("bigint").alias("quantity_sold"),
            F.countDistinct("order_id").alias("distinct_orders"),
            F.count(F.lit(1)).alias("order_items"),
            F.countDistinct(F.when(F.col("customer_key") != UNKNOWN_KEY, F.col("customer_key"))).alias("active_customers"))
        rep = (b.filter(F.col("customer_key") != UNKNOWN_KEY)
                .groupBy("grain_value", "customer_key").agg(F.countDistinct("order_id").alias("n"))
                .groupBy("grain_value").agg(F.sum(F.when(F.col("n") > 1, 1).otherwise(0)).alias("repeat_customers")))
        fgv, _ = _grain_values(grain, F.col("first_order_date"), F.col("region"))
        new = firsts.withColumn("grain_value", fgv).groupBy("grain_value").agg(F.count(F.lit(1)).alias("first_time_buyers"))
        out.append(m.join(rep, "grain_value", "left").join(new, "grain_value", "left")
                   .withColumn("grain", F.lit(grain)))
    df = union_all(out).fillna(zero_if_null("repeat_customers", "first_time_buyers"))
    return df.select(
        "grain", "grain_value", "period_start", "gross_sales", "quantity_sold", "distinct_orders", "order_items",
        "active_customers", "repeat_customers", "first_time_buyers",
        safe_div_money(F.col("gross_sales"), F.col("distinct_orders")).alias("average_order_value"),
        safe_div_money(F.col("gross_sales"), F.col("quantity_sold")).alias("average_selling_price"))


@dlt.table(
    name=gold_name("kpi_product_performance"),
    comment=("One row per product (all products, zero-sales included; Unknown product only if it has activity). "
             "Revenue/quantity/profit/margin from revenue-eligible sales; returns from fact_returns. "
             "Ranks: 1 = highest (lowest_selling_rank: 1 = fewest units)."),
    table_properties={"quality": "gold"},
)
def kpi_product_performance() -> DataFrame:
    sales = with_profit(sales_measures(["product_key"], eligible_sales()))
    rets = returns_measures(["product_key"], gold("fact_returns"))
    prod = gold("dim_product").select("product_key", "product_id", "product_name", "sku", "category_id",
                                      "category_name", "current_price", "cost_price")
    df = (prod.join(sales, "product_key", "left").join(rets, "product_key", "left")
              .fillna(zero_if_null("quantity_sold", "distinct_orders", "order_items", "items_without_cost",
                                   "return_count", "returned_order_items", "returned_eligible_items"))
              .withColumn("gross_sales", F.coalesce(F.col("gross_sales"), F.lit(0).cast(MONEY_TOTAL)))
              .withColumn("refund_amount", F.coalesce(F.col("refund_amount"), F.lit(0).cast(MONEY_TOTAL)))
              .withColumn("total_cost", F.when(F.col("order_items") == 0, F.lit(0).cast(MONEY_TOTAL))
                                         .otherwise(F.col("total_cost")))
              .withColumn("gross_profit", (F.col("gross_sales") - F.col("total_cost")).cast(MONEY_TOTAL))
              .filter((F.col("product_key") != UNKNOWN_KEY) | (F.col("order_items") > 0) | (F.col("return_count") > 0)))
    known = F.col("product_key") != UNKNOWN_KEY
    part = ["_is_known"]
    df = df.withColumn("_is_known", known)
    low = Window.partitionBy("_is_known").orderBy(F.asc("quantity_sold"), F.asc("gross_sales"),
                                                  numeric_id("product_id").asc_nulls_last(), F.asc("product_id"))
    return df.select(
        "product_key", "product_id", "product_name", "sku", "category_id", "category_name", "current_price",
        "cost_price", "gross_sales", "quantity_sold", "distinct_orders", "order_items", "total_cost",
        "gross_profit", "gross_margin_pct", "items_without_cost",
        "return_count", "returned_order_items", "refund_amount",
        safe_pct(F.col("returned_eligible_items"), F.col("order_items")).alias("return_rate_pct"),
        F.when(known, rank_desc("gross_sales", "product_id", part)).alias("revenue_rank"),
        F.when(known, rank_desc("quantity_sold", "product_id", part)).alias("best_selling_rank"),
        F.when(known, F.row_number().over(low)).alias("lowest_selling_rank"),
        F.when(known, rank_desc("gross_profit", "product_id", part)).alias("profit_rank"))


@dlt.table(
    name=gold_name("kpi_category_performance"),
    comment="One row per category (Unknown category included when it has activity): sales, profit, margin, returns.",
    table_properties={"quality": "gold"},
)
def kpi_category_performance() -> DataFrame:
    grp = ["category_id", "category_name"]
    sales = with_profit(sales_measures(grp, eligible_sales()))
    prod = gold("dim_product").select("product_key", "category_id", "category_name")
    rets = returns_measures(grp, gold("fact_returns").join(prod, "product_key", "left"))
    cats = prod.select(*grp).distinct()
    df = (cats.join(sales, grp, "left").join(rets, grp, "left")
              .fillna(zero_if_null("quantity_sold", "distinct_orders", "order_items", "items_without_cost",
                                   "return_count", "returned_order_items", "returned_eligible_items"))
              .withColumn("gross_sales", F.coalesce(F.col("gross_sales"), F.lit(0).cast(MONEY_TOTAL)))
              .withColumn("refund_amount", F.coalesce(F.col("refund_amount"), F.lit(0).cast(MONEY_TOTAL)))
              .filter((F.col("category_id") != UNKNOWN_LABEL) | (F.col("order_items") > 0)))
    total = Window.partitionBy()
    return df.select(
        *grp, "gross_sales",
        safe_pct(F.col("gross_sales"), F.sum("gross_sales").over(total)).alias("sales_share_pct"),
        "quantity_sold", "distinct_orders", "order_items", "total_cost", "gross_profit", "gross_margin_pct",
        "items_without_cost", "return_count", "returned_order_items", "refund_amount",
        safe_pct(F.col("returned_eligible_items"), F.col("order_items")).alias("return_rate_pct"),
        rank_desc("gross_sales", "category_id").alias("revenue_rank"))


@dlt.table(
    name=gold_name("kpi_customer_performance"),
    comment=("One row per known customer (customers without purchases included). purchase_frequency = distinct "
             "sales orders (lifetime); customer_segment NO_PURCHASE / ONE_TIME / REPEAT (>1 distinct order)."),
    table_properties={"quality": "gold"},
)
def kpi_customer_performance() -> DataFrame:
    orders = sales_orders().groupBy("customer_key").agg(
        F.count(F.lit(1)).alias("distinct_orders"),
        F.sum("order_item_count").alias("order_items"),
        F.sum("total_quantity").alias("quantity_ordered"),
        F.sum("gross_sales").cast(MONEY_TOTAL).alias("gross_sales"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"))
    rets = returns_measures(["customer_key"], gold("fact_returns"))
    cust = gold("dim_customer").filter(F.col("customer_key") != UNKNOWN_KEY).select(
        "customer_key", "customer_id", "customer_name", "city", "state", "region", "signup_date")
    df = (cust.join(orders, "customer_key", "left").join(rets, "customer_key", "left")
              .fillna(zero_if_null("distinct_orders", "order_items", "quantity_ordered", "return_count",
                                   "returned_order_items", "returned_eligible_items"))
              .withColumn("gross_sales", F.coalesce(F.col("gross_sales"), F.lit(0).cast(MONEY_TOTAL)))
              .withColumn("refund_amount", F.coalesce(F.col("refund_amount"), F.lit(0).cast(MONEY_TOTAL))))
    n = F.col("distinct_orders")
    return df.select(
        "customer_key", "customer_id", "customer_name", "city", "state", "region", "signup_date",
        "first_order_date", "last_order_date", "distinct_orders",
        n.alias("purchase_frequency"),
        F.when(n > 1, F.round(F.datediff("last_order_date", "first_order_date") / (n - 1), 1))
         .alias("avg_days_between_orders"),
        F.when(n == 0, F.lit("NO_PURCHASE")).when(n == 1, F.lit("ONE_TIME")).otherwise(F.lit("REPEAT"))
         .alias("customer_segment"),
        "order_items", "quantity_ordered", "gross_sales",
        safe_div_money(F.col("gross_sales"), n).alias("average_order_value"),
        "return_count", "returned_order_items", "refund_amount",
        safe_pct(F.col("returned_eligible_items"), F.col("order_items")).alias("return_rate_pct"),
        rank_desc("gross_sales", "customer_id").alias("revenue_rank"))


@dlt.table(
    name=gold_name("kpi_order_performance"),
    comment=("Order KPIs, metric_group: ORDER_STATUS (all orders), PAYMENT_METHOD, MONTH, REGION (sales orders), "
             "ORDERS_PER_CUSTOMER (distribution of customers by number of sales orders). Each order counted once."),
    table_properties={"quality": "gold"},
)
def kpi_order_performance() -> DataFrame:
    status = gold("dim_order_status").select("order_status_key", F.coalesce("order_status", F.lit(UNKNOWN_LABEL)).alias("v"))
    pay = gold("dim_payment_method").select("payment_method_key",
                                            F.coalesce("payment_method", F.lit(UNKNOWN_LABEL)).alias("v"))
    all_orders = gold("fact_orders")
    so = sales_orders()

    def group(df: DataFrame, metric_group: str, scope: str, value: Column, sort: Column) -> DataFrame:
        g = df.withColumn("group_value", value).withColumn("sort_key", sort).groupBy("group_value", "sort_key").agg(
            F.count(F.lit(1)).alias("order_count"),
            F.sum("gross_sales").cast(MONEY_TOTAL).alias("gross_sales"))
        return g.select(F.lit(metric_group).alias("metric_group"), F.lit(scope).alias("order_scope"), "group_value",
                        "sort_key", "order_count", "gross_sales",
                        safe_pct(F.col("order_count"), F.sum("order_count").over(Window.partitionBy())).alias("pct_share"),
                        safe_div_money(F.col("gross_sales"), F.col("order_count")).alias("average_order_value"),
                        F.lit(None).cast("bigint").alias("customer_count"))

    by_status = group(all_orders.join(status, "order_status_key", "left"), "ORDER_STATUS", "ALL_ORDERS",
                      F.col("v"), F.col("order_status_key").cast("string"))
    by_payment = group(so.join(pay, "payment_method_key", "left"), "PAYMENT_METHOD", "SALES_ORDERS",
                       F.col("v"), F.col("payment_method_key").cast("string"))
    by_month = group(so, "MONTH", "SALES_ORDERS", F.date_format("order_date", "yyyy-MM"),
                     F.date_format("order_date", "yyyy-MM"))
    by_region = group(so, "REGION", "SALES_ORDERS", F.col("region"), F.col("region"))

    per_customer = (so.filter(F.col("customer_key") != UNKNOWN_KEY).groupBy("customer_key")
                    .agg(F.count(F.lit(1)).alias("n")))
    bucket = (F.when(F.col("n") <= 5, F.col("n").cast("string"))
               .when(F.col("n") <= 10, F.lit("6-10")).otherwise(F.lit("11+")))
    bucket_sort = F.lpad(F.when(F.col("n") <= 5, F.col("n")).when(F.col("n") <= 10, F.lit(6)).otherwise(F.lit(11))
                         .cast("string"), 2, "0")
    dist = (per_customer.withColumn("group_value", bucket).withColumn("sort_key", bucket_sort)
            .groupBy("group_value", "sort_key").agg(F.count(F.lit(1)).alias("customer_count"),
                                                     F.sum("n").alias("order_count"))
            .select(F.lit("ORDERS_PER_CUSTOMER").alias("metric_group"), F.lit("SALES_ORDERS").alias("order_scope"),
                    "group_value", "sort_key", "order_count", F.lit(None).cast(MONEY_TOTAL).alias("gross_sales"),
                    safe_pct(F.col("customer_count"), F.sum("customer_count").over(Window.partitionBy()))
                    .alias("pct_share"),
                    F.lit(None).cast(MONEY_TOTAL).alias("average_order_value"), "customer_count"))
    return union_all([by_status, by_payment, by_month, by_region, dist])


@dlt.table(
    name=gold_name("kpi_returns_summary"),
    comment=("Return KPIs at grains TOTAL, MONTH (return_date), PRODUCT, CATEGORY, CUSTOMER. Measures: return records, "
             "distinct returned order items, refund amount. Returned quantity is not available in the source."),
    table_properties={"quality": "gold"},
)
def kpi_returns_summary() -> DataFrame:
    prod = gold("dim_product").select("product_key", "product_id", "product_name", "category_id", "category_name")
    cust = gold("dim_customer").select("customer_key", "customer_id", "customer_name")
    r = gold("fact_returns").join(prod, "product_key", "left").join(cust, "customer_key", "left")
    flagged = (F.col("is_return_before_order") | F.col("is_outside_return_window") | F.col("is_refund_above_item_value")
               | ~F.col("is_order_delivered") | F.col("is_repeat_return_for_item"))
    r = r.withColumn("_flagged", flagged)
    grains = [("TOTAL", F.lit("ALL"), F.lit("All returns")),
              ("MONTH", F.date_format("return_date", "yyyy-MM"), F.date_format("return_date", "MMM yyyy")),
              ("PRODUCT", F.col("product_id"), F.col("product_name")),
              ("CATEGORY", F.col("category_id"), F.col("category_name")),
              ("CUSTOMER", F.col("customer_id"), F.col("customer_name"))]
    out = []
    for grain, value, label in grains:
        out.append(r.withColumn("grain_value", value).withColumn("grain_label", label)
                    .groupBy("grain_value", "grain_label").agg(
                        F.count(F.lit(1)).alias("return_count"),
                        F.countDistinct("order_item_id").alias("returned_order_items"),
                        F.sum("refund_amount").cast(MONEY_TOTAL).alias("refund_amount"),
                        F.sum(F.when(F.col("_flagged"), 1).otherwise(0)).alias("flagged_returns"))
                    .withColumn("grain", F.lit(grain)))
    return union_all(out).select(
        "grain", "grain_value", "grain_label", "return_count", "returned_order_items", "refund_amount",
        safe_div_money(F.col("refund_amount"), F.col("return_count")).alias("average_refund_amount"),
        "flagged_returns")


# =====================================================================================================
# 8. CUSTOM KPIs
# =====================================================================================================

def _top_n(df: DataFrame, metrics: list[tuple[str, str]], id_col: str, keep: list[str]) -> DataFrame:
    """Long top-N: one block per ranking metric; rank by metric desc, then numeric id asc (deterministic).
    Only entities with a positive metric value are ranked."""
    parts = []
    for name, col in metrics:
        parts.append(df.filter(F.col(col) > 0).select(
            F.lit(name).alias("ranking_metric"), F.col(col).cast("double").alias("metric_value"), *keep))
    stacked = union_all(parts)
    w = Window.partitionBy("ranking_metric").orderBy(F.desc("metric_value"), numeric_id(id_col).asc_nulls_last(),
                                                     F.asc(id_col))
    return stacked.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= TOP_N)


@dlt.table(
    name=gold_name("kpi_top5_customers_orders_returns"),
    comment=("Custom KPI 1 (customers): top 5 customers per ranking_metric (DISTINCT_ORDERS, ORDER_ITEMS, "
             "QUANTITY_ORDERED, RETURN_COUNT, REFUND_AMOUNT) with all order and return measures side by side. "
             "return_rate_pct = returned order items / ordered order items * 100 (revenue-eligible items)."),
    table_properties={"quality": "gold"},
)
def kpi_top5_customers_orders_returns() -> DataFrame:
    c = gold("kpi_customer_performance")
    keep = ["customer_key", "customer_id", "customer_name", "region", "distinct_orders", "order_items",
            "quantity_ordered", "gross_sales", "return_count", "returned_order_items", "refund_amount",
            "return_rate_pct",
            safe_pct(F.col("refund_amount"), F.col("gross_sales")).alias("refund_to_sales_pct")]
    return _top_n(c, [("DISTINCT_ORDERS", "distinct_orders"), ("ORDER_ITEMS", "order_items"),
                      ("QUANTITY_ORDERED", "quantity_ordered"), ("RETURN_COUNT", "return_count"),
                      ("REFUND_AMOUNT", "refund_amount")], "customer_id", keep)


@dlt.table(
    name=gold_name("kpi_top5_products_orders_returns"),
    comment=("Custom KPI 1 (products): top 5 known products per ranking_metric (QUANTITY_ORDERED, DISTINCT_ORDERS, "
             "RETURN_COUNT, REFUND_AMOUNT) with order and return measures side by side."),
    table_properties={"quality": "gold"},
)
def kpi_top5_products_orders_returns() -> DataFrame:
    p = gold("kpi_product_performance").filter(F.col("product_key") != UNKNOWN_KEY)
    keep = ["product_key", "product_id", "product_name", "category_name", "quantity_sold", "distinct_orders",
            "order_items", "gross_sales", "return_count", "returned_order_items", "refund_amount", "return_rate_pct",
            safe_pct(F.col("refund_amount"), F.col("gross_sales")).alias("refund_to_sales_pct")]
    return _top_n(p, [("QUANTITY_ORDERED", "quantity_sold"), ("DISTINCT_ORDERS", "distinct_orders"),
                      ("RETURN_COUNT", "return_count"), ("REFUND_AMOUNT", "refund_amount")], "product_id", keep)


def _growth_status(prev: Column) -> Column:
    return (F.when(prev.isNull(), F.lit("NO_PREVIOUS_QUARTER"))
             .when(prev == 0, F.lit("PREVIOUS_QUARTER_ZERO")).otherwise(F.lit("OK")))


@dlt.table(
    name=gold_name("kpi_region_customer_growth_qoq"),
    comment=("Custom KPI 2: per region (incl. UNKNOWN and an ALL roll-up) and signup quarter - new customers, previous "
             "quarter, QoQ new-customer growth %, cumulative customer base at quarter end and its QoQ growth %, "
             "regional ranks. Acquisition = signup_date; customers without signup_date are reported separately."),
    table_properties={"quality": "gold"},
)
def kpi_region_customer_growth_qoq() -> DataFrame:
    c = gold("dim_customer").filter(F.col("customer_key") != UNKNOWN_KEY).select("customer_key", "region", "signup_date")
    c = c.unionByName(c.withColumn("region", F.lit("ALL")))
    dated = c.filter(F.col("signup_date").isNotNull()).withColumn("quarter_start", F.trunc("signup_date", "quarter"))

    region_totals = c.groupBy("region").agg(
        F.countDistinct("customer_key").alias("total_customers_in_region"),
        F.countDistinct(F.when(F.col("signup_date").isNull(), F.col("customer_key"))).alias("customers_without_signup_date"))
    bounds = dated.agg(F.min("quarter_start").alias("q_min"), F.max("signup_date").alias("as_of_date"))
    quarters = (bounds.select(F.explode(F.sequence("q_min", F.trunc("as_of_date", "quarter"),
                                                   F.expr("INTERVAL 3 MONTHS"))).alias("quarter_start"), "as_of_date"))
    spine = c.select("region").distinct().crossJoin(quarters)          # regions x quarters: small by construction
    new = dated.groupBy("region", "quarter_start").agg(F.countDistinct("customer_key").alias("new_customers"))
    df = spine.join(new, ["region", "quarter_start"], "left").fillna(zero_if_null("new_customers"))

    w = Window.partitionBy("region").orderBy("quarter_start")
    df = (df.withColumn("previous_quarter_new_customers", F.lag("new_customers").over(w))
            .withColumn("cumulative_customer_base", F.sum("new_customers").over(w.rowsBetween(Window.unboundedPreceding, 0)))
            .withColumn("previous_quarter_customer_base", F.lag("cumulative_customer_base").over(w)))
    df = (df.withColumn("new_customer_growth_qoq_pct",
                        safe_pct(F.col("new_customers") - F.col("previous_quarter_new_customers"),
                                 F.col("previous_quarter_new_customers")))
            .withColumn("new_customer_growth_status", _growth_status(F.col("previous_quarter_new_customers")))
            .withColumn("customer_base_growth_qoq_pct",
                        safe_pct(F.col("cumulative_customer_base") - F.col("previous_quarter_customer_base"),
                                 F.col("previous_quarter_customer_base")))
            .withColumn("customer_base_growth_status", _growth_status(F.col("previous_quarter_customer_base"))))
    regional = F.col("region") != "ALL"
    rank_w = Window.partitionBy("quarter_start", F.col("region") == "ALL")
    return (df.join(region_totals, "region", "left").select(
        "region", "quarter_start", quarter_label(F.col("quarter_start")).alias("year_quarter"),
        "new_customers", "previous_quarter_new_customers", "new_customer_growth_qoq_pct", "new_customer_growth_status",
        "cumulative_customer_base", "previous_quarter_customer_base", "customer_base_growth_qoq_pct",
        "customer_base_growth_status", "total_customers_in_region", "customers_without_signup_date",
        F.when(regional, F.row_number().over(rank_w.orderBy(F.desc("new_customers"), F.asc("region"))))
         .alias("region_rank_new_customers"),
        F.when(regional, F.row_number().over(rank_w.orderBy(F.desc_nulls_last("new_customer_growth_qoq_pct"),
                                                             F.asc("region"))))
         .alias("region_rank_new_customer_growth"),
        (F.add_months(F.col("quarter_start"), 3) > F.date_add(F.col("as_of_date"), 1)).alias("is_partial_quarter")))


@dlt.table(
    name=gold_name("kpi_product_sales_margin"),
    comment=("Custom KPI 3: product sales vs margin. COGS = SUM(quantity * current cost_price); margin % = gross profit "
             "/ gross sales * 100. Ranks by sales, profit and margin; sales_margin_segment splits products at the "
             "median sales and median margin of products with sales and known cost."),
    table_properties={"quality": "gold"},
)
def kpi_product_sales_margin() -> DataFrame:
    p = gold("kpi_product_performance").filter(F.col("product_key") != UNKNOWN_KEY).select(
        "product_key", "product_id", "product_name", "category_name", "current_price", "cost_price",
        F.col("quantity_sold").alias("units_sold"), "distinct_orders", "gross_sales",
        F.col("total_cost").alias("cost_of_goods_sold"), "gross_profit", "gross_margin_pct", "items_without_cost")
    rankable = (F.col("gross_sales") > 0) & F.col("gross_margin_pct").isNotNull()
    medians = p.filter(rankable).agg(F.expr("percentile(gross_sales, 0.5)").alias("median_sales"),
                                     F.expr("percentile(gross_margin_pct, 0.5)").alias("median_margin_pct"))
    df = p.crossJoin(medians).withColumn("_rankable", rankable)    # medians: single row
    part = ["_rankable"]
    segment = (F.when(F.col("gross_sales") <= 0, F.lit("NO_SALES"))
                .when(F.col("gross_margin_pct").isNull(), F.lit("COST_UNKNOWN"))
                .otherwise(F.concat(F.when(F.col("gross_sales") >= F.col("median_sales"), F.lit("HIGH_SALES"))
                                     .otherwise(F.lit("LOW_SALES")), F.lit("_"),
                                    F.when(F.col("gross_margin_pct") >= F.col("median_margin_pct"), F.lit("HIGH_MARGIN"))
                                     .otherwise(F.lit("LOW_MARGIN")))))
    return df.select(
        "product_key", "product_id", "product_name", "category_name", "current_price", "cost_price", "units_sold",
        "distinct_orders", "gross_sales", "cost_of_goods_sold", "gross_profit", "gross_margin_pct",
        "items_without_cost",
        F.when(F.col("_rankable"), rank_desc("gross_sales", "product_id", part)).alias("sales_rank"),
        F.when(F.col("_rankable"), rank_desc("gross_profit", "product_id", part)).alias("profit_rank"),
        F.when(F.col("_rankable"), rank_desc("gross_margin_pct", "product_id", part)).alias("margin_rank"),
        "median_sales", "median_margin_pct", segment.alias("sales_margin_segment"))


@dlt.table(
    name=gold_name("kpi_digital_payment_analysis"),
    comment=("Custom KPI 4: digital payment share for sales orders (not Canceled, with items), each order counted once "
             "(fact_orders; sales = SUM of its fact_sales lines). period_type ALL / QUARTER; breakdown_level TOTAL "
             "(digital % columns), CATEGORY (DIGITAL / NON_DIGITAL / UNKNOWN) and METHOD. Digital % denominators "
             "exclude UNKNOWN methods; *_incl_unknown variants include them."),
    table_properties={"quality": "gold"},
)
def kpi_digital_payment_analysis() -> DataFrame:
    pay = gold("dim_payment_method").select("payment_method_key", "payment_category",
                                            F.coalesce("payment_method", F.lit(UNKNOWN_LABEL)).alias("payment_method"))
    o = gold("fact_orders").filter(F.col("is_sales_order")).join(pay, "payment_method_key", "left")
    periods = [("ALL", F.lit("ALL")), ("QUARTER", quarter_label(F.col("order_date")))]
    out = []
    for ptype, pval in periods:
        b = o.withColumn("period_type", F.lit(ptype)).withColumn("period", pval)
        tot = Window.partitionBy("period_type", "period")
        for level, col in [("CATEGORY", "payment_category"), ("METHOD", "payment_method")]:
            g = (b.groupBy("period_type", "period", F.col(col).alias("breakdown_value"))
                  .agg(F.count(F.lit(1)).alias("order_count"), F.sum("gross_sales").cast(MONEY_TOTAL).alias("gross_sales")))
            out.append(g.withColumn("breakdown_level", F.lit(level))
                        .withColumn("pct_of_orders", safe_pct(F.col("order_count"), F.sum("order_count").over(tot)))
                        .withColumn("pct_of_sales", safe_pct(F.col("gross_sales"), F.sum("gross_sales").over(tot))))

        def cnt(cat):
            return F.sum(F.when(F.col("payment_category") == cat, 1).otherwise(0))

        def amt(cat):
            return F.sum(F.when(F.col("payment_category") == cat, F.col("gross_sales")).otherwise(0)).cast(MONEY_TOTAL)

        t = b.groupBy("period_type", "period").agg(
            F.count(F.lit(1)).alias("order_count"), F.sum("gross_sales").cast(MONEY_TOTAL).alias("gross_sales"),
            cnt("DIGITAL").alias("digital_orders"), cnt("NON_DIGITAL").alias("non_digital_orders"),
            cnt(UNKNOWN_LABEL).alias("unknown_payment_orders"),
            amt("DIGITAL").alias("digital_sales"), amt("NON_DIGITAL").alias("non_digital_sales"),
            amt(UNKNOWN_LABEL).alias("unknown_payment_sales"))
        known_orders = F.col("digital_orders") + F.col("non_digital_orders")
        known_sales = F.col("digital_sales") + F.col("non_digital_sales")
        out.append(t.select(
            "period_type", "period", F.lit("TOTAL").alias("breakdown_level"), F.lit("ALL").alias("breakdown_value"),
            "order_count", "gross_sales", "digital_orders", "non_digital_orders", "unknown_payment_orders",
            "digital_sales", "non_digital_sales", "unknown_payment_sales",
            known_orders.alias("eligible_orders"), known_sales.cast(MONEY_TOTAL).alias("eligible_sales"),
            safe_pct(F.col("digital_orders"), known_orders).alias("digital_pct_by_orders"),
            safe_pct(F.col("digital_sales"), known_sales).alias("digital_pct_by_sales"),
            safe_pct(F.col("digital_orders"), F.col("order_count")).alias("digital_pct_by_orders_incl_unknown"),
            safe_pct(F.col("digital_sales"), F.col("gross_sales")).alias("digital_pct_by_sales_incl_unknown")))
    return union_all(out)


@dlt.table(
    name=gold_name("kpi_sales_growth_qoq"),
    comment=("Custom KPI 5: quarterly sales by order_date for scope TOTAL, REGION and PRODUCT, with previous quarter, "
             "absolute change and QoQ growth % (LAG). Zero-sales quarters are included (quarter spine). "
             "is_partial_quarter marks the quarter containing the latest order date."),
    table_properties={"quality": "gold"},
)
def kpi_sales_growth_qoq() -> DataFrame:
    prod = gold("dim_product").select("product_key", "product_name")
    s = eligible_sales().join(prod, "product_key", "left").withColumn("quarter_start", F.trunc("order_date", "quarter"))
    scopes = [("TOTAL", F.lit("ALL"), F.lit("All sales")),
              ("REGION", F.col("region"), F.col("region")),
              ("PRODUCT", F.col("product_key").cast("string"), F.col("product_name"))]
    bounds = s.agg(F.min("quarter_start").alias("q_min"), F.max("order_date").alias("as_of_date"))
    quarters = bounds.select(F.explode(F.sequence("q_min", F.trunc("as_of_date", "quarter"),
                                                  F.expr("INTERVAL 3 MONTHS"))).alias("quarter_start"), "as_of_date")
    out = []
    for scope, value, label in scopes:
        b = s.withColumn("scope_value", value).withColumn("scope_label", label)
        members = b.select("scope_value", "scope_label").distinct()
        m = b.groupBy("scope_value", "quarter_start").agg(
            F.sum("gross_sales").cast(MONEY_TOTAL).alias("gross_sales"),
            F.countDistinct("order_id").alias("distinct_orders"),
            F.count(F.lit(1)).alias("order_items"),
            F.sum("quantity").cast("bigint").alias("quantity_sold"))
        out.append(members.crossJoin(quarters).join(m, ["scope_value", "quarter_start"], "left")
                   .withColumn("scope", F.lit(scope)))
    df = (union_all(out).fillna(zero_if_null("distinct_orders", "order_items", "quantity_sold"))
          .withColumn("gross_sales", F.coalesce(F.col("gross_sales"), F.lit(0).cast(MONEY_TOTAL))))
    w = Window.partitionBy("scope", "scope_value").orderBy("quarter_start")
    prev = F.lag("gross_sales").over(w)
    df = df.withColumn("previous_quarter_sales", prev)
    return df.select(
        "scope", "scope_value", "scope_label", "quarter_start", quarter_label(F.col("quarter_start")).alias("year_quarter"),
        "gross_sales", "previous_quarter_sales",
        (F.col("gross_sales") - F.col("previous_quarter_sales")).cast(MONEY_TOTAL).alias("absolute_sales_change"),
        safe_pct(F.col("gross_sales") - F.col("previous_quarter_sales"), F.col("previous_quarter_sales"))
         .alias("sales_growth_qoq_pct"),
        _growth_status(F.col("previous_quarter_sales")).alias("growth_status"),
        "distinct_orders", "order_items", "quantity_sold",
        (F.add_months(F.col("quarter_start"), 3) > F.date_add(F.col("as_of_date"), 1)).alias("is_partial_quarter"))


@dlt.table(
    name=gold_name("customer_cohort_retention"),
    comment=("Custom KPI 6 detail. Grain: cohort_month x cohort_age. Cohort = month of the customer's first sales "
             "order (order_date, not signup_date). Retained = distinct cohort customers with >= 1 sales order in the "
             "activity month. Only observable ages are emitted (no fake churn for young cohorts)."),
    table_properties={"quality": "gold"},
)
def customer_cohort_retention() -> DataFrame:
    o = (gold("fact_orders").filter(F.col("is_sales_order") & (F.col("customer_key") != UNKNOWN_KEY))
         .select("customer_key", "order_date", "gross_sales", F.trunc("order_date", "month").alias("activity_month")))
    as_of = o.agg(F.max("order_date").alias("as_of_date"))
    first = o.groupBy("customer_key").agg(F.min("activity_month").alias("cohort_month"))
    activity = (o.join(first, "customer_key")
                 .groupBy("cohort_month", "activity_month")
                 .agg(F.countDistinct("customer_key").alias("retained_customers"),
                      F.count(F.lit(1)).alias("orders"),
                      F.sum("gross_sales").cast(MONEY_TOTAL).alias("gross_sales")))
    sizes = first.groupBy("cohort_month").agg(F.count(F.lit(1)).alias("cohort_size"))
    spine = (sizes.crossJoin(as_of)
             .withColumn("max_observable_age",
                         F.months_between(F.trunc("as_of_date", "month"), F.col("cohort_month")).cast("int"))
             .withColumn("cohort_age", F.explode(F.sequence(F.lit(0), F.col("max_observable_age"))))
             .withColumn("activity_month", F.add_months(F.col("cohort_month"), F.col("cohort_age"))))
    df = (spine.join(activity, ["cohort_month", "activity_month"], "left")
               .fillna(zero_if_null("retained_customers", "orders"))
               .withColumn("gross_sales", F.coalesce(F.col("gross_sales"), F.lit(0).cast(MONEY_TOTAL))))
    return df.select(
        "cohort_month", F.date_format("cohort_month", "yyyy-MM").alias("cohort_label"), "cohort_size",
        "cohort_age", "activity_month", "retained_customers",
        safe_pct(F.col("retained_customers"), F.col("cohort_size")).alias("retention_pct"),
        "orders", "gross_sales", "max_observable_age",
        (F.col("activity_month") == F.trunc("as_of_date", "month")).alias("is_partial_activity_month"))


@dlt.table(
    name=gold_name("vw_customer_cohort_retention_matrix"),
    comment=("Custom KPI 6 matrix (materialized view): one row per cohort month, columns month_0 .. month_N = retention %. "
             "NULL = not yet observable (cohort too young), not churn."),
    table_properties={"quality": "gold"},
)
def vw_customer_cohort_retention_matrix() -> DataFrame:
    ages = list(range(COHORT_MATRIX_MONTHS + 1))
    m = (gold("customer_cohort_retention")
         .groupBy("cohort_month", "cohort_label", "cohort_size", "max_observable_age")
         .pivot("cohort_age", ages).agg(F.first("retention_pct")))
    # explicit pivot values -> fixed schema month_0..month_N; a table has no row order, so none is imposed here
    return m.select("cohort_month", "cohort_label", "cohort_size", "max_observable_age",
                    *[F.col(f"`{a}`").alias(f"month_{a}") for a in ages])


@dlt.table(
    name=gold_name("kpi_customer_cohort_summary"),
    comment=("Custom KPI 6 summary: per cohort - size, observable months, retention at months 1/3/6/12 (NULL if not "
             "yet observable), average active months, orders and revenue per customer."),
    table_properties={"quality": "gold"},
)
def kpi_customer_cohort_summary() -> DataFrame:
    r = gold("customer_cohort_retention")

    def at(age: int) -> Column:
        return F.max(F.when(F.col("cohort_age") == age, F.col("retention_pct"))).alias(f"retention_month_{age}_pct")

    return (r.groupBy("cohort_month", "cohort_label", "cohort_size", "max_observable_age")
             .agg(at(1), at(3), at(6), at(12),
                  F.sum("retained_customers").alias("_customer_months"),
                  F.sum("orders").alias("total_orders"),
                  F.sum("gross_sales").cast(MONEY_TOTAL).alias("total_gross_sales"))
             .select("cohort_month", "cohort_label", "cohort_size", "max_observable_age",
                     "retention_month_1_pct", "retention_month_3_pct", "retention_month_6_pct",
                     "retention_month_12_pct",
                     F.round(F.col("_customer_months") / F.col("cohort_size"), 2).alias("avg_active_months_per_customer"),
                     F.round(F.col("total_orders") / F.col("cohort_size"), 2).alias("orders_per_customer"),
                     safe_div_money(F.col("total_gross_sales"), F.col("cohort_size")).alias("revenue_per_customer"),
                     "total_orders", "total_gross_sales"))
