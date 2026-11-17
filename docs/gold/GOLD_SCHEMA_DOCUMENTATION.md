# Gold Layer: Schema Documentation

**Source:** `src/pipeline/gold.py`, which uses the same `dlt` API and configuration keys as `silver.py`.

**Related documents:**
- `STAR_SCHEMA.md`: diagram, keys and join paths.
- `KPI_DEFINITIONS.md`: formulas and example SQL.
- `GOLD_VALIDATION.sql`: executable checks.

**Status: not yet run on Databricks.** No Spark runtime was available where this was written. What was checked:
- Python syntax, and pyflakes lint with no findings.
- A static check of the pipeline graph: all 25 Gold tables are defined, every Gold and Silver read resolves to an existing table, and the graph has no cycles.

What still needs to happen:
- Run `GOLD_VALIDATION.sql` after the first DEV update.
- Treat no check as passed until it has actually run there.

## 1. Architecture

```
silver.<feed>  (AUTO CDC targets: current accepted state, VALID + WARNING)
silver.referential_integrity_exceptions (used only to label exclusions)
        │  batch reads (spark.read.table) inside materialized views
        ▼
gold.dim_*  ──►  gold.fact_sales  ──►  gold.fact_orders
                        │                    │
                        └──► gold.fact_returns
                                  │
                  gold.kpi_* / customer_cohort_retention / vw_customer_cohort_retention_matrix
                  gold.dq_exceptions / gold.dq_reconciliation
```

**Why every Gold object is a materialized view (MV)**
- Silver tables receive updates: late versions and corrected records are upserted by AUTO CDC.
- A streaming read of such a table either fails on the update commits, or (with `skipChangeCommits`) silently ignores them.
- An MV always reads the current Silver state. Late orders, late returns, late parents and corrections therefore land in the right period on the next update.
- This matches the project architecture: "GOLD: materialized views (rebuilt each run)".

**Incremental processing**
- On serverless pipelines, MVs are refreshed incrementally when the query plan allows it.
- Every Gold query is deterministic (no `current_timestamp()` or random functions), which keeps incremental refresh possible.
- Where incremental refresh is not possible for a query, for example windowed ranks, the MV is recomputed. At this data volume (~100k orders, ~165k lines) that is cheap.
- No full refresh is needed for routine operation.

**Placement**
- Add `gold.py` as a source file of the same pipeline as Bronze and Silver. The dependencies are inferred from the three-level table names, so Gold runs after Silver in every update.
- Create the schema once: `CREATE SCHEMA IF NOT EXISTS <catalog>.gold;`
- Gold reads Silver only, never Bronze.

**Configuration**
- *Reused from Silver:* `retail.catalog`, `retail.silver.schema`, `retail.silver.scd_type`, `retail.silver.min_valid_date`.
- *New, all optional:*

| Key | Default |
|---|---|
| `retail.gold.schema` | `gold` |
| `retail.gold.date_dim_start` | `retail.silver.min_valid_date` |
| `retail.gold.date_dim_end` | `2035-12-31` |
| `retail.gold.digital_payment_methods` | `UPI,Net Banking` |
| `retail.gold.return_window_days` | `30` |
| `retail.gold.cohort_matrix_months` | `24` |

## 2. Silver input contract (verified against `silver.py`)

| Silver table | Columns used | Guarantees from Silver |
|---|---|---|
| `categories` | `category_id`, `category_name` | `category_id` is a non-null digit string; name may be NULL |
| `products` | `product_id`, `product_name`, `category_id`, `price`, `cost_price` | `price` and `cost_price` are DECIMAL(12,2) > 0 (INVALID otherwise) |
| `customers` | `customer_id`, `name`, `email`, `phone_number`, `signup_date`, `city`, `state`, `region` | Only the key is mandatory. Attributes with WARNING-level defects are NULL. |
| `orders` | `order_id`, `customer_id`, `order_date`, `order_status`, `payment_method` | `order_date` DATE and `order_status` ∈ {Delivered, In Transit, Canceled} are mandatory. `payment_method` ∈ {Cash on Delivery, Net Banking, UPI, Loyalty Points} or NULL. |
| `order_items` | `order_item_id`, `order_id`, `product_id`, `sku`, `quantity`, `unit_price` | `quantity` INT is 1–100; `unit_price` DECIMAL(12,2) > 0; `sku` is canonical or NULL |
| `returns` | `return_id`, `order_item_id`, `return_date`, `refund_amount` | `return_date` DATE; `refund_amount` > 0 |
| all | `quality_status`, `quality_issue_summary`, `_row_hash`, `_source_file`, `_ingested_at` | Only VALID and WARNING rows; INVALID rows are in `silver.quarantine_*` |
| `referential_integrity_exceptions` | `feed`, `record_pk`, `fk_column`, `ri_status` | `UNRESOLVED_PENDING`, `ORPHANED` or `PARENT_QUARANTINED` |

The distinct values of `payment_method` and `order_status` were confirmed in the dataset: the source files were profiled, and the Silver mappings were checked against every value. That gives 4 methods + NULL, and 3 statuses.

## 3. Tables

### Dimensions
| Table | Grain | Source → target | Notes |
|---|---|---|---|
| `dim_date` | one day, 2000-01-01 to 2035-12-31, plus -1 | generated | Columns: `date_key`, `full_date`, `day`, `day_name`, `day_of_week` (ISO, 1 = Monday), `week` (ISO), `month`, `month_name`, `month_year`, `quarter`, `quarter_name`, `year`, `year_quarter`, `year_month`, `is_weekend`, `month_start`, `quarter_start`, `month_key`, `quarter_key` |
| `dim_customer` | one customer, plus -1 | `customers` | `name` → `customer_name`; `region` = state→region mapping, else source region, else `UNKNOWN`; `region_source` keeps the Silver value; `region_derivation` says which rule applied |
| `dim_product` | one product, plus -1 | `products` ⟕ `categories` (+ `order_items` for SKU) | `price` → `current_price`. A missing category becomes "Unknown category" with `is_category_resolved = false`. `sku` = the most frequent canonical SKU of the product in Silver order lines (ties: alphabetical). |
| `dim_payment_method` | one method, plus -1 | constants | `payment_type`, `payment_category` (DIGITAL / NON_DIGITAL / UNKNOWN), `is_digital_payment` (NULL for Unknown) |
| `dim_order_status` | one status, plus -1 | constants | `status_category` (FULFILLED / IN_PROGRESS / CANCELED, a 1:1 relabel), `counts_toward_sales`, `is_delivered` |

### Facts
| Table | Grain | Source → target |
|---|---|---|
| `fact_sales` | `order_item_id` | `order_items` ⋈ `orders` (inner) ⟕ dimensions. `gross_sales` = quantity × unit_price; `unit_cost` = current `cost_price`; `total_cost` = quantity × unit_cost; `gross_profit` = gross_sales − total_cost. Flags: `is_revenue_eligible`, `is_delivered`, `is_customer_resolved`, `is_product_resolved`, `has_unit_cost`, `unit_price_differs_from_list_price`, `is_order_before_signup`. |
| `fact_orders` | `order_id` | `orders` ⟕ dimensions ⟕ `fact_sales` aggregated by order. Adds `order_item_count`, `total_quantity`, `gross_sales`, `total_cost` (NULL if any line lacks cost), `has_items`, `is_sales_order`. |
| `fact_returns` | `return_id` | `returns` ⋈ `fact_sales` on `order_item_id` (inner). Keys come from the original sale. Adds `days_to_return` and these flags: `is_return_before_order`, `is_outside_return_window`, `is_refund_above_item_value`, `is_order_delivered`, `is_repeat_return_for_item` (with `return_sequence_for_item`). |

### KPI outputs
These are listed in `KPI_DEFINITIONS.md`:
- `kpi_sales_summary`, `kpi_product_performance`, `kpi_category_performance`, `kpi_customer_performance`, `kpi_order_performance`, `kpi_returns_summary`
- `kpi_top5_customers_orders_returns`, `kpi_top5_products_orders_returns`
- `kpi_region_customer_growth_qoq`, `kpi_product_sales_margin`, `kpi_digital_payment_analysis`, `kpi_sales_growth_qoq`
- `customer_cohort_retention`, `vw_customer_cohort_retention_matrix`, `kpi_customer_cohort_summary`

### Data-quality outputs
| Table | Content |
|---|---|
| `dq_exceptions` | One row per excluded Silver row (`treatment = EXCLUDED_FROM_FACT`, with the Silver `ri_status`) and per included row that has a cross-feed anomaly (`INCLUDED_WITH_FLAG`), with the rule, detail, amount and lineage |
| `dq_reconciliation` | Checks of the form *Silver = Gold + excluded* for line rows and value, and return rows and refund; orders, customers and products row counts; fact-grain uniqueness; facts against KPI totals. Each check has a PASS/FAIL `status`. |

## 4. SCD handling

**SCD Type 1 (Silver default)**
- Silver keeps one current row per key, so every Gold dimension is type 1.
- If a customer's region or a product's category changes, history is restated under the new value.
- Previous attribute values **cannot be reconstructed** in Gold. They exist only in append-only Bronze.

**SCD Type 2 (`retail.silver.scd_type = 2`)**
- Gold reads only the open version (`__END_AT IS NULL`). Each business key therefore maps to exactly one dimension row, and no fact row joins more than one version.
- Gold deliberately does **not** do point-in-time joins. Silver orders its versions by arrival: `_sequence = (_ingested_at, _source_file, _content_hash)`. These are not business-effective dates, so matching a 2024 order to "the version valid on its order date" would use load times and give wrong history.
- If the source ever provides an effective date, a type-2 dimension can be added, with keys `xxhash64(entity, business_key, effective_from)` and a range join on `order_date`.

**Product cost history**
- `cost_price` is the **current** cost. Gross profit and margin for past periods are calculated at today's cost.
- The transaction `unit_price` is historical, because it comes from the order line.

## 5. Data-quality and referential-integrity rules in Gold

| Situation | Treatment | Where it is visible |
|---|---|---|
| INVALID Silver rows | Never used: they are only in `silver.quarantine_*` | Silver |
| Line whose order is not in Silver (`UNRESOLVED_PENDING`, `ORPHANED`, `PARENT_QUARANTINED`) | **Excluded** from `fact_sales`. It cannot be dated or attributed to a customer. It comes back automatically when the order arrives, because MVs are recomputed. | `dq_exceptions` (`ORDER_NOT_RESOLVED` + `ri_status`) and `dq_reconciliation` |
| Return whose order line is not in `fact_sales` | **Excluded** from `fact_returns` | `dq_exceptions` (`ORDER_ITEM_NOT_RESOLVED` / `ORDER_ITEM_EXCLUDED_FROM_FACT_SALES`) |
| Product not in Silver | Included with `product_key = -1`. Revenue counts; cost, profit and margin are NULL, **never 0**. | `PRODUCT_NOT_RESOLVED` |
| Customer not in Silver | Included with `customer_key = -1`, region `UNKNOWN`. Excluded from customer rankings and cohorts. | `CUSTOMER_NOT_RESOLVED` |
| Unknown payment method (Silver NULL) | `payment_method_key = -1`, category UNKNOWN. Reported separately and excluded from the digital-% denominator. | `kpi_digital_payment_analysis` |
| Canceled orders | Kept in the facts; `is_revenue_eligible = false` excludes them from sales KPIs | `kpi_order_performance` (ORDER_STATUS) |
| Cross-feed anomalies: order before signup; unit_price ≠ list price; return before order; return more than 30 days after order; refund > line value; return on a non-Delivered order; repeat return for a line; order without lines | Kept and flagged. Never silently corrected or dropped. | `dq_exceptions` (`INCLUDED_WITH_FLAG`) and fact flag columns |

**Expectations in `gold.py`**
- *Guards (`expect_all_or_fail`):* stop the update if a Silver guarantee is broken. These cover non-null grain keys, positive quantity, price and refund, and a known order date.
- *Monitors (`expect_all`):* record the share of unresolved products and customers, price differences and return anomalies in the event log.

## 6. Idempotency

- Every Gold object is a pure, deterministic function of the current Silver state. Re-running an update, or a full refresh, gives identical tables.
- Surrogate keys are hashes of business keys.
- Fact grains are Silver primary keys, which are unique in SCD1 Silver.
- Uniqueness is checked by `dq_reconciliation` (`*_grain_unique`) and `GOLD_VALIDATION.sql` §3.

## 7. Performance notes

- Dimensions are small (≤ 30k rows), so the joins are broadcast joins chosen by the optimizer. No hints are needed.
- Cross joins exist only on spines: regions × quarters, scopes × quarters, and cohorts × ages. Each is at most a few thousand rows.
- No partitioning or clustering: every Gold table is far below the size where it pays off. Revisit `CLUSTER BY (order_date_key)` on `fact_sales` once it passes tens of millions of rows.
- `dq_reconciliation` runs about 40 small aggregations per update. Disable it, or move it to the validation job, if update latency matters more.
