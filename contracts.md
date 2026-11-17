# Data contracts (schema): dataset_final

The team's single source of truth for table names, columns and types. Change it only through a PR approved by
the producing and the consuming owner. Catalog = `workspace` (Free Edition default). Snapshot date = **2026-11-15**:
the dataset's "today", used for every "future date" check.

Source: `data/raw/*.csv` (dirty, read as text) · ground truth: `data/clean_reference/*.csv` · every injected
problem: `data/quality/data_quality_issue_log.csv` · generator's notes: `data/README.md`.

## 1. Landing (owner P2)

`/Volumes/workspace/landing/source_csv/<feed>.csv`: the 6 dirty CSVs as uploaded.
`/Volumes/workspace/landing/files/<feed>/part_NNNNN.csv`: the replayer's output: same header and column order
as the source, every source row written exactly once. File 0 holds all reference data; orders, items and returns
follow in date order, 2,000 rows per file.

| Feed | Rows | Header (exact order) |
|---|---|---|
| categories | 6 | `category_id,category_name` |
| products | 300 | `product_id,product_name,category_id,price,cost_price` |
| customers | 30,000 | `customer_id,name,email,phone_number,signup_date,city,state,region` |
| orders | 100,000 | `order_id,customer_id,order_date,order_status,payment_method` |
| order_items | 165,190 | `order_item_id,order_id,product_id,sku,quantity,unit_price` |
| returns | 20,000 | `return_id,order_item_id,return_date,refund_amount` |

## 2. Bronze: `workspace.bronze.<feed>` (owner P2)

Streaming tables (Auto Loader). **Every source column is STRING, exactly as received.** Plus:

| Column | Type | Meaning |
|---|---|---|
| `_rescued_data` | STRING | Unexpected extra columns (schema drift) |
| `_ingested_at` | TIMESTAMP | When the row was loaded |
| `_source_file` | STRING | Landing file path |
| `_row_hash` | STRING | sha256 of all source columns, which identifies exact duplicate rows |

Table property `pipelines.reset.allowed = false`.

## 3. Silver: `workspace.silver.<feed>` + `workspace.silver.quarantine_<feed>` (owner P3)

Silver is built in two steps: **standardise** (fix formats so a valid value has one spelling), then **validate**.
A row that fails any error rule goes to `quarantine_<feed>` with `failure_reasons ARRAY<STRING>`; otherwise it goes to
the Silver table with `dq_flags ARRAY<STRING>` (warnings). Both tables keep `_ingested_at`, `_source_file` and `_row_hash`.
Silver is a streaming table, so duplicates are **kept** here and removed in Gold.

### 3.1 Standardisation rules (all feeds)

| Rule | Applies to | What it does |
|---|---|---|
| Null tokens | every column | `trim()`; then `''`, `NULL`, `null`, `N/A`, `N.A.`, `NaN`, `none`, `-` become NULL |
| Dates | `signup_date`, `order_date`, `return_date` | Accept `yyyy-MM-dd`, `yyyy/MM/dd`, `yyyyMMdd`, `yyyy-MM-ddTHH:mm:ss`, `dd-MM-yyyy`, `dd.MM.yyyy`, `MMM dd, yyyy`, `dd MMM yyyy`, `dd MMMM yyyy`. **`xx/xx/yyyy` is mixed day-first and month-first in this data.** If the first part is > 12, read it as `dd/MM`; if the second part is > 12, as `MM/dd`; if both are ≤ 12, read it as `dd/MM` and add warning `AMBIGUOUS_DATE` (477 rows, 0.17 %) |
| Money | `unit_price`, `refund_amount`, `price`, `cost_price` | Strip `₹`, `INR`, `Rs.`, `,` and spaces, then cast to `DECIMAL(12,2)` |
| Integers | IDs, `quantity` | Cast to INT; `two` and other words become NULL |
| Case and spaces | `name` | Collapse inner spaces, `initcap()` |
| | `email` | `lower(trim())` |
| | `city`, `state` | Collapse spaces, `initcap()`, then the alias maps below |
| | `region` | `upper(trim())`; `E`→EAST, `N`→NORTH, `S`→SOUTH, `W`→WEST |
| Phone | `phone_number` | Keep digits only; if 12 digits starting `91`, or 11 digits starting `0`, keep the last 10. Valid = 10 digits starting 6–9 → `+91-XXXXXXXXXX` |
| SKU | `sku` | `upper(trim())`; `_` or space → `-`; `ELESPH0004` → `ELE-SPH-0004` |
| Status | `order_status` | `delivered`, `Deliverd`, `DELIVERED` → `Delivered`; `Cancelled`, `Cancel`, `canceled` → `Canceled`; `in_transit`, `In-Transit`, `InTransit`, `IN TRANSIT` → `In Transit` |
| Payment | `payment_method` | `COD`, `C.O.D`, `Cash On Delivery` → `Cash on Delivery`; `Netbanking`, `Net-Banking`, `net banking` → `Net Banking`; `Upi`, `upi` → `UPI`; `LoyaltyPoints`, `Loyalty Pts` → `Loyalty Points` |

**City aliases:** Bombay→Mumbai, Madras→Chennai, Calcutta→Kolkata, Bangalore→Bengaluru, Mysore→Mysuru,
Mangalore→Mangaluru, Poona→Pune, Baroda→Vadodara, Gurgaon→Gurugram, Trivandrum→Thiruvananthapuram, Cochin→Kochi,
Panjim→Panaji.
**State aliases:** AP→Andhra Pradesh, TS/Telengana→Telangana, UP/U.P.→Uttar Pradesh, RJ/Rajastan→Rajasthan,
OD/Orissa→Odisha, WB/W. Bengal→West Bengal, CG/Chattisgarh→Chhattisgarh, PB→Punjab, TN/Tamilnadu→Tamil Nadu,
KA→Karnataka, KL/Keral→Kerala, GJ/Gujrat→Gujarat, MH/Maharastra→Maharashtra, UK→Uttarakhand, GOA→Goa, Andhra→Andhra Pradesh.
(The full lists live as dicts in `src/retail/cleansing.py`; extend them when the quarantine shows a new spelling.)

### 3.2 Silver columns and validation rules

**silver.categories**: `category_id INT, category_name STRING`
- Errors: `NULL_CATEGORY_ID`, `NULL_CATEGORY_NAME`

**silver.products**: `product_id INT, product_name STRING, category_id INT, price DECIMAL(12,2), cost_price DECIMAL(12,2)`
- Errors: `NULL_<COL>`, `INVALID_PRICE` (price or cost ≤ 0), `NEGATIVE_MARGIN` (price < cost)

**silver.customers**: `customer_id INT, name STRING, email STRING, phone_number STRING, signup_date DATE, city STRING, state STRING, region STRING, region_source STRING`
- Errors: `NULL_CUSTOMER_ID`, `NULL_NAME`, `INVALID_EMAIL` (syntax), `UNKNOWN_STATE` (not one of the 28 in `conf/state_region.csv` after aliasing), `INVALID_SIGNUP_DATE` (unparseable, before 2000-01-01, or after 2026-11-15)
- Warnings: `NULL_EMAIL`, `NULL_PHONE`, `INVALID_PHONE`, `NULL_CITY`, `REGION_MISMATCH` (the given region differs from the state's region)
- `region` is **always derived from `state`** via `conf/state_region.csv`, because the source region column has invalid values (`CENTRAL`, `NORTH-EAST`, ...) and mismatches. The original is kept as `region_source`.

**silver.orders**: `order_id INT, customer_id INT, order_date DATE, order_status STRING, payment_method STRING`
- Errors: `NULL_ORDER_ID`, `NULL_CUSTOMER_ID` (also non-numeric), `INVALID_ORDER_DATE` (unparseable, before 2000-01-01, or after 2026-11-15), `UNKNOWN_STATUS` (e.g. `Pending??`, `TBD`, `0`), `UNKNOWN_PAYMENT_METHOD` (e.g. `Bitcoin`, `Cheque??`, `123`)
- Warnings: `AMBIGUOUS_DATE`

**silver.order_items**: `order_item_id INT, order_id INT, product_id INT, sku STRING, quantity INT, unit_price DECIMAL(12,2)`
- Errors: `NULL_ORDER_ITEM_ID`, `NULL_ORDER_ID`, `NULL_PRODUCT_ID`, `INVALID_QUANTITY` (NULL, non-numeric, ≤ 0 or > 100), `INVALID_PRICE` (NULL or ≤ 0)
- Warnings: `NULL_SKU`, `MALFORMED_SKU` (not `^[A-Z]{3}-[A-Z]{3}-\d{4}$` after standardising)

**silver.returns**: `return_id INT, order_item_id INT, return_date DATE, refund_amount DECIMAL(12,2)`
- Errors: `NULL_RETURN_ID`, `NULL_ORDER_ITEM_ID`, `INVALID_RETURN_DATE` (unparseable, before 2000-01-01, or after 2026-11-15), `INVALID_REFUND` (NULL or ≤ 0)
- Warnings: `AMBIGUOUS_DATE`

## 4. Gold: `workspace.gold.*` (owner P4), materialized views rebuilt each run

**Deduplication first** (all feeds):
1. Drop exact duplicates by `_row_hash`.
2. Keep one row per primary key: the latest `_ingested_at`, then the row with the fewest `dq_flags`.
3. *Duplicate entities* (the same person under two `customer_id`s, the same item under two `return_id`s) are only **reported** in `dq_exceptions`. They are not merged.

| Table | Grain / PK | Columns |
|---|---|---|
| `dim_product` | `product_id` | `product_id, product_name, category_id, category_name, price, cost_price, sku_expected` (`<CAT>-<NAME>-<NNNN>`, built as in `data/README.md`) |
| `dim_customer` | `customer_id` | `customer_id, name, email, phone_number, signup_date, city, state, region` |
| `fact_orders` | `order_id` | `order_id, customer_id, order_date, order_quarter, order_month, order_status, payment_method, is_digital` (UPI, Net Banking, Loyalty Points), `is_canceled, order_value` (Σ quantity × unit_price of its valid items; 0 if none)`, item_count, region` |
| `fact_sales` | `order_item_id` | Items of **non-canceled** orders that exist in `fact_orders`: `order_item_id, order_id, customer_id, product_id, category_name, quantity, unit_price, list_price, sales_amount` (= quantity × unit_price), `profit_amount` (= quantity × (unit_price − cost_price)), `order_date, order_quarter, order_month, region, payment_method, is_digital` |
| `fact_returns` | `return_id` | `return_id, order_item_id, order_id, customer_id, product_id, return_date, refund_amount, days_to_return, link_status` (LINKED / ORPHAN) |
| `dq_exceptions` | `check_name, feed, key` | Cross-table checks: `ORPHAN_CUSTOMER` (order→customer missing), `ORPHAN_ORDER`, `ORPHAN_PRODUCT` (item→order/product missing), `ORPHAN_ORDER_ITEM` (return→item missing), `ORDER_BEFORE_SIGNUP`, `RETURN_BEFORE_ORDER`, `RETURN_AFTER_30_DAYS`, `RETURN_ON_UNDELIVERED`, `REFUND_EXCEEDS_ITEM`, `PRICE_MISMATCH` (unit_price ≠ list price), `SKU_PRODUCT_MISMATCH`, `DUPLICATE_EMAIL`, `DUPLICATE_PHONE`, `ITEM_RETURNED_TWICE` |

Rows involved in an exception **stay** in the facts unless the row can't be placed at all: an item whose order is missing
can't be in `fact_sales`, because it has no date or status. Price mismatches use the **charged** `unit_price` for sales and keep `list_price` alongside.

## 5. KPI tables: `workspace.kpi.*` (owner P5)

| Table | Grain | Formula |
|---|---|---|
| `kpi_orders_vs_returns_top5` | entity_type × entity_id | per customer and per product: `order_count` (distinct orders), `order_qty`, `return_count`, `return_qty`, `refund_amount`; top 5 by orders and by returns |
| `kpi_region_customer_growth_qoq` | region × quarter | customer base = cumulative customers with ≥ 1 non-canceled order by quarter end; QoQ % = (base − prev) ÷ prev × 100 |
| `kpi_product_sales_margin` | product | total_sales, total_profit, margin_pct = profit ÷ sales × 100 |
| `kpi_digital_payment_pct` | quarter (+ ALL) | Σ order_value where is_digital ÷ Σ order_value × 100 (non-canceled orders) |
| `kpi_sales_growth_qoq` | quarter | Σ sales_amount per quarter; QoQ % = (S − prev) ÷ prev × 100; `is_partial_quarter` for Q4 2026 |
| `kpi_avg_order_value` | region × quarter | Σ sales_amount ÷ distinct orders |
| `kpi_cohort_retention` | cohort_month × months_since_first | % of each first-order-month cohort ordering again k months later |

Data now spans **2023-01 to 2026-11** (customers sign up from 2023), so QoQ has about 16 quarters.

## 6. How to check Silver and Gold against the truth

`data/clean_reference/` has the same rows before the problems were injected. After a full run:
- **Gold row counts:** `dim_customer` ≈ 30,000 distinct IDs; `fact_orders` ≈ 100,000 minus quarantined orders.
- For a sample of IDs, standardised Silver values should equal the clean reference.
- `data/quality/data_quality_report.md` lists how many of each problem exist. Quarantine and `dq_exceptions`
  counts should be of the same order.
