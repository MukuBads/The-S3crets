# KPI Definitions (Gold)

## Conventions for every KPI

| Term | Definition |
|---|---|
| Sales line | A `fact_sales` row with `is_revenue_eligible = true`, i.e. the order status is Delivered or In Transit. Canceled orders never count as sales. This follows the project architecture. |
| Sales order | A `fact_orders` row with `is_sales_order = true`: not Canceled and at least one line. |
| Gross sales | `SUM(quantity × unit_price)`, using the transaction `unit_price` from the order line, not the current list price |
| COGS | `SUM(quantity × cost_price)`, using the **current** product cost. Historical costs are not available in the source. |
| Gross profit | Gross sales − COGS |
| Margin % | Gross profit / gross sales × 100 |
| Return measures | Return records (`COUNT(return_id)`), returned order lines (`COUNT(DISTINCT order_item_id)`) and refund amount. The source has **no returned quantity**, so none is derived. |
| Return rate % | Returned lines whose sale is revenue-eligible / sales lines × 100. Numerator and denominator cover the same line population, so the rate is always ≤ 100%. |
| Percentages | Rounded to 2 decimals. NULL (never an error, never 0) when the denominator is 0 or missing. Growth KPIs also return a `*_status` column: `NO_PREVIOUS_QUARTER`, `PREVIOUS_QUARTER_ZERO` or `OK`. |
| Ranks | `ROW_NUMBER()` ordered by metric descending, then numeric business key ascending. The result is deterministic and has no ties. |
| Unknown members | `-1` keys (customer, product, payment method). Shown as `UNKNOWN` rows where relevant; excluded from rankings and cohorts. |
| Partial periods | The latest quarter or month is flagged (`is_partial_quarter`, `is_partial_activity_month`) instead of being presented as a decline |

---

## Standard KPIs

### `kpi_sales_summary`: sales KPIs
- **Grain:** `grain` ∈ {TOTAL, DAY, MONTH, QUARTER, YEAR, REGION} × `grain_value`. `period_start` is set for time grains.
- **Source:** sales lines + `dim_customer.region`.
- **Measures:**
  - `gross_sales`, `quantity_sold`, `distinct_orders`, `order_items`.
  - `active_customers`: distinct known customers.
  - `repeat_customers`: customers with more than 1 distinct order **within that grain value**.
  - `first_time_buyers`: customers whose first-ever sales order falls in the grain value.
  - `average_order_value` = gross_sales / distinct_orders.
  - `average_selling_price` = gross_sales / quantity_sold.
- **Limitation:** "New customers" here means first purchase. Acquisition by `signup_date` is in `kpi_region_customer_growth_qoq`.

```sql
SELECT grain_value AS month, gross_sales, distinct_orders, average_order_value
FROM gold.kpi_sales_summary WHERE grain = 'MONTH' ORDER BY period_start;
```

### `kpi_product_performance`: product KPIs
- **Grain:** one row per product. Every product appears, including zero-sales products, so "lowest selling" is meaningful. The Unknown product appears only if it has activity.
- **Measures:**
  - Sales: `gross_sales`, `quantity_sold`, `distinct_orders`, `order_items`, `total_cost`, `gross_profit`, `gross_margin_pct`, `items_without_cost`.
  - Returns: `return_count`, `returned_order_items`, `refund_amount`, `return_rate_pct`.
- **Ranks:**
  - `revenue_rank`, `best_selling_rank` (units) and `profit_rank`: 1 = highest.
  - `lowest_selling_rank`: 1 = fewest units, ties broken by sales, then id.

```sql
SELECT product_id, product_name, quantity_sold, gross_sales, gross_margin_pct
FROM gold.kpi_product_performance WHERE best_selling_rank <= 10 ORDER BY best_selling_rank;
```

### `kpi_category_performance`: category revenue
- **Grain:** one row per category. "Unknown category" is included if it has sales.
- **Measures:** sales, `sales_share_pct`, quantity, orders, cost, profit, margin, returns, return rate, `revenue_rank`.

### `kpi_customer_performance`: customer KPIs
- **Grain:** one row per known customer, including customers with no purchases.
- **Measures:**
  - `distinct_orders` (= `purchase_frequency`, lifetime sales orders), `order_items`, `quantity_ordered`, `gross_sales`, `average_order_value`.
  - `first_order_date`, `last_order_date`, `avg_days_between_orders`.
  - `customer_segment`: NO_PURCHASE / ONE_TIME / REPEAT.
  - Returns, `return_rate_pct`, `revenue_rank`.
- **Derived totals:**
  - Unique customers = rows with `distinct_orders > 0`.
  - Repeat customers = `customer_segment = 'REPEAT'`, i.e. more than 1 distinct order over the whole data period.
  - For repeat customers in a specific period, use `kpi_sales_summary.repeat_customers`.
  - Customer distribution by region: `kpi_region_customer_growth_qoq.total_customers_in_region`.

```sql
SELECT customer_id, customer_name, region, gross_sales, distinct_orders
FROM gold.kpi_customer_performance WHERE revenue_rank <= 10 ORDER BY revenue_rank;
```

### `kpi_order_performance`: order KPIs
Each order is counted once (`fact_orders`).

| `metric_group` | `order_scope` | Content |
|---|---|---|
| ORDER_STATUS | ALL_ORDERS | Orders by status, including Canceled and orders without lines; `gross_sales` = order value |
| PAYMENT_METHOD | SALES_ORDERS | Orders, sales and AOV by method (UNKNOWN shown separately) |
| MONTH | SALES_ORDERS | Monthly order trend, sales and AOV |
| REGION | SALES_ORDERS | AOV by region |
| ORDERS_PER_CUSTOMER | SALES_ORDERS | Distribution of customers by number of orders (1, 2, 3, 4, 5, 6–10, 11+): `customer_count` and `pct_share` |

`pct_share` is the share within its metric group.

### `kpi_returns_summary`: return KPIs
- **Grain:** `grain` ∈ {TOTAL, MONTH (by `return_date`), PRODUCT, CATEGORY, CUSTOMER} × `grain_value`.
- **Measures:**
  - `return_count`, `returned_order_items`, `refund_amount`, `average_refund_amount`.
  - `flagged_returns`: returns with any cross-feed anomaly (before the order, after the return window, refund above line value, order not delivered, repeat return).
- **Note:** flagged returns are included in the totals and shown in `flagged_returns` and `dq_exceptions`. Nothing is silently removed.

---

## Custom KPIs

### KPI 1: Orders vs returns by customer and product (Top 5)
**Tables:** `kpi_top5_customers_orders_returns` and `kpi_top5_products_orders_returns`. These are built from `kpi_customer_performance` and `kpi_product_performance`, which aggregate each fact separately before joining on the key. That avoids fact-to-fact fan-out.

| Table | `ranking_metric` values |
|---|---|
| Customers | DISTINCT_ORDERS, ORDER_ITEMS, QUANTITY_ORDERED, RETURN_COUNT, REFUND_AMOUNT |
| Products | QUANTITY_ORDERED, DISTINCT_ORDERS, RETURN_COUNT, REFUND_AMOUNT |

- **Rows:** up to 5 per metric (`rank` 1–5). Only entities with a positive value are ranked.
- **Comparison columns on every row:** orders, lines, quantity, sales, returns, returned lines, refund, `return_rate_pct`, and `refund_to_sales_pct`.
- **Measures kept separate:** orders (distinct `order_id`), order lines (`order_item_id`), quantity (units) and return records (`return_id`).

```sql
SELECT rank, customer_id, customer_name, metric_value AS returns, distinct_orders, return_rate_pct
FROM gold.kpi_top5_customers_orders_returns WHERE ranking_metric = 'RETURN_COUNT' ORDER BY rank;
```

### KPI 2: Region-wise customer base and QoQ growth
**Table:** `kpi_region_customer_growth_qoq`
- **Grain:** region (NORTH, SOUTH, EAST, WEST, UNKNOWN, plus an ALL roll-up) × signup quarter.
- **Quarter range:** a full quarter spine from the first signup quarter to the latest. Quarters with no signups show 0.

| Measure | Formula |
|---|---|
| New customers | Distinct customers with `signup_date` in the quarter |
| New-customer growth QoQ % | (current − previous) / previous × 100 |
| Cumulative customer base | Running sum of new customers up to quarter end |
| Base growth QoQ % | (current base − previous base) / previous base × 100 |

Also: `total_customers_in_region`, `customers_without_signup_date` (counted in the total but not placed in any quarter), `region_rank_new_customers`, `region_rank_new_customer_growth` (ALL excluded), and `is_partial_quarter`.

**Region rule:** the documented state→region mapping is used first, then the Silver region, then `UNKNOWN`. The applied rule is in `dim_customer.region_derivation`.

```sql
SELECT region, year_quarter, new_customers, new_customer_growth_qoq_pct, cumulative_customer_base
FROM gold.kpi_region_customer_growth_qoq WHERE region <> 'ALL' ORDER BY quarter_start, region;
```

### KPI 3: Product-wise sales vs margin
**Table:** `kpi_product_sales_margin`

| Measure | Formula |
|---|---|
| `gross_sales` | SUM(quantity × unit_price) |
| `cost_of_goods_sold` | SUM(quantity × current cost_price) |
| `gross_profit` | Sales − COGS |
| `gross_margin_pct` | Profit / sales × 100 |

- **Missing cost:** if any line of a product lacks a cost, COGS, profit and margin are NULL. Missing cost is never treated as 0.
- **Ranks:** `sales_rank`, `profit_rank` and `margin_rank` cover products with sales and a known cost.
- **Segments:** `sales_margin_segment` is HIGH/LOW_SALES × HIGH/LOW_MARGIN, split at the medians (`median_sales`, `median_margin_pct`), or NO_SALES, or COST_UNKNOWN.
- **Limitation:** cost is current, not historical.

```sql
SELECT product_name, gross_sales, gross_margin_pct
FROM gold.kpi_product_sales_margin WHERE sales_margin_segment = 'HIGH_SALES_LOW_MARGIN' ORDER BY gross_sales DESC;
```

### KPI 4: Percentage of digital payments
**Table:** `kpi_digital_payment_analysis`
- **Population:** sales orders, each counted once (`fact_orders`). Sales = the sum of the order's `fact_sales` lines, so payment values are not double-counted.
- **Classification:**

| Category | Methods |
|---|---|
| DIGITAL | UPI, Net Banking |
| NON_DIGITAL | Cash on Delivery, Loyalty Points |
| UNKNOWN | Payment method missing or not allowed in Silver |

- **Rows:** `period_type` ALL or QUARTER × `breakdown_level`:
  - TOTAL rows carry the percentage columns.
  - CATEGORY and METHOD rows carry `order_count`, `gross_sales`, `pct_of_orders` and `pct_of_sales`.
- **Formulas:**
  - Digital % by orders = digital orders / (digital + non-digital orders) × 100.
  - Digital % by sales = digital sales / (digital + non-digital sales) × 100.
  - UNKNOWN is kept out of the eligible denominator. The `*_incl_unknown` variants include it.
- **Assumption to confirm:** Loyalty Points is classified as non-digital because it redeems store credit rather than using an electronic payment instrument. Change it with `retail.gold.digital_payment_methods`.

```sql
SELECT period, digital_pct_by_orders, digital_pct_by_sales, unknown_payment_orders
FROM gold.kpi_digital_payment_analysis WHERE breakdown_level = 'TOTAL' AND period_type = 'QUARTER' ORDER BY period;
```

### KPI 5: Sales growth QoQ
**Table:** `kpi_sales_growth_qoq`
- **Grain:** `scope` ∈ {TOTAL, REGION, PRODUCT} × `scope_value` × quarter.
- **Period assignment:** by `order_date` only; `return_date` is never used. Zero-sales quarters are included through a quarter spine.

| Measure | Formula |
|---|---|
| `previous_quarter_sales` | `LAG(gross_sales)` |
| `absolute_sales_change` | Current − previous |
| `sales_growth_qoq_pct` | (current − previous) / previous × 100 |

- **Also:** `distinct_orders`, `order_items`, `quantity_sold`, `growth_status`, `is_partial_quarter`.
- **Returns:** sales are gross of returns; returns are reported separately.

```sql
SELECT year_quarter, gross_sales, previous_quarter_sales, sales_growth_qoq_pct, is_partial_quarter
FROM gold.kpi_sales_growth_qoq WHERE scope = 'TOTAL' ORDER BY quarter_start;
```

### KPI 6: Customer cohort retention by first-order month
**Tables:**
- `customer_cohort_retention`: grain cohort_month × cohort_age.
- `vw_customer_cohort_retention_matrix`: one row per cohort, with columns `month_0` … `month_24`. This is a materialized view, since temporary pipeline views are not published.
- `kpi_customer_cohort_summary`: one row per cohort.

| Term | Definition |
|---|---|
| Cohort month | Month of the customer's first **sales order** (`order_date`, not `signup_date`) |
| Cohort size | Distinct customers in the cohort |
| Activity month | Month in which the customer placed a sales order |
| Cohort age | Months between the cohort month and the activity month (calendar months, month-start to month-start) |
| Retained customers | Distinct cohort customers with at least one sales order in the activity month. Several orders in one month count once. |
| Retention % | Retained / cohort size × 100. Month 0 is always 100. |

- **Young cohorts:** only ages up to `max_observable_age` (the months between the cohort and the latest order month) are emitted. A matrix cell beyond that is NULL ("not yet observable"), not 0, so young cohorts are not shown as churned. `is_partial_activity_month` marks the incomplete latest month.
- **Exclusions:** orders with an unknown customer (`customer_key = -1`) are excluded. Silver guarantees `order_date` is a valid date.
- **Summary table columns:** retention at months 1, 3, 6 and 12 (NULL if not yet observable), average active months per customer, orders per customer, revenue per customer.

```sql
SELECT cohort_label, cohort_size, month_0, month_1, month_3, month_6, month_12
FROM gold.vw_customer_cohort_retention_matrix ORDER BY cohort_month;
```

---

## Limitations

1. **Costs are current, not historical.** Profit for past periods uses today's cost price.
2. **Dimensions are SCD type 1.** Customer region and product category are current values; see GOLD_SCHEMA_DOCUMENTATION §4.
3. **No returned quantity in the source.** Return rate is line-based, and the refund amount is the money measure.
4. **Pending lines are temporarily missing.** Lines whose order hasn't arrived yet are excluded until it arrives. Expect small restatements of recent periods; they are visible in `dq_exceptions`.
5. **Flagged anomalies are kept in totals.** Anomalies such as a return before its order are included, not removed. Filter them out with the fact flags if the business decides they should not count.
