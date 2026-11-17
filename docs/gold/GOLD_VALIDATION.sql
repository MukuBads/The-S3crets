-- =====================================================================================================
-- Gold layer validation. Run after a pipeline update (e.g. as a SQL task after the pipeline task).
-- Every statement uses assert_true(): it raises an error when the check fails, so a successful run of the
-- whole file means every check passed. Change the catalog to the pipeline's retail.catalog value.
-- Assumes retail.silver.scd_type = 1 (for SCD2 add "WHERE __END_AT IS NULL" to silver.* reads).
-- =====================================================================================================
USE CATALOG workspace;

-- ---------------------------------------------------------------------------------------------------
-- 1. Dimension uniqueness (surrogate and business keys)
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(count(*) = count(DISTINCT customer_key) AND count(*) = count(DISTINCT customer_id),
                   'dim_customer: duplicate customer_key or customer_id') FROM gold.dim_customer;
SELECT assert_true(count(*) = count(DISTINCT product_key) AND count(*) = count(DISTINCT product_id),
                   'dim_product: duplicate product_key or product_id') FROM gold.dim_product;
SELECT assert_true(count(*) = count(DISTINCT date_key) AND count(*) = count(DISTINCT full_date) + 1,
                   'dim_date: duplicate date_key/full_date') FROM gold.dim_date;
SELECT assert_true(count(*) = count(DISTINCT payment_method_key), 'dim_payment_method: duplicate key')
FROM gold.dim_payment_method;
SELECT assert_true(count(*) = count(DISTINCT order_status_key), 'dim_order_status: duplicate key')
FROM gold.dim_order_status;
SELECT assert_true(count_if(customer_key = -1) = 1, 'dim_customer: exactly one Unknown member') FROM gold.dim_customer;
SELECT assert_true(count_if(product_key = -1) = 1, 'dim_product: exactly one Unknown member') FROM gold.dim_product;

-- ---------------------------------------------------------------------------------------------------
-- 2. Surrogate key stability: keys are a pure function of the business key, so they cannot change between
--    refreshes (or after a full refresh). No real key may collide with the Unknown member (-1).
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(count_if(customer_key <> xxhash64('customer', customer_id)) = 0,
                   'dim_customer: customer_key is not the deterministic hash of customer_id')
FROM gold.dim_customer WHERE customer_id <> 'UNKNOWN';
SELECT assert_true(count_if(product_key <> xxhash64('product', product_id)) = 0,
                   'dim_product: product_key is not the deterministic hash of product_id')
FROM gold.dim_product WHERE product_id <> 'UNKNOWN';
-- Optional cross-refresh check: run once to snapshot, run again after the next refresh to compare.
-- CREATE TABLE IF NOT EXISTS gold.validation_customer_key_snapshot AS SELECT customer_id, customer_key FROM gold.dim_customer;
-- SELECT assert_true(count(*) = 0, 'customer_key changed between refreshes')
-- FROM gold.validation_customer_key_snapshot s JOIN gold.dim_customer d USING (customer_id) WHERE s.customer_key <> d.customer_key;

-- ---------------------------------------------------------------------------------------------------
-- 3. Fact grain uniqueness
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(count(*) = count(DISTINCT order_item_id), 'fact_sales: grain is not one row per order_item_id')
FROM gold.fact_sales;
SELECT assert_true(count(*) = count(DISTINCT return_id), 'fact_returns: grain is not one row per return_id')
FROM gold.fact_returns;
SELECT assert_true(count(*) = count(DISTINCT order_id), 'fact_orders: grain is not one row per order_id')
FROM gold.fact_orders;

-- ---------------------------------------------------------------------------------------------------
-- 4. Foreign-key integrity: every fact key exists in its dimension (Unknown members included)
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(count(*) = 0, 'fact_sales.customer_key not in dim_customer')
FROM gold.fact_sales f LEFT ANTI JOIN gold.dim_customer d USING (customer_key);
SELECT assert_true(count(*) = 0, 'fact_sales.product_key not in dim_product')
FROM gold.fact_sales f LEFT ANTI JOIN gold.dim_product d USING (product_key);
SELECT assert_true(count(*) = 0, 'fact_sales.order_date_key not in dim_date')
FROM gold.fact_sales f LEFT ANTI JOIN gold.dim_date d ON f.order_date_key = d.date_key;
SELECT assert_true(count(*) = 0, 'fact_sales.payment_method_key not in dim_payment_method')
FROM gold.fact_sales f LEFT ANTI JOIN gold.dim_payment_method d USING (payment_method_key);
SELECT assert_true(count(*) = 0, 'fact_sales.order_status_key not in dim_order_status')
FROM gold.fact_sales f LEFT ANTI JOIN gold.dim_order_status d USING (order_status_key);
SELECT assert_true(count(*) = 0, 'fact_returns date keys not in dim_date')
FROM gold.fact_returns f LEFT ANTI JOIN gold.dim_date d ON f.return_date_key = d.date_key;
SELECT assert_true(count(*) = 0, 'fact_returns.original_order_date_key not in dim_date')
FROM gold.fact_returns f LEFT ANTI JOIN gold.dim_date d ON f.original_order_date_key = d.date_key;
SELECT assert_true(count(*) = 0, 'fact_orders.customer_key not in dim_customer')
FROM gold.fact_orders f LEFT ANTI JOIN gold.dim_customer d USING (customer_key);
-- Unknown members must only be used where the business key really is unresolved in Silver
SELECT assert_true(count(*) = 0, 'fact_sales uses Unknown product although the product exists')
FROM gold.fact_sales f JOIN gold.dim_product d ON f.product_id = d.product_id WHERE f.product_key = -1;

-- ---------------------------------------------------------------------------------------------------
-- 5. Sales, cost and profit calculations (line level)
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(count_if(gross_sales <> CAST(quantity * unit_price AS DECIMAL(18,2))) = 0,
                   'fact_sales: gross_sales <> quantity * unit_price') FROM gold.fact_sales;
SELECT assert_true(count_if(unit_cost IS NOT NULL AND total_cost <> CAST(quantity * unit_cost AS DECIMAL(18,2))) = 0,
                   'fact_sales: total_cost <> quantity * unit_cost') FROM gold.fact_sales;
SELECT assert_true(count_if(total_cost IS NOT NULL AND gross_profit <> gross_sales - total_cost) = 0,
                   'fact_sales: gross_profit <> gross_sales - total_cost') FROM gold.fact_sales;
SELECT assert_true(count_if(unit_cost IS NULL AND (total_cost IS NOT NULL OR gross_profit IS NOT NULL)) = 0,
                   'fact_sales: missing cost must give NULL cost/profit, never 0') FROM gold.fact_sales;
SELECT assert_true(count_if(quantity <= 0 OR unit_price <= 0 OR gross_sales <= 0) = 0,
                   'fact_sales: non-positive quantity/price/sales') FROM gold.fact_sales;
SELECT assert_true(count_if(refund_amount <= 0) = 0, 'fact_returns: non-positive refund') FROM gold.fact_returns;

-- ---------------------------------------------------------------------------------------------------
-- 6. Profit and margin KPI (recomputed from fact_sales)
-- ---------------------------------------------------------------------------------------------------
WITH f AS (
  SELECT product_key, sum(gross_sales) s, sum(total_cost) c, count_if(total_cost IS NULL) nc
  FROM gold.fact_sales WHERE is_revenue_eligible GROUP BY product_key)
SELECT assert_true(count(*) = 0, 'kpi_product_sales_margin: sales/COGS/profit/margin mismatch')
FROM gold.kpi_product_sales_margin k JOIN f USING (product_key)
WHERE k.gross_sales <> f.s
   OR (f.nc = 0 AND (k.cost_of_goods_sold <> f.c OR k.gross_profit <> f.s - f.c
                     OR abs(k.gross_margin_pct - round((f.s - f.c) * 100.0 / f.s, 2)) > 0.01))
   OR (f.nc > 0 AND k.gross_margin_pct IS NOT NULL);

-- ---------------------------------------------------------------------------------------------------
-- 7. Return matching
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(count(*) = 0, 'fact_returns row whose order item is not in fact_sales')
FROM gold.fact_returns r LEFT ANTI JOIN gold.fact_sales s USING (order_item_id);
SELECT assert_true(count(*) = 0, 'Silver return neither in fact_returns nor in dq_exceptions')
FROM silver.returns r
LEFT ANTI JOIN gold.fact_returns f USING (return_id)
LEFT ANTI JOIN (SELECT record_key FROM gold.dq_exceptions WHERE source_table = 'returns') x ON r.return_id = x.record_key;
SELECT assert_true(count(*) = 0, 'Silver order item neither in fact_sales nor in dq_exceptions')
FROM silver.order_items i
LEFT ANTI JOIN gold.fact_sales f USING (order_item_id)
LEFT ANTI JOIN (SELECT record_key FROM gold.dq_exceptions WHERE source_table = 'order_items') x
  ON i.order_item_id = x.record_key;
SELECT assert_true(count_if(r.order_id <> s.order_id OR r.customer_key <> s.customer_key OR r.product_key <> s.product_key) = 0,
                   'fact_returns keys differ from the original sale')
FROM gold.fact_returns r JOIN gold.fact_sales s USING (order_item_id);

-- ---------------------------------------------------------------------------------------------------
-- 8. Top-5 rankings
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(max(n) <= 5 AND min(min_rank) = 1, 'top5 customers: more than 5 rows or rank does not start at 1')
FROM (SELECT ranking_metric, count(*) n, min(rank) min_rank FROM gold.kpi_top5_customers_orders_returns GROUP BY 1);
SELECT assert_true(count(*) = 0, 'top5 customers: metric not non-increasing by rank')
FROM (SELECT ranking_metric, rank, metric_value,
             lag(metric_value) OVER (PARTITION BY ranking_metric ORDER BY rank) prev
      FROM gold.kpi_top5_customers_orders_returns) WHERE metric_value > prev;
SELECT assert_true(k.metric_value = m.mx, 'top5 customers: rank 1 by DISTINCT_ORDERS is not the maximum')
FROM (SELECT metric_value FROM gold.kpi_top5_customers_orders_returns
      WHERE ranking_metric = 'DISTINCT_ORDERS' AND rank = 1) k
CROSS JOIN (SELECT max(cnt) mx FROM (SELECT customer_key, count(*) cnt FROM gold.fact_orders
                                     WHERE is_sales_order AND customer_key <> -1 GROUP BY 1)) m;
SELECT assert_true(max(n) <= 5, 'top5 products: more than 5 rows per metric')
FROM (SELECT ranking_metric, count(*) n FROM gold.kpi_top5_products_orders_returns GROUP BY 1);
SELECT assert_true(k.metric_value = m.mx, 'top5 products: rank 1 by RETURN_COUNT is not the maximum')
FROM (SELECT metric_value FROM gold.kpi_top5_products_orders_returns
      WHERE ranking_metric = 'RETURN_COUNT' AND rank = 1) k
CROSS JOIN (SELECT max(c) mx FROM (SELECT product_key, count(*) c FROM gold.fact_returns
                                   WHERE product_key <> -1 GROUP BY 1)) m;

-- ---------------------------------------------------------------------------------------------------
-- 9. Region-wise customer growth QoQ
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(count(*) = 0, 'region growth: new-customer QoQ % formula mismatch')
FROM gold.kpi_region_customer_growth_qoq
WHERE previous_quarter_new_customers > 0
  AND abs(new_customer_growth_qoq_pct
          - round((new_customers - previous_quarter_new_customers) * 100.0 / previous_quarter_new_customers, 2)) > 0.01;
SELECT assert_true(count_if(previous_quarter_new_customers = 0 AND new_customer_growth_qoq_pct IS NOT NULL) = 0,
                   'region growth: zero previous quarter must give NULL growth') FROM gold.kpi_region_customer_growth_qoq;
SELECT assert_true(count(*) = 0, 'region growth: cumulative base at last quarter <> dated customers')
FROM (SELECT region, max_by(cumulative_customer_base, quarter_start) cum,
             max(total_customers_in_region) - max(customers_without_signup_date) dated
      FROM gold.kpi_region_customer_growth_qoq GROUP BY region) WHERE cum <> dated;
SELECT assert_true(count(*) = 0, 'region growth: ALL row <> sum of regions')
FROM (SELECT quarter_start, sum(CASE WHEN region = 'ALL' THEN new_customers END) a,
             sum(CASE WHEN region <> 'ALL' THEN new_customers END) r
      FROM gold.kpi_region_customer_growth_qoq GROUP BY quarter_start) WHERE a <> r;
SELECT assert_true(t.n = d.n, 'region growth: total customers <> dim_customer')
FROM (SELECT max(total_customers_in_region) n FROM gold.kpi_region_customer_growth_qoq WHERE region = 'ALL') t
CROSS JOIN (SELECT count(*) n FROM gold.dim_customer WHERE customer_key <> -1) d;

-- ---------------------------------------------------------------------------------------------------
-- 10. Digital payment percentages
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(count_if(digital_orders + non_digital_orders + unknown_payment_orders <> order_count) = 0,
                   'digital: categories do not partition the orders')
FROM gold.kpi_digital_payment_analysis WHERE breakdown_level = 'TOTAL';
SELECT assert_true(k.order_count = f.n, 'digital: an order is counted more than once (or missing)')
FROM (SELECT order_count FROM gold.kpi_digital_payment_analysis WHERE period_type = 'ALL' AND breakdown_level = 'TOTAL') k
CROSS JOIN (SELECT count(*) n FROM gold.fact_orders WHERE is_sales_order) f;
SELECT assert_true(count(*) = 0, 'digital: percentage formula mismatch')
FROM gold.kpi_digital_payment_analysis
WHERE breakdown_level = 'TOTAL' AND eligible_orders > 0
  AND abs(digital_pct_by_orders - round(digital_orders * 100.0 / eligible_orders, 2)) > 0.01;
SELECT assert_true(k.s = f.s, 'digital: sales total <> revenue-eligible fact_sales')
FROM (SELECT gross_sales s FROM gold.kpi_digital_payment_analysis WHERE period_type = 'ALL' AND breakdown_level = 'TOTAL') k
CROSS JOIN (SELECT sum(gross_sales) s FROM gold.fact_sales WHERE is_revenue_eligible) f;

-- ---------------------------------------------------------------------------------------------------
-- 11. Quarterly sales growth
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(k.s = f.s, 'sales growth: sum of TOTAL quarters <> eligible sales')
FROM (SELECT sum(gross_sales) s FROM gold.kpi_sales_growth_qoq WHERE scope = 'TOTAL') k
CROSS JOIN (SELECT sum(gross_sales) s FROM gold.fact_sales WHERE is_revenue_eligible) f;
SELECT assert_true(count(*) = 0, 'sales growth: previous quarter / growth formula mismatch')
FROM (SELECT *, lag(gross_sales) OVER (PARTITION BY scope, scope_value ORDER BY quarter_start) lg
      FROM gold.kpi_sales_growth_qoq)
WHERE previous_quarter_sales IS DISTINCT FROM lg
   OR (lg > 0 AND abs(sales_growth_qoq_pct - round((gross_sales - lg) * 100.0 / lg, 2)) > 0.01)
   OR (lg = 0 AND sales_growth_qoq_pct IS NOT NULL);
SELECT assert_true(count(*) = 0, 'sales growth: quarter spine has gaps')
FROM (SELECT scope, scope_value, quarter_start,
             lag(quarter_start) OVER (PARTITION BY scope, scope_value ORDER BY quarter_start) p
      FROM gold.kpi_sales_growth_qoq) WHERE p IS NOT NULL AND quarter_start <> add_months(p, 3);

-- ---------------------------------------------------------------------------------------------------
-- 12. Cohort assignment and retention
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(c.n = f.n, 'cohort: sum of cohort sizes <> customers with sales orders')
FROM (SELECT sum(cohort_size) n FROM (SELECT DISTINCT cohort_month, cohort_size FROM gold.customer_cohort_retention)) c
CROSS JOIN (SELECT count(DISTINCT customer_key) n FROM gold.fact_orders WHERE is_sales_order AND customer_key <> -1) f;
SELECT assert_true(count_if(retention_pct <> 100) = 0, 'cohort: month 0 retention must be 100%')
FROM gold.customer_cohort_retention WHERE cohort_age = 0;
SELECT assert_true(count_if(retained_customers > cohort_size) = 0, 'cohort: retained > cohort size')
FROM gold.customer_cohort_retention;
SELECT assert_true(count(*) = 0, 'cohort: cohort size mismatch with first-order month')
FROM (SELECT cm, count(*) n
      FROM (SELECT customer_key, trunc(min(order_date), 'MM') cm FROM gold.fact_orders
            WHERE is_sales_order AND customer_key <> -1 GROUP BY customer_key)
      GROUP BY cm) f
FULL OUTER JOIN (SELECT DISTINCT cohort_month, cohort_size FROM gold.customer_cohort_retention) r
  ON f.cm = r.cohort_month
WHERE f.n IS DISTINCT FROM r.cohort_size;
SELECT assert_true(count(*) = 0, 'cohort: retention % formula mismatch')
FROM gold.customer_cohort_retention
WHERE abs(retention_pct - round(retained_customers * 100.0 / cohort_size, 2)) > 0.01;
SELECT assert_true(count_if(cohort_age > max_observable_age) = 0, 'cohort: unobservable ages emitted')
FROM gold.customer_cohort_retention;
SELECT assert_true(count_if(month_0 <> 100) = 0, 'cohort matrix: month_0 must be 100')
FROM gold.vw_customer_cohort_retention_matrix;

-- ---------------------------------------------------------------------------------------------------
-- 13. Sales and refund reconciliation (Silver = Gold + excluded) and KPI totals
-- ---------------------------------------------------------------------------------------------------
SELECT assert_true(count_if(status <> 'PASS') = 0,
                   concat('dq_reconciliation failures: ', array_join(collect_list(CASE WHEN status <> 'PASS' THEN check_name END), ', ')))
FROM gold.dq_reconciliation;
SELECT assert_true(k.s = f.s, 'kpi_product_performance total sales <> eligible fact_sales')
FROM (SELECT sum(gross_sales) s FROM gold.kpi_product_performance) k
CROSS JOIN (SELECT sum(gross_sales) s FROM gold.fact_sales WHERE is_revenue_eligible) f;
SELECT assert_true(k.s = f.s, 'kpi_category_performance total sales <> eligible fact_sales')
FROM (SELECT sum(gross_sales) s FROM gold.kpi_category_performance) k
CROSS JOIN (SELECT sum(gross_sales) s FROM gold.fact_sales WHERE is_revenue_eligible) f;
SELECT assert_true(k.s = f.s, 'kpi_returns_summary TOTAL refund <> fact_returns')
FROM (SELECT refund_amount s FROM gold.kpi_returns_summary WHERE grain = 'TOTAL') k
CROSS JOIN (SELECT sum(refund_amount) s FROM gold.fact_returns) f;
SELECT assert_true(k.o = f.o, 'kpi_sales_summary TOTAL distinct orders <> sales orders')
FROM (SELECT distinct_orders o FROM gold.kpi_sales_summary WHERE grain = 'TOTAL') k
CROSS JOIN (SELECT count(*) o FROM gold.fact_orders WHERE is_sales_order) f;

-- ---------------------------------------------------------------------------------------------------
-- 14. Informational (not assertions): exclusion and flag volumes
-- ---------------------------------------------------------------------------------------------------
SELECT source_table, rule, treatment, ri_status, count(*) AS records, sum(amount) AS amount
FROM gold.dq_exceptions GROUP BY ALL ORDER BY source_table, records DESC;
SELECT * FROM gold.dq_reconciliation ORDER BY check_name;
