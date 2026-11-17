-- =====================================================================================================
-- Silver invariant tests. Run after any pipeline update. Each statement raises an error (assert_true)
-- when the invariant is broken, so the file can run as a SQL task after the pipeline task.
-- Change the catalog below to the pipeline's retail.catalog value.
-- Assumes retail.silver.scd_type = 1 (for SCD2 add "WHERE __END_AT IS NULL" to the PK checks).
-- =====================================================================================================
USE CATALOG workspace;

-- T1. One row per primary key in every Silver table (idempotency / no duplicate output) ---------------
SELECT assert_true(count(*) = 0, 'duplicate customer_id in silver.customers')
FROM (SELECT customer_id FROM silver.customers GROUP BY 1 HAVING count(*) > 1);
SELECT assert_true(count(*) = 0, 'duplicate order_id in silver.orders')
FROM (SELECT order_id FROM silver.orders GROUP BY 1 HAVING count(*) > 1);
SELECT assert_true(count(*) = 0, 'duplicate order_item_id in silver.order_items')
FROM (SELECT order_item_id FROM silver.order_items GROUP BY 1 HAVING count(*) > 1);
SELECT assert_true(count(*) = 0, 'duplicate return_id in silver.returns')
FROM (SELECT return_id FROM silver.returns GROUP BY 1 HAVING count(*) > 1);
SELECT assert_true(count(*) = 0, 'duplicate product_id in silver.products')
FROM (SELECT product_id FROM silver.products GROUP BY 1 HAVING count(*) > 1);
SELECT assert_true(count(*) = 0, 'duplicate category_id in silver.categories')
FROM (SELECT category_id FROM silver.categories GROUP BY 1 HAVING count(*) > 1);

-- T2. Quarantine is idempotent: one row per quarantine_id ----------------------------------------------
SELECT assert_true(count(*) = count(DISTINCT quarantine_id), 'duplicate quarantine_id in quarantine_orders')
FROM silver.quarantine_orders;
SELECT assert_true(count(*) = count(DISTINCT quarantine_id), 'duplicate quarantine_id in quarantine_customers')
FROM silver.quarantine_customers;
SELECT assert_true(count(*) = count(DISTINCT quarantine_id), 'duplicate quarantine_id in quarantine_order_items')
FROM silver.quarantine_order_items;
SELECT assert_true(count(*) = count(DISTINCT quarantine_id), 'duplicate quarantine_id in quarantine_returns')
FROM silver.quarantine_returns;

-- T3. Routing: Silver never holds INVALID rows, quarantine only holds INVALID rows -----------------------
SELECT assert_true(count_if(quality_status NOT IN ('VALID', 'WARNING')) = 0, 'INVALID row in silver.orders')
FROM silver.orders;
SELECT assert_true(count_if(quality_status NOT IN ('VALID', 'WARNING')) = 0, 'INVALID row in silver.order_items')
FROM silver.order_items;
SELECT assert_true(count_if(quality_status NOT IN ('VALID', 'WARNING')) = 0, 'INVALID row in silver.returns')
FROM silver.returns;
SELECT assert_true(count_if(quality_status NOT IN ('VALID', 'WARNING')) = 0, 'INVALID row in silver.customers')
FROM silver.customers;
SELECT assert_true(count_if(quality_status <> 'INVALID') = 0, 'non-INVALID row in quarantine_all')
FROM silver.quarantine_all WHERE error_category <> 'REFERENTIAL_INTEGRITY';

-- T4. Every quarantined row is traceable and explained --------------------------------------------------
SELECT assert_true(count_if(size(quality_issues) = 0 OR _source_file IS NULL OR _row_hash IS NULL
                            OR _ingested_at IS NULL OR quarantined_at IS NULL) = 0,
                   'quarantine row without reasons or lineage')
FROM silver.quarantine_all;

-- T5. Status is consistent with the issues list (WARNING rows carry at least one warning) ---------------
SELECT assert_true(count_if((quality_status = 'VALID' AND size(quality_issues) > 0)
                         OR (quality_status = 'WARNING' AND NOT exists(quality_issues, x -> x.severity = 'WARNING'))
                         OR exists(quality_issues, x -> x.severity = 'INVALID')) = 0,
                   'quality_status inconsistent with quality_issues in silver.orders')
FROM silver.orders;

-- T6. Typed columns really are typed and hold only accepted values ---------------------------------------
SELECT assert_true(typeof(order_date) = 'date' AND typeof(quantity) = 'int' AND typeof(unit_price) = 'decimal(12,2)',
                   'unexpected Silver data types')
FROM silver.orders o JOIN silver.order_items i USING (order_id) LIMIT 1;
SELECT assert_true(count_if(order_status NOT IN ('Delivered', 'In Transit', 'Canceled') OR order_status IS NULL) = 0,
                   'order_status outside domain in silver.orders')
FROM silver.orders;
SELECT assert_true(count_if(quantity < 1 OR unit_price <= 0 OR quantity IS NULL OR unit_price IS NULL) = 0,
                   'non-positive quantity or price in silver.order_items')
FROM silver.order_items;
SELECT assert_true(count_if(region IS NOT NULL AND region NOT IN ('NORTH', 'SOUTH', 'EAST', 'WEST')) = 0,
                   'region outside domain in silver.customers')
FROM silver.customers;

-- T7. Records with several problems keep every reason (expected > 0 with the project dataset) ----------
SELECT assert_true(count_if(size(quality_issues) > 1) > 0, 'no multi-reason records found - check rule engine')
FROM silver.quarantine_all;

-- T8. RI view only lists children whose parent is genuinely absent from Silver ------------------------
SELECT assert_true(count(*) = 0, 'RI exception listed although parent exists')
FROM silver.referential_integrity_exceptions r
JOIN silver.customers c ON r.parent_feed = 'customers' AND r.fk_value = c.customer_id;
SELECT assert_true(count(*) = 0, 'RI exception listed although parent exists')
FROM silver.referential_integrity_exceptions r
JOIN silver.orders o ON r.parent_feed = 'orders' AND r.fk_value = o.order_id;

-- T9. Nothing silently lost: every Bronze primary key is in Silver or in quarantine (or had no usable key).
--     Only Bronze rows already covered by the last Silver run are checked (rows newer than that are in flight).
SELECT assert_true(count(*) = 0, 'order_id present in Bronze but in neither Silver nor quarantine')
FROM (SELECT DISTINCT trim(order_id) AS order_id FROM bronze.orders
      WHERE trim(order_id) RLIKE '^[0-9]+$'
        AND _ingested_at <= (SELECT greatest(max(s._ingested_at), (SELECT max(_ingested_at) FROM silver.quarantine_orders))
                             FROM silver.orders s)) b
LEFT ANTI JOIN silver.orders s ON b.order_id = s.order_id
LEFT ANTI JOIN silver.quarantine_orders q ON b.order_id = q.record_pk;
