-- =====================================================================================================
-- Silver layer monitoring queries (Databricks SQL / notebook SQL cell)
-- Change the catalog below to the pipeline's retail.catalog value.
-- event_log(TABLE(x)) returns the event log of the pipeline that owns table x.
-- =====================================================================================================
USE CATALOG workspace;

-- 1. Throughput and backlog per flow, latest updates first ---------------------------------------------
SELECT timestamp,
       origin.update_id,
       origin.flow_name,
       details:flow_progress.status                                  AS status,
       CAST(details:flow_progress.metrics.num_output_rows   AS BIGINT) AS output_rows,
       CAST(details:flow_progress.metrics.num_upserted_rows AS BIGINT) AS upserted_rows,
       CAST(details:flow_progress.metrics.backlog_bytes     AS DOUBLE) AS backlog_bytes,
       CAST(details:flow_progress.metrics.backlog_files     AS BIGINT) AS backlog_files
FROM event_log(TABLE(silver.orders))
WHERE event_type = 'flow_progress'
  AND origin.flow_name LIKE '%silver%'
ORDER BY timestamp DESC
LIMIT 200;

-- 2. Expectation metrics (warning share + guard expectations) ------------------------------------------
SELECT timestamp, origin.flow_name, e.dataset, e.name, e.passed_records, e.failed_records
FROM (
  SELECT timestamp, origin,
         explode(from_json(details:flow_progress.data_quality.expectations,
                 'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM event_log(TABLE(silver.orders))
  WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
)
ORDER BY timestamp DESC;

-- 3. Update duration, final state and retries -----------------------------------------------------------
SELECT origin.update_id,
       min(timestamp)                                                        AS started_at,
       max(timestamp)                                                        AS last_event_at,
       timestampdiff(SECOND, min(timestamp), max(timestamp))                 AS duration_s,
       max_by(details:update_progress.state, timestamp)                      AS final_state,
       count_if(details:update_progress.state = 'RETRYING' OR message ILIKE '%retry%') AS retry_events
FROM event_log(TABLE(silver.orders))
WHERE event_type = 'update_progress'
GROUP BY origin.update_id
ORDER BY started_at DESC
LIMIT 50;

-- 4. Errors (pipeline-level failures are here, never in quarantine) -----------------------------------
SELECT timestamp, origin.flow_name, level, message, error
FROM event_log(TABLE(silver.orders))
WHERE level IN ('ERROR', 'WARN')
ORDER BY timestamp DESC
LIMIT 100;

-- 5. Freshness / lag: newest Bronze arrival vs newest arrival reflected in Silver or quarantine ---------
WITH b AS (
  SELECT 'customers' feed, max(_ingested_at) ts FROM bronze.customers UNION ALL
  SELECT 'orders', max(_ingested_at) FROM bronze.orders UNION ALL
  SELECT 'order_items', max(_ingested_at) FROM bronze.order_items UNION ALL
  SELECT 'returns', max(_ingested_at) FROM bronze.returns UNION ALL
  SELECT 'products', max(_ingested_at) FROM bronze.products UNION ALL
  SELECT 'categories', max(_ingested_at) FROM bronze.categories
), s AS (
  SELECT 'customers' feed, greatest((SELECT max(_ingested_at) FROM silver.customers),
                                    (SELECT max(_ingested_at) FROM silver.quarantine_customers)) ts UNION ALL
  SELECT 'orders', greatest((SELECT max(_ingested_at) FROM silver.orders),
                            (SELECT max(_ingested_at) FROM silver.quarantine_orders)) UNION ALL
  SELECT 'order_items', greatest((SELECT max(_ingested_at) FROM silver.order_items),
                                 (SELECT max(_ingested_at) FROM silver.quarantine_order_items)) UNION ALL
  SELECT 'returns', greatest((SELECT max(_ingested_at) FROM silver.returns),
                             (SELECT max(_ingested_at) FROM silver.quarantine_returns)) UNION ALL
  SELECT 'products', greatest((SELECT max(_ingested_at) FROM silver.products),
                              (SELECT max(_ingested_at) FROM silver.quarantine_products)) UNION ALL
  SELECT 'categories', greatest((SELECT max(_ingested_at) FROM silver.categories),
                                (SELECT max(_ingested_at) FROM silver.quarantine_categories))
)
SELECT b.feed, b.ts AS bronze_latest, s.ts AS silver_latest,
       timestampdiff(SECOND, s.ts, b.ts)            AS silver_behind_bronze_s,
       timestampdiff(SECOND, b.ts, current_timestamp()) AS bronze_age_s
FROM b JOIN s USING (feed)
ORDER BY feed;

-- 6. Arrival rate vs processing: rows per minute arriving in Bronze (last hour) ------------------------
SELECT date_trunc('MINUTE', _ingested_at) AS minute, count(*) AS bronze_rows
FROM bronze.orders
WHERE _ingested_at >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY 1 ORDER BY 1 DESC;

-- 7. Quality distribution ---------------------------------------------------------------------------------
SELECT * FROM silver.dq_status_summary ORDER BY feed, layer, outcome;

-- 8. Top rules (all reasons, not only the first) --------------------------------------------------------
SELECT feed, layer, severity, category, rule, records
FROM silver.dq_rule_summary
ORDER BY records DESC
LIMIT 50;

-- 9. Quarantine work queue ------------------------------------------------------------------------------
SELECT feed, error_category, resolution_status, count(*) AS records
FROM silver.quarantine_all
GROUP BY ALL
ORDER BY records DESC;

SELECT feed, record_pk, quality_issue_summary, raw_record, _source_file, _ingested_at, _row_hash
FROM silver.quarantine_all
WHERE resolution_status = 'OPEN'
ORDER BY _ingested_at DESC
LIMIT 100;

-- 10. Referential integrity ---------------------------------------------------------------------------
SELECT feed, fk_column, parent_feed, ri_status, count(*) AS records,
       min(_ingested_at) AS oldest_child, max(_ingested_at) AS newest_child
FROM silver.referential_integrity_exceptions
GROUP BY ALL
ORDER BY feed, fk_column, ri_status;

-- 11. Duplicates delivered to Bronze --------------------------------------------------------------------
SELECT feed, duplicate_type, count(*) AS keys, sum(deliveries) AS deliveries
FROM silver.dq_bronze_duplicate_keys
GROUP BY ALL
ORDER BY feed, duplicate_type;

-- 12. Trace one record from Silver back to Bronze ------------------------------------------------------
-- SELECT * FROM bronze.orders WHERE _row_hash = '<_row_hash from silver / quarantine>';
-- SELECT * FROM bronze.orders WHERE trim(order_id) = '<order_id>' ORDER BY _ingested_at;   -- every version
