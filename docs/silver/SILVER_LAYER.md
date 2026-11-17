# Silver Layer: Design and Operations

**Files**

| File | Purpose |
|---|---|
| `src/pipeline/silver.py` | Pipeline source |
| `docs/silver/silver_monitoring.sql` | Event-log and data-quality queries |
| `docs/silver/silver_tests.sql` | Invariant assertions to run after every update |
| `notebooks/silver_scenario_tests.py` | Fixture-based end-to-end tests (DEV catalog only) |

**Inputs this design is based on**
- `data/README.md`, `data/quality/data_quality_report.*` and `data/quality/data_quality_issue_log.csv`.
- The six source CSVs. Every value shape in them was profiled.
- The architecture diagrams (`retail_3h_architecture.drawio-3`, `retail_HLD_A.drawio`).

The parsing and standardisation constants in `silver.py` were checked against the full dirty dataset: 23 checks, 0 mismatches (see section E).

**Bronze contract: checked against `src/pipeline/bronze.py`.** Both files use `import dlt`, the `COLUMNS` dicts are identical, and Bronze provides `_ingested_at`, `_source_file`, `_row_hash` and `_rescued_data` under `{CAT}.bronze.<feed>`. **Not yet run on Databricks:** no Spark runtime was available where this was written, so the first DEV pipeline update is the first real test.

---

## A. Architecture

```
bronze.<feed>  (Auto Loader streaming table, all STRING, append-only)
   │  spark.readStream.table()                          ← incremental, offsets held in the flow checkpoint
   ▼
read_bronze()     contract check · _raw struct · _content_hash · dropDuplicatesWithinWatermark(_content_hash)
   ▼
tag_feed()        clean strings → safe type conversion → every rule evaluated → quality_status / quality_issues
   ├── VALID + WARNING ─► view silver_<feed>_accepted ─► AUTO CDC  keys=[PK]            ─► silver.<feed>
   └── INVALID ────────► view silver_<feed>_rejected ─► AUTO CDC  keys=[quarantine_id] ─► silver.quarantine_<feed>

Materialized views (recomputed on each update, batch reads of Silver):
   silver.referential_integrity_exceptions   children whose parent is missing / quarantined
   silver.quarantine_all                     6 quarantine tables + ORPHANED/PARENT_QUARANTINED FKs + resolution status
   silver.dq_status_summary, dq_rule_summary, dq_bronze_duplicate_keys   monitoring
```

**Why each feed has two streaming flows.** One Bronze stream is split into the accepted and rejected sides. Each side is its own flow with its own checkpoint, which keeps both sides restartable on their own.

**Why AUTO CDC.** Silver must be idempotent and must handle updates. AUTO CDC (`create_auto_cdc_flow`, formerly `apply_changes`) is the pipeline-managed, streaming-compatible upsert. A plain append streaming table cannot upsert. A hand-written `foreachBatch` MERGE is not available inside a declarative pipeline.

**Why referential integrity is a materialized view.** It is not a filter on the stream because parents can arrive late. Section C explains this.

## B. Incremental processing and checkpointing

**How a new batch is processed**
1. Every flow reads its Bronze table with `spark.readStream.table`. Bronze is append-only (`pipelines.reset.allowed=false`), so each micro-batch contains only the Bronze rows committed since the last offset that flow saved. Historical rows are never re-read.
2. Offsets (the Bronze Delta version and index), the deduplication state and the AUTO CDC state are stored in the **pipeline-managed checkpoint for each flow**. No checkpoint paths are configured by hand.
3. A micro-batch counts as done only after its write to the target Delta table has committed, and the target commit and the checkpoint update are coordinated by the streaming engine. When Bronze ingests a file, that does **not** mean Silver has processed it: Silver has its own offsets for each flow. Use query 5 in `silver_monitoring.sql` to see how far Silver is behind Bronze.

**What happens if the pipeline stops halfway through a batch**
- If the cluster or update dies mid-batch, the uncommitted micro-batch leaves no committed data in the target.
- On restart, each flow resumes from its last committed offset and processes that batch again.
- Reprocessing is safe for three reasons: transformations are deterministic, `_content_hash` and `quarantine_id` are pure functions of the payload, and AUTO CDC upserts by key, so applying the same batch twice gives the same table.
- The only non-deterministic values are `validation_timestamp` and `quarantined_at`, which are metadata.

**Trigger mode**
- The architecture uses a **triggered** pipeline every few minutes, which fits Free Edition and serverless. Each update processes everything that is available, then stops.
- Continuous mode also works with this code where the workspace allows it.
- Either way, the pipeline does **not** need to finish a batch every 5 seconds. What matters is that, over time, the processing rate is at least the arrival rate.

**Keeping up with the generator (~2,000 rows every 5 s, about 24k rows/min)**
- Watch `backlog_bytes` and `backlog_files` in the event log (query 1), where the source reports them.
- Watch `silver_behind_bronze_s` (query 5) and update `duration_s` (query 3).
- In triggered mode: if update duration approaches the schedule interval, or `silver_behind_bronze_s` grows from one update to the next, Silver is falling behind.
- The per-row work is native Spark expressions. The only shuffle in the streaming path is the deduplication state store plus the AUTO CDC merge, which is fine at this rate.

## C. Idempotency, duplicates, updates and late data

| Situation | Handling |
|---|---|
| Retried micro-batch (crash, restart) | Restarts from the last committed offset; deterministic upsert, so no extra rows |
| Exact duplicate rows, or the same CSV re-delivered under a new file name | Layer 1: `dropDuplicatesWithinWatermark(_content_hash)` drops copies within `retail.silver.dedup_window` (default 2 h). Layer 2: a later copy becomes an AUTO CDC upsert with identical values, so there is still one row. Auto Loader itself never ingests the same file path twice. |
| Same PK, **different** payload | Treated as a new version of the entity. AUTO CDC keeps the version with the highest `_sequence = (_ingested_at, _source_file, _content_hash)`, so versions processed out of order still converge. With `scd_type=2`, every version is kept (`__START_AT` / `__END_AT`), and history is tracked only on business and quality columns, so re-deliveries never add versions. |
| Same invalid payload delivered again | Same `quarantine_id = sha2(feed ‖ pk ‖ _content_hash)`, so it updates the existing quarantine row instead of adding one |
| New invalid version of a key | Different `_content_hash`, so it gets a new quarantine row. Silver keeps the last accepted version. |
| Corrected record re-sent | Valid, so it upserts into Silver by PK. In `quarantine_all`, the old entry shows `resolution_status = SUPERSEDED_BY_ACCEPTED_VERSION`. |
| Same PK, two different payloads in one file | Ordered by `_content_hash` as the last tie-breaker. The result is deterministic but arbitrary, because the source has no row timestamp. These cases are listed in `dq_bronze_duplicate_keys` as `CONFLICTING_VERSIONS`. |

`_row_hash` from Bronze is kept for tracing back to Bronze. It is not used as the identity, because how Bronze computes it isn't documented here. Silver computes its own `_content_hash` over the raw source values.

**Watermark choice.** The deduplication watermark is on a processing-time column, `current_timestamp()` at read time. That column only moves forward, so **no row is ever dropped as "late"** by this operator. Business-time lateness (late orders, late returns, out-of-order files) is handled entirely by AUTO CDC sequencing and by the referential-integrity view, not by watermarks. State older than the window is evicted, which bounds memory. A copy arriving after eviction is still absorbed by the idempotent upsert.

**Limitation: version order.** The source has no `updated_at` column, so arrival order is the only available version order. If someone re-sends an *older* version under a new file name after a newer one, the re-sent version wins. If the generator adds an event or update timestamp later, put it first in `_sequence`.

**Late parents and referential integrity**
- A child row only has to be structurally valid to enter Silver; its parent does not have to exist yet.
- `referential_integrity_exceptions` is recomputed on every update against the *current* Silver tables, and gives each unresolved child one of three statuses:
  - `UNRESOLVED_PENDING`: the parent hasn't arrived yet, and the child is still inside `retail.silver.fk_grace_minutes` (default 360).
  - `ORPHANED`: the parent is still missing after the grace period.
  - `PARENT_QUARANTINED`: the parent arrived but was rejected as INVALID.
- When a late parent arrives, the next update no longer lists the child. No reprocessing or manual step is needed, and an orphan "heals" the same way.
- ORPHANED and PARENT_QUARANTINED rows also appear in `quarantine_all`.
- **Why the streaming flows don't check FKs:**
  - A stream-static join judges each child once, against whatever parents exist at that moment, and never revisits the decision. That would permanently reject children whose parent was just late.
  - A stream-stream join would need unbounded state.
- **Contract for consumers (Gold):** child rows in Silver can still have unresolved parents. Gold must anti-join `referential_integrity_exceptions`, or map those keys to an unknown member (−1, as in HLD A).

## D. Error handling

| Kind | Where it goes | Retry? |
|---|---|---|
| Record-level data defects: missing values, failed conversions, out-of-domain values, bad dates, rescued columns | `quality_issues` on the row → Silver (WARNING) or quarantine (INVALID). The pipeline keeps running. | No. Fix the data at the source and re-send it; the corrected version is admitted automatically. |
| Unresolved foreign keys | `referential_integrity_exceptions` / `quarantine_all` | Resolves automatically when the parent arrives |
| Bronze table missing an expected column (incompatible schema change) | `ValueError` at graph definition. The **update fails**. | No. Align the Bronze and Silver code. |
| `spark.sql.legacy.timeParserPolicy` not set to `CORRECTED` | `ValueError` at graph definition. The update fails. | No. Fix the pipeline configuration. |
| A code bug sends an INVALID row to Silver, or a non-INVALID row to quarantine | `expect_all_or_fail` guard on the views fails the update | No. Fix the code. |
| Transient infrastructure failure (node loss, storage throttling) | The pipeline retries the update automatically (production mode) and resumes from the checkpoints | Yes, automatic |

There are no `try/except` blocks around the transformations. Infrastructure and programming errors surface as failed updates in the event log (query 4); they are never turned into quarantine rows.

**Recovering from a logic bug in Silver.** Fix the code and run **Full refresh** on the affected Silver and quarantine tables only. Bronze holds every delivery and blocks full refresh, so Silver can be rebuilt from it deterministically.

## E. Data-quality matrix

`quality_issues` is an `array<struct<rule, column, severity, category>>` holding **every** failing rule. `quality_status` is INVALID if any issue is INVALID, WARNING if any issue exists, and VALID otherwise. `is_valid = quality_status <> 'INVALID'`. Typed columns hold a value only when it passed its checks. A value that failed a WARNING-level check is NULL in the typed column, and the original text is always in `_raw`.

**General rules (all feeds)**
- Strings are trimmed.
- These tokens count as NULL: empty string, `NULL`, `null`, `N/A`, `NaN`, `none`, `-`.
- Case is not changed except where listed below.

| Feed | Rule | Result |
|---|---|---|
| all | PK missing / not a positive integer | INVALID (KEY) |
| all | `_rescued_data` populated / unmapped Bronze column populated | WARNING (SCHEMA_DRIFT) |
| categories | `category_name` missing | WARNING |
| products | `category_id` missing or malformed | INVALID |
| products | `price` / `cost_price`: missing, unparseable or ≤ 0 | INVALID |
| products | `price` / `cost_price`: currency formatting | WARNING |
| products | `product_name` missing | WARNING |
| customers | name / email / phone / signup_date / city / state / region: missing | WARNING |
| customers | name: case or whitespace anomaly | WARNING (value only trimmed) |
| customers | email: not lower-case canonical | WARNING (normalised) |
| customers | email: invalid syntax | WARNING (typed NULL) |
| customers | phone: valid number in another format | WARNING (→ `+91-XXXXXXXXXX`) |
| customers | phone: invalid, or placeholder like `9999999999` | WARNING (typed NULL) |
| customers | signup_date: non-ISO format | WARNING (parsed) |
| customers | signup_date: unparseable, ambiguous, < `min_valid_date`, or after ingestion + 1 day | WARNING (typed NULL) |
| customers | city / state: case variant of a reference value | WARNING (canonical case) |
| customers | city / state: not in reference list (e.g. `Bombay`, `MH`) | WARNING (kept as received) |
| customers | region: case or whitespace variant | WARNING (upper-cased) |
| customers | region: not in {NORTH, SOUTH, EAST, WEST} | WARNING (typed NULL) |
| customers | region does not match the documented state→region mapping | WARNING |
| orders | `customer_id` missing or malformed (e.g. `CUST-02690`) | INVALID |
| orders | `order_date`: missing, unparseable, ambiguous `nn/nn/yyyy`, < `min_valid_date`, or after ingestion + 1 day | INVALID |
| orders | `order_date`: non-ISO but unambiguous | WARNING |
| orders | `order_status`: missing, or not one of the documented variants of Delivered / In Transit / Canceled (e.g. `Deliverd`, `Cancel`, `TBD`) | INVALID |
| orders | `order_status`: case / space / `-` / `_` variant, or `Cancelled` | WARNING (canonical value) |
| orders | `payment_method`: missing or not allowed (e.g. `Bitcoin`, `Loyalty Pts`) | WARNING (typed NULL) |
| orders | `payment_method`: variant (`COD`, `Netbanking`, `upi` …) | WARNING (canonical value) |
| order_items | `order_id` / `product_id` missing or malformed | INVALID |
| order_items | quantity: missing, not an integer (`two`, `2.5`), < 1, or > `max_quantity` (100) | INVALID |
| order_items | `unit_price`: missing, unparseable or ≤ 0 | INVALID |
| order_items | `unit_price`: currency formatting | WARNING |
| order_items | sku: missing or malformed | WARNING (typed NULL) |
| order_items | sku: format variant | WARNING (→ `AAA-AAA-0000`) |
| order_items | sku: digits ≠ product_id | WARNING (typed NULL) |
| returns | `order_item_id` missing or malformed | INVALID |
| returns | `return_date`: same rules as `order_date` | same as orders |
| returns | `refund_amount`: missing, unparseable or ≤ 0 | INVALID |
| returns | `refund_amount`: currency formatting | WARNING |
| children | FK not found in Silver parent | `UNRESOLVED_PENDING` → `ORPHANED` / `PARENT_QUARANTINED` (RI view) |

**Why customers have no INVALID attribute rules.** A customer is master data and the parent of every order. Rejecting a customer over a bad phone number would orphan all of that customer's orders, so only key problems reject a customer. Orders, order items and returns are the facts that KPIs aggregate, so a bad date, amount or status rejects them.

**Results of checking against the real data** (Python port of the same regexes and maps):
- **Format variants:** 100% of reformatted dates, phones, SKUs, statuses and payment methods normalise back to the clean reference value.
- **Rejections:** 100% of impossible dates, invalid phones, invalid SKUs and invalid statuses are rejected.
- **City aliases:** none are silently remapped.
- **Ambiguous dates:** about 480 `nn/nn/yyyy` dates could be read either way and are flagged as ambiguous instead of guessed.
- **Source-rounded amounts:** amounts written as `INR 41,108` are rounded at the source. They parse to within ₹0.50 of the true value and are tagged `*_reformatted`.

## F. Operations

**Integrating `silver.py`**
1. Add `silver.py` as a source file of the **existing** pipeline that contains Bronze. A separate pipeline also works, because Silver reads Bronze by its three-level name, but one pipeline keeps the dependency graph and the event log in one place.
2. Create the schema if it doesn't exist: `CREATE SCHEMA IF NOT EXISTS <catalog>.silver;`
3. Pipeline configuration:
   - `retail.catalog`: already set for Bronze.
   - `spark.sql.legacy.timeParserPolicy = CORRECTED`: **required**.
   - Optional: `retail.silver.scd_type`, `retail.silver.dedup_window`, `retail.silver.fk_grace_minutes`, `retail.silver.min_valid_date`, `retail.silver.max_quantity`, `retail.silver.enable_bronze_duplicate_audit`.
4. **API style:** if the Bronze code uses `from pyspark import pipelines as dp` instead of `import dlt`, switch Silver to the same style so the two are never mixed:
   - `dlt.view` → `dp.temporary_view`
   - `dlt.table` → `dp.materialized_view`
   - `dlt.create_streaming_table` → `dp.create_streaming_table`
   - `dlt.expect_all*` → `dp.expect_all*`
   - `dlt.create_auto_cdc_flow` → `dp.create_auto_cdc_flow`
5. **Runtime:** serverless, or a channel based on Spark 3.5 or later.
6. **Validate in DEV first:** run one update in a DEV catalog, then run `silver_tests.sql` and `silver_scenario_tests.py`.
7. **Schedule:** keep the triggered schedule from the architecture (e.g. every 5 minutes), or use continuous mode if your workspace allows it. Add `silver_tests.sql` as a SQL task after the pipeline task in the job.

**Monitoring.** Use `silver_monitoring.sql`:

| Query | What it shows |
|---|---|
| 1 | Throughput and backlog |
| 2 | Expectation metrics |
| 3 | Duration, state and retries |
| 4 | Errors |
| 5 | Silver lag behind Bronze |
| 6 | Arrival rate |
| 7–11 | Data-level counts: status, rules, quarantine queue, RI, duplicates |
| 12 | Tracing a record back to Bronze |

**Suggested SQL alerts.** These are the ones in the architecture: quarantine rate > 5% per hour, `silver_behind_bronze_s` above 10 minutes, and `ORPHANED` growing.

**Recovering from failures**
- **Failed update:** read query 4, fix the cause, and start a normal update. Do **not** full refresh unless the Silver logic changed.
- **Schema changes:**
  - *Additive* (a new source column): it lands in `_rescued_data`, or in `_raw` if Bronze adds it explicitly, and is flagged as a WARNING. To use it in Silver, add it to `COLUMNS` and write a standardiser and rules for it. AUTO CDC targets accept new columns on a normal update.
  - *Incompatible* (a removed or renamed column, or a changed type): the update fails at definition time. Change the code, then full refresh the affected Silver and quarantine tables from Bronze.

## G. Validation tests

| Requirement | Test |
|---|---|
| New batch processed | `silver_scenario_tests.py` assert1 (order 990000001 appears); query 5 lag returns to ≈ 0 after an update |
| Reprocessing creates no duplicates | `silver_tests.sql` T1, T2; scenario assert2 (re-delivered file → still one row in Silver and in quarantine) |
| Restart loses no committed data | Record `SELECT count(*)` per Silver table, stop the pipeline mid-update, start it again, then check the counts are ≥ the recorded ones, and T1, T2 and T9 pass |
| Invalid rows reach quarantine with all reasons | Scenario assert1 (990000003 has both rules); T3, T4, T7 |
| Warning rows stay in Silver | Scenario assert1 (990000004 is WARNING and standardised); T5 |
| Late parent resolves the reference | Scenario assert1 → PENDING; assert2 → resolved |
| Changed record with the same PK | Scenario assert2 (city Mumbai wins, one row; audit shows `CONFLICTING_VERSIONS`) |
| Unexpected columns are traceable | Scenario assert1 (`_rescued_data` set, `rescued_data_present`) |
| Nothing silently dropped | T9 (every well-formed Bronze PK is in Silver or quarantine) |
| Keeps up with the generator | Run the generator for 30 minutes and watch queries 1, 3 and 5. Lag must stay bounded and not trend upward. |

## Assumptions and rules that need confirmation

**Confirmed from documentation or data**
- Keys, foreign keys and the five relationships.
- Canonical formats: ISO dates, `+91-XXXXXXXXXX`, the SKU formula.
- Domains for region, state→region, order_status and payment_method.
- The null tokens and the observed date and currency formats.
- Records are entity snapshots: the source has no change timestamp.

**Assumptions to confirm with the business**

| Topic | Current rule | Question to confirm |
|---|---|---|
| Ambiguous dates | `nn/nn/yyyy` is rejected for orders and returns when both readings are valid | Is the source day-first (Indian convention)? If yes, parse it as `dd/MM/yyyy` instead. |
| Typo and abbreviation mapping | `Deliverd`, `Cancel` and `Loyalty Pts` are **not** mapped | Should they be? Extend `ORDER_STATUS_MAP` / `PAYMENT_METHOD_MAP` if so. |
| Mandatory fields | Mandatory: order_status, order_date, quantity, unit_price, return_date, refund_amount, price, cost_price. Optional: payment_method. | Is that split right? |
| Quantity limit | `max_quantity = 100` | The data only shows 1–5, and the injected bad values were 999/1000. What is the real ceiling? |
| Date bounds | `min_valid_date = 2000-01-01`; "future" means more than 1 day after ingestion | Are these bounds right? |
| FK grace period | 6 hours | Depends on how late parents can really arrive. |

**Cross-feed business rules left for Gold `dq_exceptions`** (as in the architecture): order before signup, return before order, refund larger than the item value, `unit_price` different from the list price, returns on non-Delivered orders, and the same email or phone on two customers. All of these need joins of current state across feeds, so they belong in a batch layer.
