# Databricks notebook source
# MAGIC %md
# MAGIC # Silver scenario tests (run in a DEV catalog only)
# MAGIC
# MAGIC Drops small fixture CSVs into the landing Volume and checks how Silver reacts. Bronze is append-only,
# MAGIC so fixture rows stay in Bronze; all fixture keys start with `9900000`, so they are easy to filter.
# MAGIC
# MAGIC Run order: `phase = setup1` -> pipeline update -> `assert1` -> `setup2` -> pipeline update -> `assert2`.
# MAGIC If `pipeline_id` is filled in and the pipeline is in triggered mode, setup phases start the update and
# MAGIC wait for it.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("phase", "setup1", ["setup1", "assert1", "setup2", "assert2"])
dbutils.widgets.text("pipeline_id", "")

CAT = dbutils.widgets.get("catalog")
PHASE = dbutils.widgets.get("phase")
PIPELINE_ID = dbutils.widgets.get("pipeline_id").strip()
LANDING = f"/Volumes/{CAT}/landing/files"

assert not CAT.lower().endswith("prod"), "scenario tests write fixture files - never run against prod"

HEADERS = {
    "customers": "customer_id,name,email,phone_number,signup_date,city,state,region",
    "orders": "order_id,customer_id,order_date,order_status,payment_method",
}

# COMMAND ----------

import time


def put(feed: str, name: str, rows: list[str], header: str | None = None) -> None:
    path = f"{LANDING}/{feed}/{name}"
    dbutils.fs.put(path, "\n".join([header or HEADERS[feed], *rows]) + "\n", overwrite=True)
    print("wrote", path)


def run_pipeline_if_configured() -> None:
    if not PIPELINE_ID:
        print("pipeline_id not set: start a pipeline update manually, then run the next phase.")
        return
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    update_id = w.pipelines.start_update(pipeline_id=PIPELINE_ID, full_refresh=False).update_id
    while True:
        state = str(w.pipelines.get_update(pipeline_id=PIPELINE_ID, update_id=update_id).update.state)
        if any(s in state for s in ("COMPLETED", "FAILED", "CANCELED")):
            print("pipeline update", update_id, state)
            assert "COMPLETED" in state, f"pipeline update ended in {state}"
            return
        time.sleep(15)


def one(sql: str):
    return spark.sql(sql).collect()  # small fixture-scoped result sets only


def check(cond: bool, msg: str) -> None:
    print(("PASS " if cond else "FAIL ") + msg)
    assert cond, msg


ORDERS_P1 = [
    "990000001,990000001,2026-11-10,Delivered,UPI",              # parent customer arrives only in phase 2
    "990000003,990000002,2025-02-30,Deliverd,UPI",               # 2 reasons -> quarantine
    "990000004,990000002,10-11-2026,delivered,COD",              # usable after standardisation -> WARNING
]

# COMMAND ----------

if PHASE == "setup1":
    put("customers", "zz_test_p1_customers.csv", [
        "990000002,Test Kumar,test.kumar@example.com,+91-9876500002,2026-01-15,Pune,Maharashtra,WEST",
    ])
    put("customers", "zz_test_p1_customers_extra_col.csv",   # unexpected column -> _rescued_data
        ["990000005,Test Rescue,test.rescue@example.com,+91-9876500005,2026-01-15,Pune,Maharashtra,WEST,GOLD"],
        header=HEADERS["customers"] + ",loyalty_tier")
    put("orders", "zz_test_p1_orders.csv", ORDERS_P1)
    run_pipeline_if_configured()

# COMMAND ----------

if PHASE == "assert1":
    r = one(f"SELECT quality_status FROM {CAT}.silver.orders WHERE order_id = '990000001'")
    check(len(r) == 1 and r[0][0] == "VALID", "new batch processed: order 990000001 in Silver as VALID")

    r = one(f"""SELECT ri_status FROM {CAT}.silver.referential_integrity_exceptions
                WHERE feed = 'orders' AND record_pk = '990000001'""")
    check(len(r) == 1 and r[0][0] == "UNRESOLVED_PENDING", "missing parent is PENDING, not quarantined")

    r = one(f"""SELECT transform(quality_issues, x -> x.rule) FROM {CAT}.silver.quarantine_orders
                WHERE record_pk = '990000003'""")
    check(len(r) == 1 and {"order_date_unparseable", "order_status_not_allowed"} <= set(r[0][0]),
          "invalid row quarantined once with ALL reasons")
    check(one(f"SELECT count(*) FROM {CAT}.silver.orders WHERE order_id = '990000003'")[0][0] == 0,
          "invalid row is not in Silver")

    r = one(f"""SELECT quality_status, order_status, payment_method, order_date,
                       transform(quality_issues, x -> x.rule)
                FROM {CAT}.silver.orders WHERE order_id = '990000004'""")
    check(len(r) == 1 and r[0][0] == "WARNING" and r[0][1] == "Delivered" and r[0][2] == "Cash on Delivery"
          and str(r[0][3]) == "2026-11-10", "warning row stays in Silver, standardised, with warning reasons")

    r = one(f"""SELECT _rescued_data, transform(quality_issues, x -> x.rule)
                FROM {CAT}.silver.customers WHERE customer_id = '990000005'""")
    check(len(r) == 1 and r[0][0] is not None and "rescued_data_present" in r[0][1],
          "unexpected column is preserved in _rescued_data and flagged")

# COMMAND ----------

if PHASE == "setup2":
    put("customers", "zz_test_p2_customers.csv", [
        "990000001,Late Parent,late.parent@example.com,+91-9876500001,2026-01-10,Kochi,Kerala,SOUTH",
        "990000002,Test Kumar,test.kumar@example.com,+91-9876500002,2026-01-15,Mumbai,Maharashtra,WEST",  # changed city
    ])
    put("orders", "zz_test_p2_orders_redelivery.csv", ORDERS_P1)                         # same rows, new file
    put("orders", "zz_test_p2_orders_fix.csv", ["990000003,990000002,2026-11-12,Delivered,UPI"])  # corrected
    run_pipeline_if_configured()

# COMMAND ----------

if PHASE == "assert2":
    check(one(f"""SELECT count(*) FROM {CAT}.silver.referential_integrity_exceptions
                  WHERE feed = 'orders' AND record_pk = '990000001'""")[0][0] == 0,
          "late parent resolved the earlier unresolved reference without reprocessing")

    r = one(f"SELECT city, count(*) OVER () FROM {CAT}.silver.customers WHERE customer_id = '990000002'")
    check(len(r) == 1 and r[0][0] == "Mumbai", "changed record with same PK: one row, latest version wins")

    r = one(f"""SELECT order_id, count(*) FROM {CAT}.silver.orders
                WHERE order_id IN ('990000001', '990000003', '990000004') GROUP BY order_id""")
    check(len(r) == 3 and all(c == 1 for _, c in r), "re-delivered batch created no duplicate Silver rows")

    check(one(f"SELECT count(*) FROM {CAT}.silver.quarantine_orders WHERE record_pk = '990000003'")[0][0] == 1,
          "re-delivered invalid row did not create a second quarantine row")

    r = one(f"""SELECT resolution_status FROM {CAT}.silver.quarantine_all
                WHERE feed = 'orders' AND record_pk = '990000003'""")
    check(len(r) == 1 and r[0][0] == "SUPERSEDED_BY_ACCEPTED_VERSION",
          "corrected record admitted to Silver; quarantine entry marked superseded")

    r = dict(one(f"""SELECT record_pk, duplicate_type FROM {CAT}.silver.dq_bronze_duplicate_keys
                     WHERE record_pk IN ('990000002', '990000004')"""))
    check(r.get("990000002") == "CONFLICTING_VERSIONS" and r.get("990000004") == "EXACT_REDELIVERY",
          "duplicate audit distinguishes changed versions from exact re-deliveries")
