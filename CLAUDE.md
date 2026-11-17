# CLAUDE.md: The-S3crets (Retail KPI streaming pipeline)

Claude Code reads this file at the start of each session. It describes the project, what is already decided, how to
work in this repo, and what to do next. **`contracts.md` is the schema source of truth. Read it before writing
Silver, Gold or KPI code.**

## Project in one paragraph

Hackathon project (NIT Trichy, Data Engineering track; 3-hour build, team of 5): "Enhancing Retailer Sales for Strategic
Decision-Making". A generator notebook replays 6 dirty CSVs (`data/raw/`) as a stream of small files into a Unity Catalog
Volume. One Lakeflow/DLT pipeline (Python, serverless, triggered every 5 min) runs Bronze (Auto Loader, all STRING) →
Silver (PySpark standardise + validate; failures go to `quarantine_<feed>`) → Gold (materialized views: dedup, dims, facts,
`dq_exceptions`) → KPI materialized views from a Python registry. Power BI Desktop reads the KPI tables via the free SQL
Warehouse (DirectQuery). CI/CD runs on GitHub Actions + Databricks Asset Bundles.

## Hard constraints (do not violate)

- **Mandated:** Databricks, **PySpark for all cleansing logic** (DLT Python API, no SQL-only cleansing), GitHub Actions, Power BI.
- **Free and open source only:** Databricks **Free Edition** (serverless only; one active pipeline; one 2X-Small SQL warehouse;
  5 concurrent job tasks). No Kafka, no paid Power BI service, no paid GitHub features, no classic clusters.
- **Keep it simple:** the judges expect a working demo, not an industry-grade pipeline. Prefer the smallest working change.
- **Bad data is never silently dropped:** it goes to quarantine with `failure_reasons`, or is reported in `gold.dq_exceptions`.
- Never commit secrets or tokens. Auth comes from the `DEFAULT` profile in `~/.databrickscfg` locally, and the GitHub
  secrets `DATABRICKS_HOST` / `DATABRICKS_TOKEN` in CI.

## Environment facts

- Workspace: `https://dbc-7661af1c-67ef.cloud.databricks.com` (Free Edition), user `badsewalmukund@gmail.com`.
- **Catalog: `workspace`** (Free Edition default; we cannot create catalogs). Schemas: `landing, bronze, silver, gold, kpi`.
- Volumes: `/Volumes/workspace/landing/source_csv/` (6 raw CSVs, uploaded), `/Volumes/workspace/landing/files/<feed>/`
  (generator output), `/Volumes/workspace/landing/conf/` (`state_region.csv`).
- Bundle: `retail_kpis`, target `dev` (development mode: names prefixed `[dev badsewalmukund]`, schedules paused unless set explicitly).
  Pipeline resource `retail_pipeline`; job `retail_every_5_min` (unpaused, runs the pipeline every 5 minutes).
- Local machine: macOS, zsh. Use `python3` (no `python`). Don't put inline `#` comments in commands meant for pasting.

## Commands

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements-dev.txt   # once
python3 -m pytest -q                                   # run before every commit
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run retail_pipeline -t dev           # one pipeline update
databricks fs ls dbfs:/Volumes/workspace/landing/files/
databricks fs cp <local> dbfs:/Volumes/workspace/landing/<volume>/<file> --overwrite
```
Generator: Workspace → Users → badsewalmukund@gmail.com → `.bundle/retail_kpis/dev/files/notebooks/generator` →
Serverless → widgets `catalog=workspace`, `max_files=3`, `reset=yes` → Run all. Use `max_files=0`, `reset=no` for the
full stream (144 files, one every 3 s, about 7 min).

## Repo layout and ownership

| Path | Owner | State |
|---|---|---|
| `databricks.yml`, `resources/pipeline.yml`, `.github/workflows/ci.yml` | P1 | done; uncomment silver/gold/kpi in `pipeline.yml` as they land |
| `notebooks/generator.py` | P2 | done (replays rows unchanged, date order, resumable) |
| `src/pipeline/bronze.py` | P2 | done (6 Auto Loader tables, explicit STRING schemas, `_ingested_at`, `_source_file`, `_row_hash`) |
| `src/retail/cleansing.py`, `src/pipeline/silver.py`, `tests/test_cleansing.py` | P3 | **TODO** |
| `src/pipeline/gold.py` | P4 | **TODO** |
| `src/retail/kpis.py`, `src/pipeline/kpi.py`, `tests/test_kpis.py` | P5 | **TODO** |
| `conf/state_region.csv` | P4 | done (28 states → NORTH/SOUTH/EAST/WEST, matches clean data) |
| `contracts.md` | all | schema, standardisation rules, validation codes, Gold and KPI definitions |
| `data/raw/` · `data/clean_reference/` · `data/quality/` | — | dirty input · ground truth · injected-issue log + report |
| `tests/test_config.py` | P1 | CI checks (region mapping, Bronze columns == CSV headers) |

## Decisions already made (don't reopen)

- One triggered DLT pipeline; Gold and KPI tables are **materialized views rebuilt each run**. This handles duplicates and
  late or out-of-order rows with batch logic: no watermarks, no stream state.
- SCD1 dims (latest row per key). Region is always **derived from `state`** via `conf/state_region.csv`; the source region is
  kept as `region_source`.
- Digital payment = UPI, Net Banking, Loyalty Points (COD is not). Canceled orders are excluded from sales, margin, growth,
  customer-base and Top 5 KPIs.
- Snapshot date **2026-11-15** for future-date checks. Ambiguous `xx/xx/yyyy` dates are read day-first and flagged `AMBIGUOUS_DATE`.
- KPIs: 5 baseline (Orders vs Returns Top 5; Region customer base + QoQ growth %; Product sales vs margin; % Digital
  payments; Sales growth QoQ %) + custom **Average order value** and **Cohort retention** (by first-order month).

## Coding conventions

- **Cleansing pattern:** each feed has `tag_<feed>(df) -> DataFrame`. It standardises, then adds
  `failure_reasons ARRAY<STRING>` (errors) and `dq_flags ARRAY<STRING>` (warnings). A rule whose condition is NULL counts as failed.
  In DLT: a `@dlt.view` calls `tag_<feed>`; `silver.<feed>` = `size(failure_reasons) = 0`;
  `silver.quarantine_<feed>` = `size(failure_reasons) > 0`. Functions are pure PySpark (no `dlt` import) so pytest can test them.
- **Table names** are fully qualified: `f"{CAT}.<schema>.<table>"` with `CAT = spark.conf.get("retail.catalog")`.
- **KPI registry:** `KPI(name, description, inputs: dict[alias -> "schema.table"], compute)`; `compute(inputs) -> DataFrame` is
  pure; `kpi.py` loops over `KPIS` creating one `@dlt.table(name=f"{CAT}.kpi.{k.name}")` each. Store numerators and
  denominators; divide-by-zero gives NULL.
- **Tests:** pytest + local SparkSession (`pyspark==3.5.3`, Java 17), one passing and one failing row per rule; KPI tests use
  hand-calculated expected values. Use `data/clean_reference/` and `data/quality/data_quality_issue_log.csv` to check
  that standardisation gives the clean values.
- New pipeline files must be added to `libraries` in `resources/pipeline.yml`.
- Small commits on `main` are fine for this hackathon. Always run `python3 -m pytest -q` before `git push`; CI deploys on push.

## Current status (update this when it changes)

- [x] Workspace schemas and Volumes created; 6 raw CSVs uploaded to `source_csv`
- [x] Dataset `dataset_final` committed; CLI installed and logged in (`DEFAULT` profile)
- [x] `databricks.yml` host set; catalog switched to `workspace`
- [ ] `databricks bundle deploy -t dev` succeeds (it failed earlier on catalog `retail`; rerun after the fix)
- [ ] Upload `conf/state_region.csv` to `/Volumes/workspace/landing/conf/`
- [ ] Generator: first 3 files; pipeline run; Bronze row counts checked (customers 30,000 after file 1)
- [ ] Silver (P3) → Gold (P4) → KPIs (P5) → Power BI (P5) → demo rehearsal

## Known risks / unverified on Databricks

- Pipeline files importing `retail.*` from `src/`. Fallback: `sys.path` insert of the bundle's `src` folder, or inline the functions.
- Multi-schema publishing (`workspace.bronze.x` from one pipeline). Fallback: one schema with `bronze_` / `silver_` / ... prefixes.
- `bundle run ... --full-refresh <tables>` syntax: check `databricks bundle run --help`.
- Bronze has `pipelines.reset.allowed = false`. A clean restart needs `landing/files` cleared and the property temporarily removed.

## Reference docs

- `contracts.md` (schema), `data/README.md` (dataset author notes), `data/quality/data_quality_report.md`
- HLD (older dataset version, needs refresh): https://claude.ai/artifact/7KtrUgGhLPvsreQuw7EJ3i
- Work split and timeline: https://claude.ai/artifact/NqwE3amTzJrmn1n5QankqG
