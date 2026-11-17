# Retail KPIs: 3-hour build

Generator -> landing Volume -> one DLT pipeline (Bronze -> Silver/quarantine -> Gold -> KPI) -> Power BI Desktop.
Free tools only: Databricks Free Edition, GitHub Actions, Power BI Desktop.

## Orchestration quick start (P1)
1. Run `setup/01_setup.sql` in the workspace SQL editor.
2. Upload the 6 CSVs to `/Volumes/retail/landing/source_csv/` and `conf/*.csv` to `/Volumes/retail/landing/conf/`.
3. Put the workspace URL in `databricks.yml`, then `databricks auth login --host <url>`.
4. Add GitHub secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` (Settings > Developer > Access tokens in Databricks).
5. Make sure every `landing/files/<feed>/` folder has at least one CSV (start the generator first).
6. `databricks bundle validate -t dev && databricks bundle deploy -t dev`
7. `databricks bundle run retail_pipeline -t dev`  -> 6 Bronze tables appear.
8. When Silver/Gold/KPI files are ready, uncomment them in `resources/pipeline.yml`, push to main, CI deploys.
9. Unpause the `retail-every-5-min` job in the Jobs UI after integration.
