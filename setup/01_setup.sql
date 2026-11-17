-- Run once in the Databricks SQL editor (P1). If CREATE CATALOG fails on Free Edition,
-- replace "retail" with "workspace" everywhere here and in databricks.yml (variable catalog).
CREATE CATALOG IF NOT EXISTS retail;
CREATE SCHEMA IF NOT EXISTS retail.landing;
CREATE SCHEMA IF NOT EXISTS retail.bronze;
CREATE SCHEMA IF NOT EXISTS retail.silver;
CREATE SCHEMA IF NOT EXISTS retail.gold;
CREATE SCHEMA IF NOT EXISTS retail.kpi;
CREATE VOLUME IF NOT EXISTS retail.landing.source_csv;   -- the 6 original CSVs
CREATE VOLUME IF NOT EXISTS retail.landing.files;        -- generator output, one folder per feed
CREATE VOLUME IF NOT EXISTS retail.landing.conf;         -- state_region.csv, festivals.csv
