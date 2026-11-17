# Bronze: one Auto Loader streaming table per feed. Owner: P2 (orchestration wiring: P1).
import dlt
from pyspark.sql import functions as F

CAT = spark.conf.get("retail.catalog")
LANDING = f"/Volumes/{CAT}/landing/files"
FEEDS = ["categories", "products", "customers", "orders", "order_items", "returns"]


def make_bronze(feed: str):
    @dlt.table(
        name=f"{CAT}.bronze.{feed}",
        comment=f"Raw {feed} rows exactly as received from the landing Volume",
        table_properties={"pipelines.reset.allowed": "false"},
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")      # everything lands as string
            .option("cloudFiles.schemaEvolutionMode", "rescue")  # surprises go to _rescued_data
            .load(f"{LANDING}/{feed}/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )
    return _bronze


for f in FEEDS:
    make_bronze(f)
