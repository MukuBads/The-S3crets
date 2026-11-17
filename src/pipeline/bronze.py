# Bronze: one Auto Loader streaming table per feed, every column kept as text exactly as received.
# Owner: P2 (orchestration wiring: P1). Column order must match the CSV files (Spark maps CSV columns by position
# when a schema is given). Unexpected extra columns go to _rescued_data instead of failing the stream.
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CAT = spark.conf.get("retail.catalog")
LANDING = f"/Volumes/{CAT}/landing/files"

COLUMNS = {
    "categories":  ["category_id", "category_name"],
    "products":    ["product_id", "product_name", "category_id", "price", "cost_price"],
    "customers":   ["customer_id", "name", "email", "phone_number", "signup_date", "city", "state", "region"],
    "orders":      ["order_id", "customer_id", "order_date", "order_status", "payment_method"],
    "order_items": ["order_item_id", "order_id", "product_id", "sku", "quantity", "unit_price"],
    "returns":     ["return_id", "order_item_id", "return_date", "refund_amount"],
}


def make_bronze(feed: str, cols: list):
    schema = StructType([StructField(c, StringType(), True) for c in cols])

    @dlt.table(
        name=f"{CAT}.bronze.{feed}",
        comment=f"Raw {feed} rows exactly as received from the landing Volume (all text)",
        table_properties={"pipelines.reset.allowed": "false"},
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("multiLine", "false")
            .option("rescuedDataColumn", "_rescued_data")
            .schema(schema)
            .load(f"{LANDING}/{feed}/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<null>")) for c in cols]), 256))
        )
    return _bronze


for feed, cols in COLUMNS.items():
    make_bronze(feed, cols)
