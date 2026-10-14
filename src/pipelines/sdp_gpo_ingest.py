# Databricks notebook source
# Lakeflow Spark Declarative Pipeline: UC Volume -> Bronze -> Silver
# Events originate from Azure Event Hubs, are consumed locally, and landed as JSON
# in a UC Volume (the demo workspace VNet cannot egress to Event Hubs). Auto Loader
# reads only the internal Volume. Catalog/schema come from the pipeline settings.
from pyspark import pipelines as dp
from pyspark.sql.functions import col

# Catalog/schema come from the pipeline configuration (set by the DAB bundle).
CATALOG = spark.conf.get("gpo.catalog", "main")
SCHEMA = spark.conf.get("gpo.schema", "gpo_spend_intelligence")
SRC = f"/Volumes/{CATALOG}/{SCHEMA}/po_landing"


@dp.table(name="po_events_bronze_sdp",
          comment="Bronze: Auto Loader ingest of PO/point-of-use events from the UC Volume")
def po_events_bronze_sdp():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(SRC)
            .selectExpr("*", "_metadata.file_path AS source_file"))


@dp.table(name="po_events_silver_sdp",
          comment="Silver: typed, curated PO/point-of-use events")
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
def po_events_silver_sdp():
    return (spark.readStream.table("po_events_bronze_sdp")
            .select(
                col("event_id"), col("event_type"), col("event_time"),
                col("member_id"), col("supplier_id"), col("category"),
                col("item_sku"), col("quantity").cast("int"),
                col("unit_cost").cast("double"), col("amount").cast("double"),
                col("contract_id"), col("on_contract").cast("boolean"),
                col("source_file")))
