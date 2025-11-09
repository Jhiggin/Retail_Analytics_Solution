# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Bronze Layer
# MAGIC 
# MAGIC This notebook defines the bronze layer tables using Delta Live Tables.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Raw Sales Data

# COMMAND ----------

@dlt.table(
    name="bronze_sales",
    comment="Raw sales data ingested from source systems",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_sales():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", "/tmp/schemas/sales")
            .load("/mnt/raw/retail_data/sales/")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Raw Customer Data

# COMMAND ----------

@dlt.table(
    name="bronze_customers",
    comment="Raw customer data ingested from source systems",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_customers():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/tmp/schemas/customers")
            .load("/mnt/raw/retail_data/customers/")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Raw Product Data

# COMMAND ----------

@dlt.table(
    name="bronze_products",
    comment="Raw product data ingested from source systems",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_products():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "/tmp/schemas/products")
            .load("/mnt/raw/retail_data/products/")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
    )
