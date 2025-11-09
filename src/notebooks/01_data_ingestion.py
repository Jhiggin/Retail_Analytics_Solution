# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion - Bronze Layer
# MAGIC 
# MAGIC This notebook ingests raw data from source systems into the bronze layer.

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters
dbutils.widgets.text("source_path", "/mnt/raw/retail_data", "Source Path")
dbutils.widgets.text("catalog", "retail_analytics", "Catalog Name")
dbutils.widgets.text("schema", "bronze", "Schema Name")
dbutils.widgets.text("checkpoint_path", "/tmp/checkpoints/bronze", "Checkpoint Path")

source_path = dbutils.widgets.get("source_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

print(f"Source Path: {source_path}")
print(f"Target: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Sales Data

# COMMAND ----------

# Read raw sales data
df_sales = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{source_path}/sales/*.csv")
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# Write to bronze table
(df_sales.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.sales_raw")
)

print(f"Ingested {df_sales.count()} sales records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Customer Data

# COMMAND ----------

# Read raw customer data
df_customers = (spark.read
    .format("json")
    .load(f"{source_path}/customers/*.json")
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# Write to bronze table
(df_customers.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.customers_raw")
)

print(f"Ingested {df_customers.count()} customer records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Product Data

# COMMAND ----------

# Read raw product data
df_products = (spark.read
    .format("parquet")
    .load(f"{source_path}/products/*.parquet")
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# Write to bronze table
(df_products.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.products_raw")
)

print(f"Ingested {df_products.count()} product records")

# COMMAND ----------

# Display summary
print("=" * 80)
print("Data Ingestion Complete!")
print("=" * 80)
