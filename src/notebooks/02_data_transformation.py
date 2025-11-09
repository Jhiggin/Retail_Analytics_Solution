# Databricks notebook source
# MAGIC %md
# MAGIC # Data Transformation - Silver Layer
# MAGIC
# MAGIC This notebook transforms bronze data into cleaned and validated silver layer tables.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_analytics", "Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------

# Create silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Sales Data

# COMMAND ----------

# Read from bronze
df_sales_bronze = spark.table(f"{catalog}.{bronze_schema}.sales_raw")

# Clean and transform
df_sales_silver = (
    df_sales_bronze.dropDuplicates(["transaction_id", "transaction_date"])
    .filter(col("transaction_date").isNotNull())
    .filter(col("amount") > 0)
    .withColumn("transaction_date", to_date(col("transaction_date")))
    .withColumn("transaction_year", year(col("transaction_date")))
    .withColumn("transaction_month", month(col("transaction_date")))
    .withColumn("transaction_quarter", quarter(col("transaction_date")))
    .withColumn("processed_timestamp", current_timestamp())
    .select(
        "transaction_id",
        "customer_id",
        "product_id",
        "transaction_date",
        "transaction_year",
        "transaction_month",
        "transaction_quarter",
        "quantity",
        "amount",
        "discount",
        "store_id",
        "processed_timestamp",
    )
)

# Write to silver table
(
    df_sales_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{silver_schema}.sales")
)

print(f"Transformed {df_sales_silver.count()} sales records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Customer Data

# COMMAND ----------

# Read from bronze
df_customers_bronze = spark.table(f"{catalog}.{bronze_schema}.customers_raw")

# Clean and transform
df_customers_silver = (
    df_customers_bronze.dropDuplicates(["customer_id"])
    .filter(col("customer_id").isNotNull())
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))
    .withColumn("registration_date", to_date(col("registration_date")))
    .withColumn("processed_timestamp", current_timestamp())
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "address",
        "city",
        "state",
        "zip_code",
        "registration_date",
        "customer_segment",
        "processed_timestamp",
    )
)

# Write to silver table
(
    df_customers_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{silver_schema}.customers")
)

print(f"Transformed {df_customers_silver.count()} customer records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Product Data

# COMMAND ----------

# Read from bronze
df_products_bronze = spark.table(f"{catalog}.{bronze_schema}.products_raw")

# Clean and transform
df_products_silver = (
    df_products_bronze.dropDuplicates(["product_id"])
    .filter(col("product_id").isNotNull())
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("price", col("price").cast(DecimalType(10, 2)))
    .withColumn("processed_timestamp", current_timestamp())
    .select(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "price",
        "cost",
        "supplier_id",
        "processed_timestamp",
    )
)

# Write to silver table
(
    df_products_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{silver_schema}.products")
)

print(f"Transformed {df_products_silver.count()} product records")

# COMMAND ----------

print("=" * 80)
print("Data Transformation Complete!")
print("=" * 80)
