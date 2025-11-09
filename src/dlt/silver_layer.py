# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Silver Layer
# MAGIC 
# MAGIC This notebook defines the silver layer tables with data quality constraints.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Clean Sales Data

# COMMAND ----------

@dlt.table(
    name="silver_sales",
    comment="Cleaned and validated sales data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect("valid_date", "transaction_date IS NOT NULL")
def silver_sales():
    return (
        dlt.read_stream("bronze_sales")
        .withColumn("transaction_date", F.to_date(F.col("transaction_date")))
        .withColumn("transaction_year", F.year(F.col("transaction_date")))
        .withColumn("transaction_month", F.month(F.col("transaction_date")))
        .withColumn("transaction_quarter", F.quarter(F.col("transaction_date")))
        .withColumn("amount", F.col("amount").cast(DecimalType(10, 2)))
        .withColumn("processed_timestamp", F.current_timestamp())
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Clean Customer Data

# COMMAND ----------

@dlt.table(
    name="silver_customers",
    comment="Cleaned and validated customer data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dlt.expect("has_name", "first_name IS NOT NULL AND last_name IS NOT NULL")
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("phone", F.regexp_replace(F.col("phone"), "[^0-9]", ""))
        .withColumn("registration_date", F.to_date(F.col("registration_date")))
        .withColumn("processed_timestamp", F.current_timestamp())
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Clean Product Data

# COMMAND ----------

@dlt.table(
    name="silver_products",
    comment="Cleaned and validated product data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect("has_name", "product_name IS NOT NULL")
def silver_products():
    return (
        dlt.read_stream("bronze_products")
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("price", F.col("price").cast(DecimalType(10, 2)))
        .withColumn("cost", F.col("cost").cast(DecimalType(10, 2)))
        .withColumn("processed_timestamp", F.current_timestamp())
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
