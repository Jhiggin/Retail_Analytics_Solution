# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Sales
# MAGIC Clean and validate sales data from bronze layer, merge into silver.sales table

# COMMAND

from pyspark.sql import functions as F

# COMMAND

# Configuration
CATALOG = spark.conf.get("catalog", "retail_dev")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read from Bronze Sales

# COMMAND

df_sales = spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.sales")
print(f"Read {df_sales.count()} records from bronze.sales")

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Apply Quality Checks and Cleaning

# COMMAND

# Apply quality checks:
# - TransactionID must not be null
# - Date must not be null
# - TotalSales must be > 0
# - Remove records that fail quality checks

df_sales_cleaned = (df_sales
    .filter(F.col("TransactionID").isNotNull())
    .filter(F.col("Date").isNotNull())
    .filter(F.col("TotalSales") > 0)
    .withColumn("_validated_at", F.current_timestamp())
)

print(f"After quality checks: {df_sales_cleaned.count()} records")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Create or Get Silver Sales Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.sales (
        TransactionID STRING,
        Date DATE,
        StoreID STRING,
        ProductID STRING,
        Quantity INT,
        UnitPrice DOUBLE,
        DiscountAmount DOUBLE,
        TotalSales DOUBLE,
        PromotionID STRING,
        _validated_at TIMESTAMP,
        PRIMARY KEY (TransactionID)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{SILVER_SCHEMA}.sales is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Merge into Silver Table

# COMMAND

df_sales_cleaned.createOrReplaceTempView("silver_sales_temp")

merge_query = f"""
MERGE INTO {CATALOG}.{SILVER_SCHEMA}.sales target
USING (
    SELECT * FROM silver_sales_temp
) source
ON target.TransactionID = source.TransactionID
WHEN MATCHED THEN
    UPDATE SET
        Date = source.Date,
        StoreID = source.StoreID,
        ProductID = source.ProductID,
        Quantity = source.Quantity,
        UnitPrice = source.UnitPrice,
        DiscountAmount = source.DiscountAmount,
        TotalSales = source.TotalSales,
        PromotionID = source.PromotionID,
        _validated_at = source._validated_at
WHEN NOT MATCHED THEN
    INSERT (TransactionID, Date, StoreID, ProductID, Quantity, UnitPrice, DiscountAmount, TotalSales, PromotionID, _validated_at)
    VALUES (source.TransactionID, source.Date, source.StoreID, source.ProductID, source.Quantity, source.UnitPrice, source.DiscountAmount, source.TotalSales, source.PromotionID, source._validated_at)
"""

spark.sql(merge_query)
print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 5: Verify Silver Sales Table

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{SILVER_SCHEMA}.sales LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{SILVER_SCHEMA}.sales").collect()[0][0]
print(f"Total rows in silver.sales: {row_count}")
