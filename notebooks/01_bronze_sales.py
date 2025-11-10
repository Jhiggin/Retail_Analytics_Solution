# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Sales
# MAGIC Read sales data from ADLS once and merge into bronze.sales table

# COMMAND

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND

# Configuration from orchestration job parameters (or defaults)
ADLS_BASE_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in"
CATALOG = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else spark.conf.get("catalog", "retail_dev")
BRONZE_SCHEMA = "bronze"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read Sales Data from ADLS (One-time read)

# COMMAND

# Read sales CSV from ADLS
df_sales = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{ADLS_BASE_PATH}/sales.csv")
)

display(df_sales)

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Create or Get Bronze Sales Table

# COMMAND

# Create bronze schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# Create bronze.sales table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.sales (
        TransactionID STRING,
        Date DATE,
        StoreID STRING,
        ProductID STRING,
        Quantity INT,
        UnitPrice DOUBLE,
        DiscountAmount DOUBLE,
        TotalSales DOUBLE,
        PromotionID STRING,
        _loaded_at TIMESTAMP,
        PRIMARY KEY (TransactionID)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{BRONZE_SCHEMA}.sales is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Merge New Data into Bronze Table

# COMMAND

# Add load timestamp
df_sales_with_timestamp = df_sales.withColumn("_loaded_at", F.current_timestamp())

# Convert Date column to DATE type if needed
df_sales_with_timestamp = df_sales_with_timestamp.withColumn("Date", F.col("Date").cast("date"))

# Merge into bronze.sales table
merge_query = f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.sales target
USING (
    SELECT * FROM (
        SELECT
            TransactionID,
            Date,
            StoreID,
            ProductID,
            Quantity,
            UnitPrice,
            DiscountAmount,
            TotalSales,
            PromotionID,
            _loaded_at
        FROM bronze_sales_temp
    )
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
        _loaded_at = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (TransactionID, Date, StoreID, ProductID, Quantity, UnitPrice, DiscountAmount, TotalSales, PromotionID, _loaded_at)
    VALUES (source.TransactionID, source.Date, source.StoreID, source.ProductID, source.Quantity, source.UnitPrice, source.DiscountAmount, source.TotalSales, source.PromotionID, source._loaded_at)
"""

# Register temp view for merge
df_sales_with_timestamp.createOrReplaceTempView("bronze_sales_temp")

# Execute merge
spark.sql(merge_query)

print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Verify Bronze Sales Table

# COMMAND

# Display final result
result_df = spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.sales LIMIT 10")
display(result_df)

# Get row count
row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{BRONZE_SCHEMA}.sales").collect()[0][0]
print(f"Total rows in bronze.sales: {row_count}")
