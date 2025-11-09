# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Stores
# MAGIC Clean and validate stores data from bronze layer, merge into silver.stores table

# COMMAND

from pyspark.sql import functions as F

# COMMAND

CATALOG = spark.conf.get("catalog", "retail_dev")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read from Bronze Stores

# COMMAND

df_stores = spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.stores")
print(f"Read {df_stores.count()} records from bronze.stores")

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Apply Quality Checks

# COMMAND

df_stores_cleaned = (df_stores
    .filter(F.col("StoreID").isNotNull())
    .filter(F.col("StoreName").isNotNull())
    .filter(F.col("SquareFootage") > 0)
    .withColumn("_validated_at", F.current_timestamp())
)

print(f"After quality checks: {df_stores_cleaned.count()} records")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Create or Get Silver Stores Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.stores (
        StoreID STRING,
        StoreName STRING,
        Region STRING,
        StoreType STRING,
        SquareFootage INT,
        _validated_at TIMESTAMP,
        PRIMARY KEY (StoreID)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{SILVER_SCHEMA}.stores is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Merge into Silver Table

# COMMAND

df_stores_cleaned.createOrReplaceTempView("silver_stores_temp")

merge_query = f"""
MERGE INTO {CATALOG}.{SILVER_SCHEMA}.stores target
USING (
    SELECT * FROM silver_stores_temp
) source
ON target.StoreID = source.StoreID
WHEN MATCHED THEN
    UPDATE SET
        StoreName = source.StoreName,
        Region = source.Region,
        StoreType = source.StoreType,
        SquareFootage = source.SquareFootage,
        _validated_at = source._validated_at
WHEN NOT MATCHED THEN
    INSERT (StoreID, StoreName, Region, StoreType, SquareFootage, _validated_at)
    VALUES (source.StoreID, source.StoreName, source.Region, source.StoreType, source.SquareFootage, source._validated_at)
"""

spark.sql(merge_query)
print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 5: Verify Silver Stores Table

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{SILVER_SCHEMA}.stores LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{SILVER_SCHEMA}.stores").collect()[0][0]
print(f"Total rows in silver.stores: {row_count}")
