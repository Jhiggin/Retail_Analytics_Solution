# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Store Dimension
# MAGIC Create or update store dimension table with Type 1 SCD (overwrite)

# COMMAND

from pyspark.sql import functions as F

# COMMAND

CATALOG = spark.conf.get("catalog", "retail_dev")
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read from Silver Stores

# COMMAND

df_stores = spark.sql(f"SELECT * FROM {CATALOG}.{SILVER_SCHEMA}.stores")
print(f"Read {df_stores.count()} store records from silver layer")
display(df_stores.limit(5))

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Create Store Dimension

# COMMAND

df_dim_store = (df_stores
    .withColumn("StoreKey", F.row_number().over(
        F.Window.partitionBy().orderBy(F.col("StoreID"))
    ))
    .select(
        F.col("StoreKey"),
        F.col("StoreID"),
        F.col("StoreName"),
        F.col("Region"),
        F.col("StoreType"),
        F.col("SquareFootage"),
        F.current_timestamp().alias("_updated_at")
    )
)

print(f"Processed {df_dim_store.count()} records")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Create or Get Gold Dim_Store Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.dim_store (
        StoreKey INT,
        StoreID STRING,
        StoreName STRING,
        Region STRING,
        StoreType STRING,
        SquareFootage INT,
        _updated_at TIMESTAMP,
        PRIMARY KEY (StoreKey)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{GOLD_SCHEMA}.dim_store is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Merge into Dimension (Type 1 SCD)

# COMMAND

df_dim_store.createOrReplaceTempView("dim_store_temp")

merge_query = f"""
MERGE INTO {CATALOG}.{GOLD_SCHEMA}.dim_store target
USING (
    SELECT * FROM dim_store_temp
) source
ON target.StoreID = source.StoreID
WHEN MATCHED THEN
    UPDATE SET
        StoreName = source.StoreName,
        Region = source.Region,
        StoreType = source.StoreType,
        SquareFootage = source.SquareFootage,
        _updated_at = source._updated_at
WHEN NOT MATCHED THEN
    INSERT (StoreKey, StoreID, StoreName, Region, StoreType, SquareFootage, _updated_at)
    VALUES (source.StoreKey, source.StoreID, source.StoreName, source.Region, source.StoreType, source.SquareFootage, source._updated_at)
"""

spark.sql(merge_query)
print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 5: Verify Dimension

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.dim_store LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{GOLD_SCHEMA}.dim_store").collect()[0][0]
print(f"Total rows in dim_store: {row_count}")
