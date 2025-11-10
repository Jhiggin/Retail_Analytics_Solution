# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Stores
# MAGIC Read stores data from ADLS once and merge into bronze.stores table

# COMMAND

from pyspark.sql import functions as F

# COMMAND

# Configuration
ADLS_BASE_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in"
CATALOG = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else spark.conf.get("catalog", "retail_dev")
BRONZE_SCHEMA = "bronze"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read Stores Data from ADLS (One-time read)

# COMMAND

df_stores = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{ADLS_BASE_PATH}/stores.csv")
)

display(df_stores)

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Create or Get Bronze Stores Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.stores (
        StoreID STRING,
        StoreName STRING,
        Region STRING,
        StoreType STRING,
        SquareFootage INT,
        _loaded_at TIMESTAMP,
        PRIMARY KEY (StoreID)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{BRONZE_SCHEMA}.stores is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Merge New Data into Bronze Table

# COMMAND

df_stores_with_timestamp = df_stores.withColumn("_loaded_at", F.current_timestamp())

merge_query = f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.stores target
USING (
    SELECT * FROM bronze_stores_temp
) source
ON target.StoreID = source.StoreID
WHEN MATCHED THEN
    UPDATE SET
        StoreName = source.StoreName,
        Region = source.Region,
        StoreType = source.StoreType,
        SquareFootage = source.SquareFootage,
        _loaded_at = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (StoreID, StoreName, Region, StoreType, SquareFootage, _loaded_at)
    VALUES (source.StoreID, source.StoreName, source.Region, source.StoreType, source.SquareFootage, source._loaded_at)
"""

df_stores_with_timestamp.createOrReplaceTempView("bronze_stores_temp")
spark.sql(merge_query)

print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Verify Bronze Stores Table

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.stores LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{BRONZE_SCHEMA}.stores").collect()[0][0]
print(f"Total rows in bronze.stores: {row_count}")
