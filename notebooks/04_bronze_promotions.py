# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Promotions
# MAGIC Read promotions data from ADLS once and merge into bronze.promotions table

# COMMAND

from pyspark.sql import functions as F

# COMMAND

# Configuration
ADLS_BASE_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in"
CATALOG = spark.conf.get("catalog", "retail_dev")
BRONZE_SCHEMA = "bronze"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read Promotions Data from ADLS (One-time read)

# COMMAND

df_promotions = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{ADLS_BASE_PATH}/promotions.csv")
)

display(df_promotions)

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Create or Get Bronze Promotions Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.promotions (
        PromotionID STRING,
        PromotionType STRING,
        StartDate DATE,
        EndDate DATE,
        DiscountPercent DOUBLE,
        ProductID STRING,
        MarketingSpend DOUBLE,
        _loaded_at TIMESTAMP,
        PRIMARY KEY (PromotionID)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{BRONZE_SCHEMA}.promotions is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Merge New Data into Bronze Table

# COMMAND

df_promotions_with_timestamp = df_promotions.withColumn("_loaded_at", F.current_timestamp())
df_promotions_with_timestamp = df_promotions_with_timestamp.withColumn("StartDate", F.col("StartDate").cast("date"))
df_promotions_with_timestamp = df_promotions_with_timestamp.withColumn("EndDate", F.col("EndDate").cast("date"))

merge_query = f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.promotions target
USING (
    SELECT * FROM bronze_promotions_temp
) source
ON target.PromotionID = source.PromotionID
WHEN MATCHED THEN
    UPDATE SET
        PromotionType = source.PromotionType,
        StartDate = source.StartDate,
        EndDate = source.EndDate,
        DiscountPercent = source.DiscountPercent,
        ProductID = source.ProductID,
        MarketingSpend = source.MarketingSpend,
        _loaded_at = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (PromotionID, PromotionType, StartDate, EndDate, DiscountPercent, ProductID, MarketingSpend, _loaded_at)
    VALUES (source.PromotionID, source.PromotionType, source.StartDate, source.EndDate, source.DiscountPercent, source.ProductID, source.MarketingSpend, source._loaded_at)
"""

df_promotions_with_timestamp.createOrReplaceTempView("bronze_promotions_temp")
spark.sql(merge_query)

print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Verify Bronze Promotions Table

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.promotions LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{BRONZE_SCHEMA}.promotions").collect()[0][0]
print(f"Total rows in bronze.promotions: {row_count}")
