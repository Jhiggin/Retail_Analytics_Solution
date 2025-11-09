# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Promotions
# MAGIC Clean and validate promotions data from bronze layer, merge into silver.promotions table

# COMMAND

from pyspark.sql import functions as F

# COMMAND

CATALOG = spark.conf.get("catalog", "retail_dev")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read from Bronze Promotions

# COMMAND

df_promotions = spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.promotions")
print(f"Read {df_promotions.count()} records from bronze.promotions")

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Apply Quality Checks

# COMMAND

df_promotions_cleaned = (df_promotions
    .filter(F.col("PromotionID").isNotNull())
    .filter(F.col("StartDate").isNotNull())
    .filter(F.col("EndDate").isNotNull())
    .withColumn("_validated_at", F.current_timestamp())
)

print(f"After quality checks: {df_promotions_cleaned.count()} records")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Create or Get Silver Promotions Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.promotions (
        PromotionID STRING,
        PromotionType STRING,
        StartDate DATE,
        EndDate DATE,
        DiscountPercent DOUBLE,
        ProductID STRING,
        MarketingSpend DOUBLE,
        _validated_at TIMESTAMP,
        PRIMARY KEY (PromotionID)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{SILVER_SCHEMA}.promotions is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Merge into Silver Table

# COMMAND

df_promotions_cleaned.createOrReplaceTempView("silver_promotions_temp")

merge_query = f"""
MERGE INTO {CATALOG}.{SILVER_SCHEMA}.promotions target
USING (
    SELECT * FROM silver_promotions_temp
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
        _validated_at = source._validated_at
WHEN NOT MATCHED THEN
    INSERT (PromotionID, PromotionType, StartDate, EndDate, DiscountPercent, ProductID, MarketingSpend, _validated_at)
    VALUES (source.PromotionID, source.PromotionType, source.StartDate, source.EndDate, source.DiscountPercent, source.ProductID, source.MarketingSpend, source._validated_at)
"""

spark.sql(merge_query)
print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 5: Verify Silver Promotions Table

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{SILVER_SCHEMA}.promotions LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{SILVER_SCHEMA}.promotions").collect()[0][0]
print(f"Total rows in silver.promotions: {row_count}")
