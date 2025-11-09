# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Promotion Dimension
# MAGIC Create or update promotion dimension table with Type 1 SCD (overwrite)

# COMMAND

from pyspark.sql import functions as F

# COMMAND

CATALOG = spark.conf.get("catalog", "retail_dev")
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read from Silver Promotions

# COMMAND

df_promotions = spark.sql(f"SELECT * FROM {CATALOG}.{SILVER_SCHEMA}.promotions")
print(f"Read {df_promotions.count()} promotion records from silver layer")
display(df_promotions.limit(5))

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Create Promotion Dimension

# COMMAND

df_dim_promotion = (df_promotions
    .withColumn("PromotionKey", F.row_number().over(
        F.Window.partitionBy().orderBy(F.col("PromotionID"))
    ))
    .select(
        F.col("PromotionKey"),
        F.col("PromotionID"),
        F.col("PromotionType"),
        F.col("StartDate"),
        F.col("EndDate"),
        F.col("DiscountPercent"),
        F.col("MarketingSpend"),
        F.current_timestamp().alias("_updated_at")
    )
)

print(f"Processed {df_dim_promotion.count()} records")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Create or Get Gold Dim_Promotion Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.dim_promotion (
        PromotionKey INT,
        PromotionID STRING,
        PromotionType STRING,
        StartDate DATE,
        EndDate DATE,
        DiscountPercent DOUBLE,
        MarketingSpend DOUBLE,
        _updated_at TIMESTAMP,
        PRIMARY KEY (PromotionKey)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{GOLD_SCHEMA}.dim_promotion is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Merge into Dimension (Type 1 SCD)

# COMMAND

df_dim_promotion.createOrReplaceTempView("dim_promotion_temp")

merge_query = f"""
MERGE INTO {CATALOG}.{GOLD_SCHEMA}.dim_promotion target
USING (
    SELECT * FROM dim_promotion_temp
) source
ON target.PromotionID = source.PromotionID
WHEN MATCHED THEN
    UPDATE SET
        PromotionType = source.PromotionType,
        StartDate = source.StartDate,
        EndDate = source.EndDate,
        DiscountPercent = source.DiscountPercent,
        MarketingSpend = source.MarketingSpend,
        _updated_at = source._updated_at
WHEN NOT MATCHED THEN
    INSERT (PromotionKey, PromotionID, PromotionType, StartDate, EndDate, DiscountPercent, MarketingSpend, _updated_at)
    VALUES (source.PromotionKey, source.PromotionID, source.PromotionType, source.StartDate, source.EndDate, source.DiscountPercent, source.MarketingSpend, source._updated_at)
"""

spark.sql(merge_query)
print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 5: Verify Dimension

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.dim_promotion LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{GOLD_SCHEMA}.dim_promotion").collect()[0][0]
print(f"Total rows in dim_promotion: {row_count}")
