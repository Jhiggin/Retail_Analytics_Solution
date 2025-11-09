# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Sales Fact Table
# MAGIC Create fact table with foreign keys to dimensions

# COMMAND

from pyspark.sql import functions as F

# COMMAND

CATALOG = spark.conf.get("catalog", "retail_dev")
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read Source Data

# COMMAND

df_sales = spark.sql(f"SELECT * FROM {CATALOG}.{SILVER_SCHEMA}.sales")
print(f"Read {df_sales.count()} sales records from silver layer")

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Join with Dimensions to Get Surrogate Keys

# COMMAND

# Build fact table with surrogate keys
df_fact_sales = (df_sales
    .join(
        spark.sql(f"SELECT StoreKey, StoreID FROM {CATALOG}.{GOLD_SCHEMA}.dim_store"),
        on="StoreID",
        how="left"
    )
    .join(
        spark.sql(f"SELECT ProductKey, ProductID FROM {CATALOG}.{GOLD_SCHEMA}.dim_product"),
        on="ProductID",
        how="left"
    )
    .join(
        spark.sql(f"SELECT PromotionKey, PromotionID FROM {CATALOG}.{GOLD_SCHEMA}.dim_promotion"),
        on="PromotionID",
        how="left"
    )
    .join(
        spark.sql(f"SELECT DateKey, Date FROM {CATALOG}.{GOLD_SCHEMA}.dim_date"),
        on="Date",
        how="left"
    )
    .select(
        F.col("TransactionID"),
        F.col("DateKey"),
        F.col("StoreKey"),
        F.col("ProductKey"),
        F.coalesce(F.col("PromotionKey"), F.lit(-1)).alias("PromotionKey"),  # -1 for null promotion
        F.col("Quantity"),
        F.col("UnitPrice"),
        F.col("DiscountAmount"),
        F.col("TotalSales"),
        F.current_timestamp().alias("_created_at")
    )
)

print(f"Processed {df_fact_sales.count()} fact records")
display(df_fact_sales.limit(5))

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Create or Get Gold Fact_Sales Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.fact_sales (
        TransactionID STRING,
        DateKey DATE,
        StoreKey INT,
        ProductKey INT,
        PromotionKey INT,
        Quantity INT,
        UnitPrice DOUBLE,
        DiscountAmount DOUBLE,
        TotalSales DOUBLE,
        _created_at TIMESTAMP,
        PRIMARY KEY (TransactionID)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{GOLD_SCHEMA}.fact_sales is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Merge into Fact Table (Facts are typically immutable, but we handle updates)

# COMMAND

df_fact_sales.createOrReplaceTempView("fact_sales_temp")

merge_query = f"""
MERGE INTO {CATALOG}.{GOLD_SCHEMA}.fact_sales target
USING (
    SELECT * FROM fact_sales_temp
) source
ON target.TransactionID = source.TransactionID
WHEN MATCHED THEN
    UPDATE SET
        DateKey = source.DateKey,
        StoreKey = source.StoreKey,
        ProductKey = source.ProductKey,
        PromotionKey = source.PromotionKey,
        Quantity = source.Quantity,
        UnitPrice = source.UnitPrice,
        DiscountAmount = source.DiscountAmount,
        TotalSales = source.TotalSales
WHEN NOT MATCHED THEN
    INSERT (TransactionID, DateKey, StoreKey, ProductKey, PromotionKey, Quantity, UnitPrice, DiscountAmount, TotalSales, _created_at)
    VALUES (source.TransactionID, source.DateKey, source.StoreKey, source.ProductKey, source.PromotionKey, source.Quantity, source.UnitPrice, source.DiscountAmount, source.TotalSales, source._created_at)
"""

spark.sql(merge_query)
print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 5: Verify Fact Table

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.fact_sales LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{GOLD_SCHEMA}.fact_sales").collect()[0][0]
print(f"Total rows in fact_sales: {row_count}")

# COMMAND

# MAGIC %md
# MAGIC ## Verify Star Schema Relationships

# COMMAND

result_df = spark.sql(f"""
    SELECT
        f.TransactionID,
        d.Date,
        s.StoreName,
        p.ProductName,
        pr.PromotionType,
        f.Quantity,
        f.TotalSales
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_sales f
    LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_date d ON f.DateKey = d.DateKey
    LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_store s ON f.StoreKey = s.StoreKey
    LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_product p ON f.ProductKey = p.ProductKey
    LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_promotion pr ON f.PromotionKey = pr.PromotionKey
    LIMIT 10
""")

display(result_df)
