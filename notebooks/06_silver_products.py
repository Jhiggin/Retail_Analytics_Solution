# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Products
# MAGIC Clean and validate products data from bronze layer, merge into silver.products table

# COMMAND

from pyspark.sql import functions as F

# COMMAND

CATALOG = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else spark.conf.get("catalog", "retail_dev")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read from Bronze Products

# COMMAND

df_products = spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.products")
print(f"Read {df_products.count()} records from bronze.products")

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Apply Quality Checks

# COMMAND

df_products_cleaned = (df_products
    .filter(F.col("ProductID").isNotNull())
    .filter(F.col("ProductName").isNotNull())
    .filter(F.col("BasePrice") > 0)
    .withColumn("_validated_at", F.current_timestamp())
)

print(f"After quality checks: {df_products_cleaned.count()} records")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Create or Get Silver Products Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.products (
        ProductID STRING,
        Category STRING,
        Brand STRING,
        ProductName STRING,
        BasePrice DOUBLE,
        _validated_at TIMESTAMP,
        PRIMARY KEY (ProductID)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{SILVER_SCHEMA}.products is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Merge into Silver Table

# COMMAND

df_products_cleaned.createOrReplaceTempView("silver_products_temp")

merge_query = f"""
MERGE INTO {CATALOG}.{SILVER_SCHEMA}.products target
USING (
    SELECT * FROM silver_products_temp
) source
ON target.ProductID = source.ProductID
WHEN MATCHED THEN
    UPDATE SET
        Category = source.Category,
        Brand = source.Brand,
        ProductName = source.ProductName,
        BasePrice = source.BasePrice,
        _validated_at = source._validated_at
WHEN NOT MATCHED THEN
    INSERT (ProductID, Category, Brand, ProductName, BasePrice, _validated_at)
    VALUES (source.ProductID, source.Category, source.Brand, source.ProductName, source.BasePrice, source._validated_at)
"""

spark.sql(merge_query)
print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 5: Verify Silver Products Table

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{SILVER_SCHEMA}.products LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{SILVER_SCHEMA}.products").collect()[0][0]
print(f"Total rows in silver.products: {row_count}")
