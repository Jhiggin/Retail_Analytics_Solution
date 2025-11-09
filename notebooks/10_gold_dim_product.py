# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Product Dimension
# MAGIC Create or update product dimension table with Type 1 SCD (overwrite)

# COMMAND

from pyspark.sql import functions as F

# COMMAND

CATALOG = spark.conf.get("catalog", "retail_dev")
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read from Silver Products

# COMMAND

df_products = spark.sql(f"SELECT * FROM {CATALOG}.{SILVER_SCHEMA}.products")
print(f"Read {df_products.count()} product records from silver layer")
display(df_products.limit(5))

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Create Product Dimension with Surrogate Key

# COMMAND

# Add surrogate key (in real scenario, would use a sequence or hash)
df_dim_product = (df_products
    .withColumn("ProductKey", F.row_number().over(
        F.Window.partitionBy().orderBy(F.col("ProductID"))
    ))
    .select(
        F.col("ProductKey"),
        F.col("ProductID"),
        F.col("ProductName"),
        F.col("Category"),
        F.col("Brand"),
        F.col("BasePrice"),
        F.current_timestamp().alias("_updated_at")
    )
)

print(f"Processed {df_dim_product.count()} records")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Create or Get Gold Dim_Product Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.dim_product (
        ProductKey INT,
        ProductID STRING,
        ProductName STRING,
        Category STRING,
        Brand STRING,
        BasePrice DOUBLE,
        _updated_at TIMESTAMP,
        PRIMARY KEY (ProductKey)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{GOLD_SCHEMA}.dim_product is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Merge into Dimension (Type 1 SCD - Overwrite)

# COMMAND

df_dim_product.createOrReplaceTempView("dim_product_temp")

merge_query = f"""
MERGE INTO {CATALOG}.{GOLD_SCHEMA}.dim_product target
USING (
    SELECT * FROM dim_product_temp
) source
ON target.ProductID = source.ProductID
WHEN MATCHED THEN
    UPDATE SET
        ProductName = source.ProductName,
        Category = source.Category,
        Brand = source.Brand,
        BasePrice = source.BasePrice,
        _updated_at = source._updated_at
WHEN NOT MATCHED THEN
    INSERT (ProductKey, ProductID, ProductName, Category, Brand, BasePrice, _updated_at)
    VALUES (source.ProductKey, source.ProductID, source.ProductName, source.Category, source.Brand, source.BasePrice, source._updated_at)
"""

spark.sql(merge_query)
print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 5: Verify Dimension

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.dim_product LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{GOLD_SCHEMA}.dim_product").collect()[0][0]
print(f"Total rows in dim_product: {row_count}")
