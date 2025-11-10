# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Products
# MAGIC Read products data from ADLS once and merge into bronze.products table

# COMMAND

from pyspark.sql import functions as F

# COMMAND

# Configuration
ADLS_BASE_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in"
CATALOG = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else spark.conf.get("catalog", "retail_dev")
BRONZE_SCHEMA = "bronze"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Read Products Data from ADLS (One-time read)

# COMMAND

df_products = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{ADLS_BASE_PATH}/products.csv")
)

display(df_products)

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Create or Get Bronze Products Table

# COMMAND

# Create bronze schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# Create bronze.products table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.products (
        ProductID STRING,
        Category STRING,
        Brand STRING,
        ProductName STRING,
        BasePrice DOUBLE,
        _loaded_at TIMESTAMP,
        PRIMARY KEY (ProductID)
    )
    USING DELTA
""")

print(f"Table {CATALOG}.{BRONZE_SCHEMA}.products is ready")

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Merge New Data into Bronze Table

# COMMAND

# Add load timestamp
df_products_with_timestamp = df_products.withColumn("_loaded_at", F.current_timestamp())

# Merge into bronze.products table
merge_query = f"""
MERGE INTO {CATALOG}.{BRONZE_SCHEMA}.products target
USING (
    SELECT * FROM bronze_products_temp
) source
ON target.ProductID = source.ProductID
WHEN MATCHED THEN
    UPDATE SET
        Category = source.Category,
        Brand = source.Brand,
        ProductName = source.ProductName,
        BasePrice = source.BasePrice,
        _loaded_at = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (ProductID, Category, Brand, ProductName, BasePrice, _loaded_at)
    VALUES (source.ProductID, source.Category, source.Brand, source.ProductName, source.BasePrice, source._loaded_at)
"""

# Register temp view for merge
df_products_with_timestamp.createOrReplaceTempView("bronze_products_temp")

# Execute merge
spark.sql(merge_query)

print("Merge operation completed successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Verify Bronze Products Table

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.products LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{BRONZE_SCHEMA}.products").collect()[0][0]
print(f"Total rows in bronze.products: {row_count}")
