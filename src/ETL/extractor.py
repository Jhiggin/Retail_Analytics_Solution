# Retail Analytics Solution - Delta Live Tables Pipeline
# Uses pyspark.pipelines (Lakeflow Declarative Pipelines) module
# Implements medallion architecture: Bronze -> Silver -> Gold

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Define the base path to the source data in ADLS
BASE_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in/"
SCHEMA_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in/_schemas/"

# ============================================================================
# BRONZE LAYER - Raw Data Ingestion
# ============================================================================

@dp.table(name="bronze_sales", comment="Raw Sales data from ADLS")
def bronze_sales():
    """Ingest raw sales data from CSV files in ADLS"""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "sales.csv")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_PATH}sales")
        .load(BASE_PATH)
    )

@dp.table(name="bronze_products", comment="Raw Products data from ADLS")
def bronze_products():
    """Ingest raw products data from CSV files in ADLS"""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "products.csv")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_PATH}products")
        .load(BASE_PATH)
    )

@dp.table(name="bronze_stores", comment="Raw Stores data from ADLS")
def bronze_stores():
    """Ingest raw stores data from CSV files in ADLS"""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "stores.csv")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_PATH}stores")
        .load(BASE_PATH)
    )

@dp.table(name="bronze_promotions", comment="Raw Promotions data from ADLS")
def bronze_promotions():
    """Ingest raw promotions data from CSV files in ADLS"""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "promotions.csv")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_PATH}promotions")
        .load(BASE_PATH)
    )

# ============================================================================
# SILVER LAYER - Cleaned and Validated Data
# ============================================================================

@dp.table(name="silver_sales", comment="Validated sales data")
def silver_sales():
    """Clean and validate sales data from bronze layer"""
    return spark.readStream.table("bronze_sales").filter("TransactionID IS NOT NULL AND Date IS NOT NULL")

@dp.table(name="silver_products", comment="Validated products data")
def silver_products():
    """Clean and validate products data from bronze layer"""
    return spark.readStream.table("bronze_products").filter("ProductID IS NOT NULL AND ProductName IS NOT NULL")

@dp.table(name="silver_stores", comment="Validated stores data")
def silver_stores():
    """Clean and validate stores data from bronze layer"""
    return spark.readStream.table("bronze_stores").filter("StoreID IS NOT NULL AND StoreName IS NOT NULL")

@dp.table(name="silver_promotions", comment="Validated promotions data")
def silver_promotions():
    """Clean and validate promotions data from bronze layer"""
    return spark.readStream.table("bronze_promotions").filter("PromotionID IS NOT NULL")

# ============================================================================
# GOLD LAYER - Analytics and Aggregations
# ============================================================================

@dp.materialized_view(name="gold_sales_by_store_product", comment="Sales metrics aggregated by store and product")
def gold_sales_by_store_product():
    """Aggregate sales metrics by store and product"""
    sales = spark.read.table("silver_sales")
    products = spark.read.table("silver_products")
    stores = spark.read.table("silver_stores")

    return (
        sales.alias("s")
        .join(products.alias("p"), "ProductID")
        .join(stores.alias("st"), "StoreID")
        .groupBy(
            F.col("s.StoreID"),
            F.col("st.StoreName"),
            F.col("st.Region"),
            F.col("s.ProductID"),
            F.col("p.ProductName"),
            F.col("p.Category"),
        )
        .agg(
            F.countDistinct("s.TransactionID").alias("total_transactions"),
            F.sum("s.Quantity").alias("total_quantity"),
            F.sum("s.TotalSales").alias("total_revenue"),
            F.avg("s.TotalSales").alias("avg_sale_amount"),
            F.min("s.TotalSales").alias("min_sale_amount"),
            F.max("s.TotalSales").alias("max_sale_amount"),
        )
    )

@dp.materialized_view(name="gold_daily_sales_by_store", comment="Daily sales trends by store")
def gold_daily_sales_by_store():
    """Daily aggregated sales by store"""
    sales = spark.read.table("silver_sales")
    stores = spark.read.table("silver_stores")

    return (
        sales.alias("s")
        .join(stores.alias("st"), "StoreID")
        .groupBy(
            F.col("s.Date"),
            F.col("s.StoreID"),
            F.col("st.StoreName"),
            F.col("st.Region"),
        )
        .agg(
            F.countDistinct("s.TransactionID").alias("daily_transactions"),
            F.sum("s.Quantity").alias("daily_quantity"),
            F.sum("s.TotalSales").alias("daily_revenue"),
            F.avg("s.TotalSales").alias("avg_transaction_value"),
        )
    )

@dp.materialized_view(name="gold_product_performance", comment="Overall product performance metrics")
def gold_product_performance():
    """Product-level performance aggregations"""
    products = spark.read.table("silver_products")
    sales = spark.read.table("silver_sales")

    return (
        products.alias("p")
        .join(sales.alias("s"), "ProductID", "left")
        .groupBy(
            "p.ProductID",
            "p.ProductName",
            "p.Category",
            "p.BasePrice",
        )
        .agg(
            F.countDistinct("s.TransactionID").alias("total_sales"),
            F.sum("s.Quantity").alias("total_quantity_sold"),
            F.sum("s.TotalSales").alias("total_revenue"),
            F.avg("s.Quantity").alias("avg_quantity_per_sale"),
            F.countDistinct("s.StoreID").alias("stores_sold_in"),
        )
    )

@dp.materialized_view(name="gold_store_performance", comment="Overall store performance metrics")
def gold_store_performance():
    """Store-level performance aggregations"""
    stores = spark.read.table("silver_stores")
    sales = spark.read.table("silver_sales")

    return (
        stores.alias("st")
        .join(sales.alias("s"), "StoreID", "left")
        .groupBy(
            "st.StoreID",
            "st.StoreName",
            "st.Region",
        )
        .agg(
            F.countDistinct("s.TransactionID").alias("total_transactions"),
            F.countDistinct("s.ProductID").alias("unique_products_sold"),
            F.sum("s.Quantity").alias("total_quantity_sold"),
            F.sum("s.TotalSales").alias("total_revenue"),
            F.avg("s.TotalSales").alias("avg_transaction_value"),
            F.min("s.Date").alias("first_sale_date"),
            F.max("s.Date").alias("last_sale_date"),
        )
    )

@dp.materialized_view(name="gold_promotion_effectiveness", comment="Promotion effectiveness analysis")
def gold_promotion_effectiveness():
    """Analyze promotion impact on sales"""
    promotions = spark.read.table("silver_promotions")
    sales = spark.read.table("silver_sales")

    return (
        promotions.alias("pr")
        .join(
            sales.alias("s"),
            (F.col("s.Date") >= F.col("pr.StartDate"))
            & (F.col("s.Date") <= F.col("pr.EndDate")),
            "left",
        )
        .groupBy(
            F.col("pr.PromotionID"),
            F.col("pr.PromotionType"),
            F.col("pr.StartDate"),
            F.col("pr.EndDate"),
        )
        .agg(
            F.countDistinct("s.TransactionID").alias("total_sales_during_promotion"),
            F.sum("s.Quantity").alias("total_quantity_sold"),
            F.sum("s.TotalSales").alias("total_revenue"),
            F.avg("s.TotalSales").alias("avg_sale_amount"),
            F.countDistinct("s.StoreID").alias("stores_participating"),
            F.countDistinct("s.ProductID").alias("products_sold"),
        )
    )
