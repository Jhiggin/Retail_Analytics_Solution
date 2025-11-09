# Gold Layer - Analytics and Aggregations
# Creates analytical views from silver layer data

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ============================================================================
# GOLD LAYER - Analytics and Aggregations
# ============================================================================

@dp.materialized_view(name="sales_by_store_product", comment="Sales metrics aggregated by store and product")
def sales_by_store_product():
    """Aggregate sales metrics by store and product"""
    sales = spark.read.table("silver.sales")
    products = spark.read.table("silver.products")
    stores = spark.read.table("silver.stores")

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

@dp.materialized_view(name="daily_sales_by_store", comment="Daily sales trends by store")
def daily_sales_by_store():
    """Daily aggregated sales by store"""
    sales = spark.read.table("silver.sales")
    stores = spark.read.table("silver.stores")

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

@dp.materialized_view(name="product_performance", comment="Overall product performance metrics")
def product_performance():
    """Product-level performance aggregations"""
    products = spark.read.table("silver.products")
    sales = spark.read.table("silver.sales")

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

@dp.materialized_view(name="store_performance", comment="Overall store performance metrics")
def store_performance():
    """Store-level performance aggregations"""
    stores = spark.read.table("silver.stores")
    sales = spark.read.table("silver.sales")

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

@dp.materialized_view(name="promotion_effectiveness", comment="Promotion effectiveness analysis")
def promotion_effectiveness():
    """Analyze promotion impact on sales"""
    promotions = spark.read.table("silver.promotions")
    sales = spark.read.table("silver.sales")

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
