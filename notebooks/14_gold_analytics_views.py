# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Analytics Views
# MAGIC Pre-aggregated views using the Kimball star schema

# COMMAND

from pyspark.sql import functions as F

# COMMAND

CATALOG = spark.conf.get("catalog", "retail_dev")
GOLD_SCHEMA = "gold"

# COMMAND

# MAGIC %md
# MAGIC ## View 1: Sales by Store and Product

# COMMAND

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.sales_by_store_product AS
SELECT
    s.StoreKey,
    s.StoreName,
    s.Region,
    p.ProductKey,
    p.ProductName,
    p.Category,
    COUNT(DISTINCT f.TransactionID) as total_transactions,
    SUM(f.Quantity) as total_quantity,
    SUM(f.TotalSales) as total_revenue,
    AVG(f.TotalSales) as avg_sale_amount,
    MIN(f.TotalSales) as min_sale_amount,
    MAX(f.TotalSales) as max_sale_amount
FROM {CATALOG}.{GOLD_SCHEMA}.fact_sales f
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_store s ON f.StoreKey = s.StoreKey
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_product p ON f.ProductKey = p.ProductKey
GROUP BY
    s.StoreKey, s.StoreName, s.Region,
    p.ProductKey, p.ProductName, p.Category
""")

print("View sales_by_store_product created successfully")

# COMMAND

# MAGIC %md
# MAGIC ## View 2: Daily Sales by Store

# COMMAND

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.daily_sales_by_store AS
SELECT
    d.Date,
    s.StoreKey,
    s.StoreName,
    s.Region,
    COUNT(DISTINCT f.TransactionID) as daily_transactions,
    SUM(f.Quantity) as daily_quantity,
    SUM(f.TotalSales) as daily_revenue,
    AVG(f.TotalSales) as avg_transaction_value
FROM {CATALOG}.{GOLD_SCHEMA}.fact_sales f
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_date d ON f.DateKey = d.DateKey
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_store s ON f.StoreKey = s.StoreKey
GROUP BY
    d.Date, s.StoreKey, s.StoreName, s.Region
ORDER BY d.Date DESC, daily_revenue DESC
""")

print("View daily_sales_by_store created successfully")

# COMMAND

# MAGIC %md
# MAGIC ## View 3: Product Performance

# COMMAND

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.product_performance AS
SELECT
    p.ProductKey,
    p.ProductID,
    p.ProductName,
    p.Category,
    p.Brand,
    p.BasePrice,
    COUNT(DISTINCT f.TransactionID) as total_sales,
    SUM(f.Quantity) as total_quantity_sold,
    SUM(f.TotalSales) as total_revenue,
    AVG(f.Quantity) as avg_quantity_per_sale,
    COUNT(DISTINCT f.StoreKey) as stores_sold_in
FROM {CATALOG}.{GOLD_SCHEMA}.fact_sales f
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_product p ON f.ProductKey = p.ProductKey
GROUP BY
    p.ProductKey, p.ProductID, p.ProductName, p.Category, p.Brand, p.BasePrice
ORDER BY total_revenue DESC
""")

print("View product_performance created successfully")

# COMMAND

# MAGIC %md
# MAGIC ## View 4: Store Performance

# COMMAND

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.store_performance AS
SELECT
    s.StoreKey,
    s.StoreID,
    s.StoreName,
    s.Region,
    s.StoreType,
    s.SquareFootage,
    COUNT(DISTINCT f.TransactionID) as total_transactions,
    COUNT(DISTINCT f.ProductKey) as unique_products_sold,
    SUM(f.Quantity) as total_quantity_sold,
    SUM(f.TotalSales) as total_revenue,
    AVG(f.TotalSales) as avg_transaction_value,
    MIN(d.Date) as first_sale_date,
    MAX(d.Date) as last_sale_date
FROM {CATALOG}.{GOLD_SCHEMA}.fact_sales f
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_store s ON f.StoreKey = s.StoreKey
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_date d ON f.DateKey = d.DateKey
GROUP BY
    s.StoreKey, s.StoreID, s.StoreName, s.Region, s.StoreType, s.SquareFootage
ORDER BY total_revenue DESC
""")

print("View store_performance created successfully")

# COMMAND

# MAGIC %md
# MAGIC ## View 5: Promotion Effectiveness

# COMMAND

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.promotion_effectiveness AS
SELECT
    pr.PromotionKey,
    pr.PromotionID,
    pr.PromotionType,
    pr.StartDate,
    pr.EndDate,
    pr.DiscountPercent,
    pr.MarketingSpend,
    COUNT(DISTINCT f.TransactionID) as total_sales_during_promotion,
    SUM(f.Quantity) as total_quantity_sold,
    SUM(f.TotalSales) as total_revenue,
    AVG(f.TotalSales) as avg_sale_amount,
    COUNT(DISTINCT f.StoreKey) as stores_participating,
    COUNT(DISTINCT f.ProductKey) as products_sold,
    ROUND(SUM(f.TotalSales) / pr.MarketingSpend, 2) as roi_multiplier
FROM {CATALOG}.{GOLD_SCHEMA}.fact_sales f
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_promotion pr ON f.PromotionKey = pr.PromotionKey
WHERE f.PromotionKey != -1  -- Exclude non-promotional sales
GROUP BY
    pr.PromotionKey, pr.PromotionID, pr.PromotionType, pr.StartDate, pr.EndDate,
    pr.DiscountPercent, pr.MarketingSpend
ORDER BY total_revenue DESC
""")

print("View promotion_effectiveness created successfully")

# COMMAND

# MAGIC %md
# MAGIC ## View 6: Sales Trend by Month

# COMMAND

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.sales_trend_by_month AS
SELECT
    d.Year,
    d.Month,
    d.MonthName,
    CONCAT(d.Year, '-', LPAD(d.Month, 2, '0')) as YearMonth,
    COUNT(DISTINCT f.TransactionID) as transactions,
    SUM(f.Quantity) as quantity_sold,
    SUM(f.TotalSales) as revenue
FROM {CATALOG}.{GOLD_SCHEMA}.fact_sales f
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_date d ON f.DateKey = d.DateKey
GROUP BY
    d.Year, d.Month, d.MonthName
ORDER BY d.Year DESC, d.Month DESC
""")

print("View sales_trend_by_month created successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Verify All Views

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.sales_by_store_product LIMIT 5")
display(result_df)

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.daily_sales_by_store LIMIT 5")
display(result_df)

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.product_performance LIMIT 5")
display(result_df)

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.store_performance LIMIT 5")
display(result_df)

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.promotion_effectiveness LIMIT 5")
display(result_df)

# COMMAND

print("All analytics views created successfully!")
