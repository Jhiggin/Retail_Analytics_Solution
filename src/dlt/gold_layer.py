# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Gold Layer
# MAGIC 
# MAGIC This notebook defines business-level aggregation tables in the gold layer.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Daily Sales Summary

# COMMAND ----------

@dlt.table(
    name="gold_daily_sales_summary",
    comment="Daily aggregated sales metrics by store and category",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_daily_sales_summary():
    sales = dlt.read("silver_sales")
    products = dlt.read("silver_products")
    customers = dlt.read("silver_customers")
    
    return (
        sales
            .join(products, "product_id", "left")
            .join(customers, "customer_id", "left")
            .groupBy(
                "transaction_date",
                "transaction_year",
                "transaction_month",
                "store_id",
                "category"
            )
            .agg(
                count("transaction_id").alias("total_transactions"),
                countDistinct("customer_id").alias("unique_customers"),
                sum("quantity").alias("total_quantity"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_transaction_value"),
                sum("discount").alias("total_discount")
            )
            .withColumn("processed_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Customer Lifetime Value

# COMMAND ----------

@dlt.table(
    name="gold_customer_ltv",
    comment="Customer lifetime value and engagement metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_customer_ltv():
    sales = dlt.read("silver_sales")
    customers = dlt.read("silver_customers")
    
    return (
        sales
            .join(customers, "customer_id", "left")
            .groupBy("customer_id", "customer_segment")
            .agg(
                count("transaction_id").alias("total_transactions"),
                sum("amount").alias("lifetime_value"),
                avg("amount").alias("avg_order_value"),
                min("transaction_date").alias("first_purchase_date"),
                max("transaction_date").alias("last_purchase_date")
            )
            .withColumn(
                "customer_tenure_days",
                datediff(col("last_purchase_date"), col("first_purchase_date"))
            )
            .withColumn("processed_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Product Performance

# COMMAND ----------

@dlt.table(
    name="gold_product_performance",
    comment="Product sales performance and ranking metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_product_performance():
    sales = dlt.read("silver_sales")
    products = dlt.read("silver_products")
    
    result = (
        sales
            .join(products, "product_id", "left")
            .groupBy(
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "brand"
            )
            .agg(
                count("transaction_id").alias("total_transactions"),
                sum("quantity").alias("total_quantity_sold"),
                sum("amount").alias("total_revenue"),
                avg(col("amount") / col("quantity")).alias("avg_unit_price")
            )
            .withColumn(
                "revenue_rank",
                dense_rank().over(Window.partitionBy("category").orderBy(desc("total_revenue")))
            )
            .withColumn("processed_timestamp", current_timestamp())
    )
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Monthly Trends

# COMMAND ----------

@dlt.table(
    name="gold_monthly_trends",
    comment="Monthly sales trends and growth metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_monthly_trends():
    sales = dlt.read("silver_sales")
    
    return (
        sales
            .groupBy(
                "transaction_year",
                "transaction_month",
                "transaction_quarter"
            )
            .agg(
                count("transaction_id").alias("total_transactions"),
                countDistinct("customer_id").alias("unique_customers"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_transaction_value")
            )
            .withColumn(
                "revenue_growth",
                (col("total_revenue") - lag("total_revenue").over(
                    Window.orderBy("transaction_year", "transaction_month")
                )) / lag("total_revenue").over(
                    Window.orderBy("transaction_year", "transaction_month")
                ) * 100
            )
            .withColumn("processed_timestamp", current_timestamp())
            .orderBy("transaction_year", "transaction_month")
    )
