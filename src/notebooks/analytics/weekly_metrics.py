# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly Metrics Generation
# MAGIC
# MAGIC This notebook generates weekly analytics and metrics.

# COMMAND ----------

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_analytics", "Catalog Name")
dbutils.widgets.text("gold_schema", "gold", "Gold Schema")

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Week Over Week Metrics

# COMMAND ----------

# Read gold layer tables
df_daily_sales = spark.table(f"{catalog}.{gold_schema}.daily_sales_summary")

# Get current week and previous week
current_date = datetime.now()
week_start = current_date - timedelta(days=current_date.weekday())
prev_week_start = week_start - timedelta(days=7)

# Calculate weekly aggregations
df_weekly = (
    df_daily_sales.filter(col("transaction_date") >= prev_week_start.date())
    .groupBy(
        when(col("transaction_date") >= week_start.date(), "current_week")
        .otherwise("previous_week")
        .alias("week_period"),
        "category",
    )
    .agg(
        sum("total_revenue").alias("weekly_revenue"),
        sum("total_transactions").alias("weekly_transactions"),
        sum("unique_customers").alias("weekly_customers"),
    )
)

# Display results
display(df_weekly)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Performing Products

# COMMAND ----------

df_product_performance = spark.table(f"{catalog}.{gold_schema}.product_performance")

# Get top 10 products by revenue
df_top_products = (
    df_product_performance.orderBy(desc("total_revenue"))
    .limit(10)
    .select("product_name", "category", "total_revenue", "total_quantity_sold", "revenue_rank")
)

display(df_top_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Insights

# COMMAND ----------

df_customer_ltv = spark.table(f"{catalog}.{gold_schema}.customer_lifetime_value")

# Calculate customer segment statistics
df_segment_stats = (
    df_customer_ltv.groupBy("customer_segment")
    .agg(
        count("customer_id").alias("customer_count"),
        avg("lifetime_value").alias("avg_ltv"),
        sum("lifetime_value").alias("total_ltv"),
        avg("total_transactions").alias("avg_transactions"),
    )
    .orderBy(desc("total_ltv"))
)

display(df_segment_stats)

# COMMAND ----------

print("=" * 80)
print("Weekly Metrics Generation Complete!")
print("=" * 80)
