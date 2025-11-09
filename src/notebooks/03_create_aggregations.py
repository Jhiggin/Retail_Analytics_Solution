# Databricks notebook source
# MAGIC %md
# MAGIC # Create Aggregations - Gold Layer
# MAGIC 
# MAGIC This notebook creates business-level aggregations in the gold layer.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_analytics", "Catalog Name")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema")
dbutils.widgets.text("gold_schema", "gold", "Gold Schema")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")

# COMMAND ----------

# Create gold schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Sales Summary

# COMMAND ----------

# Read silver tables
df_sales = spark.table(f"{catalog}.{silver_schema}.sales")
df_products = spark.table(f"{catalog}.{silver_schema}.products")
df_customers = spark.table(f"{catalog}.{silver_schema}.customers")

# Create daily sales summary
df_daily_sales = (df_sales
    .join(df_products, "product_id", "left")
    .join(df_customers, "customer_id", "left")
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

# Write to gold table
(df_daily_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{gold_schema}.daily_sales_summary")
)

print(f"Created {df_daily_sales.count()} daily sales summary records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Lifetime Value

# COMMAND ----------

# Calculate customer lifetime value
df_customer_ltv = (df_sales
    .join(df_customers, "customer_id", "left")
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

# Write to gold table
(df_customer_ltv.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{gold_schema}.customer_lifetime_value")
)

print(f"Created {df_customer_ltv.count()} customer LTV records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Performance

# COMMAND ----------

# Calculate product performance metrics
df_product_performance = (df_sales
    .join(df_products, "product_id", "left")
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

# Write to gold table
(df_product_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{gold_schema}.product_performance")
)

print(f"Created {df_product_performance.count()} product performance records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Trends

# COMMAND ----------

# Calculate monthly trends
df_monthly_trends = (df_sales
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

# Write to gold table
(df_monthly_trends.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{gold_schema}.monthly_trends")
)

print(f"Created {df_monthly_trends.count()} monthly trend records")

# COMMAND ----------

print("=" * 80)
print("Aggregations Complete!")
print("=" * 80)
