# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Date Dimension
# MAGIC Create a static date dimension table for the Kimball star schema

# COMMAND

from pyspark.sql import functions as F
from datetime import datetime, timedelta

# COMMAND

CATALOG = spark.conf.get("catalog", "retail_dev")
GOLD_SCHEMA = "gold"

# COMMAND

# MAGIC %md
# MAGIC ## Step 1: Generate Date Range

# COMMAND

# Get min and max dates from silver.sales
min_max_date = spark.sql(f"""
    SELECT
        MIN(Date) as min_date,
        MAX(Date) as max_date
    FROM {CATALOG}.silver.sales
""").collect()[0]

min_date = min_max_date['min_date']
max_date = min_max_date['max_date']

print(f"Date range: {min_date} to {max_date}")

# COMMAND

# MAGIC %md
# MAGIC ## Step 2: Create Date Dimension

# COMMAND

# Generate dates
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType

# Create a sequence of dates
date_range = spark.sql(f"""
    WITH date_series AS (
        SELECT CAST('{min_date}' AS DATE) as date_value
        UNION ALL
        SELECT DATE_ADD(CAST('{min_date}' AS DATE), 1)
        FROM (SELECT 1 as dummy)
    ),
    recursive_dates AS (
        SELECT date_value FROM date_series
        WHERE date_value <= CAST('{max_date}' AS DATE)
    )
    SELECT * FROM recursive_dates
    UNION ALL
    SELECT CAST('{max_date}' AS DATE) as date_value
""")

# Build dimension with attributes
df_dim_date = (spark.sql(f"""
    WITH RECURSIVE date_series AS (
        SELECT CAST('{min_date}' AS DATE) as date_value
        UNION ALL
        SELECT DATE_ADD(date_value, 1)
        FROM date_series
        WHERE DATE_ADD(date_value, 1) <= CAST('{max_date}' AS DATE)
    )
    SELECT
        date_value as DateKey,
        date_value as Date,
        YEAR(date_value) as Year,
        MONTH(date_value) as Month,
        DAY(date_value) as DayOfMonth,
        DAYOFWEEK(date_value) as DayOfWeek,
        WEEKOFYEAR(date_value) as WeekOfYear,
        QUARTER(date_value) as Quarter,
        CONCAT(YEAR(date_value), '-Q', QUARTER(date_value)) as QuarterYear,
        DAYNAME(date_value) as DayName,
        MONTHNAME(date_value) as MonthName,
        CASE
            WHEN DAYOFWEEK(date_value) IN (1, 7) THEN true
            ELSE false
        END as IsWeekend
    FROM date_series
    ORDER BY date_value
""")
)

print(f"Generated {df_dim_date.count()} date records")
display(df_dim_date.limit(10))

# COMMAND

# MAGIC %md
# MAGIC ## Step 3: Create or Replace Gold Dim_Date Table

# COMMAND

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

spark.sql(f"""
    DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.dim_date
""")

df_dim_date.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.dim_date",
    format="delta"
)

print(f"Table {CATALOG}.{GOLD_SCHEMA}.dim_date created successfully")

# COMMAND

# MAGIC %md
# MAGIC ## Step 4: Verify Dimension

# COMMAND

result_df = spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.dim_date LIMIT 10")
display(result_df)

row_count = spark.sql(f"SELECT COUNT(*) as row_count FROM {CATALOG}.{GOLD_SCHEMA}.dim_date").collect()[0][0]
print(f"Total rows in dim_date: {row_count}")
