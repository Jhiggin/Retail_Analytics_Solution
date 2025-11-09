# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC
# MAGIC This notebook performs data quality validations and monitoring.

# COMMAND ----------

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_analytics", "Catalog Name")
dbutils.widgets.text("schema", "silver", "Schema to Validate")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# Initialize results list
quality_results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for Null Values

# COMMAND ----------


def check_null_values(table_name, columns):
    """Check for null values in specified columns"""
    df = spark.table(f"{catalog}.{schema}.{table_name}")
    total_rows = df.count()

    for column in columns:
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0

        quality_results.append(
            {
                "table": table_name,
                "check": "null_values",
                "column": column,
                "total_rows": total_rows,
                "failed_rows": null_count,
                "failure_rate": null_percentage,
                "status": "PASS" if null_percentage < 5 else "FAIL",
                "timestamp": datetime.now(),
            }
        )

        print(f"{table_name}.{column}: {null_count} nulls ({null_percentage:.2f}%)")


# Check sales table
check_null_values("sales", ["transaction_id", "customer_id", "product_id", "amount"])

# Check customers table
check_null_values("customers", ["customer_id", "email"])

# Check products table
check_null_values("products", ["product_id", "product_name", "price"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for Duplicates

# COMMAND ----------


def check_duplicates(table_name, key_columns):
    """Check for duplicate records based on key columns"""
    df = spark.table(f"{catalog}.{schema}.{table_name}")
    total_rows = df.count()

    duplicates = df.groupBy(key_columns).count().filter(col("count") > 1).count()

    quality_results.append(
        {
            "table": table_name,
            "check": "duplicates",
            "column": ", ".join(key_columns),
            "total_rows": total_rows,
            "failed_rows": duplicates,
            "failure_rate": (duplicates / total_rows * 100) if total_rows > 0 else 0,
            "status": "PASS" if duplicates == 0 else "FAIL",
            "timestamp": datetime.now(),
        }
    )

    print(f"{table_name}: {duplicates} duplicate records")


# Check for duplicates
check_duplicates("sales", ["transaction_id"])
check_duplicates("customers", ["customer_id"])
check_duplicates("products", ["product_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Data Ranges

# COMMAND ----------


def check_data_ranges(table_name, column, min_value=None, max_value=None):
    """Check if data values are within expected ranges"""
    df = spark.table(f"{catalog}.{schema}.{table_name}")
    total_rows = df.count()

    condition = lit(True)
    if min_value is not None:
        condition = condition & (col(column) >= min_value)
    if max_value is not None:
        condition = condition & (col(column) <= max_value)

    out_of_range = df.filter(~condition).count()
    failure_rate = (out_of_range / total_rows * 100) if total_rows > 0 else 0

    quality_results.append(
        {
            "table": table_name,
            "check": "data_range",
            "column": column,
            "total_rows": total_rows,
            "failed_rows": out_of_range,
            "failure_rate": failure_rate,
            "status": "PASS" if failure_rate < 1 else "FAIL",
            "timestamp": datetime.now(),
        }
    )

    print(f"{table_name}.{column}: {out_of_range} out of range ({failure_rate:.2f}%)")


# Check sales ranges
check_data_ranges("sales", "amount", min_value=0)
check_data_ranges("sales", "quantity", min_value=0)

# Check product ranges
check_data_ranges("products", "price", min_value=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Referential Integrity

# COMMAND ----------


def check_referential_integrity(child_table, child_column, parent_table, parent_column):
    """Check referential integrity between tables"""
    df_child = spark.table(f"{catalog}.{schema}.{child_table}")
    df_parent = spark.table(f"{catalog}.{schema}.{parent_table}")

    total_rows = df_child.count()

    # Find orphaned records
    orphaned = df_child.select(child_column).subtract(df_parent.select(parent_column)).count()

    failure_rate = (orphaned / total_rows * 100) if total_rows > 0 else 0

    quality_results.append(
        {
            "table": f"{child_table}->{parent_table}",
            "check": "referential_integrity",
            "column": f"{child_column}->{parent_column}",
            "total_rows": total_rows,
            "failed_rows": orphaned,
            "failure_rate": failure_rate,
            "status": "PASS" if orphaned == 0 else "FAIL",
            "timestamp": datetime.now(),
        }
    )

    print(
        f"{child_table}.{child_column} -> {parent_table}.{parent_column}: {orphaned} orphaned records"
    )


# Check referential integrity
check_referential_integrity("sales", "customer_id", "customers", "customer_id")
check_referential_integrity("sales", "product_id", "products", "product_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Quality Results

# COMMAND ----------

# Create DataFrame from results
df_quality_results = spark.createDataFrame(quality_results)

# Display summary
display(df_quality_results)

# Save to quality table
(
    df_quality_results.write.format("delta")
    .mode("append")
    .saveAsTable(f"{catalog}.monitoring.data_quality_results")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Count failures
failed_checks = df_quality_results.filter(col("status") == "FAIL").count()
total_checks = df_quality_results.count()

print("=" * 80)
print("Data Quality Summary")
print("=" * 80)
print(f"Total Checks: {total_checks}")
print(f"Passed: {total_checks - failed_checks}")
print(f"Failed: {failed_checks}")
print("=" * 80)

# Exit with error if any checks failed
if failed_checks > 0:
    dbutils.notebook.exit(f"FAILED: {failed_checks} quality checks failed")
else:
    dbutils.notebook.exit("SUCCESS: All quality checks passed")
