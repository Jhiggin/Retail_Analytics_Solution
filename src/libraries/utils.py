"""Common utility functions for data processing."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Add standard audit columns to a DataFrame

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with audit columns added
    """
    return df.withColumn("processed_timestamp", current_timestamp())


def drop_duplicates_by_key(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """
    Remove duplicate rows based on specified key columns

    Args:
        df: Input DataFrame
        key_columns: List of column names to use as unique key

    Returns:
        DataFrame with duplicates removed
    """
    logger.info(f"Removing duplicates based on columns: {key_columns}")
    initial_count = df.count()
    result_df = df.dropDuplicates(key_columns)
    final_count = result_df.count()
    duplicates = initial_count - final_count

    if duplicates > 0:
        logger.warning(f"Removed {duplicates} duplicate records")
    else:
        logger.info("No duplicates found")

    return result_df


def validate_required_columns(df: DataFrame, required_columns: list[str]) -> bool:
    """
    Validate that all required columns exist in the DataFrame

    Args:
        df: DataFrame to validate
        required_columns: List of required column names

    Returns:
        True if all columns exist, False otherwise
    """
    missing_columns = set(required_columns) - set(df.columns)

    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False

    logger.info("All required columns present")
    return True


def filter_null_values(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Filter out rows where specified columns are null

    Args:
        df: Input DataFrame
        columns: List of column names to check for nulls

    Returns:
        DataFrame with null rows removed
    """
    logger.info(f"Filtering null values in columns: {columns}")
    initial_count = df.count()

    result_df = df
    for column in columns:
        result_df = result_df.filter(col(column).isNotNull())

    final_count = result_df.count()
    removed = initial_count - final_count

    if removed > 0:
        logger.warning(f"Removed {removed} rows with null values")

    return result_df


def optimize_table(spark, table_name: str, zorder_columns: list[str] | None = None):
    """
    Optimize a Delta table with optional Z-ordering

    Args:
        spark: Spark session
        table_name: Fully qualified table name (catalog.schema.table)
        zorder_columns: Optional list of columns for Z-ordering
    """
    logger.info(f"Optimizing table: {table_name}")

    if zorder_columns:
        zorder_clause = ", ".join(zorder_columns)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_clause})")
        logger.info(f"Z-ordered by: {zorder_columns}")
    else:
        spark.sql(f"OPTIMIZE {table_name}")

    logger.info("Optimization complete")


def vacuum_table(spark, table_name: str, retention_hours: int = 168):
    """
    Vacuum old files from a Delta table

    Args:
        spark: Spark session
        table_name: Fully qualified table name
        retention_hours: Retention period in hours (default 7 days)
    """
    logger.info(f"Vacuuming table: {table_name} with retention {retention_hours} hours")
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
    logger.info("Vacuum complete")


def get_table_stats(spark, table_name: str) -> dict:
    """
    Get statistics for a Delta table

    Args:
        spark: Spark session
        table_name: Fully qualified table name

    Returns:
        Dictionary containing table statistics
    """
    df = spark.table(table_name)

    stats = {
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns
    }

    logger.info(f"Table stats for {table_name}: {stats}")
    return stats
