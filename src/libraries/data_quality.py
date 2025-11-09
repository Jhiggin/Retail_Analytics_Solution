"""Data quality validation functions."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """Perform data quality validations on DataFrames."""

    def __init__(self, df: DataFrame, table_name: str):
        self.df = df
        self.table_name = table_name
        self.validation_results: list[dict] = []

    def check_null_values(self, columns: list[str], threshold: float = 0.05) -> bool:
        """
        Check if null value percentage is below threshold.

        Args:
            columns: List of columns to check
            threshold: Maximum allowed null percentage (default 5%)

        Returns:
            True if validation passes, False otherwise
        """
        total_rows = self.df.count()
        passed = True

        for column in columns:
            null_count = self.df.filter(col(column).isNull()).count()
            null_percentage = null_count / total_rows if total_rows > 0 else 0

            status = "PASS" if null_percentage <= threshold else "FAIL"
            if status == "FAIL":
                passed = False

            result = {
                "table": self.table_name,
                "check": "null_values",
                "column": column,
                "null_count": null_count,
                "null_percentage": null_percentage,
                "threshold": threshold,
                "status": status
            }

            self.validation_results.append(result)
            logger.info(f"{self.table_name}.{column}: {null_count} nulls ({null_percentage:.2%}) - {status}")

        return passed

    def check_duplicates(self, key_columns: list[str]) -> bool:
        """
        Check for duplicate records based on key columns.

        Args:
            key_columns: Columns that form the unique key

        Returns:
            True if no duplicates found, False otherwise
        """
        total_rows = self.df.count()
        distinct_rows = self.df.select(key_columns).distinct().count()
        duplicate_count = total_rows - distinct_rows

        status = "PASS" if duplicate_count == 0 else "FAIL"

        result = {
            "table": self.table_name,
            "check": "duplicates",
            "key_columns": key_columns,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
            "status": status
    }

        self.validation_results.append(result)
        logger.info(f"{self.table_name}: {duplicate_count} duplicates - {status}")

        return status == "PASS"

    def check_value_range(
        self,
        column: str,
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> bool:
        """
        Check if column values are within expected range.

        Args:
            column: Column name to check
            min_value: Minimum expected value
            max_value: Maximum expected value

        Returns:
            True if all values in range, False otherwise
        """
        condition = col(column).isNotNull()

        if min_value is not None:
            condition = condition & (col(column) >= min_value)
        if max_value is not None:
            condition = condition & (col(column) <= max_value)

        total_rows = self.df.filter(col(column).isNotNull()).count()
        valid_rows = self.df.filter(condition).count()
        invalid_rows = total_rows - valid_rows

        status = "PASS" if invalid_rows == 0 else "FAIL"

        result = {
            "table": self.table_name,
            "check": "value_range",
            "column": column,
            "min_value": min_value,
            "max_value": max_value,
            "invalid_count": invalid_rows,
            "status": status
    }

        self.validation_results.append(result)
        logger.info(f"{self.table_name}.{column}: {invalid_rows} out of range - {status}")

        return status == "PASS"

    def check_referential_integrity(
        self, foreign_key: str, reference_df: DataFrame, reference_key: str
    ) -> bool:
        """
        Check referential integrity between tables.

        Args:
            foreign_key: Foreign key column in current DataFrame
            reference_df: Reference DataFrame
            reference_key: Key column in reference DataFrame

        Returns:
            True if all foreign keys exist in reference, False otherwise
        """
        orphaned = (
            self.df.select(foreign_key).subtract(reference_df.select(reference_key)).count()
        )

        status = "PASS" if orphaned == 0 else "FAIL"

        result = {
            "table": self.table_name,
            "check": "referential_integrity",
            "foreign_key": foreign_key,
            "orphaned_count": orphaned,
            "status": status
    }

        self.validation_results.append(result)
        logger.info(f"{self.table_name}.{foreign_key}: {orphaned} orphaned records - {status}")

        return status == "PASS"

    def get_results(self) -> list[dict]:
        """
        Get all validation results

        Returns:
            List of validation result dictionaries
        """
        return self.validation_results

    def has_failures(self) -> bool:
        """
        Check if any validations failed

        Returns:
            True if any failures occurred, False otherwise
        """
        return any(result["status"] == "FAIL" for result in self.validation_results)
