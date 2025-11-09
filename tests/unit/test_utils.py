"""
Unit tests for utility functions
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.libraries.utils import (
    add_audit_columns,
    drop_duplicates_by_key,
    validate_required_columns,
    filter_null_values
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .getOrCreate()


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for testing"""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    
    data = [
        (1, "Alice", 100),
        (2, "Bob", 200),
        (3, "Charlie", 300),
        (3, "Charlie", 300),  # Duplicate
        (4, None, 400)  # Null name
    ]
    
    return spark.createDataFrame(data, schema)


def test_add_audit_columns(sample_df):
    """Test adding audit columns"""
    result = add_audit_columns(sample_df)
    
    assert "processed_timestamp" in result.columns
    assert result.count() == sample_df.count()


def test_drop_duplicates_by_key(sample_df):
    """Test dropping duplicates"""
    result = drop_duplicates_by_key(sample_df, ["id", "name"])
    
    assert result.count() == 4  # Should remove 1 duplicate
    assert len(result.columns) == len(sample_df.columns)


def test_validate_required_columns(sample_df):
    """Test column validation"""
    # Test with all columns present
    assert validate_required_columns(sample_df, ["id", "name", "value"]) is True
    
    # Test with missing column
    assert validate_required_columns(sample_df, ["id", "name", "missing"]) is False


def test_filter_null_values(sample_df):
    """Test filtering null values"""
    result = filter_null_values(sample_df, ["name"])
    
    # Should remove row with null name
    assert result.count() == 4
    assert result.filter("name IS NULL").count() == 0


def test_filter_multiple_columns(sample_df):
    """Test filtering nulls in multiple columns"""
    result = filter_null_values(sample_df, ["name", "value"])
    
    # All rows have non-null values except one with null name
    assert result.count() == 4
