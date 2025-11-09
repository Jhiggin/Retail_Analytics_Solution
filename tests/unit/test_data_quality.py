"""
Unit tests for data quality validation functions
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.libraries.data_quality import DataQualityValidator


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("test_data_quality") \
        .master("local[2]") \
        .getOrCreate()


@pytest.fixture
def sales_df(spark):
    """Create a sample sales DataFrame"""
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True)
    ])
    
    data = [
        (1, 101, 50.0, 2),
        (2, 102, 75.5, 3),
        (3, 103, None, 1),  # Null amount
        (4, 104, 100.0, 5),
        (5, None, 25.0, 1),  # Null customer_id
        (6, 105, -10.0, 2),  # Negative amount
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def customer_df(spark):
    """Create a sample customer DataFrame"""
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    
    data = [
        (101, "Alice"),
        (102, "Bob"),
        (103, "Charlie"),
        (104, "David")
    ]
    
    return spark.createDataFrame(data, schema)


def test_check_null_values_pass(sales_df):
    """Test null value check that should pass"""
    validator = DataQualityValidator(sales_df, "sales")
    
    # Check column with no nulls (below 5% threshold)
    result = validator.check_null_values(["transaction_id"], threshold=0.05)
    
    assert result is True
    assert len(validator.get_results()) == 1
    assert validator.get_results()[0]["status"] == "PASS"


def test_check_null_values_fail(sales_df):
    """Test null value check that should fail"""
    validator = DataQualityValidator(sales_df, "sales")
    
    # Check columns with nulls exceeding threshold
    result = validator.check_null_values(["amount", "customer_id"], threshold=0.05)
    
    assert result is False
    assert len(validator.get_results()) == 2
    assert any(r["status"] == "FAIL" for r in validator.get_results())


def test_check_duplicates_no_duplicates(sales_df):
    """Test duplicate check with no duplicates"""
    validator = DataQualityValidator(sales_df, "sales")
    
    result = validator.check_duplicates(["transaction_id"])
    
    assert result is True
    assert validator.get_results()[0]["status"] == "PASS"


def test_check_duplicates_with_duplicates(spark):
    """Test duplicate check with duplicates present"""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", IntegerType(), True)
    ])
    
    data = [
        (1, 100),
        (1, 100),  # Duplicate
        (2, 200)
    ]
    
    df = spark.createDataFrame(data, schema)
    validator = DataQualityValidator(df, "test")
    
    result = validator.check_duplicates(["id", "value"])
    
    assert result is False
    assert validator.get_results()[0]["status"] == "FAIL"
    assert validator.get_results()[0]["duplicate_count"] == 1


def test_check_value_range_pass(sales_df):
    """Test value range check that should pass"""
    validator = DataQualityValidator(sales_df, "sales")
    
    # Check quantity (all values between 0 and 10)
    result = validator.check_value_range("quantity", min_value=0, max_value=10)
    
    assert result is True
    assert validator.get_results()[0]["status"] == "PASS"


def test_check_value_range_fail(sales_df):
    """Test value range check that should fail"""
    validator = DataQualityValidator(sales_df, "sales")
    
    # Check amount (has negative value)
    result = validator.check_value_range("amount", min_value=0)
    
    assert result is False
    assert validator.get_results()[0]["status"] == "FAIL"
    assert validator.get_results()[0]["invalid_count"] > 0


def test_check_referential_integrity_pass(sales_df, customer_df):
    """Test referential integrity check that should pass"""
    # Filter out rows with null customer_id
    valid_sales = sales_df.filter("customer_id IS NOT NULL")
    # Remove customer 105 which doesn't exist in customer_df
    valid_sales = valid_sales.filter("customer_id != 105")
    
    validator = DataQualityValidator(valid_sales, "sales")
    result = validator.check_referential_integrity("customer_id", customer_df, "customer_id")
    
    assert result is True
    assert validator.get_results()[0]["status"] == "PASS"


def test_check_referential_integrity_fail(sales_df, customer_df):
    """Test referential integrity check that should fail"""
    # Include sales with customer_id 105 which doesn't exist in customer_df
    validator = DataQualityValidator(sales_df, "sales")
    result = validator.check_referential_integrity("customer_id", customer_df, "customer_id")
    
    assert result is False
    assert validator.get_results()[0]["status"] == "FAIL"
    assert validator.get_results()[0]["orphaned_count"] > 0


def test_has_failures(sales_df):
    """Test has_failures method"""
    validator = DataQualityValidator(sales_df, "sales")
    
    # Add passing check
    validator.check_null_values(["transaction_id"], threshold=0.05)
    assert validator.has_failures() is False
    
    # Add failing check
    validator.check_value_range("amount", min_value=0)
    assert validator.has_failures() is True


def test_get_results(sales_df):
    """Test get_results method"""
    validator = DataQualityValidator(sales_df, "sales")
    
    validator.check_null_values(["transaction_id"])
    validator.check_duplicates(["transaction_id"])
    
    results = validator.get_results()
    
    assert len(results) == 2
    assert all("status" in r for r in results)
    assert all("check" in r for r in results)
