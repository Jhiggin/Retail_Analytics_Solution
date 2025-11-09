"""
Sample test file for Retail Analytics Solution.

To add meaningful tests, create test functions that use the spark fixture
from conftest.py to test your data transformation logic.

Example:
    def test_sales_schema(spark):
        # Test that your data has the expected schema
        pass
"""

# Placeholder test to ensure test infrastructure works
def test_spark_session(spark):
    """Test that Spark session is available."""
    assert spark is not None
    df = spark.createDataFrame([(1, "test")], ["id", "value"])
    assert df.count() == 1
