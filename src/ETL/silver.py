# Silver Layer - Data Validation and Cleaning
# Validates and cleans data from bronze layer

from pyspark import pipelines as dp

# ============================================================================
# SILVER LAYER - Cleaned and Validated Data
# ============================================================================

@dp.table(name="sales", comment="Validated sales data")
def sales():
    """Clean and validate sales data from bronze layer"""
    return spark.readStream.table("bronze.sales_raw").filter("TransactionID IS NOT NULL AND Date IS NOT NULL")

@dp.table(name="products", comment="Validated products data")
def products():
    """Clean and validate products data from bronze layer"""
    return spark.readStream.table("bronze.products_raw").filter("ProductID IS NOT NULL AND ProductName IS NOT NULL")

@dp.table(name="stores", comment="Validated stores data")
def stores():
    """Clean and validate stores data from bronze layer"""
    return spark.readStream.table("bronze.stores_raw").filter("StoreID IS NOT NULL AND StoreName IS NOT NULL")

@dp.table(name="promotions", comment="Validated promotions data")
def promotions():
    """Clean and validate promotions data from bronze layer"""
    return spark.readStream.table("bronze.promotions_raw").filter("PromotionID IS NOT NULL")
