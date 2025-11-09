# Bronze Layer - Raw Data Ingestion
# Ingests raw retail data from ADLS using CloudFiles format

from pyspark import pipelines as dp

# Define the base path to the source data in ADLS
BASE_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in/"
SCHEMA_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in/_schemas/"

# ============================================================================
# BRONZE LAYER - Raw Data Ingestion
# ============================================================================

@dp.table(name="sales_raw", comment="Raw Sales data from ADLS")
def sales_raw():
    """Ingest raw sales data from CSV files in ADLS"""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "sales.csv")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_PATH}sales")
        .load(BASE_PATH)
    )

@dp.table(name="products_raw", comment="Raw Products data from ADLS")
def products_raw():
    """Ingest raw products data from CSV files in ADLS"""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "products.csv")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_PATH}products")
        .load(BASE_PATH)
    )

@dp.table(name="stores_raw", comment="Raw Stores data from ADLS")
def stores_raw():
    """Ingest raw stores data from CSV files in ADLS"""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "stores.csv")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_PATH}stores")
        .load(BASE_PATH)
    )

@dp.table(name="promotions_raw", comment="Raw Promotions data from ADLS")
def promotions_raw():
    """Ingest raw promotions data from CSV files in ADLS"""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "promotions.csv")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_PATH}promotions")
        .load(BASE_PATH)
    )
