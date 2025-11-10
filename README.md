# Retail Analytics Solution

A production-ready Databricks demo project showcasing notebook-based ETL with medallion architecture and Kimball star schema. This solution uses Databricks Notebooks, Asset Bundles, and GitHub Actions for CI/CD deployment.

## Overview

This project demonstrates best practices for building data pipelines on Databricks with:
- **Notebooks** - Interactive PySpark code for ETL logic with merge operations
- **Medallion Architecture** - Three-layer approach (Bronze → Silver → Gold)
- **Kimball Star Schema** - Fact and dimension tables in the Gold layer
- **Type 1 SCD** - Overwrite updates for dimensions and facts
- **MERGE INTO** - Idempotent upsert operations for incremental loads
- **Databricks Asset Bundles** - Infrastructure-as-code for jobs and orchestration
- **GitHub Actions** - Automated CI/CD with notebook validation and deployment
- **Unity Catalog** - Multi-tier namespace for data governance

## Architecture

### Medallion Layers with Kimball Schema

The solution implements a three-layer medallion architecture ending in a Kimball star schema:

#### Bronze Layer (`bronze` schema)
Raw data ingestion from ADLS (read once, then merged into tables):
- **Tables**: `sales`, `products`, `stores`, `promotions`
- **Operation**: MERGE INTO (upsert via TransactionID/ProductID/StoreID/PromotionID)
- **Job**: `retail_analytics_orchestration` orchestrates 4 parallel bronze ingestion jobs

#### Silver Layer (`silver` schema)
Validated and cleaned data with quality checks:
- **Tables**: `sales`, `products`, `stores`, `promotions`
- **Validations**: NOT NULL checks, value range checks, logical validations
- **Operation**: MERGE INTO from bronze tables
- **Job**: 4 parallel silver jobs depend on bronze completion

#### Gold Layer (`gold` schema)
Kimball Star Schema with fact and dimension tables:

**Dimensions (Type 1 SCD):**
- `dim_date` - Calendar dimension (auto-generated from sales dates)
- `dim_product` - Product attributes with surrogate key
- `dim_store` - Store attributes with surrogate key
- `dim_promotion` - Promotion attributes with surrogate key

**Fact Table:**
- `fact_sales` - Sales transactions with foreign keys to dimensions

**Analytics Views (Pre-aggregated):**
- `sales_by_store_product` - Sales metrics by store and product
- `daily_sales_by_store` - Daily sales trends
- `product_performance` - Product-level metrics
- `store_performance` - Store-level metrics
- `promotion_effectiveness` - Promotion ROI analysis
- `sales_trend_by_month` - Monthly sales trends

**Jobs**: 6 gold layer jobs (4 dimensions + 1 fact + 1 views) depend on silver completion

## Project Structure

```
Retail_Analytics_Solution/
├── .github/
│   ├── workflows/
│   │   ├── deploy.yml              # Production: Validate notebooks, deploy, trigger jobs
│   │   └── dev-deploy.yml          # Development: Validate notebooks, deploy
│   └── DEPLOYMENT_SETUP.md         # GitHub Actions configuration guide
├── notebooks/
│   ├── 01_bronze_sales.py          # Ingest sales from ADLS via MERGE
│   ├── 02_bronze_products.py       # Ingest products from ADLS via MERGE
│   ├── 03_bronze_stores.py         # Ingest stores from ADLS via MERGE
│   ├── 04_bronze_promotions.py     # Ingest promotions from ADLS via MERGE
│   ├── 05_silver_sales.py          # Validate & merge sales from bronze
│   ├── 06_silver_products.py       # Validate & merge products from bronze
│   ├── 07_silver_stores.py         # Validate & merge stores from bronze
│   ├── 08_silver_promotions.py     # Validate & merge promotions from bronze
│   ├── 09_gold_dim_date.py         # Create date dimension
│   ├── 10_gold_dim_product.py      # Create product dimension (Type 1 SCD)
│   ├── 11_gold_dim_store.py        # Create store dimension (Type 1 SCD)
│   ├── 12_gold_dim_promotion.py    # Create promotion dimension (Type 1 SCD)
│   ├── 13_gold_fact_sales.py       # Create sales fact table
│   └── 14_gold_analytics_views.py  # Create 6 analytics views
├── resources/
│   ├── bronze_jobs.yml             # 4 bronze ingestion job definitions
│   ├── silver_jobs.yml             # 4 silver validation job definitions
│   ├── gold_jobs.yml               # 6 gold layer job definitions
│   ├── orchestration_job.yml       # Master job: orchestrates all layers
│   └── sample_job.job.yml          # Deprecated placeholder
├── fixtures/
│   ├── sales.csv                   # Sample sales data
│   ├── products.csv                # Sample products data
│   ├── stores.csv                  # Sample stores data
│   └── promotions.csv              # Sample promotions data
├── databricks.yml                  # Databricks Asset Bundle configuration
├── pyproject.toml                  # Python project configuration
└── README.md                       # This file
```

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Azure Data Lake Storage (ADLS) with retail data files
- Databricks CLI installed locally
- Python 3.11+
- Git and GitHub account

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Retail_Analytics_Solution
   ```

2. **Install Databricks CLI**
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   ```

3. **Authenticate with Databricks**
   ```bash
   databricks configure --token
   # Enter your Databricks workspace URL and personal access token
   ```

4. **Validate notebook syntax** (optional)
   ```bash
   python -m py_compile notebooks/*.py
   ```

### Manual Deployment

**Deploy notebooks to development environment:**
```bash
databricks workspace import-dir ./notebooks /Retail_Analytics_Solution/notebooks --overwrite
```

**Deploy jobs to development environment:**
```bash
# First configure jobs in resources/*.yml with your cluster ID
databricks bundle deploy --target dev
```

**Deploy to production environment:**
```bash
databricks bundle deploy --target prod
```

### Automated Deployment (GitHub Actions)

Deployments are automatically triggered when code is pushed:

- **Push to `develop` branch** → Deploys to development environment (`dev` target)
- **Push to `main` branch** → Deploys to production environment (`prod` target)

#### Setting Up GitHub Secrets

Configure these secrets in your GitHub repository settings:

1. **DATABRICKS_HOST** - Your Databricks workspace URL
2. **DATABRICKS_TOKEN** - Your Databricks personal access token

## Job Execution

### Running Jobs

**Option 1: Manual Job Trigger (Development)**

In Databricks workspace:

1. Navigate to **Workflows** → **Jobs**
2. Click on `retail_analytics_orchestration` (master job)
3. Click **Run Now** to execute the complete pipeline

**Option 2: Daily Schedule (Production)**

The master orchestration job runs daily at UTC midnight (configurable).

### Job Dependencies

```
Bronze Layer (4 parallel jobs - read ADLS once)
├── bronze_sales
├── bronze_products
├── bronze_stores
└── bronze_promotions
        ↓
Silver Layer (4 parallel jobs - validate & clean)
├── silver_sales
├── silver_products
├── silver_stores
└── silver_promotions
        ↓
Gold Layer - Dimensions (4 parallel jobs)
├── gold_dim_date
├── gold_dim_product
├── gold_dim_store
└── gold_dim_promotion
        ↓
Gold Layer - Fact & Analytics (2 sequential jobs)
├── gold_fact_sales
└── gold_analytics_views
```

All jobs use MERGE INTO for idempotent upserts with Type 1 SCD.

## Configuration

### Development vs Production

The `databricks.yml` defines two deployment targets with separate catalogs:

- **Development (`dev`)**: Uses `retail_dev` catalog
- **Production (`prod`)**: Uses `retail_prod` catalog

### Data Source

The bronze layer notebooks ingest data from ADLS (read once, then merged). Update paths in notebook files if your data location differs:

```python
ADLS_BASE_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in"
```

**Key Design**: Files are read only once in the bronze layer. Subsequent layers operate on tables, not files.

### Notebook Parameters

Notebooks accept a `catalog` parameter via job base_parameters. In the orchestration job, all notebooks receive:

```yaml
base_parameters:
  catalog: ${var.catalog}
```

This allows the same notebooks to run against different catalogs (dev vs prod).

## Development Workflow

### Branching Strategy

- **`main`** - Production code, automatically deploys to `prod` target
- **`develop`** - Development code, automatically deploys to `dev` target

### Making Changes

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make code changes
3. Commit and push to GitHub
4. Create a pull request to `develop`
5. After review, merge to `develop` for dev deployment

## Adding New Notebooks

1. **Create notebook in `notebooks/` directory**
   - Follow naming convention: `{layer}_{entity}.py` (e.g., `bronze_customers.py`)
   - Number notebooks sequentially within each layer

2. **Structure the notebook**
   ```python
   # Databricks notebook source
   # MAGIC %md
   # MAGIC # Description

   from pyspark.sql import functions as F

   CATALOG = spark.conf.get("catalog", "retail_dev")
   SCHEMA = "bronze"  # or silver/gold

   # Read, transform, merge logic
   ```

3. **Use MERGE INTO for all table operations**
   - Ensures idempotency and incremental updates
   - Specify primary key for upsert logic

4. **Create job definition in `resources/`**
   - Add entry to appropriate layer job file
   - Reference notebook path: `/Retail_Analytics_Solution/notebooks/{notebook_name}`

5. **Update orchestration job if needed**
   - Add job task with proper dependencies
   - Ensure parallel jobs run together, sequential jobs in order

6. **Commit and push** - Triggers GitHub Actions validation and deployment

## Troubleshooting

**"Table not found" errors**
- Verify fully qualified table name: `${CATALOG}.{SCHEMA}.{TABLE}`
- Check dependent jobs completed successfully
- Verify MERGE INTO statements reference correct source tables

**Merge operation failures**
- Ensure primary key constraint is defined
- Check ON clause references valid columns
- Verify column names match between source and target

**ADLS connectivity issues**
- Verify ADLS path and credentials are correct
- Ensure service principal/cluster identity has access
- Check file format: solution expects CSV with header row

**Job dependency issues**
- Verify all dependency job IDs are correctly referenced in orchestration_job.yml
- Check job names match exactly
- Ensure task_key references are unique within the job

**Notebook parameter issues**
- Verify `spark.conf.get("catalog")` is being called correctly
- Check base_parameters in job definitions match notebook expectations
- Ensure variable names are consistent across layer notebooks

## Best Practices

1. **Always use MERGE INTO** - Ensures idempotency and handles updates/inserts
2. **Read ADLS only in bronze** - Prevents redundant scans on multiple runs
3. **Validate in silver** - Implement quality checks before analytics
4. **Use surrogate keys** - Simplify fact table foreign key relationships
5. **Create indexes on foreign keys** - Improves query performance on fact table joins
6. **Monitor job runs** - Check execution history for errors or performance issues
7. **Test locally** - Validate notebooks in dev before promoting to prod

## Resources

- [Databricks Notebooks](https://docs.databricks.com/en/notebooks/index.html)
- [Databricks Jobs](https://docs.databricks.com/en/jobs/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Delta Lake MERGE](https://docs.databricks.com/en/delta/merge.html)
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html)

## License

This project is provided as a demonstration for Databricks user group presentations.
