# Retail Analytics Solution

A production-ready Databricks demo project showcasing Delta Live Tables (DLT), Databricks Asset Bundles, and GitHub Actions for CI/CD deployment. This solution implements the medallion architecture pattern for data processing.

## Overview

This project demonstrates best practices for building data pipelines on Databricks with:
- **Delta Live Tables** - Declarative ETL framework for building reliable data pipelines
- **Medallion Architecture** - Three-layer approach to data organization (Bronze → Silver → Gold)
- **Databricks Asset Bundles** - Infrastructure-as-code for managing Databricks resources
- **GitHub Actions** - Automated CI/CD deployment workflows
- **Unity Catalog** - Multi-tier namespace for granular access control

## Architecture

### Medallion Layers

The solution implements three distinct layers with separate schemas for access control:

#### Bronze Layer (`bronze` schema)
Raw data ingestion from Azure Data Lake Storage (ADLS) using CloudFiles format:
- `sales_raw` - Raw sales transactions
- `products_raw` - Raw product catalog
- `stores_raw` - Raw store information
- `promotions_raw` - Raw promotion data

**Pipeline**: `retail_bronze_pipeline`

#### Silver Layer (`silver` schema)
Data validation and cleaning with quality checks:
- `sales` - Validated sales transactions
- `products` - Validated products
- `stores` - Validated stores
- `promotions` - Validated promotions

**Pipeline**: `retail_silver_pipeline`

#### Gold Layer (`gold` schema)
Analytical views and aggregations for business intelligence:
- `sales_by_store_product` - Sales metrics aggregated by store and product
- `daily_sales_by_store` - Daily sales trends by store
- `product_performance` - Overall product performance metrics
- `store_performance` - Overall store performance metrics
- `promotion_effectiveness` - Promotion impact analysis

**Pipeline**: `retail_gold_pipeline`

## Project Structure

```
Retail_Analytics_Solution/
├── .github/
│   └── workflows/
│       ├── deploy.yml              # Production deployment workflow
│       └── dev-deploy.yml          # Development deployment workflow
├── src/
│   └── ETL/
│       ├── bronze.py               # Bronze layer table definitions
│       ├── silver.py               # Silver layer validations
│       └── gold.py                 # Gold layer analytics
├── resources/
│   ├── bronze_pipeline.yml         # Bronze pipeline configuration
│   ├── silver_pipeline.yml         # Silver pipeline configuration
│   ├── gold_pipeline.yml           # Gold pipeline configuration
│   └── sample_job.yml              # Sample job definition
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

2. **Install dependencies**
   ```bash
   pip install databricks-cli
   ```

3. **Authenticate with Databricks**
   ```bash
   databricks configure --token
   # Enter your Databricks workspace URL and personal access token
   ```

4. **Validate the bundle configuration**
   ```bash
   databricks bundle validate -C .
   ```

### Manual Deployment

**Deploy to development environment:**
```bash
databricks bundle deploy -C . --target dev
```

**Deploy to production environment:**
```bash
databricks bundle deploy -C . --target prod
```

### Automated Deployment (GitHub Actions)

Deployments are automatically triggered when code is pushed:

- **Push to `develop` branch** → Deploys to development environment (`dev` target)
- **Push to `main` branch** → Deploys to production environment (`prod` target)

#### Setting Up GitHub Secrets

Configure these secrets in your GitHub repository settings:

1. **DATABRICKS_HOST** - Your Databricks workspace URL
2. **DATABRICKS_TOKEN** - Your Databricks personal access token

## Pipeline Execution

### Running Pipelines

In Databricks workspace:

1. Navigate to **Workflows** → **Delta Live Tables**
2. Select the desired pipeline:
   - `retail_bronze_pipeline` - Start with this to ingest raw data
   - `retail_silver_pipeline` - Runs after bronze completes
   - `retail_gold_pipeline` - Runs after silver completes

3. Click **Start** to begin execution

### Pipeline Dependencies

```
retail_bronze_pipeline
    ↓
retail_silver_pipeline (reads from bronze schema)
    ↓
retail_gold_pipeline (reads from silver schema)
```

## Configuration

### Development vs Production

The `databricks.yml` defines two deployment targets with separate catalogs:

- **Development (`dev`)**: Uses `retail_dev` catalog
- **Production (`prod`)**: Uses `retail_prod` catalog

### Data Source

The pipelines ingest data from ADLS. Update paths in `src/ETL/bronze.py` if your data location differs:

```python
BASE_PATH = "abfss://data@dbmetastorecs.dfs.core.windows.net/retail-in/"
```

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

## Adding New Tables

1. Add table definition to appropriate layer file (`src/ETL/bronze.py`, `src/ETL/silver.py`, or `src/ETL/gold.py`)
2. Use `@dp.table()` for standard tables or `@dp.materialized_view()` for analytical views
3. Commit and push to trigger automatic deployment
4. Monitor execution in Databricks

## Troubleshooting

**"Table not found" errors**
- Verify fully qualified table name: `catalog.schema.table`
- Ensure dependent pipeline has completed
- Check correct catalog and schema targeted

**"Key not found" errors**
- Verify source table exists with expected columns
- Check dependent pipelines completed successfully
- Review bronze layer for schema mismatches

**ADLS connectivity issues**
- Verify ADLS path and credentials
- Ensure cluster has storage account access
- Check file format matches configuration (CSV with header)

## Resources

- [Databricks Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)

## License

This project is provided as a demonstration for Databricks user group presentations.
