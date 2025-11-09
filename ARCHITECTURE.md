# Project Architecture Overview

## 🏛️ High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         GitHub Repository                            │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────┐    │
│  │   Source Code  │  │  Configuration  │  │   CI/CD Workflows │    │
│  │   (src/)       │  │  (config/)      │  │   (.github/)      │    │
│  └────────────────┘  └─────────────────┘  └──────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                │ Git Push / PR Merge
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         GitHub Actions                               │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐            │
│  │   Validate  │→ │   Test       │→ │    Deploy      │            │
│  │   Bundle    │  │   & Lint     │  │    via DAB     │            │
│  └─────────────┘  └──────────────┘  └────────────────┘            │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                │ Databricks CLI
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Databricks Workspace                              │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                     Asset Bundle Deployment                   │  │
│  │   ┌─────────────┐  ┌──────────────┐  ┌──────────────┐      │  │
│  │   │  Notebooks  │  │     Jobs     │  │   Pipelines   │      │  │
│  │   └─────────────┘  └──────────────┘  └──────────────┘      │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                │ Executes on Clusters
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Data Processing (Medallion)                       │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐     │
│  │    BRONZE    │  →   │    SILVER    │  →   │     GOLD     │     │
│  │  (Raw Data)  │      │  (Cleaned)   │      │ (Analytics)  │     │
│  └──────────────┘      └──────────────┘      └──────────────┘     │
│         │                      │                      │             │
│         └──────────────────────┴──────────────────────┘             │
│                                │                                     │
│                                ▼                                     │
│                      ┌──────────────────┐                           │
│                      │  Unity Catalog   │                           │
│                      │   Delta Tables   │                           │
│                      └──────────────────┘                           │
└─────────────────────────────────────────────────────────────────────┘
```

## 📁 Detailed Directory Structure

```
retail_analytics_solution/
│
├── 📋 databricks.yml                    # Main DAB configuration
│   ├── Bundle settings
│   ├── Target environments (dev/staging/prod)
│   ├── Workspace configuration
│   └── Variable definitions
│
├── 📂 .github/workflows/                # CI/CD Automation
│   ├── ci.yml                           # Validation, lint, test
│   ├── deploy-dev.yml                   # Auto-deploy to dev
│   ├── deploy-staging.yml               # Auto-deploy to staging
│   └── deploy-prod.yml                  # Manual prod deployment
│
├── 📂 resources/                        # Databricks Resource Definitions
│   ├── jobs.yml                         # Workflow job configurations
│   │   ├── daily_etl_job
│   │   └── weekly_analytics_job
│   └── pipelines.yml                    # DLT pipeline configurations
│       └── retail_dlt_pipeline
│
├── 📂 src/                              # Source Code
│   │
│   ├── 📂 notebooks/                    # Standard Databricks Notebooks
│   │   ├── 01_data_ingestion.py         # Bronze layer ingestion
│   │   ├── 02_data_transformation.py    # Silver layer cleaning
│   │   ├── 03_create_aggregations.py    # Gold layer analytics
│   │   └── 04_data_quality.py           # Data quality checks
│   │
│   ├── 📂 dlt/                          # Delta Live Tables Pipelines
│   │   ├── bronze_layer.py              # Raw data ingestion (streaming)
│   │   ├── silver_layer.py              # Data cleaning + validation
│   │   └── gold_layer.py                # Business aggregations
│   │
│   └── 📂 libraries/                    # Reusable Python Modules
│       ├── __init__.py
│       ├── utils.py                     # Common utilities
│       └── data_quality.py              # Quality validation classes
│
├── 📂 config/                           # Environment Configurations
│   ├── dev.yml                          # Development settings
│   ├── staging.yml                      # Staging settings
│   └── prod.yml                         # Production settings
│
├── 📂 tests/                            # Test Suite
│   ├── __init__.py
│   ├── 📂 unit/                         # Unit tests
│   │   ├── test_utils.py
│   │   └── test_data_quality.py
│   └── 📂 integration/                  # Integration tests
│       └── test_pipeline.py
│
├── 📄 .gitignore                        # Git ignore patterns
├── 📄 requirements.txt                  # Python dependencies
├── 📄 pyproject.toml                    # Project configuration
├── 📄 README.md                         # Main documentation
├── 📄 QUICKSTART.md                     # Quick start guide
├── 📄 DEPLOYMENT.md                     # Deployment guide
└── 📄 CONTRIBUTING.md                   # Contributing guidelines
```

## 🔄 Data Flow

```
┌─────────────────┐
│  Source Systems │
│  - Files (CSV)  │
│  - APIs (JSON)  │
│  - DBs (Parquet)│
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│              BRONZE LAYER (Raw)                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │ sales_raw    │  │customers_raw │  │products  │ │
│  │              │  │              │  │_raw      │ │
│  └──────────────┘  └──────────────┘  └──────────┘ │
│  • Auto Loader / Batch ingestion                   │
│  • Schema inference                                 │
│  • Add metadata columns                            │
└────────┬────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│             SILVER LAYER (Cleaned)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │   sales      │  │  customers   │  │ products │ │
│  │              │  │              │  │          │ │
│  └──────────────┘  └──────────────┘  └──────────┘ │
│  • Data validation & quality checks                │
│  • Deduplication                                   │
│  • Type casting & standardization                  │
│  • Null handling                                   │
└────────┬────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│              GOLD LAYER (Analytics)                 │
│  ┌──────────────────┐  ┌──────────────────────┐   │
│  │ daily_sales_     │  │ customer_lifetime_   │   │
│  │ summary          │  │ value                │   │
│  └──────────────────┘  └──────────────────────┘   │
│  ┌──────────────────┐  ┌──────────────────────┐   │
│  │ product_         │  │ monthly_trends       │   │
│  │ performance      │  │                      │   │
│  └──────────────────┘  └──────────────────────┘   │
│  • Business-level aggregations                     │
│  • KPIs and metrics                                │
│  • Ready for BI tools                              │
└────────┬────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│  Consumption    │
│  - Dashboards   │
│  - Reports      │
│  - ML Models    │
└─────────────────┘
```

## 🚀 Deployment Pipeline

```
┌──────────────┐
│   Developer  │
│   Local Dev  │
└──────┬───────┘
       │
       │ git push origin develop
       ▼
┌──────────────────────────────────────────────┐
│  Feature Branch → develop                    │
│  ┌────────────────────────────────────────┐ │
│  │  GitHub Actions: CI Pipeline           │ │
│  │  1. Validate Bundle                    │ │
│  │  2. Lint Code (ruff, black, isort)    │ │
│  │  3. Run Unit Tests                     │ │
│  │  4. Security Scan (bandit, safety)    │ │
│  └────────────────────────────────────────┘ │
│         │                                    │
│         │ ✅ All checks pass                 │
│         ▼                                    │
│  ┌────────────────────────────────────────┐ │
│  │  Deploy to Development                 │ │
│  │  - databricks bundle deploy -t dev     │ │
│  │  - Run integration tests               │ │
│  └────────────────────────────────────────┘ │
└──────────────────────────────────────────────┘
       │
       │ PR merge to main
       ▼
┌──────────────────────────────────────────────┐
│  develop → main                              │
│  ┌────────────────────────────────────────┐ │
│  │  Deploy to Staging                     │ │
│  │  - databricks bundle deploy -t staging │ │
│  │  - Run smoke tests                     │ │
│  │  - Performance validation              │ │
│  └────────────────────────────────────────┘ │
└──────────────────────────────────────────────┘
       │
       │ Manual trigger with approval
       ▼
┌──────────────────────────────────────────────┐
│  Staging → Production                        │
│  ┌────────────────────────────────────────┐ │
│  │  Manual Approval Required              │ │
│  └────────────────────────────────────────┘ │
│         │                                    │
│         ▼                                    │
│  ┌────────────────────────────────────────┐ │
│  │  Deploy to Production                  │ │
│  │  - databricks bundle deploy -t prod    │ │
│  │  - Create Git tag                      │ │
│  │  - Create GitHub Release               │ │
│  │  - Post-deployment verification        │ │
│  │  - Send notifications                  │ │
│  └────────────────────────────────────────┘ │
└──────────────────────────────────────────────┘
```

## 🎯 Key Components

### Databricks Asset Bundle (DAB)
- **Purpose**: Infrastructure as Code for Databricks
- **Benefits**: Version control, repeatable deployments, environment parity
- **Components**: Jobs, pipelines, notebooks, configurations

### Delta Live Tables (DLT)
- **Purpose**: Declarative ETL framework
- **Benefits**: Automatic data quality, lineage, streaming support
- **Layers**: Bronze (raw) → Silver (clean) → Gold (analytics)

### GitHub Actions
- **Purpose**: Automated CI/CD
- **Workflows**: 
  - CI: Validation, testing, linting
  - CD: Automated deployment to dev/staging
  - Production: Manual with approval gates

### Unity Catalog
- **Purpose**: Data governance and access control
- **Benefits**: Fine-grained permissions, lineage, discovery
- **Structure**: Catalog → Schema → Tables

## 🔐 Security Layers

```
┌──────────────────────────────────────┐
│  GitHub Secrets                      │
│  - Databricks tokens                 │
│  - Service principal credentials     │
└────────────┬─────────────────────────┘
             │
             ▼
┌──────────────────────────────────────┐
│  Service Principals                  │
│  - Staging automation account        │
│  - Production automation account     │
└────────────┬─────────────────────────┘
             │
             ▼
┌──────────────────────────────────────┐
│  Unity Catalog Permissions           │
│  - Catalog-level access              │
│  - Schema-level permissions          │
│  - Table-level grants                │
└────────────┬─────────────────────────┘
             │
             ▼
┌──────────────────────────────────────┐
│  Data Encryption                     │
│  - At rest (Delta Lake)              │
│  - In transit (TLS)                  │
└──────────────────────────────────────┘
```

## 📊 Monitoring & Observability

- **Job Monitoring**: Databricks workflow UI
- **Pipeline Health**: DLT event logs
- **Data Quality**: Quality check results tables
- **CI/CD Status**: GitHub Actions dashboard
- **Notifications**: Email alerts on failures
- **Metrics**: Job duration, data volumes, error rates
