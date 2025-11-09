# Retail Analytics Solution - Databricks

A production-ready Databricks implementation using Databricks Asset Bundles (DAB) and GitHub Actions for CI/CD deployment.

## 🏗️ Architecture

This project implements a **Medallion Architecture** (Bronze → Silver → Gold) for retail analytics:

- **Bronze Layer**: Raw data ingestion from source systems
- **Silver Layer**: Cleaned, validated, and transformed data
- **Gold Layer**: Business-level aggregations and analytics

## 📁 Project Structure

```
retail_analytics_solution/
├── .github/
│   └── workflows/              # GitHub Actions CI/CD workflows
│       ├── ci.yml              # Validation, linting, and testing
│       ├── deploy-dev.yml      # Development deployment
│       ├── deploy-staging.yml  # Staging deployment
│       └── deploy-prod.yml     # Production deployment
├── src/
│   ├── notebooks/              # Standard Databricks notebooks
│   │   ├── 01_data_ingestion.py
│   │   ├── 02_data_transformation.py
│   │   ├── 03_create_aggregations.py
│   │   └── 04_data_quality.py
│   ├── dlt/                    # Delta Live Tables pipelines
│   │   ├── bronze_layer.py
│   │   ├── silver_layer.py
│   │   └── gold_layer.py
│   ├── libraries/              # Reusable Python modules
│   └── tests/                  # Unit and integration tests
│       ├── unit/
│       └── integration/
├── resources/                  # Bundle resource definitions
│   ├── jobs.yml               # Job configurations
│   └── pipelines.yml          # DLT pipeline configurations
├── config/                     # Environment configurations
│   ├── dev.yml
│   ├── staging.yml
│   └── prod.yml
├── databricks.yml             # Main bundle configuration
├── requirements.txt           # Python dependencies
├── .gitignore
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Databricks CLI
- Git
- Access to Databricks workspace(s)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd retail_analytics_solution
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install Databricks CLI**
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   ```

4. **Configure Databricks authentication**
   ```bash
   databricks configure --token
   ```

## 🔧 Configuration

### Environment Variables

Set up the following secrets in GitHub:

**Development:**
- `DATABRICKS_HOST_DEV`: Databricks workspace URL for development
- `DATABRICKS_TOKEN_DEV`: Access token for development

**Staging:**
- `DATABRICKS_HOST_STAGING`: Databricks workspace URL for staging
- `DATABRICKS_TOKEN_STAGING`: Access token for staging

**Production:**
- `DATABRICKS_HOST_PROD`: Databricks workspace URL for production
- `DATABRICKS_TOKEN_PROD`: Access token for production

### Bundle Configuration

Edit `databricks.yml` to customize:
- Workspace paths
- Cluster configurations
- Job schedules
- Resource naming conventions

## 📦 Databricks Asset Bundles

### Validate Bundle

```bash
databricks bundle validate -t dev
```

### Deploy Bundle

**Development:**
```bash
databricks bundle deploy -t dev
```

**Staging:**
```bash
databricks bundle deploy -t staging
```

**Production:**
```bash
databricks bundle deploy -t prod
```

### Run a Job

```bash
databricks bundle run daily_etl_job -t dev
```

### Destroy Bundle Resources

```bash
databricks bundle destroy -t dev
```

## 🔄 CI/CD Pipeline

### Workflow Overview

1. **CI Pipeline** (`ci.yml`)
   - Triggers on PR to `main` or `develop`
   - Validates bundle configuration
   - Runs linting (ruff, black, isort)
   - Executes unit tests
   - Performs security scans (bandit, safety)

2. **Development Deployment** (`deploy-dev.yml`)
   - Triggers on push to `develop`
   - Deploys to development environment
   - Runs integration tests

3. **Staging Deployment** (`deploy-staging.yml`)
   - Triggers on push to `main`
   - Deploys to staging environment
   - Runs smoke tests

4. **Production Deployment** (`deploy-prod.yml`)
   - Manual trigger with version input
   - Requires approval
   - Deploys to production
   - Creates Git tags and releases
   - Runs post-deployment verification

### Branching Strategy

- `develop`: Development branch
- `main`: Staging/pre-production branch
- `prod-*`: Production release tags

## 🧪 Testing

### Run Unit Tests

```bash
pytest tests/unit -v
```

### Run Integration Tests

```bash
pytest tests/integration -v
```

### Run with Coverage

```bash
pytest tests/ -v --cov=src --cov-report=html
```

## 📊 Data Pipeline

### Jobs

1. **Daily ETL Job** (`daily_etl_job`)
   - Ingests raw data from source systems
   - Transforms data through bronze → silver layers
   - Creates gold layer aggregations
   - Performs data quality checks

2. **Weekly Analytics Job** (`weekly_analytics_job`)
   - Generates weekly metrics and reports
   - Runs every Monday at 8 AM EST

### Delta Live Tables Pipeline

**retail_dlt_pipeline**: Streaming/batch pipeline that:
- Continuously ingests data using Auto Loader
- Applies data quality constraints
- Maintains the full medallion architecture
- Provides automatic data lineage

## 🔍 Monitoring

### Data Quality

Data quality checks are built into the pipeline:
- Null value validation
- Duplicate detection
- Data range verification
- Referential integrity checks

Results are logged to `{catalog}.monitoring.data_quality_results`

### Pipeline Observability

- Job run history in Databricks workspace
- GitHub Actions workflow logs
- DLT pipeline event logs
- Data quality metrics

## 🛠️ Development

### Adding a New Notebook

1. Create notebook in `src/notebooks/`
2. Add to job configuration in `resources/jobs.yml`
3. Test locally
4. Commit and push to `develop` branch

### Adding a New DLT Table

1. Define table in appropriate layer file (`src/dlt/`)
2. Add expectations for data quality
3. Update pipeline configuration if needed
4. Deploy to development for testing

### Modifying Bundle Configuration

1. Edit `databricks.yml` or resource files
2. Validate: `databricks bundle validate -t dev`
3. Deploy to dev for testing
4. Promote through environments

## 🔐 Security Best Practices

- ✅ Use service principals for production deployments
- ✅ Store credentials in GitHub Secrets
- ✅ Implement least privilege access
- ✅ Enable Unity Catalog for data governance
- ✅ Use secret scopes for sensitive data
- ✅ Regular security scanning with bandit
- ✅ Dependency vulnerability checks with safety

## 📈 Performance Optimization

- Use Photon engine for DLT pipelines
- Enable auto-optimization for Delta tables
- Implement proper partitioning strategies
- Use Z-ordering for frequently filtered columns
- Configure appropriate cluster sizes per environment

## 🤝 Contributing

1. Create a feature branch from `develop`
2. Make your changes
3. Write/update tests
4. Submit a pull request
5. Ensure CI pipeline passes
6. Request code review

## 📝 License

[Your License Here]

## 📞 Support

For questions or issues:
- Create an issue in GitHub
- Contact the data engineering team
- Check Databricks documentation

## 🔗 Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Delta Live Tables Guide](https://docs.databricks.com/delta-live-tables/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/)
