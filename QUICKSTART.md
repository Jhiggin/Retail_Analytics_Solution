# Quick Start Guide

Get up and running with the Retail Analytics Solution in minutes.

## 🎯 Prerequisites

- Python 3.10+
- Git
- Databricks workspace access
- GitHub account

## 🚀 5-Minute Setup

### 1. Clone and Install

```powershell
# Clone the repository
git clone <your-repo-url>
cd Retail_Analytics_Solution

# Create virtual environment
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Install Databricks CLI

```powershell
# Install via PowerShell
Invoke-WebRequest -Uri "https://github.com/databricks/cli/releases/latest/download/databricks_windows_amd64.zip" -OutFile "databricks.zip"
Expand-Archive -Path "databricks.zip" -DestinationPath "$env:USERPROFILE\databricks"
# Add to PATH manually or use the installer
```

Or use the simpler approach:
```powershell
# Using curl (if available)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

### 3. Configure Databricks

```powershell
# Configure authentication
databricks configure --token

# When prompted, enter:
# - Databricks Host: https://your-workspace.cloud.databricks.com
# - Token: <your-personal-access-token>
```

### 4. Validate Bundle

```powershell
# Validate the bundle configuration
databricks bundle validate -t dev
```

### 5. Deploy to Development

```powershell
# Deploy resources to your dev workspace
databricks bundle deploy -t dev
```

## 📋 What Just Happened?

Your deployment created:

✅ **Notebooks** in your workspace:
- `/Workspace/.bundle/retail_analytics_solution/dev/files/src/notebooks/`

✅ **Jobs**:
- `retail_analytics_solution_dev_daily_etl_job`
- `retail_analytics_solution_dev_weekly_analytics_job`

✅ **DLT Pipeline**:
- `retail_analytics_solution_dev_retail_dlt_pipeline`

## 🎮 Try It Out

### Run Your First Job

```powershell
# Run the daily ETL job
databricks bundle run daily_etl_job -t dev

# Check the status
databricks jobs list
```

### View in Workspace

1. Open your Databricks workspace
2. Navigate to **Workflows** → **Jobs**
3. Find jobs starting with `retail_analytics_solution_dev_`
4. Click to view details and run history

### Explore Notebooks

1. Go to **Workspace**
2. Navigate to `.bundle/retail_analytics_solution/dev/files/src/notebooks/`
3. Open any notebook to view the code

## 🔄 Next Steps

### Set Up GitHub Actions (Optional)

1. **Fork the repository** to your GitHub account

2. **Add GitHub Secrets**:
   - Go to Settings → Secrets and variables → Actions
   - Add new repository secrets:
     ```
     DATABRICKS_HOST_DEV=https://your-workspace.cloud.databricks.com
     DATABRICKS_TOKEN_DEV=<your-token>
     ```

3. **Push to develop branch**:
   ```powershell
   git checkout -b develop
   git push origin develop
   ```

4. **Watch CI/CD in action**:
   - GitHub Actions will automatically validate and deploy

### Customize for Your Needs

1. **Update catalog names** in `databricks.yml`:
   ```yaml
   variables:
     catalog_name:
       default: "your_catalog_name"
   ```

2. **Modify notebooks** in `src/notebooks/` for your data sources

3. **Adjust job schedules** in `resources/jobs.yml`

4. **Configure data paths** in environment configs

## 🔧 Common Commands

```powershell
# Validate configuration
databricks bundle validate -t dev

# Deploy changes
databricks bundle deploy -t dev

# Run a specific job
databricks bundle run <job-name> -t dev

# View bundle summary
databricks bundle summary -t dev

# Destroy all resources (be careful!)
databricks bundle destroy -t dev
```

## 📊 Project Structure Overview

```
retail_analytics_solution/
├── databricks.yml              # Main bundle config
├── resources/
│   ├── jobs.yml               # Job definitions
│   └── pipelines.yml          # DLT pipelines
├── src/
│   ├── notebooks/             # Data processing notebooks
│   ├── dlt/                   # Delta Live Tables
│   └── libraries/             # Reusable code
├── config/                    # Environment configs
├── tests/                     # Unit & integration tests
└── .github/workflows/         # CI/CD pipelines
```

## ⚠️ Troubleshooting

### Issue: "databricks command not found"
```powershell
# Add Databricks CLI to PATH
$env:PATH += ";$env:USERPROFILE\databricks"
```

### Issue: "Authentication failed"
```powershell
# Reconfigure authentication
databricks configure --token
```

### Issue: "Bundle validation failed"
```powershell
# Check for syntax errors
databricks bundle validate -t dev --debug
```

### Issue: "Permission denied"
- Verify your token has correct permissions
- Check workspace access rights
- Ensure you can create resources in the workspace

## 💡 Tips

1. **Start with dev environment** - Always test in dev first
2. **Use bundle commands** - Let bundle handle resource management
3. **Check logs** - Job logs are available in Databricks UI
4. **Version control** - Commit changes regularly
5. **Read the docs** - Check README.md for detailed information

## 📚 Learn More

- **[Full README](README.md)** - Complete documentation
- **[Deployment Guide](DEPLOYMENT.md)** - Production deployment
- **[Contributing](CONTRIBUTING.md)** - Development guidelines
- **[Databricks Docs](https://docs.databricks.com/dev-tools/bundles/)** - Asset Bundles guide

## 🆘 Getting Help

- Check the [issues](../../issues) page
- Review [documentation](README.md)
- Ask your team lead
- Check Databricks documentation

## ✅ Success Checklist

- [ ] Repository cloned
- [ ] Dependencies installed
- [ ] Databricks CLI configured
- [ ] Bundle validated
- [ ] Deployed to dev environment
- [ ] First job run successfully
- [ ] Explored notebooks in workspace
- [ ] GitHub Actions configured (optional)

**Congratulations! You're ready to start building data pipelines! 🎉**
