# Deployment Guide

This guide provides detailed instructions for deploying the Retail Analytics Solution using Databricks Asset Bundles.

## Prerequisites

- Databricks workspace (AWS, Azure, or GCP)
- Databricks CLI installed and configured
- Git repository access
- GitHub Actions secrets configured
- Appropriate permissions in Databricks workspace

## Environment Setup

### 1. Databricks Workspace Configuration

Each environment requires:
- Unity Catalog enabled (recommended)
- Appropriate compute resources
- Service principals (for staging and production)
- Secret scopes configured

### 2. GitHub Secrets

Configure the following secrets in your GitHub repository:

**Development:**
- `DATABRICKS_HOST_DEV`: https://your-workspace-dev.cloud.databricks.com
- `DATABRICKS_TOKEN_DEV`: Personal access token or service principal token

**Staging:**
- `DATABRICKS_HOST_STAGING`: Staging workspace URL
- `DATABRICKS_TOKEN_STAGING`: Service principal token

**Production:**
- `DATABRICKS_HOST_PROD`: Production workspace URL
- `DATABRICKS_TOKEN_PROD`: Service principal token

### 3. Service Principal Setup

For production environments, create service principals:

```bash
# Create service principal
databricks service-principals create --display-name "retail-analytics-prod"

# Grant permissions
databricks workspace-access grant \
  --service-principal <sp-id> \
  --permission CAN_USE
```

## Deployment Process

### Development Deployment

Automatic deployment on push to `develop` branch:

```bash
# Manual deployment
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

**What gets deployed:**
- All notebooks to workspace
- Job definitions
- DLT pipeline configurations
- Development mode settings

### Staging Deployment

Automatic deployment on push to `main` branch:

```bash
# Manual deployment
databricks bundle validate -t staging
databricks bundle deploy -t staging
```

**What gets deployed:**
- Production-mode resources
- Scheduled jobs (paused by default)
- Production-grade cluster configurations

### Production Deployment

Manual deployment via GitHub Actions workflow:

1. Navigate to Actions tab in GitHub
2. Select "CD - Deploy to Production" workflow
3. Click "Run workflow"
4. Enter version tag (e.g., v1.0.0)
5. Confirm deployment

Or via CLI:

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

## Post-Deployment Verification

### 1. Verify Jobs

```bash
# List deployed jobs
databricks jobs list

# Get job details
databricks jobs get --job-id <job-id>
```

### 2. Verify Pipelines

```bash
# List pipelines
databricks pipelines list

# Get pipeline details
databricks pipelines get --pipeline-id <pipeline-id>
```

### 3. Run Test Job

```bash
# Run a job
databricks bundle run daily_etl_job -t dev

# Check job status
databricks jobs runs get --run-id <run-id>
```

## Rollback Procedures

### Rolling Back a Deployment

1. **Identify the previous version:**
   ```bash
   git log --oneline
   ```

2. **Deploy the previous version:**
   ```bash
   git checkout <previous-commit>
   databricks bundle deploy -t prod
   ```

3. **Verify rollback:**
   - Check job definitions
   - Verify pipeline configurations
   - Run smoke tests

### Emergency Hotfix

1. **Create hotfix branch from main:**
   ```bash
   git checkout main
   git pull
   git checkout -b hotfix/critical-fix
   ```

2. **Make and test fix:**
   ```bash
   # Make changes
   databricks bundle deploy -t dev
   # Test thoroughly
   ```

3. **Deploy to production:**
   ```bash
   git push origin hotfix/critical-fix
   # Create PR and fast-track approval
   # Deploy via workflow
   ```

4. **Merge back to main and develop:**
   ```bash
   git checkout main
   git merge hotfix/critical-fix
   git push origin main
   
   git checkout develop
   git merge hotfix/critical-fix
   git push origin develop
   ```

## Configuration Management

### Environment-Specific Configuration

Edit configuration files in `config/` directory:

- `config/dev.yml`: Development settings
- `config/staging.yml`: Staging settings
- `config/prod.yml`: Production settings

### Bundle Variables

Override variables during deployment:

```bash
databricks bundle deploy -t prod \
  --var="catalog_name=retail_prod" \
  --var="notification_email=ops@company.com"
```

## Monitoring Deployments

### GitHub Actions

Monitor deployments in GitHub Actions:
- View workflow runs
- Check deployment logs
- Review test results
- Download artifacts

### Databricks Workspace

Monitor resources in Databricks:
- Job run history
- Pipeline updates
- Notebook revisions
- Cluster usage

## Troubleshooting

### Common Issues

**1. Bundle validation fails:**
```bash
# Check syntax
databricks bundle validate -t dev

# View detailed errors
databricks bundle deploy -t dev --debug
```

**2. Permission errors:**
- Verify service principal has correct permissions
- Check workspace access
- Validate secret scope access

**3. Resource conflicts:**
- Check for naming conflicts
- Verify workspace paths
- Review cluster configurations

**4. Job failures:**
- Check job logs in Databricks
- Verify notebook paths
- Check cluster specifications
- Review parameter passing

### Getting Help

1. Check deployment logs in GitHub Actions
2. Review Databricks job run logs
3. Validate bundle configuration locally
4. Contact DevOps team
5. Check Databricks documentation

## Best Practices

1. **Always test in dev first**
2. **Use meaningful version tags**
3. **Document deployment changes**
4. **Monitor resource usage**
5. **Maintain deployment logs**
6. **Schedule deployments during low-traffic periods**
7. **Keep stakeholders informed**
8. **Have rollback plan ready**

## Maintenance

### Regular Tasks

**Weekly:**
- Review failed job runs
- Check resource utilization
- Update dependencies
- Clean up old artifacts

**Monthly:**
- Review and optimize costs
- Update Databricks runtime versions
- Security patching
- Performance optimization

**Quarterly:**
- Review and update documentation
- Audit permissions
- Disaster recovery testing
- Capacity planning

## Support

For deployment issues:
- Create an issue in GitHub
- Contact platform team
- Check internal documentation
- Review Databricks support portal
