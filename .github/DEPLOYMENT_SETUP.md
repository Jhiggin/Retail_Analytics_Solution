# GitHub Actions Deployment Setup Guide

This guide walks you through setting up GitHub Actions for automated deployment of the Retail Analytics Solution to Databricks.

## Prerequisites

- GitHub repository created and cloned locally
- Databricks workspace with Unity Catalog enabled
- Admin access to both GitHub and Databricks

## Step-by-Step Setup

### 1. Create Databricks Personal Access Token

1. Log in to your Databricks workspace
2. Click your profile icon in the top right corner
3. Select **User Settings**
4. Click **Developer** tab
5. Click **Generate new token**
6. Set expiration period (e.g., 90 days)
7. Click **Generate**
8. **Copy the token** - you'll need this in the next step

### 2. Gather Databricks Workspace URL

1. In your Databricks workspace, look at the URL in your browser
2. It should look like: `https://adb-123456789.azuredatabricks.net/`
3. Copy the complete URL (you'll need this as `DATABRICKS_HOST`)

### 3. Add GitHub Secrets

1. Go to your GitHub repository
2. Click **Settings** (top navigation bar)
3. Click **Secrets and variables** → **Actions** (left sidebar)
4. Click **New repository secret** button

**Add the following secrets:**

#### Secret 1: DATABRICKS_HOST
- **Name**: `DATABRICKS_HOST`
- **Value**: Your complete Databricks workspace URL (e.g., `https://adb-123456789.azuredatabricks.net/`)
- Click **Add secret**

#### Secret 2: DATABRICKS_TOKEN
- **Name**: `DATABRICKS_TOKEN`
- **Value**: The personal access token you generated in step 1
- Click **Add secret**

### 4. Configure Databricks Deployment Targets

The project is pre-configured with two deployment targets in `databricks.yml`:

**Development (`dev`)**
- Triggered when you push to `develop` branch
- Deploys to `retail_dev` catalog
- Used for testing and experimentation

**Production (`prod`)**
- Triggered when you push to `main` branch
- Deploys to `retail_prod` catalog
- Used for production workloads

Ensure both catalogs exist in your Databricks workspace, or update `databricks.yml` to use your desired catalog names.

### 5. Push Code to Trigger Deployment

Once secrets are configured:

```bash
# Push to develop branch (triggers dev deployment)
git push origin develop

# Push to main branch (triggers prod deployment)
git push origin main
```

The GitHub Actions workflows will automatically:
1. Validate the bundle configuration
2. Deploy the pipelines
3. Run basic tests

### 6. Monitor Deployment

1. Go to your GitHub repository
2. Click **Actions** tab (top navigation)
3. View the running or completed workflows
4. Click a workflow to see detailed logs
5. Check Databricks workspace to verify pipelines were created

## Deployment Workflows

### `deploy.yml` - Production Deployment

Triggers on push to `main` branch:
1. **Validate** - Checks bundle configuration is valid
2. **Deploy** - Creates/updates pipelines in production
3. **Test Pipelines** - Runs basic validation tests

### `dev-deploy.yml` - Development Deployment

Triggers on push to `develop` branch:
1. **Validate** - Checks bundle configuration is valid
2. **Deploy** - Creates/updates pipelines in development

## Troubleshooting

### Deployment Fails with "Authentication Error"

**Problem**: GitHub Actions cannot authenticate with Databricks

**Solution**:
- Verify `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are set correctly in GitHub Secrets
- Check the token hasn't expired in Databricks
- Ensure your Databricks user has permission to create pipelines

### Pipelines Not Appearing in Databricks

**Problem**: Deployment succeeds but pipelines don't show up in workspace

**Solution**:
- Check you're looking in the correct catalog (specified in `databricks.yml`)
- Verify the target you deployed to matches the catalog you're viewing
- Manually run: `databricks bundle list -C .` to see deployed resources

### "File not found" Errors During Deployment

**Problem**: Deployment fails saying files cannot be found

**Solution**:
- Verify all pipeline configuration files exist
- Check that ETL Python files are in the correct location
- Ensure there are no typos in file paths in YAML configurations

### Token Expiration

**Problem**: Deployments fail with authentication errors after some time

**Solution**:
- Generate a new Databricks personal access token
- Update the `DATABRICKS_TOKEN` secret in GitHub
- The workflows will use the new token on next push

## Updating Pipelines

To update an existing pipeline after initial deployment:

1. Make changes to ETL code or configurations
2. Commit and push to appropriate branch:
   - `develop` branch → updates dev environment
   - `main` branch → updates prod environment
3. GitHub Actions automatically validates and deploys changes
4. Monitor the workflow in the Actions tab

## Rollback

If a deployment causes issues:

1. Revert the code changes: `git revert <commit-hash>`
2. Push the revert commit to the branch
3. GitHub Actions will automatically deploy the previous working version

## Manual Deployment (Without GitHub Actions)

If you need to deploy without GitHub Actions:

```bash
# Validate configuration
databricks bundle validate -C .

# Deploy to development
databricks bundle deploy -C . --target dev

# Deploy to production
databricks bundle deploy -C . --target prod
```

## Security Best Practices

- Never commit `DATABRICKS_TOKEN` to version control
- Rotate tokens periodically (at least every 90 days)
- Use separate tokens for dev and prod if possible (create multiple secrets)
- Review GitHub Actions logs only with authorized team members
- Restrict secret access to necessary workflows only

## Next Steps

1. Configure and test GitHub Actions workflows
2. Push code to trigger first automated deployment
3. Verify pipelines appear in Databricks workspace
4. Monitor execution in Databricks
5. Set up alerts for pipeline failures (optional)

For additional help, see:
- [Databricks Asset Bundles Documentation](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
