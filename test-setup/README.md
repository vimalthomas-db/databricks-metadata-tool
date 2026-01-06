# Test Environment Setup

This folder contains scripts to create a test environment for the Databricks Metadata Tool.

## What Gets Created

### Azure Resources (via Terraform)

| Resource | Name | Purpose |
|----------|------|---------|
| Resource Group | `rg-dbmeta-test` | Container for all resources |
| Admin Workspace | `dbw-dbmeta-test-admin` | Unity Catalog enabled, collection point |
| Analytics Workspace | `dbw-dbmeta-test-analytics` | Unity Catalog enabled, has test tables |
| Legacy Workspace | `dbw-dbmeta-test-legacy` | HMS only (no UC) |
| Storage Account | `stdbmetatestsuc` | Unity Catalog metastore storage |

### Test Catalogs (via Notebook)

| Catalog | Tables | Purpose |
|---------|--------|---------|
| `small_catalog` | ~50 | Test Tier 2 (SQL Warehouse) |
| `large_catalog` | ~300 | Test Tier 3 (Spark Job) |
| `mixed_catalog` | ~20 | Various table types |

## Setup Steps

### Step 1: Create Azure Resources

```bash
cd test-setup

# Copy and edit the variables file
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your subscription ID

# Initialize and apply Terraform
terraform init
terraform plan
terraform apply
```

### Step 2: Configure Unity Catalog

After Terraform completes:

1. Go to Azure Portal → Databricks Account Console
2. Create a Unity Catalog Metastore in the same region
3. Assign the metastore to `dbw-dbmeta-test-admin` and `dbw-dbmeta-test-analytics`
4. Do NOT assign to `dbw-dbmeta-test-legacy` (keep it HMS-only)

### Step 3: Configure Storage for Collection Outputs

This creates the storage infrastructure for the `--write-to-volume` feature:

```bash
cd test-setup

# Set environment variables
export DATABRICKS_CLIENT_ID="your-sp-client-id"
export DATABRICKS_CLIENT_SECRET="your-sp-secret"
export DATABRICKS_ACCOUNT_ID="your-account-id"

# Run the storage configuration script
python configure_metastore_storage.py
```

This creates:
| Resource | Name | Purpose |
|----------|------|---------|
| Access Connector | `ac-dbmeta-test` | Azure managed identity for storage |
| Storage Credential | `metastore-credential` | Unity Catalog credential |
| External Location | `collection-storage` | Maps to storage account |
| Catalog | `collection_catalog` | For collection outputs |
| Schema | `collection_schema` | Under catalog |
| Volume | `collection_volume` | Stores output CSVs |

### Step 4: Create Test Tables

1. Open `dbw-dbmeta-test-analytics` workspace
2. Import `create_test_tables.py` as a notebook
3. Attach to a cluster and run all cells
4. This creates ~370 test tables across 3 catalogs

### Step 5: Create Service Principal (if not done)

1. Go to Azure AD → App Registrations → New Registration
2. Name: `sp-dbmeta-test`
3. Create a client secret
4. In Databricks Account Console:
   - Add the service principal
   - Grant Account Admin role
5. Set environment variables:
   ```bash
   export DATABRICKS_CLIENT_ID="your-app-id"
   export DATABRICKS_CLIENT_SECRET="your-secret"
   export DATABRICKS_ACCOUNT_ID="your-account-id"
   ```

### Step 6: Test the Metadata Tool

```bash
cd ..  # Back to project root

# Activate venv
source venv/bin/activate
set -a && source .env && set +a

# Test scan only
python main.py --scan

# Test collect (dry-run, no sizes)
python main.py --collect-dryrun \
  --admin-workspace "https://adb-xxx.azuredatabricks.net" \
  --warehouse-id <your-warehouse-id>

# Test collect with volume upload
python main.py --collect-dryrun \
  --admin-workspace "https://adb-xxx.azuredatabricks.net" \
  --warehouse-id <your-warehouse-id> \
  --write-to-volume

# Full collect with sizes and volume upload
python main.py --collect \
  --admin-workspace "https://adb-xxx.azuredatabricks.net" \
  --warehouse-id <your-warehouse-id> \
  --write-to-volume
```

Volume path after upload:
```
/Volumes/collection_catalog/collection_schema/collection_volume/staging/
```

## Cleanup

### Delete Test Tables

1. Open `dbw-dbmeta-test-analytics` workspace
2. Import and run `cleanup_test_tables.py`
3. Set "Confirm Delete?" to "YES"

### Delete Azure Resources

```bash
cd test-setup
terraform destroy
```

## Tier Testing Reference

| Tier | Condition | What to Test |
|------|-----------|--------------|
| Tier 1 | `system.information_schema.table_storage_info` works | Any catalog |
| Tier 2 | < 200 tables | `small_catalog` (50 tables) |
| Tier 3 | >= 200 tables + cluster_id | `large_catalog` (300 tables) |
| Tier 3 fallback | >= 200 tables, no cluster_id | `large_catalog` without `--cluster-id` |

