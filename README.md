# Databricks Metadata Tool

A multi-cloud account-level metadata collection tool for Databricks. Discovers and collects metadata from all workspaces, metastores, catalogs, and tables across your Databricks account.

## Features

- **Account-level discovery**: Scans all workspaces and metastores via Databricks Account API
- **Multi-cloud support**: Works with Azure, AWS, and GCP Databricks accounts
- **Unity Catalog metadata**: Collects catalogs, schemas, tables, volumes, and external locations
- **Table size collection**: Tiered approach for efficient size collection (Tier 1: bulk query, Tier 2: SQL warehouse, Tier 3: Spark cluster)
- **Workspace features**: Collects admin info, IP access lists, private endpoints, and configuration settings
- **Output options**: CSV files, Unity Catalog volume upload

## Installation

### From Wheel (Recommended)

```bash
pip install databricks_metadata_tool-1.0.0-py3-none-any.whl
```

### From Source

```bash
git clone https://github.com/your-org/databricks-metadata-tool.git
cd databricks-metadata-tool
pip install -e .
```

## Authentication

Set the following environment variables:

```bash
export DATABRICKS_ACCOUNT_ID=<your-account-id>
export DATABRICKS_CLIENT_ID=<service-principal-app-id>
export DATABRICKS_CLIENT_SECRET=<service-principal-secret>
```

## Usage

### Scan Mode (Discovery Only)

```bash
dbmeta --scan
```

### Collect Mode (Full Collection)

```bash
dbmeta --collect --admin-workspace <workspace-url> --warehouse-id <warehouse-id>
```

### Dry Run (Show Tier Selection)

```bash
dbmeta --collect-dryrun --admin-workspace <workspace-url> --warehouse-id <warehouse-id>
```

### With Volume Upload

```bash
dbmeta --collect --admin-workspace <workspace-url> --warehouse-id <warehouse-id> --write-to-volume
```

## Configuration

Copy `config.yaml.example` to `config.yaml` and customize as needed:

```yaml
databricks:
  account_id: null  # Set via DATABRICKS_ACCOUNT_ID env var
  cloud: azure      # azure, aws, or gcp

collection:
  collect_tables: true
  collect_sizes: true
  size_workers: 20
  size_threshold: 200  # Use Spark for catalogs with > 200 tables
```

## Output Files

### Account-level (Scan)
- `account_workspaces_*.csv` - All workspaces in the account
- `account_metastores_*.csv` - All metastores
- `account_summary_*.csv` - Summary statistics

### Metastore-level (Collect)
- `collect_workspaces_*.csv` - Enriched workspace details
- `collect_catalogs_*.csv` - All catalogs
- `collect_tables_<catalog>_*.csv` - Tables per catalog
- `collect_schemas_<catalog>_*.csv` - Schemas per catalog
- `collect_volumes_<catalog>_*.csv` - Volumes per catalog
- `collect_external_locations_*.csv` - External locations
- `collect_workspace_features_*.csv` - Workspace configuration features

## Running in Databricks

### Install in Notebook

```python
%pip install /Volumes/catalog/schema/volume/databricks_metadata_tool-1.0.0-py3-none-any.whl
```

### Run Collection

```python
!python -m databricks_metadata_tool.cli --collect \
    --admin-workspace "https://adb-xxx.azuredatabricks.net" \
    --warehouse-id "xxx" \
    --write-to-volume
```

## License

MIT License

