# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Metadata Collection Tool
# MAGIC 
# MAGIC This notebook runs the metadata collection tool directly in a Databricks workspace.
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Cluster with Unity Catalog access
# MAGIC - SQL Warehouse ID for table size queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for configuration
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.dropdown("mode", "collect-dryrun", ["scan", "collect", "collect-dryrun"], "Mode")
dbutils.widgets.text("output_catalog", "collection_catalog", "Output Catalog")
dbutils.widgets.text("output_schema", "collection_schema", "Output Schema")

# COMMAND ----------

# Get widget values
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
MODE = dbutils.widgets.get("mode")
OUTPUT_CATALOG = dbutils.widgets.get("output_catalog")
OUTPUT_SCHEMA = dbutils.widgets.get("output_schema")

print(f"Mode: {MODE}")
print(f"Warehouse ID: {WAREHOUSE_ID}")
print(f"Output: {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install databricks-sdk python-dotenv pyyaml requests -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Collection

# COMMAND ----------

import os
import sys
from datetime import datetime

# Set up environment
workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
account_id = spark.conf.get("spark.databricks.clusterUsageTags.accountId", "")

print(f"Workspace: {workspace_url}")
print(f"Account ID: {account_id}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient, AccountClient

# Initialize clients
ws_client = WorkspaceClient()
account_client = AccountClient(host="https://accounts.azuredatabricks.net", account_id=account_id)

# Get current metastore
current_metastore = ws_client.metastores.current()
print(f"Metastore: {current_metastore.name} ({current_metastore.metastore_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scan: Discover Workspaces and Metastores

# COMMAND ----------

# Get widget values again after restart
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
MODE = dbutils.widgets.get("mode")
OUTPUT_CATALOG = dbutils.widgets.get("output_catalog")
OUTPUT_SCHEMA = dbutils.widgets.get("output_schema")

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_path = f"/Volumes/{OUTPUT_CATALOG}/{OUTPUT_SCHEMA}/collection_volume/staging"

print(f"Output path: {output_path}")

# COMMAND ----------

# Collect workspaces from account
workspaces = list(account_client.workspaces.list())
print(f"Found {len(workspaces)} workspaces in account")

# Collect metastores from account  
metastores = list(account_client.metastores.list())
print(f"Found {len(metastores)} metastores in account")

# COMMAND ----------

# Create workspaces DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

workspace_data = []
for ws in workspaces:
    workspace_data.append({
        'workspace_id': str(ws.workspace_id),
        'workspace_name': ws.workspace_name,
        'deployment_name': ws.deployment_name,
        'cloud': ws.cloud or 'azure',
        'region': ws.location,
        'workspace_status': ws.workspace_status.value if ws.workspace_status else 'UNKNOWN'
    })

df_workspaces = spark.createDataFrame(workspace_data)
display(df_workspaces)

# COMMAND ----------

# Save workspaces to volume
df_workspaces.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", "|").csv(f"{output_path}/account_workspaces_{timestamp}")
print(f"Saved workspaces to {output_path}/account_workspaces_{timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect: Catalog Metadata

# COMMAND ----------

if MODE in ['collect', 'collect-dryrun']:
    # List catalogs
    catalogs = list(ws_client.catalogs.list())
    catalogs = [c for c in catalogs if c.name not in ['system', 'samples']]
    print(f"Found {len(catalogs)} catalogs to process")
    
    for cat in catalogs:
        print(f"  - {cat.name}")

# COMMAND ----------

if MODE in ['collect', 'collect-dryrun']:
    # Collect tables from all catalogs
    all_tables = []
    
    for catalog in catalogs:
        print(f"Processing catalog: {catalog.name}")
        
        try:
            schemas = list(ws_client.schemas.list(catalog_name=catalog.name))
            schemas = [s for s in schemas if s.name != 'information_schema']
            
            for schema in schemas:
                try:
                    tables = list(ws_client.tables.list(catalog_name=catalog.name, schema_name=schema.name))
                    for tbl in tables:
                        all_tables.append({
                            'catalog_name': catalog.name,
                            'schema_name': schema.name,
                            'table_name': tbl.name,
                            'table_type': tbl.table_type.value if tbl.table_type else 'UNKNOWN',
                            'data_source_format': tbl.data_source_format.value if tbl.data_source_format else '',
                            'storage_location': tbl.storage_location or '',
                            'owner': tbl.owner or ''
                        })
                except Exception as e:
                    print(f"  Error in schema {schema.name}: {e}")
        except Exception as e:
            print(f"  Error in catalog {catalog.name}: {e}")
    
    print(f"\nTotal tables collected: {len(all_tables)}")

# COMMAND ----------

if MODE in ['collect', 'collect-dryrun'] and all_tables:
    # Create and save tables DataFrame
    df_tables = spark.createDataFrame(all_tables)
    display(df_tables.limit(20))
    
    df_tables.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", "|").csv(f"{output_path}/collect_tables_{timestamp}")
    print(f"Saved {len(all_tables)} tables to {output_path}/collect_tables_{timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# List output files
files = dbutils.fs.ls(output_path)
print("Output files:")
for f in files:
    print(f"  {f.name} ({f.size} bytes)")

