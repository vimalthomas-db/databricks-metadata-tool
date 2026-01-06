# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader Ingestion for Metadata Collection
# MAGIC 
# MAGIC This notebook creates streaming tables that automatically ingest new CSV files 
# MAGIC from the collection volume using Auto Loader.
# MAGIC 
# MAGIC **Source:** `/Volumes/collection_catalog/collection_schema/collection_volume/staging/`
# MAGIC **Target:** Delta tables in `collection_catalog.collection_schema`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "collection_catalog"
SCHEMA = "collection_schema"
VOLUME = "collection_volume"

SOURCE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/staging"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/_checkpoints"

print(f"Source: {SOURCE_PATH}")
print(f"Checkpoints: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Definitions
# MAGIC 
# MAGIC Each table corresponds to a file pattern from the collection tool.

# COMMAND ----------

# Define tables and their file patterns
TABLES = {
    # Account-level tables
    "account_metastores": {
        "pattern": "account_metastores_*.csv",
        "schema": """
            metastore_id STRING,
            metastore_name STRING,
            region STRING,
            workspace_count INT,
            workspaces STRING,
            suggested_admin_workspace STRING,
            scan_timestamp STRING
        """
    },
    "account_summary": {
        "pattern": "account_summary_*.csv",
        "schema": """
            metric STRING,
            value STRING
        """
    },
    
    # Collect-level tables
    "collect_catalogs": {
        "pattern": "collect_catalogs_*.csv",
        "schema": """
            catalog_name STRING,
            catalog_type STRING,
            owner STRING,
            comment STRING,
            is_legacy_hms BOOLEAN,
            metastore_id STRING,
            metastore_name STRING,
            workspace_id STRING,
            workspace_name STRING,
            region STRING,
            created_at STRING,
            created_by STRING,
            collected_at STRING,
            collection_timestamp STRING
        """
    },
    "collect_tables": {
        "pattern": "collect_tables_*_*.csv",
        "schema": """
            table_name STRING,
            schema_name STRING,
            catalog_name STRING,
            full_name STRING,
            table_type STRING,
            data_source_format STRING,
            table_format STRING,
            is_delta BOOLEAN,
            is_iceberg BOOLEAN,
            is_uniform BOOLEAN,
            storage_location STRING,
            owner STRING,
            comment STRING,
            size_bytes LONG,
            num_files INT,
            metastore_id STRING,
            metastore_name STRING,
            workspace_id STRING,
            workspace_name STRING,
            region STRING,
            created_at STRING,
            created_by STRING,
            updated_at STRING,
            updated_by STRING,
            collected_at STRING,
            collection_timestamp STRING
        """
    },
    "collect_schemas": {
        "pattern": "collect_schemas_*_*.csv",
        "schema": """
            schema_name STRING,
            catalog_name STRING,
            owner STRING,
            comment STRING,
            workspace_id STRING,
            workspace_name STRING,
            created_at STRING,
            created_by STRING,
            updated_at STRING,
            updated_by STRING,
            collection_timestamp STRING
        """
    },
    "collect_volumes": {
        "pattern": "collect_volumes_*_*.csv",
        "schema": """
            volume_name STRING,
            schema_name STRING,
            catalog_name STRING,
            full_name STRING,
            volume_type STRING,
            storage_location STRING,
            owner STRING,
            comment STRING,
            size_bytes LONG,
            metastore_id STRING,
            metastore_name STRING,
            workspace_id STRING,
            workspace_name STRING,
            region STRING,
            created_at STRING,
            created_by STRING,
            updated_at STRING,
            updated_by STRING,
            collected_at STRING,
            collection_timestamp STRING
        """
    },
    "collect_external_locations": {
        "pattern": "collect_external_locations_*.csv",
        "schema": """
            location_name STRING,
            url STRING,
            credential_name STRING,
            storage_account STRING,
            storage_type STRING,
            owner STRING,
            comment STRING,
            read_only BOOLEAN,
            metastore_id STRING,
            metastore_name STRING,
            workspace_id STRING,
            workspace_name STRING,
            region STRING,
            created_at STRING,
            created_by STRING,
            updated_at STRING,
            updated_by STRING,
            collected_at STRING,
            collection_timestamp STRING
        """
    },
    "collect_workspaces": {
        "pattern": "collect_workspaces_*.csv",
        "schema": """
            workspace_id STRING,
            workspace_name STRING,
            workspace_url STRING,
            workspace_type STRING,
            workspace_status STRING,
            has_unity_catalog BOOLEAN,
            metastore_id STRING,
            metastore_name STRING,
            sku STRING,
            cloud STRING,
            cloud_region STRING,
            subscription_id STRING,
            resource_group STRING,
            aws_account_id STRING,
            credentials_id STRING,
            storage_configuration_id STRING,
            gcp_project_id STRING,
            cmk_managed_services BOOLEAN,
            cmk_managed_disk BOOLEAN,
            cmk_dbfs BOOLEAN,
            cmk_key_vault_uri STRING,
            cmk_key_name STRING,
            admin_user_count INT,
            admin_group_count INT,
            admin_users STRING,
            admin_groups STRING,
            ip_access_list_enabled BOOLEAN,
            ip_access_list_count INT,
            private_access_enabled BOOLEAN,
            serverless_private_endpoint_count INT,
            serverless_compute_enabled BOOLEAN,
            model_serving_enabled BOOLEAN,
            tokens_enabled BOOLEAN,
            max_token_lifetime_days STRING,
            results_downloading_enabled BOOLEAN,
            web_terminal_enabled BOOLEAN,
            user_isolation_enabled BOOLEAN,
            feature_count INT,
            collected_at STRING,
            collection_timestamp STRING
        """
    },
    "collect_workspace_features": {
        "pattern": "collect_workspace_features_*.csv",
        "schema": """
            workspace_id STRING,
            workspace_name STRING,
            feature_name STRING,
            feature_value STRING,
            metastore_id STRING,
            collection_timestamp STRING
        """
    }
}

print(f"Defined {len(TABLES)} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Tables with Auto Loader

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

def create_streaming_table(table_name: str, config: dict):
    """Create a streaming table using Auto Loader."""
    
    pattern = config["pattern"]
    schema = config["schema"]
    
    source = f"{SOURCE_PATH}/{pattern}"
    target = f"{CATALOG}.{SCHEMA}.{table_name}"
    checkpoint = f"{CHECKPOINT_PATH}/{table_name}"
    
    print(f"Creating: {target}")
    print(f"  Source pattern: {pattern}")
    
    # Read with Auto Loader
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", checkpoint)
        .option("header", "true")
        .option("delimiter", "|")
        .option("inferSchema", "false")
        .schema(schema)
        .load(source)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
    
    # Write as streaming table
    query = (df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # Process all available files, then stop
        .toTable(target)
    )
    
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Ingestion

# COMMAND ----------

# Create all streaming tables
queries = {}

for table_name, config in TABLES.items():
    try:
        queries[table_name] = create_streaming_table(table_name, config)
        print(f"  ✓ Started: {table_name}")
    except Exception as e:
        print(f"  ✗ Error with {table_name}: {e}")

# COMMAND ----------

# Wait for all queries to complete
import time

print("Waiting for ingestion to complete...")
for name, query in queries.items():
    try:
        query.awaitTermination()
        print(f"  ✓ Completed: {name}")
    except Exception as e:
        print(f"  ✗ Error: {name} - {e}")

print("\nIngestion complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables

# COMMAND ----------

# Show row counts for all tables
print("Table row counts:")
print("-" * 50)

for table_name in TABLES.keys():
    try:
        count = spark.table(f"{CATALOG}.{SCHEMA}.{table_name}").count()
        print(f"  {table_name}: {count:,} rows")
    except Exception as e:
        print(f"  {table_name}: ERROR - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data

# COMMAND ----------

# Preview collect_workspaces (unified workspace data)
display(spark.table(f"{CATALOG}.{SCHEMA}.collect_workspaces").limit(10))

# COMMAND ----------

# Preview collect_tables
display(spark.table(f"{CATALOG}.{SCHEMA}.collect_tables").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Examples

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tables by catalog
# MAGIC SELECT 
# MAGIC     catalog_name,
# MAGIC     COUNT(*) as table_count,
# MAGIC     SUM(size_bytes) / 1024 / 1024 / 1024 as total_size_gb
# MAGIC FROM collection_catalog.collection_schema.collect_tables
# MAGIC GROUP BY catalog_name
# MAGIC ORDER BY table_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workspaces by metastore
# MAGIC SELECT 
# MAGIC     metastore_name,
# MAGIC     COUNT(*) as workspace_count,
# MAGIC     SUM(CASE WHEN has_unity_catalog THEN 1 ELSE 0 END) as uc_enabled
# MAGIC FROM collection_catalog.collection_schema.collect_workspaces
# MAGIC GROUP BY metastore_name

