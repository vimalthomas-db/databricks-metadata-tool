# Databricks notebook source
# MAGIC %md
# MAGIC # Create Test Tables for Metadata Tool Testing
# MAGIC 
# MAGIC This notebook creates sample schemas and tables to test the tier selection logic:
# MAGIC - **small_catalog**: ~50 tables (tests Tier 2 - SQL Warehouse)
# MAGIC - **large_catalog**: ~300 tables (tests Tier 3 - Spark Job)
# MAGIC - **mixed_catalog**: Various table types (views, different formats)
# MAGIC 
# MAGIC **Note:** Uses EXTERNAL tables with existing storage location.

# COMMAND ----------

# Parameters
dbutils.widgets.text("small_table_count", "50", "Tables in small_catalog")
dbutils.widgets.text("large_table_count", "300", "Tables in large_catalog")
dbutils.widgets.text("storage_location", "abfss://external-data@stdbmetaucpkp7dz.dfs.core.windows.net", "External Storage Location")

# COMMAND ----------

small_count = int(dbutils.widgets.get("small_table_count"))
large_count = int(dbutils.widgets.get("large_table_count"))
location_base = dbutils.widgets.get("storage_location")

print(f"Configuration:")
print(f"  - small_catalog: {small_count} tables")
print(f"  - large_catalog: {large_count} tables")
print(f"  - Storage location: {location_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalogs

# COMMAND ----------

# Create catalogs
catalogs = ["small_catalog", "large_catalog", "mixed_catalog"]
for cat in catalogs:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cat}")
    print(f"✓ Created catalog: {cat}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create small_catalog (Tier 2 test - < 200 tables)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS small_catalog.test_schema")

for i in range(small_count):
    table_name = f"small_catalog.test_schema.table_{i:04d}"
    location = f"{location_base}/test_tables/small_catalog/table_{i:04d}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT,
            name STRING,
            created_at TIMESTAMP
        ) USING DELTA LOCATION '{location}'
    """)
    if (i + 1) % 10 == 0:
        print(f"  Created {i + 1}/{small_count} tables...")

print(f"✓ small_catalog: {small_count} tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create large_catalog (Tier 3 test - >= 200 tables)

# COMMAND ----------

# Split across multiple schemas to be realistic
schemas_for_large = ["sales", "marketing", "finance", "operations", "analytics"]
tables_per_schema = large_count // len(schemas_for_large)

for schema in schemas_for_large:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS large_catalog.{schema}")
    
    for i in range(tables_per_schema):
        table_name = f"large_catalog.{schema}.table_{i:04d}"
        location = f"{location_base}/test_tables/large_catalog/{schema}/table_{i:04d}"
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT,
                data STRING,
                amount DECIMAL(18,2),
                event_date DATE,
                updated_at TIMESTAMP
            ) USING DELTA LOCATION '{location}'
        """)
    
    print(f"  ✓ large_catalog.{schema}: {tables_per_schema} tables")

print(f"✓ large_catalog: {large_count} total tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create mixed_catalog (various table types)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS mixed_catalog.managed_tables")
spark.sql("CREATE SCHEMA IF NOT EXISTS mixed_catalog.external_tables")
spark.sql("CREATE SCHEMA IF NOT EXISTS mixed_catalog.views")

# External Delta tables
for i in range(5):
    location = f"{location_base}/test_tables/mixed_catalog/delta_table_{i}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS mixed_catalog.external_tables.delta_table_{i} (
            id INT, value STRING
        ) USING DELTA LOCATION '{location}'
    """)
print("✓ Created 5 external Delta tables")

# External Parquet table
location = f"{location_base}/test_tables/mixed_catalog/parquet_table"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS mixed_catalog.external_tables.parquet_table (
        id INT, data STRING
    ) USING PARQUET LOCATION '{location}'
""")
print("✓ Created 1 external Parquet table")

# Views
spark.sql("""
    CREATE OR REPLACE VIEW mixed_catalog.views.sample_view AS
    SELECT 1 as id, 'test' as name
""")

spark.sql("""
    CREATE OR REPLACE VIEW mixed_catalog.views.complex_view AS
    SELECT id, value, current_timestamp() as queried_at
    FROM mixed_catalog.external_tables.delta_table_0
""")
print("✓ Created 2 views")

print("✓ mixed_catalog: Various table types created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Insert sample data (minimal - just for testing sizes)

# COMMAND ----------

from pyspark.sql import Row

# Add data to some tables so they have actual size
sample_data = [Row(id=i, name=f"Name_{i}", created_at=None) for i in range(100)]
df = spark.createDataFrame(sample_data)
df.write.mode("append").format("delta").save(f"{location_base}/test_tables/small_catalog/table_0000")

sample_data2 = [Row(id=i, data=f"Data_{i}", amount=float(i), event_date=None, updated_at=None) for i in range(100)]
df2 = spark.createDataFrame(sample_data2)
df2.write.mode("append").format("delta").save(f"{location_base}/test_tables/large_catalog/sales/table_0000")

print("✓ Sample data inserted into table_0000 in small_catalog and large_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

# Count tables in each catalog
catalogs = ["small_catalog", "large_catalog", "mixed_catalog"]

print("\n" + "="*60)
print("TEST ENVIRONMENT SUMMARY")
print("="*60)

total_tables = 0
for catalog in catalogs:
    try:
        # Count schemas
        schemas = spark.sql(f"SHOW SCHEMAS IN {catalog}").filter("databaseName != 'default'").count()
        
        # Count tables across all schemas
        tables = 0
        for row in spark.sql(f"SHOW SCHEMAS IN {catalog}").filter("databaseName != 'default'").collect():
            schema_name = row.databaseName
            tables += spark.sql(f"SHOW TABLES IN {catalog}.{schema_name}").count()
        
        total_tables += tables
        print(f"\n{catalog}:")
        print(f"  Schemas: {schemas}")
        print(f"  Tables:  {tables}")
    except Exception as e:
        print(f"\n{catalog}: Error - {str(e)[:50]}")

print(f"\n{'='*60}")
print(f"TOTAL TABLES: {total_tables}")
print("="*60)
print("\nReady for metadata tool testing!")
print(f"  - Tier 2 test: small_catalog ({small_count} tables)")
print(f"  - Tier 3 test: large_catalog ({large_count} tables)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Run only when done testing)

# COMMAND ----------

# Uncomment to delete all test catalogs and data
# spark.sql("DROP CATALOG IF EXISTS small_catalog CASCADE")
# spark.sql("DROP CATALOG IF EXISTS large_catalog CASCADE")
# spark.sql("DROP CATALOG IF EXISTS mixed_catalog CASCADE")

# To also delete the storage:
# dbutils.fs.rm(f"{location_base}/test_tables/", recurse=True)

# print("✓ All test catalogs and data deleted")
