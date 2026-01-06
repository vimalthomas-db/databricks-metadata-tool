# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup Test Tables
# MAGIC 
# MAGIC Run this notebook to delete all test catalogs and tables.

# COMMAND ----------

# Confirm before deleting
dbutils.widgets.dropdown("confirm_delete", "NO", ["NO", "YES"], "Confirm Delete?")

# COMMAND ----------

confirm = dbutils.widgets.get("confirm_delete")

if confirm != "YES":
    print("❌ Delete NOT confirmed. Set 'Confirm Delete?' to 'YES' to proceed.")
    dbutils.notebook.exit("Aborted - not confirmed")

# COMMAND ----------

# Delete catalogs
catalogs_to_delete = ["small_catalog", "large_catalog", "mixed_catalog"]

for catalog in catalogs_to_delete:
    try:
        spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")
        print(f"✓ Deleted: {catalog}")
    except Exception as e:
        print(f"✗ Error deleting {catalog}: {str(e)[:50]}")

# COMMAND ----------

print("\n" + "="*60)
print("CLEANUP COMPLETE")
print("="*60)
print("All test catalogs have been deleted.")

