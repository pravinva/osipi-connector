# Databricks notebook source
# MAGIC %md
# MAGIC # Setup DLT Environment
# MAGIC
# MAGIC This notebook prepares the environment for DLT pipeline execution.
# MAGIC It handles cleanup of any pre-existing tables that might conflict with DLT.
# MAGIC
# MAGIC **Run this ONCE before the first DLT pipeline run.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "osipi"
SCHEMA = "bronze"
CHECKPOINT_SCHEMA = "checkpoints"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Checkpoint Schema: {CHECKPOINT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check for Existing Tables

# COMMAND ----------

# Check what tables exist
existing_tables = []

try:
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
    existing_tables.extend([(SCHEMA, row.tableName) for row in tables])
    print(f"Found {len(tables)} table(s) in {CATALOG}.{SCHEMA}")
    for row in tables:
        print(f"  - {row.tableName}")
except Exception as e:
    print(f"Schema {CATALOG}.{SCHEMA} does not exist or is empty: {e}")

try:
    checkpoint_tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{CHECKPOINT_SCHEMA}").collect()
    existing_tables.extend([(CHECKPOINT_SCHEMA, row.tableName) for row in checkpoint_tables])
    print(f"\nFound {len(checkpoint_tables)} table(s) in {CATALOG}.{CHECKPOINT_SCHEMA}")
    for row in checkpoint_tables:
        print(f"  - {row.tableName}")
except Exception as e:
    print(f"\nSchema {CATALOG}.{CHECKPOINT_SCHEMA} does not exist or is empty: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Identify DLT-Managed Tables

# COMMAND ----------

# Check if tables are DLT-managed
dlt_tables = []
non_dlt_tables = []

for schema, table_name in existing_tables:
    full_table_name = f"{CATALOG}.{schema}.{table_name}"

    try:
        # Check table properties for DLT markers
        describe = spark.sql(f"DESCRIBE EXTENDED {full_table_name}").collect()
        properties = {row.col_name: row.data_type for row in describe}

        # DLT tables have specific properties
        is_dlt = any('pipelines' in str(properties.get('Table Properties', '')).lower())

        if is_dlt:
            dlt_tables.append(full_table_name)
        else:
            non_dlt_tables.append(full_table_name)
    except Exception as e:
        print(f"Could not check {full_table_name}: {e}")
        non_dlt_tables.append(full_table_name)

print("\nDLT-Managed Tables:")
for table in dlt_tables:
    print(f"  ✓ {table}")

print("\nNon-DLT Tables (potential conflicts):")
for table in non_dlt_tables:
    print(f"  ⚠ {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Cleanup Non-DLT Tables
# MAGIC
# MAGIC **IMPORTANT:** This will drop tables that are not managed by DLT.
# MAGIC Only run this if you want to allow DLT to recreate these tables.

# COMMAND ----------

# Set to True to actually drop tables
DRY_RUN = True

if DRY_RUN:
    print("DRY RUN MODE - No tables will be dropped")
    print("\nTables that WOULD be dropped:")
    for table in non_dlt_tables:
        print(f"  - DROP TABLE {table}")
    print("\n⚠ Set DRY_RUN = False to actually drop these tables")
else:
    print("DROPPING non-DLT tables...")
    for table in non_dlt_tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"  ✓ Dropped {table}")
        except Exception as e:
            print(f"  ✗ Failed to drop {table}: {e}")

    print("\n✓ Cleanup complete. DLT can now create and manage these tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Schemas Exist

# COMMAND ----------

# Create schemas if they don't exist
print("Ensuring required schemas exist...")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"  ✓ Catalog: {CATALOG}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"  ✓ Schema: {CATALOG}.{SCHEMA}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CHECKPOINT_SCHEMA}")
print(f"  ✓ Schema: {CATALOG}.{CHECKPOINT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("SETUP COMPLETE")
print("=" * 80)
print("\nNext Steps:")
print("1. If DRY_RUN=True, review the tables that would be dropped")
print("2. If you're ready, set DRY_RUN=False and re-run Step 3")
print("3. Once cleanup is done, run your DLT pipeline")
print("\nNote: This is a one-time setup. Once DLT owns the tables,")
print("      you won't need to run this notebook again.")
print("=" * 80)
