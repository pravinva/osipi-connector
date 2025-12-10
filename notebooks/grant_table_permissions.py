# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Table Permissions
# MAGIC
# MAGIC Run this notebook AFTER the DLT pipeline completes to grant SELECT permissions
# MAGIC on all bronze tables to account users.
# MAGIC
# MAGIC This allows the Databricks App and all users to query the ingested data.

# COMMAND ----------

# Configuration
UC_CATALOG = "osipi"
UC_SCHEMA = "bronze"

tables = ["pi_timeseries", "pi_af_hierarchy", "pi_event_frames"]

print("=" * 80)
print("GRANTING TABLE PERMISSIONS")
print("=" * 80)
print()

# COMMAND ----------

# Grant SELECT on all tables to account users
for table in tables:
    try:
        full_table_name = f"{UC_CATALOG}.{UC_SCHEMA}.{table}"

        # Grant SELECT to account users
        spark.sql(f"""
            GRANT SELECT ON TABLE {full_table_name}
            TO `account users`
        """)

        print(f"✓ Granted SELECT on {full_table_name} to `account users`")

    except Exception as e:
        print(f"⚠️  Error granting permissions on {table}: {e}")

# COMMAND ----------

print()
print("=" * 80)
print("PERMISSIONS GRANTED")
print("=" * 80)
print()
print("All account users can now query:")
for table in tables:
    print(f"  - {UC_CATALOG}.{UC_SCHEMA}.{table}")
