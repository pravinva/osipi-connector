# Databricks notebook source
# MAGIC %md
# MAGIC # Optimize Delta Tables
# MAGIC
# MAGIC Optimizes Delta tables after parallel ingestion.

# COMMAND ----------

# Get parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Time-Series Table

# COMMAND ----------

timeseries_table = f"{catalog}.{schema}.pi_timeseries"

print(f"🔄 Optimizing {timeseries_table}...")

# Run OPTIMIZE with ZORDER
spark.sql(f"""
    OPTIMIZE {timeseries_table}
    ZORDER BY (tag_webid, timestamp)
""")

print(f"✅ Optimized {timeseries_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize AF Hierarchy Table

# COMMAND ----------

hierarchy_table = f"{catalog}.{schema}.pi_asset_hierarchy"

print(f"🔄 Optimizing {hierarchy_table}...")

spark.sql(f"""
    OPTIMIZE {hierarchy_table}
""")

print(f"✅ Optimized {hierarchy_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Event Frames Table

# COMMAND ----------

events_table = f"{catalog}.{schema}.pi_event_frames"

print(f"🔄 Optimizing {events_table}...")

spark.sql(f"""
    OPTIMIZE {events_table}
    ZORDER BY (start_time, template_name)
""")

print(f"✅ Optimized {events_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vacuum Old Data (Optional)

# COMMAND ----------

# Vacuum files older than 7 days
retention_hours = 168  # 7 days

print(f"🔄 Vacuuming files older than {retention_hours} hours...")

for table in [timeseries_table, hierarchy_table, events_table]:
    spark.sql(f"""
        VACUUM {table} RETAIN {retention_hours} HOURS
    """)
    print(f"✅ Vacuumed {table}")

# COMMAND ----------

# Summary
print(f"""
╔══════════════════════════════════════════════════════════════╗
║           Delta Table Optimization Complete                  ║
╚══════════════════════════════════════════════════════════════╝

Tables Optimized:
  - {timeseries_table}
  - {hierarchy_table}
  - {events_table}

✅ Optimization complete!
""")



