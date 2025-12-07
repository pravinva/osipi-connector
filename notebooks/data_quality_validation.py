# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Validation
# MAGIC
# MAGIC Validates data quality after ingestion from all partitions.

# COMMAND ----------

from datetime import datetime

# Get parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 1: Row Counts

# COMMAND ----------

# Check total rows ingested
df_stats = spark.sql(f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT tag_webid) as unique_tags,
        COUNT(DISTINCT partition_id) as partitions_completed,
        MIN(timestamp) as earliest_timestamp,
        MAX(timestamp) as latest_timestamp
    FROM {catalog}.{schema}.pi_timeseries
    WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
""")

stats = df_stats.collect()[0]

print(f"""
📊 Ingestion Statistics:
   Total Rows: {stats.total_rows:,}
   Unique Tags: {stats.unique_tags:,}
   Partitions: {stats.partitions_completed}
   Time Range: {stats.earliest_timestamp} to {stats.latest_timestamp}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 2: Partition Completeness

# COMMAND ----------

# Check all partitions completed successfully
df_partition_stats = spark.sql(f"""
    SELECT 
        partition_id,
        COUNT(*) as rows_ingested,
        COUNT(DISTINCT tag_webid) as unique_tags,
        MAX(timestamp) as latest_timestamp
    FROM {catalog}.{schema}.pi_timeseries
    WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
    GROUP BY partition_id
    ORDER BY partition_id
""")

print("\n📦 Per-Partition Statistics:")
df_partition_stats.display()

# Check for missing partitions
expected_partitions = 10  # From DABS config
actual_partitions = df_partition_stats.count()

if actual_partitions < expected_partitions:
    print(f"⚠️  WARNING: Expected {expected_partitions} partitions, found {actual_partitions}")
else:
    print(f"✅ All {expected_partitions} partitions completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 3: Data Quality

# COMMAND ----------

# Check data quality flags
df_quality = spark.sql(f"""
    SELECT 
        SUM(CASE WHEN quality_good THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as quality_good_pct,
        SUM(CASE WHEN quality_questionable THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as quality_questionable_pct,
        SUM(CASE WHEN quality_substituted THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as quality_substituted_pct,
        SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as null_value_pct
    FROM {catalog}.{schema}.pi_timeseries
    WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
""")

quality_stats = df_quality.collect()[0]

print(f"""
✅ Data Quality Metrics:
   Good Quality: {quality_stats.quality_good_pct:.2f}%
   Questionable: {quality_stats.quality_questionable_pct:.2f}%
   Substituted: {quality_stats.quality_substituted_pct:.2f}%
   Null Values: {quality_stats.null_value_pct:.2f}%
""")

# Alert if quality is poor
if quality_stats.quality_good_pct < 90:
    print("⚠️  WARNING: Good quality percentage is below 90%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 4: Freshness

# COMMAND ----------

# Check data freshness
df_freshness = spark.sql(f"""
    SELECT 
        MAX(timestamp) as latest_data_timestamp,
        MAX(ingestion_timestamp) as latest_ingestion_timestamp,
        TIMESTAMPDIFF(MINUTE, MAX(timestamp), CURRENT_TIMESTAMP()) as data_age_minutes
    FROM {catalog}.{schema}.pi_timeseries
""")

freshness = df_freshness.collect()[0]

print(f"""
🕐 Data Freshness:
   Latest Data: {freshness.latest_data_timestamp}
   Latest Ingestion: {freshness.latest_ingestion_timestamp}
   Data Age: {freshness.data_age_minutes} minutes
""")

if freshness.data_age_minutes > 60:
    print(f"⚠️  WARNING: Data is {freshness.data_age_minutes} minutes old")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Create validation summary
validation_results = {
    "validation_timestamp": datetime.now(),
    "total_rows": stats.total_rows,
    "unique_tags": stats.unique_tags,
    "partitions_completed": stats.partitions_completed,
    "expected_partitions": expected_partitions,
    "quality_good_pct": quality_stats.quality_good_pct,
    "data_age_minutes": freshness.data_age_minutes,
    "status": "PASSED" if actual_partitions == expected_partitions and quality_stats.quality_good_pct >= 90 else "WARNING"
}

# Save validation results
df_validation = spark.createDataFrame([validation_results])
validation_table = f"{catalog}.{schema}.pipeline_validation_results"
df_validation.write.format("delta").mode("append").saveAsTable(validation_table)

print(f"""
╔══════════════════════════════════════════════════════════════╗
║           Data Quality Validation Complete                   ║
╚══════════════════════════════════════════════════════════════╝

Overall Status: {validation_results['status']}

✅ Validation complete!
""")

