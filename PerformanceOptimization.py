# Databricks notebook source
# MAGIC %md
# MAGIC Diagnosing Table Health

# COMMAND ----------

from pyspark.sql import Row

def diagnose_table_health(table_name: str):
    """
    Diagnose file count and size issues for a Delta TABLE (not a path).

    Example: diagnose_table_health("orders_silver_unified")
    """

    print(f"\n{'=' * 80}")
    print(f"TABLE HEALTH DIAGNOSIS: {table_name}")
    print(f"{'=' * 80}")

    # Get table statistics from Delta table
    df_stats = spark.sql(f"DESCRIBE DETAIL {table_name}")
    stats: Row = df_stats.collect()[0]

    num_files = stats.numFiles
    size_bytes = stats.sizeInBytes
    partition_cols = stats.partitionColumns

    print(f"\nTable Statistics:")
    print(f"  Number of Files: {num_files:,}")
    print(f"  Size in Bytes:   {size_bytes:,} ({size_bytes/1024/1024:.2f} MB)")
    print(f"  Partition Cols:  {partition_cols}")

    # Average file size
    avg_file_size = size_bytes / num_files if num_files > 0 else 0
    print(f"  Avg File Size:   {avg_file_size/1024:.2f} KB")

    # Health Assessment
    print(f"\nHealth Assessment:")

    if num_files > 10000:
        print(f"  ❌ FILE COUNT: {num_files:,} files (Target: <500)")
    elif num_files > 1000:
        print(f"  ⚠️  FILE COUNT: {num_files:,} files (Target: <500)")
    else:
        print(f"  ✅ FILE COUNT: {num_files:,} files")

    optimal_min = 128 * 1024 * 1024  # 128 MB
    optimal_max = 256 * 1024 * 1024  # 256 MB

    if avg_file_size < optimal_min:
        print(f"  ❌ FILE SIZE: {avg_file_size/1024:.2f} KB avg (Target: 128–256 MB)")
    elif avg_file_size > optimal_max:
        print(f"  ⚠️  FILE SIZE: {avg_file_size/1024/1024:.2f} MB avg (Target: 128–256 MB)")
    else:
        print(f"  ✅ FILE SIZE: {avg_file_size/1024/1024:.2f} MB avg")

    return stats

# COMMAND ----------

diagnose_table_health("live_challenge.silver.orders_silver_unified")
diagnose_table_health("live_challenge.gold.daily_revenue_by_vendor")
diagnose_table_health("live_challenge.gold.daily_product_revenue")
diagnose_table_health("live_challenge.gold.daily_revenue_by_country")
diagnose_table_health("live_challenge.gold.promo_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC OPTIMIZE and Z-ORDER

# COMMAND ----------

from pyspark.sql import Row

def optimize_table(table_name: str, zorder_columns=None):
    """
    Compact small files and optionally apply Z-ORDER on non-partition columns.
    """

    print(f"\n{'=' * 80}")
    print(f"OPTIMIZING TABLE: {table_name}")
    print(f"{'=' * 80}")

    # Describe table
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    partition_cols = set(detail.partitionColumns)

    print(f"Partition columns: {partition_cols}")

    # Filter out partition columns from ZORDER list
    if zorder_columns:
        z_cols_filtered = [c for c in zorder_columns if c not in partition_cols]
        if len(z_cols_filtered) < len(zorder_columns):
            dropped = set(zorder_columns) - set(z_cols_filtered)
            print(f"⚠️ Skipping partition columns in ZORDER: {dropped}")
        zorder_columns = z_cols_filtered or None

    print(f"\nBefore OPTIMIZE:")
    print(f"  Files: {detail.numFiles:,}")
    print(f"  Size:  {detail.sizeInBytes/1024/1024:.2f} MB")

    import time
    start_time = time.time()

    if zorder_columns:
        zorder_clause = ", ".join(zorder_columns)
        print(f"\nRunning OPTIMIZE with Z-ORDER BY ({zorder_clause})...")
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_clause})")
    else:
        print("\nRunning OPTIMIZE without Z-ORDER...")
        spark.sql(f"OPTIMIZE {table_name}")

    elapsed = time.time() - start_time

    after = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    print(f"\nAfter OPTIMIZE:")
    print(f"  Files: {after.numFiles:,}")
    print(f"  Size:  {after.sizeInBytes/1024/1024:.2f} MB")
    print(f"  Time:  {elapsed:.2f} seconds")

    if detail.numFiles > 0:
        reduction = (1 - after.numFiles / detail.numFiles) * 100
        print(f"\n✅ File count reduced by {reduction:.1f}% "
              f"({detail.numFiles:,} → {after.numFiles:,})")

    return after



# COMMAND ----------

optimize_table(
    table_name="live_challenge.silver.orders_silver_unified",
    zorder_columns=["order_timestamp", "country_code", "source_vendor"]
)


# COMMAND ----------

# MAGIC %md
# MAGIC VACUUM Old Files

# COMMAND ----------

def vacuum_table(table_name: str, retention_hours: int = 168):
    """
    Remove old files no longer referenced by a Delta TABLE.

    Default retention: 168 hours (7 days) - allows time travel
    Minimum: 0 hours (not recommended for production)
    """

    print(f"\n{'=' * 80}")
    print(f"VACUUM TABLE: {table_name}")
    print(f"{'=' * 80}")

    print(f"\nRetention period: {retention_hours} hours ({retention_hours/24:.1f} days)")
    
    print("This will remove files older than retention period that are no longer referenced.")

    # Run VACUUM on the table
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")

    print(f"\n✅ VACUUM completed")


# COMMAND ----------

vacuum_table("live_challenge.silver.orders_silver_unified", retention_hours=168)

# Optionally, Gold tables too:
vacuum_table("live_challenge.gold.daily_revenue_by_vendor", retention_hours=168)
vacuum_table("live_challenge.gold.daily_product_revenue", retention_hours=168)
vacuum_table("live_challenge.gold.daily_revenue_by_country", retention_hours=168)
vacuum_table("live_challenge.gold.promo_analysis", retention_hours=168)
