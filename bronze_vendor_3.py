# Databricks notebook source
# MAGIC %md
# MAGIC ## Vendor C : Custom Platform (Asian Markets)

# COMMAND ----------

# MAGIC %md
# MAGIC **Import Statements**

# COMMAND ----------

from pyspark.sql.functions import(input_file_name,current_timestamp,lit,col,when,coalesce,from_json,regexp_replace)

# COMMAND ----------

# MAGIC %sql
# MAGIC Use catalog live_challenge;

# COMMAND ----------

catalog = "workspace"
schema = "de_session_dataset"
volume = "vendor_c"

vendor_data_dir = "vendor_c_customplatform_data"
target_table = "live_challenge.bronze.orders_vendor_c_custom_platform"
records = "orders*.tsv"

path = f"/Volumes/{catalog}/{schema}/{volume}/{vendor_data_dir}/{records}"

# COMMAND ----------

df_raw=(spark.read.option('header','true').option('inferSchema','true')
        .option("delimiter","\t")
        .option("quote",'"')
        .option("escape",'"')
        .csv(f"{path}"))
display(df_raw)

# COMMAND ----------

df_bronze= (df_raw
            .withColumn("_source_file",col("_metadata.file_path"))
            .withColumn("_ingestion_timestamp",current_timestamp())
            .withColumn("_source_vendor",lit("vendor_c"))
)
display(df_bronze)

(
df_bronze.write.mode("overwrite").format("delta")
.option("mergeSchema", "true")
.saveAsTable(target_table)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from live_challenge.bronze.orders_vendor_c_custom_platform

# COMMAND ----------

# MAGIC %sql
# MAGIC desc live_challenge.bronze.orders_vendor_c_custom_platform