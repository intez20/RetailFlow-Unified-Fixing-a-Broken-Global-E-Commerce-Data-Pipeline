# Databricks notebook source
# MAGIC %md
# MAGIC # ## Vendor A : Shopify Export (US Market)

# COMMAND ----------

# MAGIC %md
# MAGIC Import Code
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import(input_file_name,current_timestamp,lit,col,when,coalesce,from_json,regexp_replace)

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing Bronze Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS live_challenge;
# MAGIC Use catalog live_challenge;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Common Variables

# COMMAND ----------

catalog = "workspace"
schema = "de_session_dataset"
volume = "vendor_a"

vendor_data_dir = "vendor_a_shopify_data"
target_table = "bronze.orders_vendor_a_shopify"
records = "orders*.csv"

path = f"/Volumes/{catalog}/{schema}/{volume}/{vendor_data_dir}/{records}"

# COMMAND ----------

df_raw= spark.read.option("header","true").option("inferschema","true").csv(f"{path}")
df_raw.printSchema()

# COMMAND ----------

df_bronze= (df_raw
            .withColumn("_source_file",col("_metadata.file_path"))
            .withColumn("_ingestion_timestamp",current_timestamp())
            .withColumn("_source_vendor",lit("vendor_a"))
)
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Writng to Delta

# COMMAND ----------

# DBTITLE 1,Write Bronze Table to Delta
# df_bronze.write.format("delta").saveAsTable("live_challenge.bronze.orders_vendor_a_shopify")
df_bronze.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("live_challenge.bronze.orders_vendor_a_shopify")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from live_challenge.bronze.orders_vendor_a_shopify LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC desc live_challenge.bronze.orders_vendor_a_shopify 