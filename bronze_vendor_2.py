# Databricks notebook source
# MAGIC %md
# MAGIC # ## Vendor B : Magento Export (European Market)

# COMMAND ----------

# MAGIC %md
# MAGIC **Import Statements**

# COMMAND ----------

from pyspark.sql.functions import(input_file_name,current_timestamp,lit,col,when,coalesce,from_json,regexp_replace)

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing Bronze Layer**

# COMMAND ----------

# MAGIC %sql
# MAGIC Use catalog live_challenge;

# COMMAND ----------

# MAGIC %md
# MAGIC **Common Variables**

# COMMAND ----------

catalog = "workspace"
schema = "de_session_dataset"
volume = "vendor_b"

vendor_data_dir = "vendor_b_magento_data"
target_table = "bronze.orders_vendor_b_magento"
records = "orders*.csv"

path = f"/Volumes/{catalog}/{schema}/{volume}/{vendor_data_dir}/{records}"

# COMMAND ----------

df_raw=(spark.read.option('header','true').option('inferSchema','true').option("delimiter","|").csv(f"{path}"))
display(df_raw)

# COMMAND ----------

df_bronze= (df_raw
            .withColumn("_source_file",col("_metadata.file_path"))
            .withColumn("_ingestion_timestamp",current_timestamp())
            .withColumn("_source_vendor",lit("vendor_b"))
)
display(df_bronze)

# COMMAND ----------

df_bronze.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("live_challenge.bronze.orders_vendor_b_magento")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from live_challenge.bronze.orders_vendor_b_magento

# COMMAND ----------

# MAGIC %sql
# MAGIC desc live_challenge.bronze.orders_vendor_b_magento