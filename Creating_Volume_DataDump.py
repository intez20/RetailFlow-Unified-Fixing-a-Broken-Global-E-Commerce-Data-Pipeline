# Databricks notebook source
# MAGIC %md
# MAGIC Schema for Project
# MAGIC

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.de_session_dataset
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Vendor C Volume Custom Platform

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS workspace.de_session_dataset.vendor_c
""")