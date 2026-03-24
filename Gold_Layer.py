# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Gold Layer Implementation (Business Analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC #Common Code

# COMMAND ----------

from pyspark.sql.functions import (
    sum as _sum, avg as _avg, round as _round, count, col, current_timestamp,  lit
)

from delta.tables import DeltaTable

silver_table = "live_challenge.silver.orders_silver_unified"

from pyspark.sql import functions as F

df_silver = spark.table(silver_table)

# Create schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS live_challenge.gold")


# COMMAND ----------

# DBTITLE 1,Cell 3
#select source_vendor,Date(order_timestamp),sum(total_amount_usd),count(order_id),sum(discount_amt_usd) from silver_layer where payment_status='completed' and is_duplicate is false Group by 1,2


df_result = (
    spark.table("live_challenge.silver.orders_silver_unified")
  
    .groupBy(
        F.col("source_vendor"),
        F.to_date(F.col("order_timestamp")).cast("date").alias("order_date")
    )
    .agg(
        F.sum("total_amount_usd").alias("total_amount_usd"),
        F.count("order_id").alias("order_count"),
        F.sum("discount_amt_usd").alias("total_discount_usd")
    )
    .withColumn(
        "avg_order_value_usd",
        _round(col("total_amount_usd") / col("order_count"), 2)
    )
    .withColumn("duplicate_count", lit(0))
    .withColumn("_updated_at", current_timestamp())
    .orderBy("order_date", "source_vendor")
)



from delta.tables import DeltaTable

daily_revenue_by_vendor_gold_table = "live_challenge.gold.daily_revenue_by_vendor"

# Create schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS live_challenge.gold")

# MERGE — upsert on business key (vendor + date)
if not spark.catalog.tableExists(daily_revenue_by_vendor_gold_table):
    (df_result.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(daily_revenue_by_vendor_gold_table)
    )
else:
    gold_table = DeltaTable.forName(spark, daily_revenue_by_vendor_gold_table)
    (
        gold_table.alias("target")
        .merge(
            df_result.alias("source"),
            "target.source_vendor = source.source_vendor AND target.order_date = source.order_date"
        )
        .whenMatchedUpdate(set={
            "total_amount_usd"   : "source.total_amount_usd",
            "order_count"        : "source.order_count",
            "total_discount_usd" : "source.total_discount_usd"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )



# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT * FROM live_challenge.gold.daily_revenue_by_vendor

# COMMAND ----------

# MAGIC %md
# MAGIC 2.Daily revenue by product SKU
# MAGIC

# COMMAND ----------

#select product_sku,Date(order_timestamp),sum(total_amount_usd),count(order_id),sum(discount_amt_usd) from silver_layer where payment_status='completed' and is_duplicate is false Group by 1,2

df_result = (
    spark.table("live_challenge.silver.orders_silver_unified")
  
    .groupBy(
        F.col("product_sku"),
        F.to_date(F.col("order_timestamp")).cast("date").alias("order_date")
    )
    .agg(
        F.sum("total_amount_usd").alias("total_amount_usd"),
        F.count("order_id").alias("order_count"),
        F.sum("discount_amt_usd").alias("total_discount_usd")
    )
    .withColumn(
        "avg_order_value_usd",
        _round(col("total_amount_usd") / col("order_count"), 2)
    )
    .withColumn("duplicate_count", lit(0))
    .withColumn("_updated_at", current_timestamp())
    .orderBy("order_date", "product_sku")
)
df_result.display()

from delta.tables import DeltaTable

daily_product_revenue_gold_table = "live_challenge.gold.daily_product_revenue"

# MERGE — upsert on business key (vendor + date)
if not spark.catalog.tableExists(daily_product_revenue_gold_table):
    (df_result.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(daily_product_revenue_gold_table)
    )
else:
    gold_table = DeltaTable.forName(spark, daily_product_revenue_gold_table)
    (
        gold_table.alias("target")
        .merge(
            df_result.alias("source"),
            "target.product_sku = source.product_sku AND target.order_date = source.order_date"
        )
        .whenMatchedUpdate(set={
            "total_amount_usd"   : "source.total_amount_usd",
            "order_count"        : "source.order_count",
            "total_discount_usd" : "source.total_discount_usd"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )



# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT * FROM live_challenge.gold.daily_product_revenue

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 3.Daily revenue by country
# MAGIC
# MAGIC

# COMMAND ----------

#select country_code,Date(order_timestamp),sum(total_amount_usd),count(order_id),sum(discount_amt_usd) from silver_layer where payment_status='completed' and is_duplicate is false Group by 1,2

df_result = (
    spark.table("live_challenge.silver.orders_silver_unified")
  
    .groupBy(
        F.col("country_code"),
        F.to_date(F.col("order_timestamp")).cast("date").alias("order_date")
    )
    .agg(
        F.sum("total_amount_usd").alias("total_amount_usd"),
        F.count("order_id").alias("order_count"),
        F.sum("discount_amt_usd").alias("total_discount_usd")
    )
    .withColumn(
        "avg_order_value_usd",
        _round(col("total_amount_usd") / col("order_count"), 2)
    )
    .withColumn("_updated_at", current_timestamp())
    .orderBy("order_date", "country_code")
)
df_result.display()

from delta.tables import DeltaTable

daily_revenue_by_country_gold_table = "live_challenge.gold.daily_revenue_by_country"

# MERGE — upsert on business key (vendor + date)
if not spark.catalog.tableExists(daily_revenue_by_country_gold_table):
    (df_result.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(daily_revenue_by_country_gold_table)
    )
else:
    gold_table = DeltaTable.forName(spark, daily_revenue_by_country_gold_table)
    (
        gold_table.alias("target")
        .merge(
            df_result.alias("source"),
            "target.country_code = source.country_code AND target.order_date = source.order_date"
        )
        .whenMatchedUpdate(set={
            "total_amount_usd"   : "source.total_amount_usd",
            "order_count"        : "source.order_count",
            "total_discount_usd" : "source.total_discount_usd"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )



# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT * FROM  live_challenge.gold.daily_revenue_by_country

# COMMAND ----------

# MAGIC %md
# MAGIC 4.Customer purchase frequency

# COMMAND ----------

#select customer_email,count(order_id) from silver_layer where payment_status='completed' and is_duplicate is false Group by 1

df_result = (
    spark.table("live_challenge.silver.orders_silver_unified")
  
    .groupBy(
        F.col("customer_email"),
    )
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount_usd").alias("total_amount_usd"),
        F.sum("discount_amt_usd").alias("total_discount_usd")
    )
    .withColumn(
        "avg_order_value_usd",
        _round(col("total_amount_usd") / col("order_count"), 2)
    )
    .withColumn("_updated_at", current_timestamp())
    # .orderBy("customer_email")
)
df_result.display()

from delta.tables import DeltaTable

customer_purchase_frequency_gold_table = "live_challenge.gold.customer_purchase_frequency"

# MERGE — upsert on business key (vendor + date)
if not spark.catalog.tableExists(customer_purchase_frequency_gold_table):
    (df_result.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(customer_purchase_frequency_gold_table)
    )
else:
    gold_table = DeltaTable.forName(spark, customer_purchase_frequency_gold_table)
    (
        gold_table.alias("target")
        .merge(
            df_result.alias("source"),
            "target.customer_email = source.customer_email"
        )
        .whenMatchedUpdate(set={
            "total_amount_usd"   : "source.total_amount_usd",
            "order_count"        : "source.order_count",
            "total_discount_usd" : "source.total_discount_usd"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT * FROM  live_challenge.gold.customer_purchase_frequency

# COMMAND ----------

# MAGIC %md
# MAGIC 5.Top 10 products by revenue
# MAGIC

# COMMAND ----------

#select product_sku,sum(total_amount_usd),sum(discount_amt_usd) from silver_layer where payment_status='completed' and is_duplicate is false Group by 1 order by 2 desc limit 10

df_result = (
    spark.table("live_challenge.silver.orders_silver_unified")
  
    .groupBy(
        F.col("product_sku"),
    )
    .agg(
        F.sum("total_amount_usd").alias("total_amount_usd"),
        F.sum("discount_amt_usd").alias("total_discount_usd")
    )
    .withColumn("_updated_at", current_timestamp())
    .orderBy(F.col("total_amount_usd").desc())
    .limit(10)
)
df_result.display()

from delta.tables import DeltaTable

product_by_revenue_gold_table = "live_challenge.gold.product_by_revenue"

# MERGE — upsert on business key (vendor + date)
if not spark.catalog.tableExists(product_by_revenue_gold_table):
    (df_result.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(product_by_revenue_gold_table)
    )
else:
    gold_table = DeltaTable.forName(spark, product_by_revenue_gold_table)
    (
        gold_table.alias("target")
        .merge(
            df_result.alias("source"),
            "target.customer_email = source.customer_email"
        )
        .whenMatchedUpdate(set={
            "total_amount_usd"   : "source.total_amount_usd",
            "order_count"        : "source.order_count",
            "total_discount_usd" : "source.total_discount_usd"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )



# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT * FROM  live_challenge.gold.product_by_revenue

# COMMAND ----------

# MAGIC %md
# MAGIC 6.Promotional campaign effectiveness (after Day 30)

# COMMAND ----------

promo_analysis_gold_table = "live_challenge.gold.promo_analysis"
silver_table="live_challenge.silver.orders_silver_unified"
# Read unified Silver Delta table
df_silver = spark.table(silver_table)

# Filter to records with promo codes (effectively Vendor A v2)
df_promo = df_silver.filter(col("promo_code").isNotNull())

df_gold = (
    df_promo
    .groupBy("promo_code")
    .agg(
        count("order_id").alias("usage_count"),
        _round(_sum("discount_amt_usd"), 2).alias("total_discount_given_usd"),
        _round(_sum("total_amount_usd"), 2).alias("total_revenue_with_promo_usd"),
        _round(_avg("total_amount_usd"), 2).alias("avg_order_value_usd"),
        _sum("loyalty_points").alias("total_loyalty_points_awarded")
    )
    .withColumn("_updated_at", current_timestamp())
    .orderBy(col("usage_count").desc())
)
df_gold.display()
# Write as managed Delta table (Gold)
delta_table = DeltaTable.forName(spark, promo_analysis_gold_table)
if not spark.catalog.tableExists(promo_analysis_gold_table):
    (
        df_gold.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(promo_analysis_gold_table)
    )
else:
    (
        delta_table.alias("t")
        .merge(
            df_gold.alias("s"),
            "t.promo_code = s.promo_code"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM live_challenge.gold.promo_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 7.Duplicate analysis report.

# COMMAND ----------

