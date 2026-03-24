# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer Implementation

# COMMAND ----------

# MAGIC %md
# MAGIC Import Statements
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import(input_file_name,current_timestamp,lit,col,to_timestamp,when,coalesce,from_json,explode_outer,decimal,regexp_replace)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType,ArrayType

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Use Catalog live_challenge;
# MAGIC create schema if not exists silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vendor `A` Data Processing

# COMMAND ----------

# DBTITLE 1,Vendor A Normalization (Fixed Column Name)
def normalize_vendor_a(df_raw):
    for col_name,dtype in [
        ('email','string'),
        ('customer_email','string'),
        ('promo_code','string'),
        ('loyalty_points','int'),
        ('discount_pct','decimal(0,2)'),
        ('unit_price','decimal(0,2)'),
        ('total_amount','decimal(0,2)'),
    ]:
        if col_name not in df_raw.columns:
            df_raw=df_raw.withColumn(col_name, lit(None).cast(dtype))

    df_raw=df_raw.withColumn("customer_email",coalesce(col("email"),col("customer_email")))

    df_raw=df_raw.withColumn("order_timestamp",to_timestamp(col("order_date"),"MM/dd/yyyy"))

    df_raw=df_raw.withColumn("line_number",lit(1))

    df_raw=df_raw.withColumn("unit_price_usd",coalesce(col("unit_price")))
    df_raw=df_raw.withColumn("total_amount_usd",coalesce(col("total_amount")))
    # df_raw.selectExpr("count(*)", "count(discount_pct)").show()
    df_raw=df_raw.withColumn("discount_amt_usd",
                            when (col("discount_pct").isNotNull(),
                                (col("unit_price").cast("decimal(10,2)")*col("quantity").cast("int"))*col("discount_pct")/100)
                            .otherwise(lit(None).cast("decimal(10,2)")))

    df_raw=df_raw.withColumn("payment_method",when (col("payment_method")=="google_pay",lit("mobile_wallet"))
                                            .when (col("payment_method")=="apple_pay", lit("mobile_wallet"))
                            .otherwise(col("payment_method"))
                            )

    df_raw=df_raw.withColumn("country_code",lit("US"))
    df_raw=df_raw.withColumn("source_currency",lit("USD"))
    df_raw=df_raw.withColumn("exchange_rate",lit(1.0).cast("decimal(10,6)"))
    df_raw=df_raw.withColumn("vat_amount_usd",lit(None).cast("decimal(10,2)"))

    # # df_raw.printSchema()
    # display(
    # df_raw.select(
    #     col("order_id"),
    #     col("discount_amt_usd")
    # ).where(col("discount_pct").isNotNull())
    # .orderBy(col("discount_amt_usd").asc())
    # )
    return df_raw.select(
    col("order_id"),
    col("line_number"),
    col("order_timestamp"),
    col("customer_email"),
    col("product_sku"),
    col("quantity").cast("integer"),
    col("unit_price_usd"),
    col("total_amount_usd"),
    col("discount_amt_usd"),
    col("payment_method"),
    col("country_code"),
    col("promo_code"),
    col("loyalty_points").cast("integer"),
    col("_source_vendor").alias ("source_vendor"),
    col("source_currency"),
    col("exchange_rate"),
    col("vat_amount_usd"),
    col("_ingestion_timestamp"),
    col("_source_file"),
    )

df_raw=spark.read.table("live_challenge.bronze.orders_vendor_a_shopify")
# # display(df_raw)
display(normalize_vendor_a(df_raw))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vendor `B` Data Processing

# COMMAND ----------

# MAGIC  %md
# MAGIC
# MAGIC  **1) Makes sure required columns exist**<br/>
# MAGIC       If any of these are missing: order_date, unit_price, total_amount, vat_amount, product_code, customer_email, quantity, country_code, payment_method → it creates them as NULL with the right type. This avoids errors later.
# MAGIC  
# MAGIC   **2) Parses the order date**<br/>
# MAGIC       order_timestamp = to_timestamp(order_date, "dd-MM-yyyy")
# MAGIC  
# MAGIC   **3) Cleans and parses EUR money strings**<br/>
# MAGIC       Input like: €24,99
# MAGIC       Removes €, replaces , with ., casts to decimal(10,2)
# MAGIC       → unit_price_eur, total_amount_eur, vat_amount_eur
# MAGIC  
# MAGIC   **4) Converts EUR → USD**<br/>
# MAGIC       Using EUR_TO_USD = 0.92:<br/>
# MAGIC       unit_price_usd = unit_price_eur / 0.92<br/>
# MAGIC       total_amount_usd = total_amount_eur / 0.92<br/>
# MAGIC       vat_amount_usd = vat_amount_eur / 0.92<br/>
# MAGIC  
# MAGIC   **5) Normalizes SKUs**<br/>
# MAGIC       Maps short codes to canonical SKUs:<br/>
# MAGIC       WID500 → WIDGET-500<br/>
# MAGIC       GADX1 → GADGET-X1<br/>
# MAGIC       etc.
# MAGIC       Result column: product_sku.
# MAGIC       Standardizes payment method
# MAGIC  
# MAGIC   **6) carte_bancaire → bank_card**<br/>
# MAGIC       Others kept as-is.<br/>
# MAGIC       Fills promotion-related fields<br/>
# MAGIC  
# MAGIC   **7) Vendor B has no promos, so:**
# MAGIC       discount_amount_usd, promo_code, loyalty_points are set to NULL.
# MAGIC  
# MAGIC   **8) Adds currency metadata**
# MAGIC       source_currency = 'EUR'<br/>
# MAGIC       exchange_rate = 0.92<br/>
# MAGIC  
# MAGIC   **9) Returns canonical Silver columns**<br/>
# MAGIC       A clean projection with:<br/>
# MAGIC       order_id, order_timestamp, customer_email, product_sku, quantity, unit_price_usd, total_amount_usd, discount_amount_usd, payment_method, country_code, promo_code, loyalty_points, source_vendor, source_currency, exchange_rate, vat_amount_usd, _ingestion_timestamp, _source_file, _data_version
# MAGIC

# COMMAND ----------

def normalize_vendor_b(df_raw):
  required_cols = [
    ("order_date","string"),
    ("customer_email","string"),
    ("product_code","string"),
    ("quantity","int"),
    ("unit_price","decimal(0,2)"),
    ("total_amount","decimal(0,2)"),
    ("payment_method","string"),
    ("country_code","string"),
    ("vat_amount","decimal(0,2)")
  ]

  for column,dtype in required_cols:
      if column not in df_raw.columns:
          df_raw=df_raw.withColumn(column, lit(None).cast(dtype))

  df_raw=df_raw.withColumn("order_timestamp",to_timestamp("order_date","dd-MM-yyyy"))

  df_raw=df_raw.withColumn("line_number",lit(1))

  df_raw=df_raw.withColumn("product_sku",col("product_code"))

  df_raw = df_raw.withColumn(
      "unit_price_eur",
      regexp_replace(col("unit_price"), "[^0-9.]", "").cast("decimal(10,2)")
  )
  df_raw = df_raw.withColumn(
      "total_amount_eur",
      regexp_replace(col("total_amount"), "[^0-9.]", "").cast("decimal(10,2)")
  )
  df_raw = df_raw.withColumn(
      "vat_amount_eur",
      regexp_replace(col("vat_amount"), "[^0-9.]", "").cast("decimal(10,2)")
  )

  df_raw=df_raw.withColumn("unit_price_usd",(col("unit_price_eur")/lit(.92)).cast("decimal(10,2)"))
  df_raw=df_raw.withColumn("total_amount_usd",(col("total_amount_eur")/lit(.92)).cast("decimal(10,2)"))
  df_raw=df_raw.withColumn("vat_amount_usd",(col("vat_amount_eur")/lit(.92)).cast("decimal(10,2)"))

  df_raw=df_raw.withColumn("payment_method",when (col("payment_method")=="carte_bancaire",lit("bank_card"))
                                            .otherwise(col("payment_method"))
                                            )
  df_raw=df_raw.withColumn("discount_amt_usd",lit(None).cast("decimal(10,2)"))
  df_raw=df_raw.withColumn("promo_code",lit(None).cast("string"))
  df_raw=df_raw.withColumn("loyalty_points",lit(None).cast("int"))

  df_raw=df_raw.withColumn("source_currency",lit("EUR"))
  df_raw=df_raw.withColumn("exchange_rate",lit(.92).cast("decimal(10,2)"))

#   df_raw.display()
  return df_raw.select(
    col("order_id"),
    col("line_number"),
    col("order_timestamp"),
    col("customer_email"),
    col("product_sku"),
    col("quantity").cast("integer"),
    col("unit_price_usd"),
    col("total_amount_usd"),
    col("discount_amt_usd"),
    col("payment_method"),
    col("country_code"),
    col("promo_code"),
    col("loyalty_points").cast("integer"),
    col("_source_vendor").alias ("source_vendor"),
    col("source_currency"),
    col("exchange_rate"),
    col("vat_amount_usd"),
    col("_ingestion_timestamp"),
    col("_source_file"),
      )
df_raw=spark.read.table("live_challenge.bronze.orders_vendor_b_magento")
display(normalize_vendor_b(df_raw))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vendor `C` Data Processing

# COMMAND ----------

# MAGIC %md
# MAGIC 1) Defines schemas for the three JSON string columns:
# MAGIC customer_info → customer struct (email, phone, country)
# MAGIC line_items → items array of structs (sku, qty, price)
# MAGIC payment_details payment struct (method, currency, amount, status)
# MAGIC 2) Cleans the JSON text (because it was CSV/TSV-escaped):
# MAGIC Removes outer quotes converts ""Creates customer_info_clean, line_items_clean, payment_details_clean
# MAGIC 3) Parses JSON into real structs/arrays using from_json.
# MAGIC 4) Ensures metadata columns exist:
# MAGIC _source_vendor, ingestion_timestamp, _source_file, _data_version.
# MAGIC 5) Flattens the nested JSON:
# MAGIC explode_outer(items) one row per line item.
# MAGIC Derives:
# MAGIC order_id from order number
# MAGIC customer_email from customer.email
# MAGIC country_code from customer.country
# MAGIC product_sku, quantity, unit_price_local from item.
# MAGIC total_amount_local, payment method, source_currency from payment.
# MAGIC 6) Parses timestamp:
# MAGIC created_at (ISO string) → order_timestamp.
# MAGIC 7) Converts local currency → USD:
# MAGIC If JPY divide by 150
# MAGIC If KRW → divide by 1320
# MAGIC Else - 1.0
# MAGIC Computes unit_price_usd, total_amount_usd.
# MAGIC 8) Fills promo/VAT fields with NULL (Vendor C has none):
# MAGIC discount_amount_usd, promo_code, loyalty points, vat_amount_usd.
# MAGIC 9) Returns a clean, canonical Silver schema with all standard columns for downstream use.

# COMMAND ----------


def normalize_vendor_c(df_raw):
    customer_schema = StructType([
            StructField("email",   StringType(), True),
            StructField("phone",   StringType(), True),
            StructField("country", StringType(), True),
        ])

    line_item_schema = StructType([
            StructField("sku",   StringType(),  True),
            StructField("qty",   IntegerType(), True),
            StructField("price", IntegerType(), True),
        ])

    payment_schema = StructType([
            StructField("method",   StringType(), True),
            StructField("currency", StringType(), True),
            StructField("amount",   IntegerType(), True),
            StructField("status",   StringType(), True),
        ])

        # 2) Parse JSON into structs/arrays
    df_raw = (
        df_raw
            .withColumn("customer", from_json(col("customer_info"), customer_schema))
            .withColumn("items",    from_json(col("line_items"), ArrayType(line_item_schema)))
            .withColumn("payment",  from_json(col("payment_details"), payment_schema))
        )

    for c, t in [
            ("_source_vendor",       "string"),
            ("_ingestion_timestamp", "timestamp"),
            ("_source_file",         "string")
        ]:
            if c not in df_raw.columns:
                df_raw = df_raw.withColumn(c, lit(None).cast(t))

        # 4) Flatten JSON - explode items array (one row per line item)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    window_spec = Window.partitionBy("order_number").orderBy(lit(1))

    df_raw = df_raw.withColumn("item", explode_outer(col("items")))
    df_raw = df_raw.withColumn("line_number", row_number().over(window_spec))

    df_raw = df_raw.withColumn("order_id",           col("order_number"))
    df_raw = df_raw.withColumn("customer_email",     col("customer.email"))
    df_raw = df_raw.withColumn("country_code",       col("customer.country"))
    df_raw = df_raw.withColumn("product_sku",        col("item.sku"))
    df_raw = df_raw.withColumn("quantity",           col("item.qty"))
    df_raw = df_raw.withColumn("unit_price_local",   col("item.price"))
    df_raw = df_raw.withColumn("total_amount_local", col("payment.amount"))
    df_raw = df_raw.withColumn("payment_method",     col("payment.method"))
    df_raw = df_raw.withColumn("source_currency",    col("payment.currency"))


    df_raw = df_raw.withColumn(
            "order_timestamp",
            to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )

    df_raw = df_raw.withColumn(
            "exchange_rate",
            when(col("source_currency") == "JPY", lit(150.0))
            .when(col("source_currency") == "KRW", lit(1320.0))
            .otherwise(lit(1.0))
            .cast("decimal(10,6)")
            )

    df_raw = df_raw.withColumn(
            "unit_price_usd",
            (col("unit_price_local") / col("exchange_rate")).cast("decimal(10,2)")
    )
    df_raw = df_raw.withColumn(
            "total_amount_usd",
            (col("total_amount_local") / col("exchange_rate")).cast("decimal(10,2)")
    )

    df_raw = df_raw.withColumn("discount_amt_usd", lit(None).cast("decimal(10,2)"))
    df_raw = df_raw.withColumn("promo_code",         lit(None).cast("string"))
    df_raw = df_raw.withColumn("loyalty_points",     lit(None).cast("integer"))
    df_raw = df_raw.withColumn("vat_amount_usd",     lit(None).cast("decimal(10,2)"))

    return df_raw.select(
        col("order_id"),
        col("line_number"),
        col("order_timestamp"),
        col("customer_email"),
        col("product_sku"),
        col("quantity").cast("integer"),
        col("unit_price_usd"),
        col("total_amount_usd"),
        col("discount_amt_usd"),
        col("payment_method"),
        col("country_code"),
        col("promo_code"),
        col("loyalty_points"),
        col("_source_vendor").alias("source_vendor"),
        col("source_currency"),
        col("exchange_rate"),
        col("vat_amount_usd"),
        col("_ingestion_timestamp"),
        col("_source_file")
        )
    
# df_raw=spark.read.table("live_challenge.bronze.orders_vendor_c_custom_platform")
# (normalize_vendor_c(df_raw)).display()
# df_raw.printSchema()
# df_raw.display(20)


# COMMAND ----------

# MAGIC %md
# MAGIC ###DeDuplication Logic 
# MAGIC
# MAGIC deduplicate_orders(df)
# MAGIC
# MAGIC Treats any repeated order_id as potential duplicates.
# MAGIC If payment_status column is missing → adds it with default "completed".
# MAGIC Gives each row a priority:
# MAGIC       settled → 1 (best)
# MAGIC       completed → 2
# MAGIC       pending → 3
# MAGIC  
# MAGIC Inside each order_id:
# MAGIC Orders rows by status priority (best first) and then by latest order_timestamp.
# MAGIC Marks the best row as _is_duplicate = False and all others as _is_duplicate = True.
# MAGIC
# MAGIC Adds _duplicate_reason text only for rows marked as duplicates.
# MAGIC  
# MAGIC Drops helper columns and returns the DataFrame with:
# MAGIC
# MAGIC _is_duplicate (True/False) 
# MAGIC _duplicate_reason
# MAGIC  
# MAGIC
# MAGIC get_deduplicated_silver(df)
# MAGIC
# MAGIC Keeps only non-duplicate rows (_is_duplicate = False) → for Silver table.
# MAGIC  

# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, lit, row_number, count

def deduplicate_orders(df):
    """
    Deduplicate orders using business rules.

    Priority Rules:
    1. Status: SETTLED > COMPLETED > PENDING
    2. If same status: Keep latest timestamp

    Tracking:
    - Mark duplicates with _is_duplicate flag
    - Record reason in _duplicate_reason
    - Keep all records in Bronze, remove only in Silver
    """

    # Ensure payment_status exists (Vendor A/B may not have it)
    if "payment_status" not in df.columns:
        df = df.withColumn("payment_status", lit("completed"))

    df = df.withColumn(
        "_status_priority",
        when(col("payment_status") == "settled",   lit(1))
        .when(col("payment_status") == "completed", lit(2))
        .when(col("payment_status") == "pending",   lit(3))
        .otherwise(lit(2))
    )

    window_spec = (
        Window.partitionBy("order_id","line_number")
              .orderBy(col("_status_priority").asc(),
                       col("order_timestamp").desc())
    )

    df = df.withColumn("_row_rank", row_number().over(window_spec))

    window_count = Window.partitionBy("order_id", "line_number")
    df = df.withColumn("_duplicate_count", count("*").over(window_count))

    df = df.withColumn("_is_duplicate", col("_row_rank") > 1)

    df = df.withColumn(
        "_duplicate_reason",
        when(col("_is_duplicate") & (col("_duplicate_count") > 1),
             lit("Duplicate order - lower status priority or older timestamp"))
        .otherwise(lit(None))
    )

    return df.drop("_status_priority", "_row_rank", "_duplicate_count")


def get_deduplicated_silver(df):
    """Return only non-duplicate records for Silver layer."""
    return df.filter(col("_is_duplicate") == False)
 

# COMMAND ----------

df_a_bronze = spark.table("bronze.orders_vendor_a_shopify")
df_b_bronze = spark.table("bronze.orders_vendor_b_magento")
df_c_bronze = spark.table("bronze.orders_vendor_c_custom_platform")


# Normalize each vendor
df_a_norm = normalize_vendor_a(df_a_bronze)
df_b_norm = normalize_vendor_b(df_b_bronze)
df_c_norm = normalize_vendor_c(df_c_bronze)

# Union all into unified DF
df_unified = (
    df_a_norm
      .unionByName(df_b_norm)
      .unionByName(df_c_norm)
)

print("Unified rows:", df_unified.count())

df_all = deduplicate_orders(df_unified)

# How many are duplicates vs not
df_all.groupBy("_is_duplicate").count().show()

# display(get_deduplicated_silver(df_all))
df_silver = get_deduplicated_silver(df_all)
target_table = "live_challenge.silver.orders_silver_unified"


(
    df_silver.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT * FROM silver.orders_silver_unified