# 🛒 RetailFlow Unified — Fixing a Broken Global E-Commerce Data Pipeline

> **Medallion Architecture on Databricks** | Multi-vendor ingestion · Deduplication · Delta Lake optimization · Real-time analytics

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io)
[![AWS S3](https://img.shields.io/badge/AWS%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)

---

## 📌 Project Overview

RetailFlow Inc. acquired **three regional e-commerce competitors** with completely different tech stacks. This project builds a **production-grade unified data warehouse** that consolidates ~35,000 orders/day across 3 vendors, fixes critical data quality issues, and delivers sub-60-second dashboard performance.

**The real challenge?** The vendors kept pushing data during migration — with schema changes, duplicates, currency mismatches, and a partition explosion that killed query performance.

| Metric | Before | After |
|--------|--------|-------|
| Duplicate Rate | ~8.5% (265K records) | 0% |
| File Count | 6,000+ tiny files | < 50 optimized files |
| Avg File Size | 18 KB | 128–256 MB |
| Query Time | 22 min (timeout) | < 60 seconds |
| Revenue Accuracy | ±15–25% inflated | ±0.5% (CFO target) |

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          AWS S3 (Source)                                │
│                                                                         │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────────┐   │
│  │  Vendor A    │   │  Vendor B    │   │       Vendor C            │   │
│  │  Shopify     │   │  Magento     │   │   CustomPlatform (Node)   │   │
│  │  US Market   │   │  EU Market   │   │   Asia (JP + KR)          │   │
│  │  CSV comma   │   │  CSV pipe    │   │   TSV + nested JSON       │   │
│  │  5-min batch │   │  Hourly      │   │   5-min streaming         │   │
│  └──────┬───────┘   └──────┬───────┘   └───────────┬──────────────┘   │
└─────────┼──────────────────┼───────────────────────┼──────────────────┘
          │                  │                        │
          ▼                  ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     🥉 BRONZE LAYER (Raw Ingestion)                     │
│                                                                         │
│  • Read all 3 formats (comma / pipe / tab delimited)                   │
│  • Handle Vendor A schema drift on Day 30 (mergeSchema=true)           │
│  • Add metadata: _source_file, _ingestion_timestamp, _source_vendor    │
│  • Store as Delta Lake tables — one per vendor                         │
│                                                                         │
│  Tables: bronze.vendor_a_raw | bronze.vendor_b_raw | bronze.vendor_c_raw│
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     🥈 SILVER LAYER (Clean & Unified)                   │
│                                                                         │
│  • Normalize all vendors to single canonical schema                    │
│  • Parse nested JSON (Vendor C multi-line orders)                      │
│  • Convert all currencies to USD (EUR→USD, JPY→USD, KRW→USD)          │
│  • Standardize dates to ISO 8601                                       │
│  • Deduplicate with business rules:                                    │
│       Priority: SETTLED > COMPLETED > PENDING                          │
│       Tiebreak: latest updated_timestamp wins                          │
│       Mark duplicates (is_duplicate=true), never delete                │
│  • MERGE for idempotent incremental writes                             │
│                                                                         │
│  Table: silver.orders_silver_unified                                   │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     🥇 GOLD LAYER (Business Analytics)                  │
│                                                                         │
│  • Daily revenue by vendor (USD)                                       │
│  • Daily revenue by product SKU                                        │
│  • Daily revenue by country                                            │
│  • Customer purchase frequency                                         │
│  • Top 10 products by revenue                                          │
│  • Promo campaign effectiveness (Day 30+)                              │
│  • Duplicate analysis report                                           │
│                                                                         │
│  Tables: gold.daily_revenue_by_vendor | gold.product_sku_revenue       │
│          gold.revenue_by_country | gold.customer_frequency             │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
                   ┌───────────────────────┐
                   │   📊 BI Dashboards    │
                   │   < 60 sec load time  │
                   └───────────────────────┘
```

---

## ⚙️ Incremental Load Strategy

```
┌─────────────────────────────────────────────────────┐
│           Incremental Pipeline Flow                  │
│                                                     │
│  1. Read watermark from ingestion_control_table     │
│           │                                         │
│  2. Filter: order_timestamp > last_processed_ts     │
│           │                                         │
│  3. Transform & aggregate → df_result               │
│           │                                         │
│  4. MERGE into Gold table (upsert, no duplicates)   │
│           │                                         │
│  5. Update watermark (only on success)              │
└─────────────────────────────────────────────────────┘
```

---

## 🚨 Problems Solved

### Problem 1 — Multi-Vendor Format Hell
Three vendors, three completely different formats:

| Vendor | Format | Date Style | Currency | Delivery |
|--------|--------|-----------|----------|----------|
| A (Shopify / US) | CSV comma | MM/DD/YYYY | USD | Every 5 min |
| B (Magento / EU) | CSV pipe | DD-MM-YYYY | EUR + VAT | Hourly |
| C (CustomPlatform / Asia) | TSV + nested JSON | ISO 8601 | JPY, KRW | Every 5 min |

**Solution:** Vendor-specific readers with unified normalization into a canonical schema. Nested JSON parsed using `from_json()` with an explicit StructType schema.

---

### Problem 2 — Schema Drift (Day 30 Surprise)
Vendor A renamed `customer_email → email` and added `promo_code`, `discount_pct`, `loyalty_points` on Day 30. The pipeline crashed with `AnalysisException: Column not found`.

**Solution:**
```python
df.write.format("delta")
  .option("mergeSchema", "true")   # absorbs new columns
  .mode("append")
  .saveAsTable("bronze.vendor_a_raw")

# Column aliasing to handle the rename
F.coalesce(F.col("email"), F.col("customer_email")).alias("customer_email")
```

---

### Problem 3 — Duplicate Data (Revenue Inflated by 15–25%)
Same order appearing 2–5 times with different statuses (`PENDING → COMPLETED → SETTLED`) due to network retries. CFO reported $4.5M — actual was $3.8M.

**Solution:** Window function deduplication with business priority rules:
```python
status_priority = F.when(F.col("payment_status") == "settled", 1)  \
                   .when(F.col("payment_status") == "completed", 2) \
                   .otherwise(3)

window = Window.partitionBy("order_id").orderBy(
    status_priority.asc(),
    F.col("updated_timestamp").desc()
)

df_deduped = df.withColumn("row_num", F.row_number().over(window)) \
               .withColumn("is_duplicate", F.col("row_num") > 1)
# Mark duplicates, never delete — full audit trail preserved
```

---

### Problem 4 — Partition Explosion (6,000+ Tiny Files)
Micro-batching created 6,000+ files averaging 18KB each. File listing alone took 8 minutes. Queries timed out at 22 minutes.

**Solution:**
```sql
-- Compact small files into 128-256MB chunks
OPTIMIZE silver.orders_silver_unified
ZORDER BY (source_vendor, order_date);

-- Redesign partitioning: date-only (not vendor+date)
-- Reclaim deleted file storage
VACUUM silver.orders_silver_unified RETAIN 168 HOURS;
```

| Metric | Before | After |
|--------|--------|-------|
| File count | 6,000+ | ~40 |
| Avg file size | 18 KB | 200 MB |
| Query time | 22 min (timeout) | 48 sec |
| Storage cost | Baseline | ~40% reduction |

---

## 📁 Repository Structure

```
retailflow-unified/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py         # Raw ingestion for all 3 vendors
│   ├── 02_silver_transformation.py    # Normalize, deduplicate, convert currencies
│   ├── 03_gold_aggregation.py         # Business analytics aggregations
│   └── 04_performance_optimization.py # OPTIMIZE, ZORDER, VACUUM, benchmarks
│
├── utils/
│   ├── currency_converter.py          # EUR/JPY/KRW → USD with exchange rates
│   ├── deduplication.py               # Window function dedup logic
│   ├── schema_config.py               # Canonical schema + vendor mappings
│   └── watermark_manager.py           # Incremental load control table helpers
│
├── schemas/
│   ├── vendor_a_schema.json           # Shopify schema (pre & post Day 30)
│   ├── vendor_b_schema.json           # Magento schema
│   └── vendor_c_schema.json           # CustomPlatform nested JSON schema
│
├── tests/
│   ├── test_deduplication.py
│   ├── test_currency_conversion.py
│   └── test_schema_evolution.py
│
├── docs/
│   ├── architecture_diagram.png
│   ├── performance_benchmarks.md
│   └── challenges.md
│
└── README.md
```

---

## 🚀 Setup & Getting Started

### Prerequisites
- Databricks workspace (Runtime 13.x+ recommended)
- AWS S3 bucket with vendor data files
- Databricks cluster with Delta Lake enabled (included by default)

### Step 1 — Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/retailflow-unified.git
```

### Step 2 — Configure Your Databricks Workspace
Import notebooks into Databricks:
1. Go to **Workspace → Import**
2. Select all `.py` files from the `notebooks/` folder
3. Or use the Databricks CLI:
```bash
databricks workspace import_dir notebooks/ /Workspace/retailflow-unified/
```

### Step 3 — Set Up Secrets & Config
In your Databricks cluster or a config notebook, set:
```python
# S3 paths
VENDOR_A_PATH = "s3://your-bucket/vendor_a_shopify_data/"
VENDOR_B_PATH = "s3://your-bucket/vendor_b_magento_data/"
VENDOR_C_PATH = "s3://your-bucket/vendor_c_customplatform_data/"

# Catalog & Schema
CATALOG      = "live_challenge"
BRONZE_DB    = f"{CATALOG}.bronze"
SILVER_DB    = f"{CATALOG}.silver"
GOLD_DB      = f"{CATALOG}.gold"
```

### Step 4 — Initialize Schemas
```python
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_DB}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_DB}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_DB}")
```

### Step 5 — Run the Pipeline (In Order)

| Order | Notebook | Purpose |
|-------|----------|---------|
| 1 | `01_bronze_ingestion.py` | Ingest raw files from S3 → Bronze Delta tables |
| 2 | `02_silver_transformation.py` | Normalize, deduplicate, convert → Silver unified table |
| 3 | `03_gold_aggregation.py` | Aggregate → Gold BI-ready tables |
| 4 | `04_performance_optimization.py` | OPTIMIZE + ZORDER + VACUUM |

### Step 6 — Schedule Incremental Loads
Use **Databricks Jobs** to schedule:
- Bronze + Silver: Every 5 minutes (matches Vendor A & C cadence)
- Gold: Hourly or daily depending on dashboard SLA
- Optimization: Nightly (off-peak)

---

## 📊 Gold Layer Tables Reference

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `gold.daily_revenue_by_vendor` | CFO daily revenue report | `source_vendor`, `order_date`, `total_amount_usd`, `order_count` |
| `gold.product_sku_revenue` | Top products by revenue | `product_sku`, `order_date`, `total_revenue_usd` |
| `gold.revenue_by_country` | Regional breakdown | `country`, `order_date`, `total_revenue_usd` |
| `gold.customer_frequency` | Purchase frequency analysis | `customer_id`, `order_count`, `total_spend_usd` |
| `gold.promo_campaign_effectiveness` | Post Day-30 promo analysis | `promo_code`, `orders`, `discount_given_usd`, `revenue_usd` |
| `gold.duplicate_analysis_report` | Audit trail for deduplication | `source_vendor`, `duplicate_count`, `revenue_impact_usd` |

---

## 🔑 Key Technical Decisions

**Why Delta Lake MERGE over overwrite?**
Idempotent writes mean the pipeline can safely re-run after failures without double-counting or data loss. Critical for a financial reporting pipeline.

**Why mark duplicates instead of deleting them?**
Audit requirements and reconciliation. The `is_duplicate` flag lets analysts validate dedup logic and recover records if business rules change.

**Why Z-Ordering on `source_vendor` and `order_date`?**
These are the most frequent filter columns across all Gold queries. Z-ordering co-locates related data, dramatically reducing files scanned per query.

**Why date-only partitioning (not vendor + date)?**
Vendor + date creates too many small partitions. Date-only keeps partition count manageable while still enabling efficient time-range pruning.

---

## 📈 Performance Benchmarks

| Query | Before Optimization | After Optimization |
|-------|--------------------|--------------------|
| Daily revenue by vendor (90 days) | 22 min (timeout) | 48 sec |
| Top 10 products all-time | 18 min | 32 sec |
| Customer frequency report | 14 min | 41 sec |
| File listing overhead | 8 min | < 5 sec |

---

## 🛠️ Tech Stack

| Tool | Usage |
|------|-------|
| **Databricks** | Unified analytics platform, notebook orchestration |
| **Apache Spark (PySpark)** | Distributed data processing |
| **Delta Lake** | ACID transactions, MERGE, time travel, schema evolution |
| **AWS S3** | Source data storage (vendor file delivery) |
| **Python** | Pipeline logic, utility functions |

---

## 👤 Author

Built as part of the **codebasics.io** Data Engineering challenge.  
Feel free to ⭐ star the repo if you found it useful!

---

## 📄 License

MIT License — use freely, attribution appreciated.
