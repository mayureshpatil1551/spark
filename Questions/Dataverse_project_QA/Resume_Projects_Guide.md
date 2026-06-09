# Resume Projects & eCDP Architecture Guide
**Mayuresh | Data Engineer | Cognizant Technology Solutions**

---

## RESUME BULLETS

---

### PROJECT 1 — FinOps Dataverse (AbbVie)
**Role:** Data Engineer | **Stack:** PySpark, Apache Iceberg, AWS S3, Oracle JDBC, Salesforce REST API, Smartsheet API, Snowflake, Azure Databricks, Modak Nabu, GitHub

**Resume Bullets:**

- Engineered a Medallion Architecture data lakehouse on Apache Iceberg (AWS S3) to migrate **20TB+** of legacy pharmaceutical data, ingesting from heterogeneous sources — S3 flat files (CSV/XML/JSON), Salesforce REST API, Smartsheet API, Oracle RDBMS, and Snowflake — achieving **99.9% pipeline uptime**.
- Architected **generic, reusable PySpark ingestion frameworks** for each source type (REST API, JDBC, flat file), enabling full-load and incremental (MERGE-based) patterns with dynamic configuration — eliminating the need for source-specific one-off pipelines.
- Built Python scripts to auto-generate SQL configuration files, eliminating **15+ hours/week** of manual infrastructure setup and reducing onboarding time for new data sources.
- Implemented Raw → TL → ATL → Outbound layer transformations in PySpark, incorporating query optimization techniques (Spark shuffle tuning, broadcast joins, partitioning, bucketing, skew handling, small-file compaction), improving **300+ analytical query speeds by 40%** and cutting **pipeline execution time by 30%**.
- Built token expiry handling for REST API sources (Salesforce, Smartsheet) to ensure uninterrupted long-running ingestion pipelines without manual intervention.
- Operationalized outbound sync from Data Lakehouse to Oracle and CSV flat files with **zero manual intervention**, orchestrated via Modak Nabu pipelines with end-to-end lineage tracking.
- Managed version-controlled PySpark codebase on **GitHub**, coordinating configs, pipeline definitions, and reusable utilities across the team.

**FinOps PoC Sub-Bullets (SAP Migration):**

- Delivered a Big Data Migration for complex **SAP financial hierarchies** in **2.5 weeks vs. an 8-week estimate (60% faster delivery)**.
- Engineered dual-target migration of SAP financial data simultaneously into **Delta Lake (Azure Databricks)** and **Snowflake**, ensuring zero data loss across both targets.
- Flattened deeply nested SAP XML structures into **29 normalized relational Hive tables**; PoC sign-off directly secured **$5,000 in immediate revenue** for the business unit.

---

### PROJECT 2 — eCDP (Clinical Data Platform)
**Role:** Data Engineer | **Stack:** PySpark, Azure Databricks, Delta Lake, ADF, Azure DevOps (CI/CD), AWS S3, Oracle JDBC, GitHub

**Resume Bullets:**

- Designed and implemented a **Medallion Architecture** (Bronze → Silver → Gold) data pipeline on **Azure Databricks** and **Delta Lake** for a **16TB+ financial/clinical dataset**, processing incremental and full loads from CSV flat files and Oracle RDBMS sources.
- Developed **Azure Data Factory (ADF)** pipelines using Copy Activity, Linked Services (ADLS Gen2, Oracle), and parameterized pipelines to orchestrate end-to-end data ingestion from source to Bronze layer, reducing manual handoff effort.
- Built **PySpark transformation notebooks** in Databricks for Silver and Gold layer processing — including schema validation, deduplication, SCD Type 2 handling, and business-level aggregations — loaded into Delta tables with ACID compliance.
- Implemented **CI/CD pipelines using Azure DevOps** to automate deployment of ADF ARM templates and Databricks notebooks across Dev, QA, and Prod environments, reducing deployment effort and minimizing environment drift.
- Applied **Delta Lake optimization techniques** — OPTIMIZE, ZORDER, partitioning, and incremental MERGE — to improve query performance and manage data growth at scale.
- Configured **ADF trigger-based orchestration** to invoke Databricks notebooks in sequence across pipeline stages, enabling dependency management and failure alerting across the Medallion layers.
- Maintained version-controlled notebooks and ADF pipeline definitions in **GitHub**, following branching strategy (feature → dev → main) aligned with CI/CD promotion workflow.

---

### INTERNSHIP (6 Months)
**Role:** Intern — Data Engineering | **Stack:** PySpark, SQL, Azure Data Factory (ADF)

**Resume Bullets:**

- Assisted in building and testing ADF pipelines for data ingestion and transformation tasks, gaining hands-on experience with Copy Activity, Linked Services, and pipeline parameterization.
- Wrote PySpark scripts for data cleansing and transformation tasks, including null handling, type casting, and column-level filtering on structured datasets.
- Executed SQL queries for data validation, reconciliation, and ad-hoc analysis, supporting QA activities for pipeline output verification.
- Collaborated with senior engineers to understand end-to-end ETL design patterns and data flow across source, staging, and target layers.

---

---

## eCDP PROJECT — FROM SCRATCH (Full Architecture Breakdown)

---

### Overview

eCDP (Clinical Data Platform) is a cloud-native data engineering project that ingests **16TB+** of financial/clinical data from Oracle RDBMS and CSV flat files, processes it through a **Medallion Architecture (Bronze → Silver → Gold)** on **Azure Databricks + Delta Lake**, orchestrated by **Azure Data Factory (ADF)**, with CI/CD managed through **Azure DevOps**.

---

### Architecture Diagram (Textual)

```
[Source Systems]
    Oracle DB  ──────────────────────┐
    CSV Flat Files (ADLS Gen2 / S3)  │
                                     ▼
                            [ADF Copy Activity]
                                     │
                                     ▼
                         [Bronze Layer - Delta Lake]
                      (Raw, no transformation, append-only)
                                     │
                         [ADF triggers Databricks]
                                     │
                                     ▼
                         [Silver Layer - Delta Lake]
                      (Cleaned, deduplicated, standardized)
                                     │
                         [ADF triggers Databricks]
                                     │
                                     ▼
                          [Gold Layer - Delta Lake]
                      (Business aggregations, analytics-ready)
                                     │
                                     ▼
                     [Downstream: Reporting / BI / Export]
```

---

### Step 1 — Infrastructure Setup

**Storage: Azure Data Lake Storage Gen2 (ADLS Gen2)**
- Create containers: `bronze/`, `silver/`, `gold/`
- Each container follows a folder structure: `{container}/{source_system}/{entity_name}/YYYY/MM/DD/`

**Databricks Workspace:**
- Create clusters (standard or job clusters for production)
- Use Databricks Repos (connected to GitHub) for notebook version control

**Azure Data Factory:**
- Create ADF workspace
- Configure Linked Services:
  - Oracle (JDBC connection with credentials from Azure Key Vault)
  - ADLS Gen2 (for CSV files)
  - Azure Databricks (for notebook execution)
- Store secrets (DB passwords, connection strings) in **Azure Key Vault**

---

### Step 2 — Bronze Layer (Raw Ingestion via ADF)

**Goal:** Land raw data from Oracle and CSV into Bronze Delta tables with zero transformation.

**For Oracle Source:**
```
ADF Pipeline: oracle_to_bronze
  ├── Lookup Activity → fetch watermark (last_run_timestamp) from control table
  ├── Copy Activity
  │     Source: Oracle Linked Service → SQL query with WHERE clause on updated_at > watermark
  │     Sink: ADLS Gen2 → Parquet (or directly as Delta via Databricks)
  └── Stored Procedure Activity → update watermark in control table
```

**For CSV Source:**
```
ADF Pipeline: csv_to_bronze
  ├── Get Metadata Activity → list files in source folder (ADLS Gen2 / S3)
  ├── ForEach Activity → loop through each file
  │     └── Copy Activity
  │           Source: ADLS Gen2 (CSV with header detection)
  │           Sink: ADLS Gen2 Bronze path (Parquet/Delta)
  └── Move processed files to archive folder
```

**PySpark: Bronze Delta Write (Databricks Notebook triggered by ADF)**
```python
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Read raw CSV landed by ADF Copy Activity
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@<storage>.dfs.core.windows.net/csv_source/entity_name/")

# Add audit columns
from pyspark.sql.functions import current_timestamp, lit
df_raw = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("csv_source"))

# Write to Bronze Delta table
df_raw.write.format("delta") \
    .mode("append") \
    .partitionBy("year", "month") \
    .save("abfss://bronze@<storage>.dfs.core.windows.net/delta/entity_name/")
```

**Key Bronze Layer Rules:**
- No transformations — exactly what comes from source
- Schema enforcement OFF (allow schema evolution)
- Append-only (never overwrite raw data)
- Always add: `ingestion_timestamp`, `source_system`, `file_name` audit columns

---

### Step 3 — Silver Layer (Cleansing & Standardization via Databricks)

**Goal:** Clean, deduplicate, standardize, and apply schema enforcement.

**ADF triggers Databricks Notebook** with parameters: `source_entity`, `run_date`

**PySpark: Silver Transformation Notebook**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date, when, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Read from Bronze Delta
df_bronze = spark.read.format("delta") \
    .load("abfss://bronze@<storage>.dfs.core.windows.net/delta/entity_name/") \
    .filter(col("ingestion_timestamp") >= run_date)  # incremental read

# --- Data Quality & Cleansing ---
df_clean = df_bronze \
    .filter(col("primary_key").isNotNull()) \           # drop nulls on PK
    .withColumn("name", trim(upper(col("name")))) \     # standardize strings
    .withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd")) \  # cast dates
    .withColumn("amount", col("amount").cast("double")) # cast numeric

# --- Deduplication (keep latest record per PK) ---
window_spec = Window.partitionBy("primary_key").orderBy(col("updated_at").desc())
df_dedup = df_clean \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# --- SCD Type 2 MERGE into Silver Delta ---
silver_path = "abfss://silver@<storage>.dfs.core.windows.net/delta/entity_name/"

if DeltaTable.isDeltaTable(spark, silver_path):
    silver_table = DeltaTable.forPath(spark, silver_path)
    silver_table.alias("target").merge(
        df_dedup.alias("source"),
        "target.primary_key = source.primary_key"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    df_dedup.write.format("delta") \
        .partitionBy("event_date") \
        .save(silver_path)
```

**Silver Layer Rules:**
- Schema enforcement ON
- Deduplication on primary key
- Null handling for critical columns
- Standardized date formats, string casing
- MERGE (upsert) for incremental loads — not full overwrite

---

### Step 4 — Gold Layer (Business Aggregations via Databricks)

**Goal:** Analytics-ready, pre-aggregated tables for reporting / BI consumption.

**PySpark: Gold Aggregation Notebook**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, max

spark = SparkSession.builder.getOrCreate()

# Read from Silver
df_silver = spark.read.format("delta") \
    .load("abfss://silver@<storage>.dfs.core.windows.net/delta/entity_name/")

# Business aggregation example
df_gold = df_silver \
    .groupBy("region", "product_category", "event_date") \
    .agg(
        sum("amount").alias("total_revenue"),
        count("transaction_id").alias("transaction_count"),
        avg("amount").alias("avg_transaction_value"),
        max("amount").alias("max_transaction_value")
    )

# Write to Gold Delta table (full refresh or MERGE)
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("event_date") \
    .save("abfss://gold@<storage>.dfs.core.windows.net/delta/entity_name_agg/")
```

**Gold Layer Rules:**
- Pre-aggregated for reporting use cases
- Typically full refresh daily (or incremental partition overwrite)
- Delta tables registered in Databricks Unity Catalog / Hive Metastore for SQL access
- Downstream: Power BI, Databricks SQL, or export to downstream systems

---

### Step 5 — ADF Master Orchestration Pipeline

```
ADF Master Pipeline: master_pipeline
  │
  ├── 1. oracle_to_bronze (incremental load with watermark)
  ├── 2. csv_to_bronze (file-based ingestion)
  ├── 3. Execute Databricks Notebook → bronze_to_silver
  ├── 4. Execute Databricks Notebook → silver_to_gold
  └── 5. Notification Activity → email on failure / success
```

**ADF Execution of Databricks Notebook:**
- Use Databricks Linked Service (with PAT token from Key Vault)
- Pass parameters: `run_date`, `entity_name`, `env` (dev/qa/prod)
- Use Job Cluster (auto-terminate after job) to minimize cost

**ADF Triggers:**
- Tumbling Window Trigger for daily incremental runs
- Schedule Trigger for full loads (weekly)

---

### Step 6 — CI/CD with Azure DevOps

**Repository Structure (GitHub):**
```
ecdp/
├── adf/
│   ├── pipeline/           ← ADF ARM templates (exported from ADF UI)
│   ├── linkedService/
│   └── trigger/
├── databricks/
│   ├── notebooks/
│   │   ├── bronze_to_silver.py
│   │   ├── silver_to_gold.py
│   │   └── utils/
│   └── tests/
├── sql/
│   └── control_table_ddl.sql
└── azure-pipelines.yml     ← CI/CD definition
```

**Branching Strategy:**
```
feature/xyz → dev → qa → main (prod)
```
- Every change goes through a Pull Request
- Automated tests run on PR (unit tests for PySpark notebooks)

**Azure DevOps Pipeline (azure-pipelines.yml):**
```yaml
stages:
  - stage: Deploy_Dev
    jobs:
      - job: Deploy_ADF
        steps:
          - task: AzureResourceManagerTemplateDeployment@3
            inputs:
              resourceGroupName: 'ecdp-dev-rg'
              templateLocation: 'adf/pipeline/ARMTemplateForFactory.json'
              overrideParameters: '-factoryName ecdp-dev-adf'

      - job: Deploy_Databricks_Notebooks
        steps:
          - task: DatabricksNotebooks@0
            inputs:
              notebooksPath: 'databricks/notebooks/'
              targetPath: '/Repos/ecdp/notebooks/'
              workspaceUrl: $(DEV_DATABRICKS_URL)
              token: $(DEV_DATABRICKS_TOKEN)

  - stage: Deploy_QA
    dependsOn: Deploy_Dev
    # approval gate before QA

  - stage: Deploy_Prod
    dependsOn: Deploy_QA
    # manual approval gate required
```

**What CI/CD achieves:**
- No manual notebook uploads or ADF copy-paste across environments
- Environment-specific configs via Azure DevOps Variable Groups (dev/qa/prod)
- Rollback via Git revert + re-run pipeline
- Audit trail: every deployment linked to a commit and PR

---

### Step 7 — Delta Lake Optimization Techniques

Applied at Silver and Gold layers:

```python
# OPTIMIZE + ZORDER for query acceleration on frequently filtered columns
spark.sql("""
    OPTIMIZE delta.`abfss://silver@<storage>.dfs.core.windows.net/delta/entity_name/`
    ZORDER BY (region, event_date)
""")

# VACUUM to remove old files (default 7-day retention)
spark.sql("""
    VACUUM delta.`abfss://silver@<storage>.dfs.core.windows.net/delta/entity_name/`
    RETAIN 168 HOURS
""")

# Partition pruning: always filter on partition columns in queries
df = spark.read.format("delta").load(silver_path) \
    .filter(col("event_date") == "2024-01-15")  # hits only relevant partition
```

---

### Key Interview Points for eCDP

| Topic | What to Say |
|---|---|
| Why ADF? | "ADF handles orchestration and ingestion — it's great for moving data between sources (Oracle, CSV) and ADLS Gen2 without writing custom ingestion code. It also lets us trigger Databricks notebooks in sequence with dependency management." |
| Why Delta Lake? | "ACID transactions, time travel for debugging/rollback, schema enforcement, and efficient MERGE for incremental loads — all critical for production-grade pipelines at 16TB scale." |
| Why Medallion? | "Separation of concerns — Bronze keeps raw data for auditability, Silver gives clean business-ready data, Gold is pre-aggregated for fast BI consumption. Each layer has its own SLA and optimization." |
| CI/CD benefit? | "Without CI/CD, deploying notebooks and ADF pipelines manually across dev/qa/prod was error-prone. With Azure DevOps, we have automated deployment with approval gates, consistent environments, and full audit history." |
| Incremental load? | "We use watermark-based incremental loads from Oracle (filtering on updated_at > last run timestamp, stored in a control table). For CSV, we archive processed files and only pick up new ones per run." |

---

*Last Updated: June 2026*


Good question — your understanding is correct. ADF acts as the mover. But the incremental logic is different for Oracle vs CSV because **Oracle has timestamps on records**, CSV files don't. Let me break both down fully.

---

## Oracle → Bronze Delta (Incremental)

**Core idea: Watermark-based. You track "how far you've read" using a timestamp stored in a control table.**

### Full Flow:

```
Control Table (Azure SQL / Databricks)
  └── entity_name | last_watermark
      oracle_claims | 2024-01-14 23:59:59

          ↓ Step 1: ADF Lookup reads watermark

ADF Copy Activity
  Source: Oracle
    → SELECT * FROM claims WHERE updated_at > '2024-01-14 23:59:59'
  Sink: ADLS Gen2 (Parquet files in Bronze path)

          ↓ Step 2: ADF triggers Databricks notebook

Databricks reads Parquet → appends to Bronze Delta table

          ↓ Step 3: ADF updates watermark to current run time
Control Table → last_watermark = 2024-01-15 23:59:59
```

### ADF Pipeline Structure:

```
Pipeline: oracle_incremental_to_bronze
│
├── 1. Lookup Activity
│       Query: SELECT last_watermark FROM control_table
│               WHERE entity_name = 'oracle_claims'
│       → Output: last_watermark = "2024-01-14 23:59:59"
│
├── 2. Copy Activity
│       Source: Oracle Linked Service
│         Query: SELECT * FROM claims
│                WHERE updated_at > '@{activity('Lookup').output.firstRow.last_watermark}'
│       Sink: ADLS Gen2
│         Path: bronze/oracle/claims/run_date=2024-01-15/
│         Format: Parquet
│
├── 3. Databricks Notebook Activity
│       Notebook: /bronze_load/oracle_claims
│       Parameters: run_date = 2024-01-15
│
└── 4. Stored Procedure Activity (or Script Activity)
        Updates control_table SET last_watermark = CURRENT_TIMESTAMP
        WHERE entity_name = 'oracle_claims'
```

### Databricks Notebook (Bronze Write):

```python
# Parameters passed from ADF
run_date = dbutils.widgets.get("run_date")

# Read Parquet landed by ADF Copy Activity
df = spark.read.format("parquet") \
    .load(f"abfss://bronze@storage.dfs.core.windows.net/oracle/claims/run_date={run_date}/")

# Add audit columns
from pyspark.sql.functions import current_timestamp, lit
df = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("oracle")) \
    .withColumn("run_date", lit(run_date))

# APPEND to Bronze Delta — never overwrite
df.write.format("delta") \
    .mode("append") \
    .partitionBy("run_date") \
    .save("abfss://bronze@storage.dfs.core.windows.net/delta/claims/")
```

### One important edge case — what if the pipeline fails mid-run?

You do **NOT** update the watermark until the Databricks notebook succeeds. That's why the watermark update is the **last step** in the ADF pipeline. If step 3 (Databricks) fails, ADF pipeline fails, watermark stays at the old value, and next run re-fetches from the same point. No data loss.

---

## CSV → Bronze Delta (Incremental)

**Core idea: File-based tracking. CSV records have no `updated_at` column, so you track by which files have already been processed.**

You have **3 options** depending on how your source drops files:

---

### Option A — Archive Pattern (Most Common)

Source drops CSVs into a landing folder. After processing, you move them to an archive folder. Next run only sees unprocessed files.

```
Landing:  bronze/csv/claims/landing/    ← new files arrive here
Archive:  bronze/csv/claims/archive/    ← processed files moved here
```

```
ADF Pipeline: csv_incremental_to_bronze
│
├── 1. Get Metadata Activity
│       Dataset: ADLS Gen2 → landing folder
│       Field list: childItems  ← lists all files in folder
│
├── 2. If Condition Activity
│       Condition: @greater(length(activity('GetMeta').output.childItems), 0)
│       → TRUE: files exist, proceed
│       → FALSE: no files, skip pipeline
│
├── 3. ForEach Activity (loop through each file)
│   │
│   ├── 3a. Copy Activity
│   │       Source: landing/filename.csv
│   │       Sink: ADLS Gen2
│   │         Path: bronze/csv/claims/run_date=2024-01-15/
│   │         Format: Parquet
│   │
│   └── 3b. Delete Activity (or Copy + Delete)
│           Moves file from landing/ to archive/2024-01-15/filename.csv
│
└── 4. Databricks Notebook Activity
        Reads all Parquet from run_date=2024-01-15/ → appends to Delta
```

---

### Option B — Date Partitioned Folder Pattern

Source system drops files into date-named folders each day. ADF reads only today's folder.

```
Landing:
  bronze/csv/claims/2024-01-13/file1.csv
  bronze/csv/claims/2024-01-14/file1.csv
  bronze/csv/claims/2024-01-15/file1.csv  ← today, process this
```

ADF Copy Activity source path becomes dynamic:
```
Path: bronze/csv/claims/@{formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')}/
```

This is the simplest pattern. No archive needed. ADF always knows which folder is "today's."

---

### Option C — Control Table Pattern (Most Robust)

You maintain a table tracking exactly which files have been processed.

```
file_tracking_table:
  file_name          | processed | processed_at
  claims_20240114.csv | TRUE      | 2024-01-14 06:00
  claims_20240115.csv | FALSE     | NULL          ← pick this up
```

```
ADF Pipeline:
  1. Lookup → SELECT file_name FROM file_tracking WHERE processed = FALSE
  2. ForEach → loop through unprocessed files
       a. Copy Activity → land file into Bronze path
       b. Databricks Notebook → load to Delta
       c. Script Activity → UPDATE file_tracking SET processed = TRUE WHERE file_name = X
```

This gives you full audit history and handles reruns safely.

---

## Side-by-Side Comparison

| | Oracle | CSV |
|---|---|---|
| How to detect new data | `updated_at > watermark` timestamp | New files in folder / unarchived |
| State tracking | Control table (timestamp) | Archive folder / file tracking table |
| Risk of reprocessing | Low (watermark prevents it) | Medium (archive pattern prevents it) |
| ADF activity used | Copy Activity + Lookup + Stored Proc | Copy Activity + Get Metadata + ForEach |
| Bronze write mode | APPEND | APPEND |

---

## Key Rule for Both

**Bronze Delta is always APPEND-only.** You never MERGE or OVERWRITE in Bronze. Every incremental run adds new rows with the `run_date` partition. This gives you:

- Full history of every load
- Ability to reprocess from Bronze if Silver breaks
- Audit trail for debugging

The MERGE/upsert logic happens only at Silver layer — not here.

---

Want me to go deeper into Step 3 (Silver layer) next — specifically how the MERGE logic works for incremental records coming from Bronze?


## Silver Layer — Full Deep Dive (Bronze → Silver)

---

## First, Understand What's in Bronze

Before writing Silver logic, remember what Bronze contains:

```
Bronze Delta Table: claims
  ┌─────────────┬────────────┬──────────┬──────────────────────┬────────────┐
  │ claim_id    │ amount     │ status   │ ingestion_timestamp  │ run_date   │
  ├─────────────┼────────────┼──────────┼──────────────────────┼────────────┤
  │ C001        │ 5000       │ APPROVED │ 2024-01-14 06:00     │ 2024-01-14 │
  │ C002        │ 3000       │ PENDING  │ 2024-01-14 06:00     │ 2024-01-14 │
  │ C001        │ 5000       │ REJECTED │ 2024-01-15 06:00     │ 2024-01-15 │  ← same C001, status changed
  │ C003        │ 7000       │ APPROVED │ 2024-01-15 06:00     │ 2024-01-15 │  ← new record
  └─────────────┴────────────┴──────────┴──────────────────────┴────────────┘
```

C001 appears **twice** — Bronze keeps both versions (it's append-only). Silver needs to figure out: is C001 new or an update? And keep only the latest.

---

## Silver Layer — 4 Stage Pipeline

```
Bronze Delta
    │
    ▼
Stage 1: Incremental Read (only new Bronze records since last Silver run)
    │
    ▼
Stage 2: Data Quality & Cleansing
    │
    ▼
Stage 3: Deduplication (handle duplicates within this batch)
    │
    ▼
Stage 4: MERGE into Silver Delta (handle new vs existing records)
```

---

## Stage 1 — Incremental Read from Bronze

You don't read ALL of Bronze every run. That would be full scan on 16TB — very slow.

**Two approaches:**

### Approach A — Partition Filter (Simpler)

```python
# ADF passes run_date as parameter
run_date = dbutils.widgets.get("run_date")  # "2024-01-15"

df_bronze_incremental = spark.read.format("delta") \
    .load("abfss://bronze@storage.dfs.core.windows.net/delta/claims/") \
    .filter(col("run_date") == run_date)  # only today's partition

print(f"Records fetched from Bronze: {df_bronze_incremental.count()}")
```

This works because Bronze is partitioned by `run_date`. Spark only reads that one partition folder — not the full table.

---

### Approach B — Delta Change Data Feed / CDF (More Advanced)

Delta Lake has a built-in feature called **Change Data Feed** — tracks every INSERT/UPDATE/DELETE at the Delta level.

```python
# Enable CDF on Bronze table (one-time setup)
spark.sql("""
    ALTER TABLE delta.`abfss://bronze@storage.dfs.core.windows.net/delta/claims/`
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Read only changes since last Silver run (using Delta version or timestamp)
df_bronze_incremental = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", last_silver_run_timestamp) \
    .load("abfss://bronze@storage.dfs.core.windows.net/delta/claims/")
```

For your intermediate level, **Approach A (partition filter) is what you'd use and explain in an interview.** CDF is good to mention as an optimization.

---

## Stage 2 — Data Quality & Cleansing

```python
from pyspark.sql.functions import col, trim, upper, lower, to_date, regexp_replace, when

df_cleaned = df_bronze_incremental \
    # --- Null checks: drop records with null on critical columns ---
    .filter(col("claim_id").isNotNull()) \
    .filter(col("amount").isNotNull()) \

    # --- String standardization ---
    .withColumn("status", upper(trim(col("status")))) \       # "approved " → "APPROVED"
    .withColumn("region", lower(trim(col("region")))) \       # " North " → "north"

    # --- Type casting ---
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("claim_date", to_date(col("claim_date"), "yyyy-MM-dd")) \

    # --- Remove special characters from string fields ---
    .withColumn("claim_id", regexp_replace(col("claim_id"), "[^A-Za-z0-9]", "")) \

    # --- Handle bad values ---
    .withColumn("amount", when(col("amount") < 0, 0).otherwise(col("amount"))) \  # no negative amounts

    # --- Add Silver audit column ---
    .withColumn("silver_load_timestamp", current_timestamp())
```

**Add a data quality summary log (good practice to mention):**

```python
total_input = df_bronze_incremental.count()
total_after_clean = df_cleaned.count()
dropped = total_input - total_after_clean

print(f"Input records    : {total_input}")
print(f"After cleansing  : {total_after_clean}")
print(f"Dropped (bad data): {dropped}")

# Optionally write rejected records to a quarantine/bad-records Delta table
df_rejected = df_bronze_incremental.filter(col("claim_id").isNull())
df_rejected.write.format("delta").mode("append") \
    .save("abfss://silver@storage.dfs.core.windows.net/delta/quarantine/claims/")
```

---

## Stage 3 — Deduplication Within the Batch

Before merging into Silver, your incoming batch itself may have duplicates — same `claim_id` appearing multiple times in today's Bronze load (e.g., source sent the record twice).

You keep only the **latest version per primary key** within this batch:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Define window: for each claim_id, rank by latest updated_at
window_spec = Window \
    .partitionBy("claim_id") \
    .orderBy(desc("updated_at"))   # most recent record first

df_deduped = df_cleaned \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print(f"After dedup: {df_deduped.count()}")
```

**Why this matters:**

```
Incoming batch (before dedup):
  C001 | 5000 | REJECTED | updated_at: 2024-01-15 06:00   ← keep this
  C001 | 5000 | PENDING  | updated_at: 2024-01-15 05:00   ← drop this

After dedup:
  C001 | 5000 | REJECTED | updated_at: 2024-01-15 06:00   ← only this goes to MERGE
```

---

## Stage 4 — MERGE into Silver Delta

This is the most critical step. MERGE handles 3 scenarios:

```
Incoming record vs Silver table:
  ├── Record EXISTS in Silver + data has CHANGED  → UPDATE
  ├── Record EXISTS in Silver + data is SAME      → do nothing (optional)
  └── Record does NOT exist in Silver             → INSERT
```

### Simple MERGE (SCD Type 1 — overwrite with latest):

```python
from delta.tables import DeltaTable

silver_path = "abfss://silver@storage.dfs.core.windows.net/delta/claims/"

if DeltaTable.isDeltaTable(spark, silver_path):
    # Silver table exists — do MERGE
    silver_table = DeltaTable.forPath(spark, silver_path)

    silver_table.alias("target") \
        .merge(
            df_deduped.alias("source"),
            "target.claim_id = source.claim_id"    # join condition (primary key)
        ) \
        .whenMatchedUpdateAll() \     # if claim_id exists → overwrite all columns
        .whenNotMatchedInsertAll() \  # if new claim_id → insert full row
        .execute()
else:
    # First run — Silver doesn't exist yet, just write
    df_deduped.write.format("delta") \
        .partitionBy("claim_date") \
        .save(silver_path)
```

**What happens for C001 in our example:**

```
Silver before MERGE:
  C001 | 5000 | APPROVED | 2024-01-14

Incoming batch (from Bronze 2024-01-15):
  C001 | 5000 | REJECTED | 2024-01-15   ← claim_id matches → UPDATE
  C003 | 7000 | APPROVED | 2024-01-15   ← new claim_id   → INSERT

Silver after MERGE:
  C001 | 5000 | REJECTED | 2024-01-15   ← updated
  C002 | 3000 | PENDING  | 2024-01-14   ← unchanged
  C003 | 7000 | APPROVED | 2024-01-15   ← new record
```

---

### Advanced MERGE — SCD Type 2 (Keep full history)

SCD Type 2 means: don't overwrite the old record. **Close it** and **insert a new version.** Used when business needs full audit history of how a record changed over time.

```python
from pyspark.sql.functions import lit, current_date

silver_table = DeltaTable.forPath(spark, silver_path)

silver_table.alias("target") \
    .merge(
        df_deduped.alias("source"),
        # Match on PK + currently active record only
        "target.claim_id = source.claim_id AND target.is_current = true"
    ) \
    .whenMatchedUpdate(
        # Only update if something actually changed (avoid unnecessary writes)
        condition = "target.status != source.status OR target.amount != source.amount",
        set = {
            "is_current"  : lit(False),           # close the old record
            "end_date"    : current_date(),        # set expiry date
            "updated_at"  : current_timestamp()
        }
    ) \
    .whenNotMatchedInsertAll() \   # insert new records as-is
    .execute()

# Now insert the NEW version of updated records as active rows
df_new_versions = df_deduped.join(
    spark.read.format("delta").load(silver_path).filter(col("is_current") == False),
    on="claim_id", how="inner"
).select(df_deduped["*"]) \
 .withColumn("is_current", lit(True)) \
 .withColumn("start_date", current_date()) \
 .withColumn("end_date", lit(None).cast("date"))

df_new_versions.write.format("delta").mode("append").save(silver_path)
```

**SCD Type 2 result for C001:**

```
Silver after SCD Type 2 MERGE:
  C001 | APPROVED | is_current=FALSE | start=2024-01-14 | end=2024-01-15  ← closed
  C001 | REJECTED | is_current=TRUE  | start=2024-01-15 | end=NULL        ← active
  C002 | PENDING  | is_current=TRUE  | start=2024-01-14 | end=NULL        ← unchanged
  C003 | APPROVED | is_current=TRUE  | start=2024-01-15 | end=NULL        ← new
```

---

## What to Say in an Interview

| Question | Answer |
|---|---|
| **SCD Type 1 vs Type 2 — when to use which?** | "Type 1 when you only need the latest state — e.g., a customer's current address. Type 2 when business needs history — e.g., how a claim's status changed over time for audit or compliance." |
| **Why dedup before MERGE?** | "MERGE fails if the source has two rows with the same primary key matching the same target row — it throws an ambiguous match error in Delta. Dedup before MERGE prevents that." |
| **Why not just overwrite Silver daily?** | "Full overwrite on 16TB every day is expensive and slow. MERGE touches only changed records. Also, overwrite breaks Delta's time travel — MERGE preserves version history." |
| **What if a MERGE run fails halfway?** | "Delta Lake MERGE is atomic — either the full MERGE commits or nothing does. No partial state. Next run will re-read the same Bronze partition and retry cleanly." |

---

Ready to move to **Step 4 — Gold Layer**, or do you want to go sideways into something specific like the **quarantine/bad-records pattern** or **how to unit test Silver notebooks**?

## Gold Layer — Full Deep Dive (Silver → Gold)

---

## First, Understand the Purpose of Gold

Bronze = raw. Silver = clean. **Gold = business-ready.**

Gold is what analysts, BI tools, and downstream systems actually consume. The key shift in Gold is you stop thinking like an engineer and start thinking like a **business user.**

```
Silver (row-level, normalized)          Gold (aggregated, use-case specific)
────────────────────────────────        ──────────────────────────────────────
C001 | APPROVED | 5000 | North          region | month   | total  | count
C002 | PENDING  | 3000 | South    →     North  | Jan-24  | 12000  | 3
C003 | APPROVED | 7000 | North          South  | Jan-24  | 3000   | 1
C004 | APPROVED | 6000 | East           East   | Jan-24  | 6000   | 1
```

Silver has **one row per entity.** Gold has **one row per business question.**

---

## Gold Layer Design — Key Decision First

Before writing any code, answer: **who is consuming this Gold table and what are they asking?**

In eCDP (financial/clinical data), typical consumers and questions:

```
Consumer              Business Question
──────────────────    ────────────────────────────────────────────────
Finance Team          Total claim amount by region, month, status
Compliance Team       Count of REJECTED claims per product per quarter
Operations Team       Average processing time per claim category
BI / Power BI         Daily revenue trend across all regions
Downstream System     Export approved claims above $10,000
```

Each of these becomes a **separate Gold table** (or Gold view). Gold is not one giant table — it's multiple purpose-built tables.

---

## Gold Layer — 3 Types of Tables You Build

---

### Type 1 — Aggregated Summary Table (Most Common)

Pre-computed metrics grouped by business dimensions.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, min,
    round, month, year, date_format,
    current_timestamp, lit
)

spark = SparkSession.builder.getOrCreate()

# Read from Silver — only active/current records
df_silver = spark.read.format("delta") \
    .load("abfss://silver@storage.dfs.core.windows.net/delta/claims/") \
    .filter(col("is_current") == True) \           # SCD Type 2 — only active records
    .filter(col("status") == "APPROVED")            # business filter: only approved claims

# Build Gold aggregation
df_gold_summary = df_silver \
    .withColumn("claim_month", date_format(col("claim_date"), "yyyy-MM")) \
    .groupBy("region", "product_category", "claim_month") \
    .agg(
        sum("amount").alias("total_claim_amount"),
        count("claim_id").alias("total_claims"),
        avg("amount").alias("avg_claim_amount"),
        max("amount").alias("max_claim_amount"),
        min("amount").alias("min_claim_amount"),
        round(sum("amount") / count("claim_id"), 2).alias("revenue_per_claim")
    ) \
    .withColumn("gold_load_timestamp", current_timestamp()) \
    .withColumn("report_date", lit(run_date))

# Write to Gold Delta
gold_path = "abfss://gold@storage.dfs.core.windows.net/delta/claims_summary/"

df_gold_summary.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"report_date = '{run_date}'") \
    .partitionBy("claim_month") \
    .save(gold_path)
```

**Why `replaceWhere` instead of full overwrite:**

```python
# BAD — overwrites ENTIRE Gold table every run (expensive, risky)
.mode("overwrite")

# GOOD — overwrites only today's partition (fast, safe)
.option("replaceWhere", f"report_date = '{run_date}'")
```

This means historical Gold partitions (Jan, Feb, Mar) are untouched. Only today's data is refreshed.

---

### Type 2 — Flattened / Denormalized Table

Silver is normalized (multiple tables joined via foreign keys). Gold denormalizes everything into one wide flat table — so BI tools don't need complex JOINs.

```python
# Read multiple Silver tables
df_claims   = spark.read.format("delta").load(silver_path + "claims/").filter(col("is_current") == True)
df_patients = spark.read.format("delta").load(silver_path + "patients/").filter(col("is_current") == True)
df_products = spark.read.format("delta").load(silver_path + "products/").filter(col("is_current") == True)

# Join into one wide flat table
df_gold_flat = df_claims \
    .join(df_patients, on="patient_id", how="left") \
    .join(df_products, on="product_id", how="left") \
    .select(
        # Claim fields
        df_claims["claim_id"],
        df_claims["amount"],
        df_claims["status"],
        df_claims["claim_date"],
        # Patient fields
        df_patients["patient_name"],
        df_patients["age_group"],
        df_patients["region"],
        # Product fields
        df_products["product_name"],
        df_products["product_category"],
        df_products["therapeutic_area"]
    ) \
    .withColumn("gold_load_timestamp", current_timestamp())

# Write to Gold
df_gold_flat.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"claim_date = '{run_date}'") \
    .partitionBy("claim_date") \
    .save("abfss://gold@storage.dfs.core.windows.net/delta/claims_flat/")
```

**Use case:** Power BI report connects directly to `claims_flat` Gold table — no JOINs needed in the report. Everything is pre-joined.

---

### Type 3 — Filtered Export Table (Downstream System)

Sometimes Gold is not for BI — it's for a downstream system that only needs a specific subset of data.

```python
# Only APPROVED claims above $10,000 from last 30 days → export to Oracle / CSV
df_gold_export = df_silver \
    .filter(col("status") == "APPROVED") \
    .filter(col("amount") >= 10000) \
    .filter(col("claim_date") >= date_sub(current_date(), 30)) \
    .select("claim_id", "patient_id", "amount", "claim_date", "region") \
    .withColumn("export_timestamp", current_timestamp())

# Write to Gold Delta (as staging for export)
df_gold_export.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@storage.dfs.core.windows.net/delta/claims_export/")

# ADF then picks this up and copies to Oracle / CSV outbound
```

---

## Gold Layer — Incremental vs Full Refresh

Unlike Bronze (always append) and Silver (always MERGE), Gold has **both patterns** depending on the table type:

```
Gold Table Type          Load Strategy          Why
──────────────────────   ───────────────────    ──────────────────────────────────────
Aggregated Summary       Partition overwrite     Recalculate only today's partition
Flattened / Denorm       Partition overwrite     Re-join only today's new Silver data  
Rolling window metric    Full overwrite          "Last 30 days" always recalculates all
Export / Outbound        Full overwrite          Always fresh snapshot for downstream
```

**Rolling window example:**

```python
# "Last 30 days active claims" — must always full overwrite
# because yesterday's 30-day window is different from today's
df_rolling = df_silver \
    .filter(col("claim_date") >= date_sub(current_date(), 30)) \
    .groupBy("region") \
    .agg(sum("amount").alias("rolling_30d_revenue"))

df_rolling.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_path + "rolling_30d_summary/")
```

---

## Gold Layer — Delta Optimizations

Gold tables are read **very frequently** by BI tools. You must optimize them aggressively.

```python
# Run after every Gold write

# 1. OPTIMIZE — compact small files into large ones (better read performance)
spark.sql(f"""
    OPTIMIZE delta.`abfss://gold@storage.dfs.core.windows.net/delta/claims_summary/`
    ZORDER BY (region, claim_month)
""")
# ZORDER co-locates data by region + claim_month on disk
# Power BI filtering by region+month becomes 10x faster

# 2. VACUUM — remove old deleted files (keep 7 days for time travel)
spark.sql(f"""
    VACUUM delta.`abfss://gold@storage.dfs.core.windows.net/delta/claims_summary/`
    RETAIN 168 HOURS
""")
```

**ZORDER rule of thumb — pick columns that BI users filter on most:**
```
If Power BI always filters: WHERE region = 'North' AND claim_month = '2024-01'
Then ZORDER BY (region, claim_month)  ← matches the filter pattern
```

---

## Gold Layer — Register as SQL Table (Databricks Catalog)

After writing Gold Delta files, register them as tables so analysts can query with plain SQL:

```python
# Register in Databricks Unity Catalog / Hive Metastore
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.claims_summary
    USING DELTA
    LOCATION 'abfss://gold@storage.dfs.core.windows.net/delta/claims_summary/'
""")

# Now analysts can query directly:
# SELECT region, SUM(total_claim_amount) FROM gold.claims_summary WHERE claim_month = '2024-01'
# Power BI connects to Databricks SQL endpoint → queries this table
```

---

## Full Bronze → Silver → Gold Flow (End to End)

```
ADF Master Pipeline
│
├── 1. oracle_to_bronze          (ADF Copy Activity + watermark)
│        ↓ appends to Bronze Delta partitioned by run_date
│
├── 2. csv_to_bronze             (ADF ForEach + Archive)
│        ↓ appends to Bronze Delta partitioned by run_date
│
├── 3. bronze_to_silver          (Databricks Notebook)
│        ↓ reads today's Bronze partition
│        ↓ clean → dedup → MERGE into Silver Delta
│
├── 4. silver_to_gold            (Databricks Notebook)
│        ↓ reads Silver active records
│        ↓ aggregate/flatten/filter → partition overwrite into Gold Delta
│        ↓ OPTIMIZE + ZORDER + VACUUM
│
└── 5. Notify on success/failure (ADF Web Activity → email/Teams alert)
```

---

## What to Say in Interview

| Question | Answer |
|---|---|
| **Why separate Gold tables instead of one big one?** | "Each Gold table is purpose-built for a specific consumer. One wide table for everything would make queries slow and hard to maintain. Separate tables let us optimize partitioning and ZORDER per use case." |
| **Why not let BI tools query Silver directly?** | "Silver is normalized and row-level — every Power BI report would need complex JOINs and GROUP BYs. Gold pre-computes those so report refresh is fast. Also separates transformation concerns from reporting concerns." |
| **Why `replaceWhere` over full overwrite in Gold?** | "Full overwrite rewrites 16TB every day even though only today's data changed. `replaceWhere` targets only the affected partition — much faster, cheaper, and preserves Delta's time travel history for older partitions." |
| **What does ZORDER actually do?** | "It physically co-locates related data on disk based on the ZORDER columns. When a query filters on those columns, Delta's data skipping reads far fewer files. Without ZORDER, Spark scans all files even for a narrow filter." |

---

Want to go to **Step 5 — ADF Master Orchestration** (how all 4 stages are wired together with error handling, retries, and alerts), or jump to **Step 6 — CI/CD** in detail?
