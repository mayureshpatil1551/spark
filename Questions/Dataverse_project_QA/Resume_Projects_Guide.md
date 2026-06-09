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
