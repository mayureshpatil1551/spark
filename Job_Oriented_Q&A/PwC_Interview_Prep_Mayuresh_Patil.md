# 🎯 PwC Data Engineering – Interview Preparation Guide
### Tailored for Mayuresh Patil | Azure Data Engineer & Databricks Specialist

---

## 📌 Table of Contents
1. [HR & Behavioural Questions](#1-hr--behavioural-questions)
2. [Azure & Cloud Architecture](#2-azure--cloud-architecture)
3. [Azure Databricks & Delta Lake](#3-azure-databricks--delta-lake)
4. [PySpark & Big Data](#4-pyspark--big-data)
5. [Azure Data Factory (ADF)](#5-azure-data-factory-adf)
6. [CI/CD & DevOps](#6-cicd--devops)
7. [Data Modeling & Medallion Architecture](#7-data-modeling--medallion-architecture)
8. [Snowflake & Data Migration](#8-snowflake--data-migration)
9. [SQL Coding Questions](#9-sql-coding-questions)
10. [PySpark Coding Questions](#10-pyspark-coding-questions)
11. [Python Coding Questions](#11-python-coding-questions)
12. [Scenario-Based Questions](#12-scenario-based-questions)

---

## 1. HR & Behavioural Questions

---

### Q1. Tell me about yourself.

**Answer:**
"I am Mayuresh Patil, a certified Databricks Data Engineer with around 3 years of experience working at Cognizant. I specialize in building scalable Lakehouse solutions on Microsoft Azure using Databricks, PySpark, and Delta Lake. In my current role, I have architected Medallion-based data pipelines, migrated 20+ TB of clinical data to ADLS Gen2, and achieved a 40% reduction in query latency through optimization techniques like Z-Ordering and liquid clustering. I am also certified in Databricks Data Engineer Associate, Azure Data Fundamentals, and AWS Cloud Practitioner. I am now looking to bring this expertise to a global consulting firm like PwC, where I can solve complex data challenges for diverse clients."

---

### Q2. Why do you want to join PwC?

**Answer:**
"PwC Acceleration Centers offer exactly the kind of environment I want to grow in — exposure to multi-industry clients, large-scale Azure data projects, and a culture of continuous learning. My background in building data pipelines, Medallion architectures, and CI/CD automation aligns strongly with PwC's data engineering mandate. I also value PwC's commitment to professional development, which matches my learning mindset."

---

### Q3. Describe a time you took ownership of a critical task.

**Answer (STAR format):**
- **Situation:** At Cognizant, our Databricks deployment pipeline for the eCDP project was entirely manual, taking 15+ hours of weekly effort and often causing deployment errors.
- **Task:** I was given the task to automate the entire process using Azure DevOps.
- **Action:** I designed a CI/CD pipeline that automated notebook deployments, integrated error-handling scripts, and built environment-specific configuration management.
- **Result:** The release cycle was reduced by 50%, eliminated 15+ hours of weekly manual intervention, and near-zero downtime in production deployments.

---

### Q4. Tell me about a time you worked in a team under pressure.

**Answer:**
"During the FinOps Dataverse PoC, I collaborated with the SAP team, business analysts, and Snowflake DBAs under a tight deadline. We had to build a dual-target migration strategy from SAP to both Delta Lake and Snowflake. I handled the PySpark transformation layer using Spark-XML, coordinated daily syncs, and ensured stakeholder alignment. The PoC was successful, secured $5,000 in immediate revenue, and fast-tracked the client's cloud adoption roadmap."

---

### Q5. How do you handle ambiguous or unclear requirements?

**Answer:**
"I believe in proactive communication. When requirements are unclear, I schedule a quick discovery session with stakeholders to define key data flows, expected outputs, and edge cases. I document my understanding and share it for sign-off before development begins. This approach saved significant rework in the Airlines Data Modeling project, where upstream schema changes were frequent."

---

## 2. Azure & Cloud Architecture

---

### Q6. Explain the Azure data ecosystem you have worked with.

**Answer:**
I have worked across the full Azure data stack:
- **ADLS Gen2** – Primary data lake storage (Bronze/Silver/Gold layers)
- **Azure Databricks** – Core compute for ETL, ML prep, and analytics workloads
- **Azure Data Factory (ADF)** – Orchestration of ingestion pipelines from Oracle, Salesforce
- **Azure Synapse Analytics** – For SQL-based analytical queries
- **Azure Stream Analytics** – Real-time streaming data processing
- **Azure Key Vault** – Secrets management (credentials, connection strings)
- **Azure Active Directory** – Identity and access management
- **Azure DevOps** – CI/CD for Databricks notebook deployments

---

### Q7. What is ADLS Gen2 and how does it differ from Blob Storage?

**Answer:**
ADLS Gen2 (Azure Data Lake Storage Gen2) is built on top of Azure Blob Storage but adds a hierarchical namespace (HNS), enabling:
- **Directory-level operations** (rename, delete) at high performance
- **POSIX-compliant ACLs** for fine-grained security at the file and folder level
- **Better Spark compatibility** through optimized I/O

In Blob Storage, operations like renaming a directory require copying and deleting all objects. ADLS Gen2 makes these atomic and fast, which is critical for big data workloads.

---

### Q8. How do you secure secrets in Azure pipelines?

**Answer:**
I use **Azure Key Vault** to store all sensitive information (connection strings, API keys, SAS tokens). In Databricks, I create a secret scope backed by Key Vault and reference secrets using `dbutils.secrets.get(scope, key)`. In ADF, I use linked services with Key Vault references. This ensures no credentials are hardcoded in notebooks or JSON configs.

```python
# Example: Accessing Key Vault secret in Databricks
storage_key = dbutils.secrets.get(scope="kv-scope", key="adls-storage-key")
spark.conf.set(
    "fs.azure.account.key.<storage_account>.dfs.core.windows.net",
    storage_key
)
```

---

## 3. Azure Databricks & Delta Lake

---

### Q9. What is the Medallion Architecture and how did you implement it?

**Answer:**
The Medallion Architecture organizes data into three layers:
- **Bronze (Raw):** Raw ingested data from source systems, stored as-is (JSON, CSV, Parquet). No transformations.
- **Silver (Cleansed):** Deduplicated, validated, and joined data. Schema enforcement applied.
- **Gold (Curated):** Business-ready aggregated tables for reporting and analytics.

In eCDP, I migrated 20+ TB of clinical data into this pattern on ADLS Gen2 using Delta Lake. Bronze tables used Autoloader for incremental ingestion. Silver used schema evolution and SCD logic. Gold tables drove 300+ reports.

---

### Q10. What is Delta Lake and what advantages does it provide?

**Answer:**
Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. Key features:
- **ACID Transactions:** Ensures data consistency even during concurrent writes
- **Schema Enforcement & Evolution:** Prevents bad data from corrupting tables; allows adding new columns
- **Time Travel:** Query historical snapshots using `VERSION AS OF` or `TIMESTAMP AS OF`
- **Upserts (MERGE):** Efficient CDC handling
- **Scalable Metadata:** Handles large tables with billions of rows

---

### Q11. Explain Z-Ordering and Liquid Clustering. When would you use each?

**Answer:**

**Z-Ordering:**
- Colocates related data in the same set of files based on one or more columns.
- Optimizes query performance by reducing the amount of data scanned.
- Use when you have well-known, static high-cardinality filter columns (e.g., `patient_id`, `date`).
- Applied using `OPTIMIZE table ZORDER BY (col1, col2)`.

**Liquid Clustering (Databricks Runtime 13.1+):**
- Next-generation replacement for Z-Ordering and partitioning.
- Automatically manages data layout without requiring full table rewrites.
- Supports incremental clustering — only newly inserted/updated data is clustered.
- More flexible — clustering keys can be changed without rewriting the full table.
- Use for large, frequently updated tables where Z-Ordering causes full rewrites.

In eCDP, I used both: Z-Ordering for historical analytics tables and Liquid Clustering for high-frequency ingestion tables.

---

### Q12. What is Databricks Autoloader and why did you use it?

**Answer:**
Databricks Autoloader (`cloudFiles`) is a structured streaming source that incrementally ingests new files from cloud storage (ADLS, S3, GCS) into Delta tables. 

Key advantages:
- **Automatic file discovery:** No need to track which files have been processed
- **Scalable:** Handles millions of files without listing the entire directory
- **Schema inference and evolution:** Automatically detects schema changes
- **Exactly-once semantics:** Checkpoint-based state tracking

I used it in eCDP to ingest clinical data files landing in ADLS Gen2 in near real-time, replacing a brittle batch job that required manual file tracking.

```python
# Autoloader example
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schema/bronze/")
    .load("/mnt/bronze/incoming/"))

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/bronze/") \
    .outputMode("append") \
    .table("bronze.clinical_events")
```

---

### Q13. How does Delta Schema Evolution work?

**Answer:**
By default, Delta Lake enforces schema — writing data with extra or mismatched columns raises an error. Schema Evolution relaxes this:

- **mergeSchema:** Allows adding new columns when writing.
- **overwriteSchema:** Replaces the entire schema (use with caution).

```python
# Enable schema evolution on write
df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/mnt/silver/patient_records")
```

In eCDP, upstream Oracle and Salesforce schemas changed frequently. Schema evolution prevented pipeline failures and allowed Silver and Gold layers to absorb new columns without redeployment.

---

### Q14. Explain Delta Lake Time Travel.

**Answer:**
Delta Lake maintains a transaction log (`_delta_log`) that records every change to a table. Time Travel lets you query historical versions:

```sql
-- Query by version
SELECT * FROM clinical_events VERSION AS OF 5;

-- Query by timestamp
SELECT * FROM clinical_events TIMESTAMP AS OF '2024-01-15T00:00:00';

-- Restore a table to previous version
RESTORE TABLE clinical_events TO VERSION AS OF 3;
```

This is extremely useful for auditing, debugging pipelines, and recovering from accidental deletes or bad writes.

---

## 4. PySpark & Big Data

---

### Q15. What is the difference between transformations and actions in Spark?

**Answer:**

| Type | Description | Examples | Lazy? |
|------|-------------|----------|-------|
| Transformation | Returns a new DataFrame/RDD | `filter`, `select`, `groupBy`, `join`, `map` | Yes (Lazy) |
| Action | Triggers computation and returns results | `show`, `count`, `collect`, `write`, `save` | No (Eager) |

Spark builds a **DAG (Directed Acyclic Graph)** of transformations and only executes them when an action is triggered. This enables optimization through the **Catalyst optimizer**.

---

### Q16. What is the difference between narrow and wide transformations?

**Answer:**
- **Narrow transformations:** Each partition of the parent RDD is used by at most one partition of the child. No shuffle required. Examples: `map`, `filter`, `select`.
- **Wide transformations:** Data from multiple partitions must be combined (shuffled across the network). Expensive. Examples: `groupBy`, `join`, `distinct`, `repartition`.

Minimizing wide transformations (especially shuffle joins) is key to Spark performance.

---

### Q17. How do you handle data skew in PySpark?

**Answer:**
Data skew occurs when one partition has significantly more data than others, causing one executor to run much longer.

**Solutions:**
1. **Salting:** Add a random suffix to the skewed key to distribute data evenly.
2. **Broadcast Join:** Broadcast the smaller DataFrame to avoid shuffle for the large one.
3. **Repartition:** Repartition on a column with better cardinality.
4. **AQE (Adaptive Query Execution):** Databricks automatically handles skew with `spark.sql.adaptive.enabled = true`.

```python
# Broadcast join example
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_df), "patient_id")
```

---

## 5. Azure Data Factory (ADF)

---

### Q18. How did you use ADF in your projects?

**Answer:**
In eCDP, I used ADF as the primary orchestration layer:
- **Linked Services** for connecting to Oracle, Salesforce, and ADLS Gen2
- **Copy Activity** for initial bulk loads from Oracle
- **Lookup + ForEach** activities for dynamic, configuration-driven pipeline execution
- **Databricks Notebook Activity** to trigger PySpark transformation notebooks
- **Triggers** (scheduled and event-based) for pipeline automation

I implemented a metadata-driven framework where pipeline configurations (table names, watermarks, load types) were stored in a control table, making the pipeline reusable across 50+ tables.

---

### Q19. What is the difference between ADF datasets, linked services, and pipelines?

**Answer:**
- **Linked Service:** Connection definition to a data source or destination (e.g., Oracle connection string, ADLS account).
- **Dataset:** Representation of the data structure within a linked service (e.g., a specific table or file path).
- **Pipeline:** Logical grouping of activities that perform a unit of work (Copy, Notebook, Lookup, etc.).
- **Activity:** A single step inside a pipeline (Copy Data, Execute Databricks Notebook, etc.).

---

## 6. CI/CD & DevOps

---

### Q20. How did you implement CI/CD for Databricks?

**Answer:**
I implemented CI/CD using **Azure DevOps** with the following setup:

1. **Source Control:** All Databricks notebooks and configuration JSONs were stored in a Git repository (Azure Repos).
2. **Build Pipeline (CI):** Triggered on pull request to `main`. Ran unit tests using `pytest` and validated notebook syntax.
3. **Release Pipeline (CD):** On merge to `main`, the pipeline used the **Databricks CLI** to import notebooks to the target workspace and update job configurations via REST API.
4. **Environment Separation:** Separate pipelines for Dev → QA → Prod with environment-specific variable groups.

This eliminated 15+ hours of weekly manual deployment effort.

---

### Q21. What is the role of Git branching strategy in your data engineering work?

**Answer:**
I follow a **Gitflow** strategy:
- `main` – Production-ready code only
- `develop` – Integration branch for feature merges
- `feature/<name>` – Individual feature development
- `hotfix/<name>` – Emergency production fixes

Before merging to `develop`, all PRs go through code review and automated tests. This ensures pipeline stability and prevents breaking changes from reaching production.

---

## 7. Data Modeling & Medallion Architecture

---

### Q22. What is Star Schema vs Snowflake Schema?

**Answer:**

| Aspect | Star Schema | Snowflake Schema |
|--------|-------------|-----------------|
| Structure | Fact table surrounded by flat dimension tables | Dimension tables normalized into sub-dimensions |
| Joins | Fewer joins, faster queries | More joins, complex queries |
| Storage | More redundancy | Less redundancy |
| Use Case | OLAP / BI reporting | Complex hierarchical dimensions |

For the Airlines project, I used a Star Schema with a central `flight_facts` table and dimensions for `airports`, `airlines`, `date`, and `weather`.

---

### Q23. What are SCD (Slowly Changing Dimensions) types?

**Answer:**
- **SCD Type 1:** Overwrite old value. No history kept. (e.g., address correction)
- **SCD Type 2:** Add a new row with new value. Keeps full history with `start_date`, `end_date`, `is_current` flags. Most common.
- **SCD Type 3:** Add a new column for the old value. Keeps only one historical version.

I implemented SCD Type 2 in the Silver layer of eCDP using Delta `MERGE` statements.

---

## 8. Snowflake & Data Migration

---

### Q24. How does Snowflake differ from Azure Synapse Analytics?

**Answer:**

| Feature | Snowflake | Azure Synapse Analytics |
|---------|-----------|------------------------|
| Separation of Storage & Compute | Yes (Virtual Warehouses) | Partial (Dedicated SQL Pool) |
| Multi-cloud | Yes (AWS, Azure, GCP) | Azure-only |
| Auto-scaling | Seamless | Requires manual configuration |
| Concurrency | Excellent (multi-cluster) | Limited without additional config |
| Integration | Native Databricks connector | Tight Azure ecosystem integration |

In the FinOps PoC, I chose Snowflake for executive reporting due to its superior concurrency and easy integration with BI tools like Tableau.

---

### Q25. How did you migrate SAP data to Snowflake?

**Answer:**
1. **Extraction:** SAP exported XML files to ADLS Gen2.
2. **Parsing:** Used `spark-xml` library in PySpark to parse complex nested SAP schemas into flat DataFrames.
3. **Transformation:** Applied business rules, data type casting, and key mapping.
4. **Loading:** Used the Snowflake Spark connector to write transformed DataFrames directly into Snowflake tables.
5. **Validation:** Row count reconciliation and checksum validation between source and target.

```python
# Writing to Snowflake from Databricks
df.write \
  .format("snowflake") \
  .options(**snowflake_options) \
  .option("dbtable", "FINOPS.SAP_FINANCIAL") \
  .mode("overwrite") \
  .save()
```

---

## 9. SQL Coding Questions

---

### Q26. Write a query to find the second highest salary.

```sql
SELECT MAX(salary) AS second_highest_salary
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);

-- Alternative using DENSE_RANK
SELECT salary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employees
) ranked
WHERE rnk = 2;
```

---

### Q27. Write a query to find duplicate records in a table.

```sql
SELECT patient_id, visit_date, COUNT(*) AS duplicate_count
FROM clinical_events
GROUP BY patient_id, visit_date
HAVING COUNT(*) > 1;
```

---

### Q28. Write a query to calculate a rolling 7-day average of flight cancellations.

```sql
SELECT
    flight_date,
    cancellations,
    AVG(cancellations) OVER (
        ORDER BY flight_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_avg
FROM flight_stats
ORDER BY flight_date;
```

---

### Q29. Write a query using MERGE (Upsert) in Delta Lake / SQL.

```sql
MERGE INTO silver.patient_records AS target
USING bronze.patient_staging AS source
ON target.patient_id = source.patient_id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (patient_id, name, created_at, updated_at)
    VALUES (source.patient_id, source.name, source.created_at, source.updated_at);
```

---

### Q30. Write a SQL query to find employees whose salary is above the department average.

```sql
SELECT e.employee_id, e.name, e.department_id, e.salary, dept_avg.avg_salary
FROM employees e
JOIN (
    SELECT department_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department_id
) dept_avg ON e.department_id = dept_avg.department_id
WHERE e.salary > dept_avg.avg_salary;
```

---

## 10. PySpark Coding Questions

---

### Q31. Read a JSON file from ADLS Gen2 and write it to a Delta table.

```python
# Mount ADLS (or use service principal / Key Vault)
storage_account = "myadlsgen2"
container = "bronze"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="adls-key")
)

# Read JSON from ADLS
df = spark.read \
    .option("multiLine", True) \
    .json(f"abfss://{container}@{storage_account}.dfs.core.windows.net/clinical/events/")

# Basic transformations
from pyspark.sql.functions import current_timestamp, col, upper

df_clean = df \
    .filter(col("patient_id").isNotNull()) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("status", upper(col("status")))

# Write to Delta table
df_clean.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(f"abfss://silver@{storage_account}.dfs.core.windows.net/patient_events")
```

---

### Q32. Implement SCD Type 2 using Delta MERGE in PySpark.

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

delta_table = DeltaTable.forPath(spark, "/mnt/silver/patient_records")

# Incoming new/updated records
updates_df = spark.table("bronze.patient_staging") \
    .withColumn("is_current", lit(True)) \
    .withColumn("start_date", current_timestamp()) \
    .withColumn("end_date", lit(None).cast("timestamp"))

# Expire old records and insert new ones
delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.patient_id = source.patient_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="target.name != source.name OR target.status != source.status",
    set={
        "is_current": "false",
        "end_date": "current_timestamp()"
    }
).whenNotMatchedInsertAll() \
 .execute()
```

---

### Q33. Write a PySpark job to calculate total revenue per product category, filtered to top 5.

```python
from pyspark.sql.functions import sum as spark_sum, desc, rank
from pyspark.sql.window import Window

df = spark.table("sales.transactions")

# Total revenue per category
revenue_df = df.groupBy("product_category") \
    .agg(spark_sum("revenue").alias("total_revenue"))

# Top 5 categories
top5_df = revenue_df.orderBy(desc("total_revenue")).limit(5)
top5_df.show()
```

---

### Q34. Write PySpark code to detect schema changes between two DataFrames.

```python
def detect_schema_changes(existing_df, new_df):
    existing_cols = set(existing_df.schema.fieldNames())
    new_cols = set(new_df.schema.fieldNames())

    added = new_cols - existing_cols
    removed = existing_cols - new_cols

    type_changes = {}
    for field in new_df.schema.fields:
        if field.name in existing_cols:
            existing_type = existing_df.schema[field.name].dataType
            if existing_type != field.dataType:
                type_changes[field.name] = {
                    "old": str(existing_type),
                    "new": str(field.dataType)
                }

    return {
        "added_columns": list(added),
        "removed_columns": list(removed),
        "type_changes": type_changes
    }

changes = detect_schema_changes(silver_df, incoming_df)
print(changes)
```

---

### Q35. Implement a metadata-driven pipeline in PySpark.

```python
# Config table stored in Delta
config_df = spark.sql("""
    SELECT source_table, target_table, load_type, watermark_col, last_watermark
    FROM pipeline_control.config
    WHERE is_active = true
""")

configs = config_df.collect()

for row in configs:
    source = row["source_table"]
    target = row["target_table"]
    load_type = row["load_type"]  # FULL or INCREMENTAL
    wm_col = row["watermark_col"]
    last_wm = row["last_watermark"]

    if load_type == "INCREMENTAL":
        df = spark.sql(f"""
            SELECT * FROM {source}
            WHERE {wm_col} > '{last_wm}'
        """)
    else:
        df = spark.table(source)

    df.write.format("delta").mode("append").saveAsTable(target)
    print(f"Loaded {df.count()} rows from {source} to {target}")
```

---

## 11. Python Coding Questions

---

### Q36. Write a Python function to parse a complex nested JSON (SAP-like).

```python
import json

def flatten_json(nested_json, parent_key='', sep='_'):
    """Flatten a nested JSON dictionary."""
    items = {}
    for k, v in nested_json.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep))
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.update(flatten_json(item, f"{new_key}_{i}", sep))
                else:
                    items[f"{new_key}_{i}"] = item
        else:
            items[new_key] = v
    return items

# Example usage
sap_record = {
    "header": {"id": "SAP001", "date": "2024-01-01"},
    "line_items": [
        {"amount": 1000, "currency": "USD"},
        {"amount": 2000, "currency": "EUR"}
    ]
}

flat = flatten_json(sap_record)
print(flat)
# Output: {'header_id': 'SAP001', 'header_date': '2024-01-01', 
#          'line_items_0_amount': 1000, ...}
```

---

### Q37. Write a Python function to retry a failed API call with exponential backoff.

```python
import time
import requests
from functools import wraps

def retry_with_backoff(retries=3, backoff_factor=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            wait = 1
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries:
                        raise
                    print(f"Attempt {attempt} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                    wait *= backoff_factor
        return wrapper
    return decorator

@retry_with_backoff(retries=3, backoff_factor=2)
def fetch_data_from_api(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()
```

---

### Q38. Write a Python class for a configuration-driven pipeline.

```python
from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class PipelineConfig:
    source_table: str
    target_table: str
    load_type: str  # FULL or INCREMENTAL
    watermark_col: Optional[str] = None
    last_watermark: Optional[datetime] = None
    is_active: bool = True

class MetadataDrivenPipeline:
    def __init__(self, configs: list[PipelineConfig]):
        self.configs = configs
        self.run_log = []

    def run(self):
        for config in self.configs:
            if not config.is_active:
                continue
            try:
                rows_loaded = self._execute(config)
                self.run_log.append({
                    "table": config.target_table,
                    "status": "SUCCESS",
                    "rows": rows_loaded
                })
            except Exception as e:
                self.run_log.append({
                    "table": config.target_table,
                    "status": "FAILED",
                    "error": str(e)
                })

    def _execute(self, config: PipelineConfig) -> int:
        # Simulate pipeline execution
        print(f"Running {config.load_type} load: {config.source_table} -> {config.target_table}")
        return 1000  # Simulated row count

    def summary(self):
        for log in self.run_log:
            print(log)
```

---

## 12. Scenario-Based Questions

---

### Q39. Your Databricks pipeline is running 3x slower than expected. How do you diagnose and fix it?

**Answer — Step-by-step approach:**

1. **Check Spark UI:** Look at the stages and tasks. Identify which stage is the bottleneck.
2. **Look for shuffle read/write:** High shuffle = wide transformation issue (groupBy, join).
3. **Check for data skew:** One task taking 10x longer than others = key skew. Fix with salting or AQE.
4. **Check partition count:** Too few partitions (under-parallelism) or too many (task overhead). Target 100–200 MB per partition. Use `repartition()` or `coalesce()`.
5. **Check file sizes:** Too many small files in Delta = "small file problem". Run `OPTIMIZE` to compact them.
6. **Enable AQE:** `spark.conf.set("spark.sql.adaptive.enabled", "true")` for automatic skew handling and partition coalescing.
7. **Use caching:** If a DataFrame is reused multiple times, cache it with `.cache()` or `.persist()`.
8. **Review joins:** Replace shuffle joins with broadcast joins for small lookup tables.

---

### Q40. How would you design an end-to-end data pipeline for a new client at PwC?

**Answer:**

1. **Discovery:** Understand source systems (Oracle, Salesforce, SAP), data volumes, SLAs, and business requirements.
2. **Architecture Design:** Design a Medallion Lakehouse on Azure. Define Bronze/Silver/Gold layers. Document in an Architecture Decision Record (ADR).
3. **Ingestion Layer:** Use ADF for batch ingestion. Databricks Autoloader for streaming/file-based ingestion.
4. **Transformation:** PySpark notebooks in Databricks for Silver (cleansing, SCD) and Gold (aggregations, KPIs).
5. **Orchestration:** ADF pipelines with dependency management and error alerting.
6. **Data Quality:** Great Expectations or Delta constraints for schema and value validation.
7. **CI/CD:** Azure DevOps pipelines for automated notebook deployment across Dev/QA/Prod.
8. **Monitoring:** Azure Monitor + Log Analytics for pipeline health dashboards.
9. **Documentation:** Architecture diagrams (draw.io), data dictionaries, and user manuals.

---

### Q41. A downstream report is showing incorrect data after a recent pipeline run. How do you do a Root Cause Analysis (RCA)?

**Answer:**

1. **Identify the scope:** Which tables, reports, and date ranges are affected?
2. **Use Delta Time Travel:** Compare current data with the last known-good version.
   ```sql
   SELECT * FROM gold.report_table VERSION AS OF 10
   EXCEPT
   SELECT * FROM gold.report_table VERSION AS OF 11;
   ```
3. **Check pipeline logs:** ADF run history and Databricks cluster logs for errors or warnings.
4. **Trace the data lineage:** Trace from Gold → Silver → Bronze to find where incorrect values were introduced.
5. **Check schema changes:** Were any new columns added or types changed upstream?
6. **Fix and re-run:** Restore the affected table to a good version using `RESTORE TABLE` and re-run the affected pipeline stages.
7. **Document the RCA:** Create a formal incident report with timeline, root cause, fix, and preventive measures.

---

*Prepared specifically for Mayuresh Patil | PwC Data Engineering Associate Interview*
*Generated: April 2026*
