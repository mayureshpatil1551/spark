# Interview Questions & Answers — Devesh Premani
### Azure Data Engineer | Databricks & Azure Specialist

---

## SECTION 1: AZURE DATABRICKS & SPARK

---

### Q1. What is the Medallion Architecture and how did you implement it?

**Answer:**
Medallion Architecture is a layered data design pattern that organizes data into three progressive quality tiers:

- **Bronze (Raw Layer):** Ingests raw, unprocessed data as-is from source systems (Oracle, Salesforce, SAP). Data is stored in Delta format with minimal transformation. Schema is preserved exactly as received.
- **Silver (Cleansed Layer):** Applies business rules, deduplication, schema validation, and joins. Delta Schema Evolution is used here to handle upstream schema drifts seamlessly.
- **Gold (Aggregated Layer):** Delivers business-ready, aggregated datasets optimized for reporting (Power BI, Spark SQL). Z-Ordering and liquid clustering are applied here for query performance.

**In eCDP Project:** Migrated 20+ TB of clinical data to ADLS Gen2 using this architecture. Used ADF pipelines for Bronze ingestion, PySpark notebooks for Silver transformations, and optimized Gold tables for 300+ downstream reports — achieving 40% latency reduction.

---

### Q2. Explain Z-Ordering and Liquid Clustering in Delta Lake. When would you use each?

**Answer:**

**Z-Ordering:**
- A multi-dimensional clustering technique that co-locates related data in the same set of files.
- Used for columns frequently used in `WHERE` or `JOIN` filters.
- Applied using: `OPTIMIZE table_name ZORDER BY (column1, column2)`
- Limitation: Must be re-run periodically; does not auto-update on new data.

**Liquid Clustering (introduced in Databricks Runtime 13.x):**
- Next-gen replacement for Z-Ordering and static partitioning.
- Incrementally clusters data on write — no need to run `OPTIMIZE` manually.
- More flexible: clustering keys can be changed without full table rewrites.
- Applied using: `CLUSTER BY (column)` at table creation.

**When to use:**
| Scenario | Use |
|---|---|
| Static tables, infrequent updates | Z-Ordering |
| Frequently updated/streamed tables | Liquid Clustering |
| Need to change cluster keys often | Liquid Clustering |
| Legacy Databricks runtimes | Z-Ordering |

**In my project:** Used Z-Ordering on Gold layer clinical reports (filtered by `patient_id`, `study_date`) and Liquid Clustering on actively ingested Silver tables.

---

### Q3. How does Databricks Autoloader work? Why is it preferred over traditional file ingestion?

**Answer:**
Autoloader (`cloudFiles` format) is a structured streaming source in Databricks that incrementally ingests new files as they arrive in cloud storage (ADLS Gen2, S3).

**How it works:**
1. Uses **file notification mode** (Azure Event Grid + Queue) or **directory listing mode** to detect new files.
2. Tracks processed files in a checkpoint location — ensures exactly-once semantics.
3. Automatically infers schema and handles schema evolution.

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/mnt/schema/") \
    .load("/mnt/bronze/incoming/")

df.writeStream.format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/bronze/") \
    .trigger(availableNow=True) \
    .start("/mnt/bronze/delta_table/")
```

**Advantages over traditional ingestion:**
- No need to track "last processed" timestamps manually.
- Built-in schema inference and evolution.
- Handles millions of files efficiently without listing entire directories.
- Replay capability via checkpointing.

**In eCDP:** Used Autoloader for Oracle and Salesforce data loads, reducing ingestion time by 50%.

---

### Q4. What is Delta Schema Evolution and how do you handle schema drift?

**Answer:**
Delta Lake supports **schema enforcement** (default) and **schema evolution** (opt-in).

**Schema Enforcement:** Rejects writes where incoming schema doesn't match table schema.

**Schema Evolution:** Automatically merges new columns into the table schema.

```python
# Enable schema evolution on write
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/mnt/silver/clinical_table/")
```

**Types of schema changes handled:**
- New columns added → merged automatically
- Data type widening (int → long) → handled
- Data type narrowing or column deletion → NOT automatically handled (requires explicit DDL)

**In eCDP:** Clinical trial systems frequently added new fields. By enabling `mergeSchema`, Silver layer tables auto-adapted without breaking downstream Gold pipelines, maintaining synchronization across layers.

---

### Q5. How do you optimize Spark performance in large-scale PySpark jobs?

**Answer:**

**1. Partitioning:**
```python
df = df.repartition(200, "partition_col")  # For shuffles
df = df.coalesce(10)  # Reduce partitions without shuffle
```

**2. Broadcast Joins (for small dimension tables):**
```python
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

**3. Caching frequently used DataFrames:**
```python
df.cache()  # or df.persist(StorageLevel.MEMORY_AND_DISK)
```

**4. Avoid UDFs (use native Spark functions):**
```python
# Bad
from pyspark.sql.functions import udf
clean_udf = udf(lambda x: x.strip())

# Good
from pyspark.sql.functions import trim
df = df.withColumn("col", trim(df["col"]))
```

**5. Predicate Pushdown & Column Pruning:**
```python
df = spark.read.format("delta").load(path) \
    .filter("date >= '2024-01-01'") \
    .select("id", "name", "value")
```

**6. File Compaction (OPTIMIZE):**
```sql
OPTIMIZE delta.`/mnt/silver/table` WHERE date >= '2024-01-01'
```

**7. AQE (Adaptive Query Execution):** Enabled by default in Spark 3.x — dynamically optimizes join strategies and partition sizes at runtime.

---

## SECTION 2: DELTA LAKE & ACID TRANSACTIONS

---

### Q6. How does Delta Lake ensure ACID compliance?

**Answer:**
Delta Lake achieves ACID properties through its **transaction log** (`_delta_log`):

- **Atomicity:** Every write creates a new JSON commit file. If a write fails mid-way, the incomplete transaction is never committed — the log entry is simply absent.
- **Consistency:** Schema enforcement ensures all writes conform to the defined schema.
- **Isolation:** Optimistic concurrency control — each transaction reads a snapshot and validates no conflicting writes occurred before committing.
- **Durability:** Once committed to the transaction log (backed by cloud storage), data is permanent.

**Transaction Log Example:**
```
_delta_log/
  00000000000000000000.json  ← Initial table creation
  00000000000000000001.json  ← First write
  00000000000000000002.json  ← Second write (merge/update)
  00000000000000000010.checkpoint.parquet ← Checkpoint every 10 commits
```

**In eCDP:** 20+ TB clinical data migration maintained ACID compliance — critical for regulatory compliance (FDA, GCP standards).

---

### Q7. What is Delta Lake Time Travel and when is it useful?

**Answer:**
Time Travel allows querying historical versions of a Delta table using version numbers or timestamps.

```python
# By version
df = spark.read.format("delta").option("versionAsOf", 5).load("/mnt/silver/table")

# By timestamp
df = spark.read.format("delta") \
    .option("timestampAsOf", "2024-06-01") \
    .load("/mnt/silver/table")

# SQL syntax
SELECT * FROM clinical_table VERSION AS OF 10;
SELECT * FROM clinical_table TIMESTAMP AS OF '2024-06-01';
```

**Use cases:**
- Audit and compliance queries (regulatory requirements in clinical data)
- Rollback after accidental deletes/corrupted writes
- Reproduce ML training datasets as of a specific point in time
- Debugging pipeline failures

**Retention:** Controlled by `delta.logRetentionDuration` (default: 30 days).

---

## SECTION 3: AZURE DATA FACTORY & PIPELINES

---

### Q8. How did you implement configuration-driven/metadata-driven pipelines in ADF?

**Answer:**
A metadata-driven pipeline uses a configuration table (stored in Azure SQL DB or ADLS) to dynamically control pipeline behavior — eliminating hardcoded source/target paths.

**Architecture:**
1. **Config Table** (Azure SQL DB):
```
| pipeline_id | source_type | source_path        | target_path         | active |
|-------------|-------------|--------------------|---------------------|--------|
| 1           | Oracle      | /raw/oracle/table1 | /bronze/clinical/t1 | true   |
| 2           | Salesforce  | /raw/sfdc/account  | /bronze/crm/acct    | true   |
```

2. **ADF Master Pipeline:** Uses `Lookup Activity` to fetch config → `ForEach Activity` to iterate → `Execute Pipeline Activity` to trigger child pipelines with dynamic parameters.

3. **Child Pipeline:** Uses `@pipeline().parameters.source_path` expressions throughout.

**Benefits:**
- Add new data sources by inserting a row — no code changes.
- Central control of pipeline execution.
- Easy enable/disable of specific feeds.

**In eCDP:** This pattern accelerated Oracle and Salesforce loads by 50% by parallelizing ingestion across feeds.

---

### Q9. How do you handle errors and retries in ADF pipelines?

**Answer:**

**1. Activity-Level Retry:**
- Set `Retry` count and `Retry interval` on each activity in ADF GUI.

**2. Failure Path (On Failure dependency):**
```
Copy Activity → [On Failure] → Log Error to SQL Table Activity
             → [On Success]  → Trigger Databricks Notebook
```

**3. Pipeline-level error logging in Databricks:**
```python
try:
    # Transformation logic
    df.write.format("delta").mode("overwrite").save(target_path)
    log_status("SUCCESS", table_name, record_count)
except Exception as e:
    log_status("FAILED", table_name, str(e))
    raise  # Re-raise to mark pipeline as failed
```

**4. Dead Letter Pattern:**
- Failed records written to a `/dead_letter/` path for investigation without blocking the main pipeline.

**In eCDP:** Standardized error handling via Azure DevOps CI/CD eliminated 15+ hours of weekly manual intervention.

---

## SECTION 4: SNOWFLAKE & MIGRATION

---

### Q10. How did you migrate SAP data to Snowflake using PySpark?

**Answer:**

**Challenge:** SAP exports data in complex hierarchical XML format. Snowflake requires flat, structured tables.

**Solution using Spark-XML:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.16.0") \
    .getOrCreate()

# Read SAP XML
df = spark.read.format("xml") \
    .option("rowTag", "FinancialDocument") \
    .option("attributePrefix", "attr_") \
    .load("/mnt/bronze/sap/financial_data.xml")

# Flatten nested schema
from pyspark.sql.functions import col, explode

flat_df = df.select(
    col("DocumentID"),
    col("Header.CompanyCode").alias("company_code"),
    col("Header.PostingDate").alias("posting_date"),
    explode(col("LineItems.Item")).alias("line_item")
).select(
    "DocumentID", "company_code", "posting_date",
    col("line_item.Amount").alias("amount"),
    col("line_item.GLAccount").alias("gl_account")
)
```

**Writing to Snowflake:**
```python
flat_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "FINANCIAL_DOCUMENTS") \
    .mode("append") \
    .save()
```

**Dual-target strategy:** Same PySpark job wrote to both Delta Lake (for Databricks heavy ETL) and Snowflake (for executive reporting) — demonstrating interoperability.

---

### Q11. What are key differences between Databricks Delta Lake and Snowflake?

**Answer:**

| Feature | Delta Lake (Databricks) | Snowflake |
|---|---|---|
| Compute Model | Open-source, bring your own cluster | Fully managed, auto-scaling warehouses |
| Storage | Open format (Parquet + transaction log) | Proprietary internal storage |
| Data Format | Open (Parquet/Iceberg interoperable) | Closed (micro-partition format) |
| Best For | Heavy ETL, ML, streaming | Reporting, SQL analytics, sharing |
| Cost Model | Pay for compute separately | Credits for compute + storage |
| Streaming | Native (Structured Streaming) | Limited (Snowpipe) |
| Time Travel | 30 days (configurable) | 90 days (Enterprise) |
| Schema Evolution | Flexible (mergeSchema) | More rigid |

---

## SECTION 5: CI/CD & DEVOPS

---

### Q12. How did you implement CI/CD for Databricks using Azure DevOps?

**Answer:**

**Pipeline Structure:**

```yaml
# azure-pipelines.yml
stages:
- stage: Build
  jobs:
  - job: Validate
    steps:
    - script: pip install databricks-cli pytest
    - script: python -m pytest tests/ -v  # Unit tests for PySpark logic

- stage: Deploy_Dev
  condition: succeeded()
  jobs:
  - job: DeployNotebooks
    steps:
    - script: |
        databricks workspace import_dir ./notebooks /Shared/eCDP/dev \
          --overwrite --profile dev
    - script: |
        databricks jobs run-now --job-id $(DEV_JOB_ID)

- stage: Deploy_Prod
  condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
  jobs:
  - job: DeployProd
    steps:
    - script: |
        databricks workspace import_dir ./notebooks /Shared/eCDP/prod \
          --overwrite --profile prod
```

**Key Practices:**
- Notebooks stored as `.py` or `.ipynb` files in Git (not in Databricks workspace).
- Environment-specific configs stored as Azure DevOps Variable Groups (secrets via Key Vault).
- Databricks Asset Bundles (DABs) for modern deployments.
- Automated rollback on deployment failure.

**Impact:** Eliminated 15+ hours of weekly manual deployments and reduced release cycle time by 50%.

---

## SECTION 6: CODING QUESTIONS

---

### Coding Q1. Write a PySpark job to deduplicate records keeping the latest entry.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Deduplication").getOrCreate()

df = spark.read.format("delta").load("/mnt/silver/clinical_patients")

# Window function: partition by patient_id, order by updated_at desc
window_spec = Window.partitionBy("patient_id").orderBy(col("updated_at").desc())

deduped_df = df.withColumn("row_num", row_number().over(window_spec)) \
               .filter(col("row_num") == 1) \
               .drop("row_num")

deduped_df.write.format("delta").mode("overwrite").save("/mnt/silver/clinical_patients_clean")
```

---

### Coding Q2. Implement a MERGE (Upsert) operation in Delta Lake using PySpark.

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MergeUpsert").getOrCreate()

# Load target Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/silver/oracle_customers")

# Load source (new/updated records)
source_df = spark.read.format("parquet").load("/mnt/bronze/oracle_customers_updates")

# MERGE operation
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdate(set={
    "customer_name": "source.customer_name",
    "email":         "source.email",
    "updated_at":    "source.updated_at"
}).whenNotMatchedInsert(values={
    "customer_id":   "source.customer_id",
    "customer_name": "source.customer_name",
    "email":         "source.email",
    "created_at":    "source.created_at",
    "updated_at":    "source.updated_at"
}).execute()
```

---

### Coding Q3. Write PySpark code to flatten a nested/complex JSON structure.

```python
from pyspark.sql.functions import col, explode_outer, to_date

raw_df = spark.read.format("json").load("/mnt/bronze/clinical_trials.json")

# Schema (example):
# root
#  |-- trial_id: string
#  |-- site: struct
#  |    |-- site_id: string
#  |    |-- country: string
#  |-- subjects: array
#  |    |-- element: struct
#  |    |    |-- subject_id: string
#  |    |    |-- dob: string

flat_df = raw_df \
    .select(
        col("trial_id"),
        col("site.site_id").alias("site_id"),
        col("site.country").alias("country"),
        explode_outer(col("subjects")).alias("subject")
    ) \
    .select(
        "trial_id", "site_id", "country",
        col("subject.subject_id").alias("subject_id"),
        to_date(col("subject.dob"), "yyyy-MM-dd").alias("dob")
    )

flat_df.show(5)
```

---

### Coding Q4. Write a Python/PySpark function to implement SCD Type 2.

```python
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws
from delta.tables import DeltaTable

def apply_scd_type2(spark, source_df, target_path, key_cols, tracked_cols):
    """
    Apply Slowly Changing Dimension Type 2 logic.
    key_cols: list of business key columns (e.g., ['customer_id'])
    tracked_cols: list of columns to track changes on
    """
    delta_table = DeltaTable.forPath(spark, target_path)

    # Add hash of tracked columns to detect changes
    source_df = source_df.withColumn(
        "record_hash",
        sha2(concat_ws("|", *[col(c) for c in tracked_cols]), 256)
    ).withColumn("is_current", lit(True)) \
     .withColumn("start_date", current_timestamp()) \
     .withColumn("end_date", lit(None).cast("timestamp"))

    # Step 1: Expire old records where key matches and hash changed
    delta_table.alias("target").merge(
        source_df.alias("source"),
        " AND ".join([f"target.{k} = source.{k}" for k in key_cols])
        + " AND target.is_current = true"
        + " AND target.record_hash != source.record_hash"
    ).whenMatchedUpdate(set={
        "is_current": lit(False),
        "end_date":   current_timestamp()
    }).execute()

    # Step 2: Insert new versions (new records + changed records)
    existing = DeltaTable.forPath(spark, target_path).toDF()
    new_records = source_df.join(
        existing.filter(col("is_current") == True),
        key_cols, "left_anti"
    )

    changed = source_df.join(
        existing.filter(col("is_current") == False),
        key_cols, "inner"
    ).select(source_df["*"])

    final_inserts = new_records.union(changed).distinct()
    final_inserts.write.format("delta").mode("append").save(target_path)
```

---

### Coding Q5. Write Spark SQL to find top 3 flight routes by cancellation count.

```sql
-- Based on Airlines Data Modeling project
WITH cancellations AS (
    SELECT
        origin_airport,
        destination_airport,
        cancellation_reason,
        COUNT(*) AS cancel_count
    FROM flight_data
    WHERE is_cancelled = 1
    GROUP BY origin_airport, destination_airport, cancellation_reason
),
ranked AS (
    SELECT *,
           RANK() OVER (ORDER BY cancel_count DESC) AS rnk
    FROM cancellations
)
SELECT origin_airport, destination_airport, cancellation_reason, cancel_count
FROM ranked
WHERE rnk <= 3
ORDER BY cancel_count DESC;
```

---

### Coding Q6. Write a Python function to read config from a JSON file and build an ADF-like dynamic pipeline config.

```python
import json
from dataclasses import dataclass
from typing import List

@dataclass
class PipelineConfig:
    pipeline_id: int
    source_type: str
    source_path: str
    target_path: str
    active: bool
    load_type: str  # "full" or "incremental"
    watermark_column: str = None

def load_pipeline_configs(config_path: str) -> List[PipelineConfig]:
    """Load metadata-driven pipeline configs from JSON."""
    with open(config_path, "r") as f:
        raw = json.load(f)
    
    configs = []
    for item in raw.get("pipelines", []):
        if item.get("active", False):
            configs.append(PipelineConfig(
                pipeline_id=item["pipeline_id"],
                source_type=item["source_type"],
                source_path=item["source_path"],
                target_path=item["target_path"],
                active=item["active"],
                load_type=item.get("load_type", "full"),
                watermark_column=item.get("watermark_column")
            ))
    return configs

def run_ingestion(spark, config: PipelineConfig):
    """Execute ingestion based on config."""
    if config.load_type == "full":
        df = spark.read.format(config.source_type.lower()).load(config.source_path)
        df.write.format("delta").mode("overwrite").save(config.target_path)
    elif config.load_type == "incremental":
        # Get last watermark
        last_wm = get_last_watermark(config.pipeline_id)
        df = spark.read.format(config.source_type.lower()).load(config.source_path) \
                  .filter(f"{config.watermark_column} > '{last_wm}'")
        df.write.format("delta").mode("append").save(config.target_path)
        update_watermark(config.pipeline_id, df.agg({config.watermark_column: "max"}).collect()[0][0])
```

---

## SECTION 7: BEHAVIORAL / SCENARIO QUESTIONS

---

### Q13. Tell me about a time you improved pipeline performance significantly.

**Answer (STAR Method):**
- **Situation:** Gold layer clinical reports (300+ reports) had high query latency — analysts complained about slow dashboards.
- **Task:** Reduce query latency without restructuring the entire pipeline.
- **Action:** Profiled slow queries using Databricks Query History. Identified data skew and lack of file organization on key filter columns (`patient_id`, `study_date`). Implemented Z-Ordering on those columns and set up automated OPTIMIZE jobs via Databricks Workflows running nightly. Also enabled file compaction to reduce small-file problem.
- **Result:** 40% reduction in query latency across all 300+ reports.

---

### Q14. How do you ensure data quality in your pipelines?

**Answer:**
1. **Schema enforcement** via Delta Lake — rejects malformed records.
2. **Great Expectations / Custom PySpark checks** for null counts, uniqueness, referential integrity.
3. **Delta Schema Evolution monitoring** — alert when unexpected schema changes occur.
4. **Data reconciliation** — row count and checksum comparisons between source and target.
5. **Dead-letter tables** — invalid records routed for review without blocking pipeline.
6. **Monitoring dashboards** — ADF Monitor + Databricks Job alerts for SLA breach.

---

## SECTION 8: ARCHITECTURAL / DESIGN QUESTIONS

---

### Q15. How would you design a real-time streaming pipeline on Azure?

**Answer:**

```
IoT / App Events
       ↓
Azure Event Hub (ingestion buffer)
       ↓
Azure Stream Analytics (lightweight filtering/windowing) OR
Databricks Structured Streaming (complex transformations)
       ↓
Delta Lake Bronze (append-only, raw events)
       ↓
Delta Live Tables (DLT) — Silver transformations
       ↓
Gold Delta Table → Power BI DirectQuery / Synapse Analytics
```

**Key Design Decisions:**
- Use **Event Hub Kafka endpoint** so Databricks Structured Streaming can consume events natively.
- Use **Trigger.ProcessingTime("30 seconds")** for near-real-time or **Trigger.AvailableNow()** for micro-batch.
- Store secrets (Event Hub connection strings) in **Azure Key Vault** and reference via Databricks secret scope.
- Use **DLT (Delta Live Tables)** for declarative, auto-managed streaming pipelines with built-in data quality rules.

---

*Document prepared for Devesh Premani | Azure Data Engineer Interview Preparation*
*Skills: Azure Databricks • Delta Lake • PySpark • ADF • Snowflake • Azure DevOps*
