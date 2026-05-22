# 🎯 30 Resume-Based Interview Questions & Answers
> **Tailored for: Mayuresh Patil | Data Engineer | Cognizant → AbbVie**
> **Based on your actual resume bullets — answer in your own words**

---

## 🟢 LEVEL 1 — Introduction & Background

---

**Q1. Tell me about yourself.**

*"I'm Mayuresh Patil, a Data Engineer at Cognizant working on the AbbVie pharmaceutical project since July 2023. My core work involves building PySpark pipelines on Apache Iceberg, migrating 20TB+ of legacy data to AWS S3, and ingesting 10M+ records daily from Oracle, Snowflake, Salesforce, and Smartsheet APIs. I've reduced pipeline execution time by 30% through Spark optimization. I hold Databricks, Azure, and AWS certifications."*

---

**Q2. Walk me through your current project.**

*"I work on AbbVie's enterprise data platform — a pharma/healthcare data lakehouse. My responsibilities include:*
- *Migrating 20TB+ legacy data to Apache Iceberg on AWS S3*
- *Building ingestion pipelines from Oracle JDBC, Snowflake, Salesforce, Smartsheet APIs*
- *Implementing Medallion Architecture (Raw → Trusted → Curated)*
- *Building Golden Record identity resolution using XREFs*
- *Optimizing Spark jobs — achieved 30% reduction in execution time"*

---

**Q3. What is your tech stack?**

| Layer | Tools |
|---|---|
| Processing | PySpark, Apache Spark |
| Storage | Apache Iceberg, AWS S3, ADLS Gen2 |
| Orchestration | Azure Data Factory (ADF), Azure Databricks |
| Databases | Oracle (JDBC), Snowflake, Hive |
| CI/CD | Azure DevOps, Git |
| Other | Delta Lake, Unity Catalog, DLT, Nabu |

---

**Q4. What is Medallion Architecture? How did you use it?**

Medallion = 3-layer data architecture:

```
RAW (Bronze)     → Raw ingestion, no transformation, full history
TRUSTED (Silver) → Cleaned, validated, deduplicated
CURATED (Gold)   → Business-ready aggregations for analytics/reporting
```

*"At AbbVie, I refactored legacy SQL scripts into modular PySpark pipelines following this pattern. Raw layer stored everything from S3 as-is. Trusted layer applied DQ checks and deduplication. Curated layer had business-aggregated views for MDM and analytics teams."*

---

**Q5. What is Apache Iceberg? Why did you choose it over Delta Lake?**

Iceberg = open-source table format with ACID transactions, time travel, schema evolution.

**Why Iceberg over Delta for AbbVie:**
- Engine-agnostic — works with Spark, Trino, Presto, Flink (not locked to Databricks)
- **Hidden partitioning** — no need to add physical partition columns to queries
- **Partition evolution** — change partition strategy without rewriting 20TB data
- **Audit trails** — time-travel queries for pharmaceutical compliance

---

## 🟡 LEVEL 2 — Project Deep Dive

---

**Q6. You migrated 20TB+ of data to Iceberg. What were the challenges?**

*"Three main challenges:*

*1. **Data quality across 18+ years of data** — inconsistent formats, nulls, duplicate records. We built validation layers before loading into Iceberg.*

*2. **Partition design at scale** — wrong partitioning on 20TB means billions of tiny files or massive scan costs. We used `days(event_date)` hidden partitioning.*

*3. **Zero-downtime migration** — the legacy system was live. We ran parallel loads, validated record counts at each stage, then cut over after sign-off."*

---

**Q7. What is a Golden Record? How did you build the identity resolution system?**

**Golden Record** = a single, trusted, unified view of a customer/entity built by merging data from multiple source systems.

*"At AbbVie, patient/HCP records came from multiple sources — Oracle, Salesforce, Veeva. Each had different IDs for the same person. I built an XREF (cross-reference mapping) system:*

```python
# XREF table: maps source system IDs to master ID
# source_system | source_id | master_id
# ORACLE        | ORC_001   | MASTER_001
# SALESFORCE    | SF_A12    | MASTER_001
# VEEVA         | V_X99     | MASTER_001

# Join all source records to XREF → unify on master_id
df_golden = df_oracle.join(df_xref, "source_id") \
    .union(df_salesforce.join(df_xref, "source_id")) \
    .groupBy("master_id") \
    .agg(...)  # Apply survivorship rules
```

*This became the single source of truth for downstream MDM workflows."*

---

**Q8. You process 10M+ records daily. How do you ensure 99.9% data uptime?**

*"Three practices:*

*1. **Idempotent pipelines** — every run can be re-executed safely without duplicating data (using MERGE INTO Iceberg)*

*2. **Monitoring & alerting** — used Modak Nabu for tracking/logging. Every pipeline writes audit records: rows_read, rows_written, errors, run_timestamp*

*3. **Error handling & retry logic** — API calls have exponential backoff + retry. Failed partitions are logged and reprocessed in the next run without full reload"*

---

**Q9. How did you achieve 30% reduction in pipeline execution time?**

*"It wasn't one fix — it was a combination:*

- **Broadcast joins** — replaced Sort-Merge Joins with broadcast for small reference tables (XREFs, lookup tables < 100MB)
- **Partition tuning** — aligned `spark.sql.shuffle.partitions` with actual data volume instead of leaving default 200
- **Predicate pushdown** — ensured Iceberg partition pruning was triggering by matching filter columns to partition columns
- **Caching** — cached base DataFrames reused in 3+ downstream operations
- **Removed redundant shuffles** — found joins that shuffled then immediately shuffled again; restructured the pipeline"*

---

**Q10. What is XREF (Cross-Reference Mapping)? Why is it important in MDM?**

XREF = a mapping table that links IDs from different source systems to a single master ID.

```
Source System | Source ID  | Master ID
Oracle        | ORC-001    | MDM-PATIENT-001
Salesforce    | SF-A123    | MDM-PATIENT-001
Snowflake     | SNF-XY99   | MDM-PATIENT-001
```

**Why important in MDM (Master Data Management):**
- Without XREF, the same patient appears as 3 different records
- With XREF, all source systems point to one "golden" record
- Enables downstream analytics to query unified data without knowing source system IDs

---

**Q11. How did you handle full load vs incremental load in your pipelines?**

```python
def ingest(mode="incremental"):
    if mode == "full":
        # Read all, overwrite target
        df = spark.read.jdbc(oracle_url, table, properties)
        df.write.format("iceberg").mode("overwrite").save(target_path)
    
    elif mode == "incremental":
        # Read only new/changed records since last run
        last_run = get_last_run_timestamp()  # From audit table
        df = spark.read.jdbc(
            oracle_url, table, properties,
            predicates=[f"updated_at > '{last_run}'"]
        )
        # Upsert to Iceberg
        iceberg_table.merge(df, "target.id = source.id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
```

---

**Q12. How did you ingest XML data from S3 into Iceberg?**

```python
# Read XML using spark-xml library
df_xml = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "Record") \
    .option("inferSchema", "true") \
    .load("s3://bucket/raw/xml/")

# Flatten nested XML structure
df_flat = df_xml \
    .withColumn("patient_id",   F.col("Patient._id")) \
    .withColumn("patient_name", F.col("Patient.Name")) \
    .withColumn("load_date",    F.current_date())

# Write to Iceberg
df_flat.write.format("iceberg") \
    .mode("append") \
    .partitionBy("load_date") \
    .save("s3://bucket/iceberg/patients/")
```

---

**Q13. How did you build the outbound sync from Data Lakehouse to Oracle and CSV?**

```python
# Outbound: Curated Iceberg → Oracle
df_curated = spark.read.format("iceberg").load("s3://iceberg/curated/records/")

# Write to Oracle via JDBC
df_curated.write \
    .format("jdbc") \
    .option("url", oracle_jdbc_url) \
    .option("dbtable", "target_schema.target_table") \
    .option("user", db_user) \
    .option("password", db_password) \
    .mode("overwrite") \
    .save()

# Write to CSV flat file
df_curated.coalesce(1) \
    .write.option("header", "true") \
    .mode("overwrite") \
    .csv("s3://bucket/outbound/daily_export/")
```

---

## 🟠 LEVEL 3 — Technical Deep Dive

---

**Q14. What is Spark partitioning? How did you optimize it at AbbVie?**

Partitioning = dividing data into chunks processed in parallel.

**Two types:**
- **Input partitioning** — how data is read (Iceberg/Parquet partitions on disk)
- **Shuffle partitioning** — how data is repartitioned during joins/groupBy

**What I optimized:**

```python
# Bad: default 200 partitions for a 50MB dataset → 199 empty tasks
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default

# Good: match partitions to actual data volume
spark.conf.set("spark.sql.shuffle.partitions", "20")   # For small/medium jobs

# Enable AQE to auto-coalesce empty partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

**Q15. What is ACID in Apache Iceberg? How does it help pharmaceutical data?**

| ACID | Iceberg Implementation |
|---|---|
| Atomicity | Entire commit succeeds or fails — no partial writes |
| Consistency | Schema enforcement on every write |
| Isolation | Snapshot isolation — readers see consistent view even during writes |
| Durability | Committed snapshots are permanent in metadata |

**Pharma relevance:** Audit trails require that historical data is never silently modified. With Iceberg's ACID + time travel, regulators can query any historical snapshot:

```sql
-- Query data as it was 6 months ago (regulatory audit)
SELECT * FROM patients TIMESTAMP AS OF '2024-01-01';
SELECT * FROM patients VERSION AS OF 42;
```

---

**Q16. What is Unity Catalog? Have you used it?**

Unity Catalog = Databricks' centralized governance layer for data, models, and notebooks.

```
Unity Catalog Hierarchy:
  Metastore → Catalog → Schema → Table/View/Volume
  
  Example:
  abbvie_prod → clinical_data → patients → patient_records
```

**Features:**
- Fine-grained access control (row/column level security)
- Data lineage tracking (which notebook wrote which table)
- Centralized audit logs
- Works across Databricks workspaces

*"I used Unity Catalog at AbbVie to manage access to PHI (Protected Health Information) data — ensuring only authorized teams could query patient records."*

---

**Q17. What is Delta Live Tables (DLT)?**

DLT = Databricks framework for building reliable, maintainable ETL pipelines declaratively.

```python
import dlt
from pyspark.sql import functions as F

# Bronze — raw ingestion
@dlt.table(name="raw_patients")
def raw_patients():
    return spark.read.csv("s3://raw/patients/")

# Silver — cleaned
@dlt.table(name="clean_patients")
@dlt.expect("valid_id", "patient_id IS NOT NULL")  # Built-in DQ
def clean_patients():
    return dlt.read("raw_patients") \
              .filter(F.col("patient_id").isNotNull())

# Gold — aggregated
@dlt.table(name="patient_summary")
def patient_summary():
    return dlt.read("clean_patients") \
              .groupBy("region") \
              .agg(F.count("*").alias("patient_count"))
```

**Benefits:** Auto-dependency management, built-in data quality expectations, auto-retry on failure.

---

**Q18. What is Azure Data Factory? How do you use triggers?**

ADF = Azure's cloud ETL orchestration service.

**Trigger types I've used:**

| Trigger | When it fires | My use case |
|---|---|---|
| Schedule | Fixed cron time | Daily Oracle JDBC ingestion at 2 AM |
| Event-based | File arrives in ADLS/S3 | CSV/XML from S3 triggers pipeline |
| Tumbling Window | Fixed non-overlapping windows | Hourly API ingestion with backfill |
| Manual | On demand | Re-runs after failure |

**Linked Services** = connection definitions (Oracle JDBC URL, S3 credentials, Databricks token).

---

**Q19. How do you use Azure Key Vault for secrets management?**

Never store passwords in code. Use Key Vault + Databricks secret scope:

```python
# In Databricks — access Key Vault via secret scope
jdbc_password = dbutils.secrets.get(scope="abbvie-keyvault", key="oracle-db-password")
api_token     = dbutils.secrets.get(scope="abbvie-keyvault", key="salesforce-api-token")

# Use in JDBC connection
df = spark.read.format("jdbc") \
    .option("url", f"jdbc:oracle:thin:@{host}:{port}/{sid}") \
    .option("user", "db_user") \
    .option("password", jdbc_password) \  # ← from Key Vault
    .load()
```

---

**Q20. How did you build CI/CD with Azure DevOps for Databricks?**

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include: [main, release/*]

stages:
- stage: Deploy
  jobs:
  - job: DeployToDatabricks
    steps:
    - task: UsePythonVersion@0
      inputs:
        versionSpec: '3.9'
    
    - script: pip install databricks-cli
      displayName: 'Install Databricks CLI'
    
    - script: |
        databricks configure --token <<EOF
        $(DATABRICKS_HOST)
        $(DATABRICKS_TOKEN)
        EOF
        databricks workspace import_dir ./notebooks /Shared/prod --overwrite
      displayName: 'Deploy Notebooks'
      env:
        DATABRICKS_HOST: $(DATABRICKS_HOST)
        DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
```

Secrets stored in Azure DevOps Variable Groups (linked to Key Vault).

---

**Q21. How do you handle schema evolution in your Iceberg pipelines?**

```python
# New columns appear in API response — don't break the pipeline

# Option 1: mergeSchema for Iceberg
df_new.write.format("iceberg") \
    .option("mergeSchema", "true") \  # Auto-add new columns
    .mode("append") \
    .save(target_path)

# Option 2: Explicit ALTER TABLE (more controlled)
spark.sql("ALTER TABLE patients ADD COLUMN email STRING")

# Option 3: Detect and log schema drift
expected_cols = set(["id", "name", "dob", "facility_id"])
actual_cols   = set(df.columns)
new_cols      = actual_cols - expected_cols
if new_cols:
    log_alert(f"New columns detected: {new_cols}")
    # Decide: add to schema or reject
```

---

**Q22. What is Modak Nabu? How did you use it for data auditing?**

Modak Nabu = data pipeline observability and audit logging framework.

```python
# Pattern: write audit record after every pipeline run
audit_record = {
    "pipeline_name":  "oracle_to_iceberg_patients",
    "run_id":         run_id,
    "run_timestamp":  datetime.now().isoformat(),
    "source":         "ORACLE",
    "target":         "ICEBERG:patients",
    "rows_read":      df_source.count(),
    "rows_written":   df_target.count(),
    "status":         "SUCCESS",
    "error_message":  None
}
nabu_client.log(audit_record)
```

This supported:
- **Traceability** — every record can be traced to its source pipeline and run
- **Regulatory compliance** — pharma requires full audit trails
- **Incident response** — quickly identify which run introduced bad data

---

## 🔴 LEVEL 4 — Scenario / Behavioral

---

**Q23. ADF pipeline failed in production. How do you debug it?**

*"My step-by-step approach:*

1. **ADF Monitor UI** → find the failed pipeline run → check error message
2. **Activity-level error** → which activity failed? Copy? Databricks Notebook? Web?
3. **Databricks job logs** → if notebook failed, open cluster logs → check stderr/stdout
4. **Common root causes I've seen:**
   - Oracle JDBC timeout → increase `queryTimeout` parameter
   - S3 file not arrived yet → fix trigger (event-based instead of schedule)
   - Schema mismatch → source added new column → fix with `mergeSchema=true`
   - Memory error on Spark → increase executor memory in cluster config
5. **After fix** → rerun from failed activity (ADF supports partial rerun)"*

---

**Q24. How would you migrate data from S3 → Databricks → Delta Lake?**

```python
# Step 1: Read from S3
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3://source-bucket/data/")

# Step 2: Transform in Databricks (Medallion)
df_clean = df_raw \
    .filter(F.col("id").isNotNull()) \
    .withColumn("load_date", F.current_date()) \
    .dropDuplicates(["id"])

# Step 3: Write to Delta Lake
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .partitionBy("load_date") \
    .save("abfss://container@storageaccount.dfs.core.windows.net/delta/clean_data/")

# Step 4: Register in Unity Catalog
spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog.schema.clean_data
    USING DELTA
    LOCATION 'abfss://container@storageaccount.dfs.core.windows.net/delta/clean_data/'
""")
```

---

**Q25. How do you apply Data Quality checks in Bronze → Silver layer?**

```python
from pyspark.sql import functions as F

def silver_dq_checks(df, table_name):
    total = df.count()
    issues = []

    # 1. Null checks on critical columns
    for col in ["patient_id", "facility_id", "record_date"]:
        nulls = df.filter(F.col(col).isNull()).count()
        if nulls > 0:
            issues.append(f"NULL in {col}: {nulls} rows")

    # 2. Duplicate check
    dupes = total - df.dropDuplicates(["patient_id"]).count()
    if dupes > 0:
        issues.append(f"Duplicates: {dupes} rows")

    # 3. Business rule — no future dates
    future = df.filter(F.col("record_date") > F.current_date()).count()
    if future > 0:
        issues.append(f"Future dates: {future} rows")

    # 4. Referential check
    orphans = df.join(df_facilities, "facility_id", "left_anti").count()
    if orphans > 0:
        issues.append(f"Orphan facility_ids: {orphans} rows")

    if issues:
        print(f"⚠️  DQ ISSUES in {table_name}:")
        for i in issues: print(f"   - {i}")
        raise Exception("DQ check failed — blocking Silver write")
    else:
        print(f"✅ DQ PASSED: {table_name}")
```

---

**Q26. How did you handle 18+ years of historical pharma data?**

*"Three key practices:*

1. **Snapshot management** — Iceberg time travel preserved every historical state. We could query 'patient records as of 2010' for regulatory audits without keeping separate archive tables.

2. **Partitioning strategy** — partitioned by `year` + `month` at the top level, `record_type` within. Queries for recent data only scan recent partitions — don't touch 18 years of history.

3. **Incremental processing** — never reload 18 years on every run. Watermark-based incremental loads process only changed records. Full reload was only done once during the 20TB migration."*

---

**Q27. What is the biggest challenge you faced and how did you solve it?**

*"The 20TB migration was the hardest. The legacy system was live — we couldn't afford downtime.*

*My approach:*
- *Phase 1: Mirror — run new Iceberg pipeline in parallel with legacy for 2 weeks*
- *Phase 2: Validate — compare row counts, checksums, and sample records between legacy and Iceberg at each layer*
- *Phase 3: Cutover — once validation passed, redirect downstream consumers to Iceberg*
- *Phase 4: Decommission — kept legacy live for 30 days as fallback, then shut down*

*Biggest technical issue: partition column cardinality. Initial design partitioned by `patient_id` — created millions of tiny files. Fixed by switching to `year(event_date)` + Z-ordering on `patient_id`."*

---

**Q28. Write a PySpark job to ingest data from Oracle to Iceberg.**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("OracleToIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://bucket/iceberg/") \
    .getOrCreate()

# Read from Oracle JDBC
jdbc_url  = "jdbc:oracle:thin:@host:1521/SID"
jdbc_user = dbutils.secrets.get("keyvault-scope", "oracle-user")
jdbc_pass = dbutils.secrets.get("keyvault-scope", "oracle-password")

df_oracle = spark.read \
    .format("jdbc") \
    .option("url",      jdbc_url) \
    .option("dbtable",  "SCHEMA.PATIENT_RECORDS") \
    .option("user",     jdbc_user) \
    .option("password", jdbc_pass) \
    .option("numPartitions", "20") \
    .option("partitionColumn", "PATIENT_ID") \
    .option("lowerBound", "1") \
    .option("upperBound", "10000000") \
    .load()

# Transform
df_clean = df_oracle \
    .withColumn("load_date", F.current_date()) \
    .withColumn("source_system", F.lit("ORACLE")) \
    .dropDuplicates(["PATIENT_ID"])

# Write to Iceberg (upsert)
df_clean.createOrReplaceTempView("source_data")
spark.sql("""
    MERGE INTO iceberg.pharma.patient_records AS target
    USING source_data AS source
    ON target.PATIENT_ID = source.PATIENT_ID
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ Loaded {df_clean.count()} records to Iceberg")
```

---

**Q29. How do you handle duplicate records in CDC ingestion?**

```python
from pyspark.sql.window import Window

# CDC stream may deliver same event twice
# Strategy: keep latest record per key using window function

window = Window.partitionBy("record_id").orderBy(F.desc("updated_at"))

df_deduped = df_cdc \
    .withColumn("row_num", F.row_number().over(window)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

# Then MERGE into Iceberg target
spark.sql("""
    MERGE INTO iceberg.schema.target AS t
    USING deduped_cdc AS s
    ON t.record_id = s.record_id
    WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

---

**Q30. Why should they hire you? (Closing question)**

*"I bring three things most candidates at my level don't combine:*

*1. **Production scale experience** — 20TB migration, 10M+ daily records, 18+ years of historical pharma data. I've dealt with real-world complexity — data quality issues, schema drift, API failures, zero-downtime cutover.*

*2. **Full-stack data engineering** — I can handle the entire pipeline: source ingestion (Oracle, Salesforce, APIs), transformation (PySpark, Medallion), storage (Iceberg, Delta), orchestration (ADF, Databricks), and governance (Unity Catalog, Nabu audit logging).*

*3. **Ownership mindset** — I built the Python SQL generation scripts that saved 15+ hours/week. I led production deployments. I'm not someone who waits for a task — I proactively find and fix bottlenecks.*

*The 30% pipeline reduction I achieved wasn't assigned to me — I identified the problem from Spark UI, analysed the query plans, and drove the optimization end-to-end."*

---

## 📋 Quick Answer Reference

```
┌─────────────────────────────────────────────────────────────────┐
│            RESUME BULLET → INTERVIEW ANSWER MAP                 │
├────────────────────────────┬────────────────────────────────────┤
│ 20TB Iceberg migration     │ Q6 + Q27 (challenges + approach)  │
│ Golden Record / XREF       │ Q7 (MDM + identity resolution)    │
│ 10M+ records / 99.9% SLA  │ Q8 (idempotent + monitoring)      │
│ 30% performance reduction  │ Q9 (broadcast + partition tuning) │
│ Oracle JDBC ingestion      │ Q11 + Q28 (full code)             │
│ XML from S3                │ Q12 (spark-xml library)           │
│ Medallion Architecture     │ Q4 (Raw→Trusted→Curated)          │
│ Azure ADF / Databricks     │ Q18 + Q20 (triggers + CI/CD)      │
│ Key Vault                  │ Q19 (secrets management)          │
│ Unity Catalog              │ Q16 (governance + access control) │
│ DLT                        │ Q17 (declarative pipelines)       │
│ Modak Nabu                 │ Q22 (audit logging pattern)       │
│ 15hrs/week SQL automation  │ Q2 + Q27 (Python dynamic SQL)     │
│ Schema evolution           │ Q21 (mergeSchema + ALTER TABLE)   │
│ CDC deduplication          │ Q29 (window + MERGE pattern)      │
└────────────────────────────┴────────────────────────────────────┘
```

---

*Resume-Based Interview Prep | Mayuresh Patil | May 2026*
