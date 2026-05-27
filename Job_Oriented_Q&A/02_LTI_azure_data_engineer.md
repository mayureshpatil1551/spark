# 🎯 Mixed Interview Q&A — ADF, Databricks, SQL, Optimization
> **Mayuresh Patil | Data Engineer | Cognizant | AbbVie**

---

## Q1. Why Use Databricks Instead of ADF for Raw Ingestion?

**Short answer:**
> ADF is best for **connecting and moving** data between systems. Databricks is best for **transforming** data at scale with Spark. For raw ingestion from APIs and S3, Databricks wins because the data needs real transformation — not just copy-paste.

---

**Detailed answer:**

ADF's Copy Activity is great when the source data can land in the destination **as-is** — no complex logic, just movement. But in real projects, raw ingestion is rarely that simple.

**Why Databricks for raw ingestion in your project:**

| Reason | Detail |
|---|---|
| **Complex API logic** | Veeva, Salesforce, Smartsheet APIs need pagination, OAuth token refresh, rate-limit backoff — Python code handles this better than ADF's REST connector |
| **Multithreading** | 50+ API endpoints called in parallel using `ThreadPoolExecutor` — ADF has no native parallel API call support |
| **XREF resolution** | Every incoming record needs a lookup join against the XREF table — Spark broadcast join does this at scale, ADF Copy Activity cannot |
| **Schema handling** | JSON/XML from APIs has nested structures — PySpark flattens with `explode`, `struct` dot notation. ADF mapping is manual per field |
| **Iceberg writes** | ADF has no native Iceberg sink connector — writing to Iceberg requires Spark or custom code |
| **DQ at ingest** | Null checks, domain validation, quarantine routing all happen in the same Spark job |

**When to use ADF instead:**
```
ADF is the right choice when:
  ✅ Simple file copy: S3 → ADLS (no transformation)
  ✅ Scheduled triggers reacting to file arrival
  ✅ On-premise Oracle → Cloud (Self-Hosted Integration Runtime)
  ✅ Orchestrating the overall pipeline (trigger Databricks notebook)
  ✅ 200+ native connectors without writing code
```

**Combined pattern (most common in production):**
```
ADF Trigger (schedule / file arrival)
       ↓
ADF Copy Activity: Oracle → ADLS Bronze raw files
       ↓
ADF Databricks Notebook Activity: trigger PySpark job
       ↓
Databricks: flatten, DQ, XREF resolve, write Iceberg Trusted
```

---

## Q2. End-to-End Flow of Copy Activity — Step by Step

**What Copy Activity does:**
> Moves data from a **source** to a **sink** — with optional column mapping, format conversion, and compression. It is ADF's core data movement engine.

---

**Step-by-step flow:**

```
STEP 1: Pipeline Triggered
─────────────────────────────────────────────────────
  Trigger fires (Schedule / Tumbling Window / File Arrival)
  ADF Pipeline starts execution
  Parameters injected (e.g., load_date = "2024-03-14")

STEP 2: Integration Runtime Allocated
─────────────────────────────────────────────────────
  Azure IR    → for cloud-to-cloud (S3 → ADLS, REST API → Blob)
  Self-Hosted IR → for on-premise sources (Oracle on-prem → Azure)
  IR is the compute that physically moves the data

STEP 3: Source Linked Service Connected
─────────────────────────────────────────────────────
  ADF reads the source Linked Service config
  Fetches credentials from Azure Key Vault at runtime
  Opens connection to: Oracle / REST API / S3 / Snowflake etc.

STEP 4: Source Dataset Evaluated
─────────────────────────────────────────────────────
  Dataset defines: which table / file / endpoint to read
  Dynamic expressions evaluated:
    e.g., table = "PATIENT_MASTER"
          query = "SELECT * FROM PATIENT_MASTER WHERE UPDATED > '@{activity('Lookup').output.firstRow.watermark}'"

STEP 5: Data Read from Source
─────────────────────────────────────────────────────
  ADF reads in parallel using DIU (Data Integration Units)
  More DIUs = more parallel threads = faster read
  Source data is staged in IR memory temporarily

STEP 6: Column Mapping / Type Conversion (optional)
─────────────────────────────────────────────────────
  Map source column names → sink column names
  Convert data types (Oracle DATE → string, NUMBER → decimal)
  Apply expressions if needed

STEP 7: Sink Linked Service Connected
─────────────────────────────────────────────────────
  Connect to destination: ADLS Gen2 / Delta / Snowflake etc.
  Credentials again from Key Vault

STEP 8: Data Written to Sink
─────────────────────────────────────────────────────
  Write mode: Append / Overwrite / Upsert
  For ADLS: writes as Parquet / CSV / JSON files
  For Delta: can write via Databricks connector
  Partition by: date, region etc. (if configured)

STEP 9: Activity Output Recorded
─────────────────────────────────────────────────────
  Output JSON available to next activity:
  {
    "rowsRead": 500000,
    "rowsWritten": 500000,
    "copyDuration": 120,          ← seconds
    "throughput": 4.17            ← MB/s
  }

STEP 10: Next Activity Triggered
─────────────────────────────────────────────────────
  e.g., Databricks Notebook Activity reads the ADLS files
  e.g., Script Activity updates watermark in control table
  e.g., If Condition checks success/failure → alert or continue
```

---

**Copy Activity config example:**
```json
{
  "name": "Copy_Oracle_to_ADLS",
  "type": "Copy",
  "inputs": [{ "referenceName": "DS_Oracle_PatientMaster" }],
  "outputs": [{ "referenceName": "DS_ADLS_Bronze_Parquet" }],
  "typeProperties": {
    "source": {
      "type": "OracleSource",
      "oracleReaderQuery": "SELECT * FROM PATIENT_MASTER WHERE UPDATED_AT > '@{activity('GetWatermark').output.firstRow.last_ts}'"
    },
    "sink": {
      "type": "ParquetSink",
      "storeSettings": { "type": "AzureBlobFSWriteSettings" }
    },
    "parallelCopies": 4,
    "dataIntegrationUnits": 8
  }
}
```

---

## Q3. Store Data from ADLS Gen2 or Oracle → Delta Table: Step-by-Step

### Path A: ADLS Gen2 (CSV/Parquet) → Delta Table

```
STEP 1: Create Linked Service for ADLS Gen2
  ADF → Manage → Linked Services → New
  Type: Azure Data Lake Storage Gen2
  Auth: Account Key (fetched from Key Vault)
  Name: LS_ADLS_Prod

STEP 2: Create Source Dataset
  Type: DelimitedText (CSV) or Parquet
  Linked Service: LS_ADLS_Prod
  File path: bronze/patient_events/*.csv
  First row as header: true

STEP 3: Create Sink Dataset
  Type: Delta
  Linked Service: LS_ADLS_Prod (or LS_Databricks if using Delta via Databricks)
  File path: silver/patient_events/    ← Delta table location on ADLS

STEP 4: Copy Activity — Source config
  Read all CSV files from Bronze container
  Schema auto-detect or explicit mapping

STEP 5: Copy Activity — Sink config
  Write mode: Append (or Upsert for Delta MERGE)
  Partition by: load_date, region

STEP 6: Post-copy — Trigger Databricks notebook
  Databricks Notebook Activity
  Notebook: /Production/silver/patient_events_transform
  Cluster: existing warm cluster
```

---

### Path B: Oracle → Delta Table

```
STEP 1: Install Self-Hosted Integration Runtime (if Oracle is on-premise)
  Download SHIR on the server that has Oracle network access
  Register with ADF using authentication key
  Verify: ADF → Manage → Integration Runtimes → Status = Running

STEP 2: Create Linked Service for Oracle
  Type: Oracle
  Connection string: Host=oracle.abbvie.com;Port=1521;Service Name=PRODDB
  User: @{linkedService().oracle_user}      ← from Key Vault
  Password: AzureKeyVaultSecret reference

STEP 3: Lookup Activity — Get watermark
  Read last processed timestamp from control table
  Output: { "last_ts": "2024-03-14 02:00:00" }

STEP 4: Copy Activity — Source
  Type: OracleSource
  Query: SELECT * FROM PATIENT_MASTER
         WHERE UPDATED_AT > '@{activity('Lookup').output.firstRow.last_ts}'
  Parallel read: partitionColumn = PATIENT_ID
                 lowerBound = 1, upperBound = 10000000
                 numPartitions = 20    ← 20 parallel JDBC reads

STEP 5: Copy Activity — Sink
  Type: Parquet (land to ADLS Bronze first)
  Path: bronze/oracle/patient_master/load_date=@{formatDateTime(utcnow(),'yyyy-MM-dd')}

STEP 6: Databricks Notebook Activity
  Reads Bronze Parquet → applies transforms → writes Delta Silver
  (or ADF can write directly to Delta if schema is simple)

STEP 7: Script Activity — Update watermark
  UPDATE control_table SET last_ts = pipeline run start time
  Only runs after successful copy ← ensures no gap on retry
```

---

**Full ADF pipeline structure:**
```
Pipeline: Oracle_to_Delta_Incremental
│
├── Activity 1: Lookup_Watermark
│       → SELECT last_ts FROM control.watermarks WHERE source='ORACLE_PM'
│
├── Activity 2: Copy_Oracle_to_Bronze    (depends on Lookup)
│       → Source: Oracle (filtered by watermark)
│       → Sink:   ADLS Bronze Parquet
│
├── Activity 3: Notebook_Bronze_to_Delta (depends on Copy)
│       → Databricks: flatten, DQ, write Delta Silver
│
├── Activity 4: Update_Watermark         (depends on Notebook)
│       → UPDATE control.watermarks SET last_ts = pipeline run time
│
└── Activity 5: If_Condition             (on any failure)
        → True: Web Activity → send Teams/Slack alert
```

---

## Q4. How to Use Azure Key Vault + Read Oracle + Write Delta

### Why Key Vault?
> Never hardcode passwords, API keys, or connection strings in code, notebooks, or ADF configs. Key Vault stores them centrally and serves them at runtime. Credentials never appear in logs, Git, or exports.

---

### Step 1: Store Oracle credentials in Key Vault
```
Azure Portal → Key Vault: abbvie-prod-kv
→ Secrets → + Generate/Import

Secret name:  oracle-username    Value: svc_spark_prod
Secret name:  oracle-password    Value: xK#9mP$2qR...
Secret name:  oracle-jdbc-url    Value: jdbc:oracle:thin:@//oracle.abbvie.com:1521/PRODDB
```

---

### Step 2: Grant access
```
Key Vault → Access Policies → Add:
  Databricks Cluster MSI → GET (read secrets only)
  ADF Managed Identity   → GET (read secrets only)
  DevOps Service Principal → GET + SET (for CI/CD)
  Developers             → LIST only (see names, not values)
```

---

### Step 3: Read Oracle in Databricks using Key Vault
```python
# Databricks Secret Scope backed by Key Vault
# (Set up once by admin: dbutils.secrets.createScope linked to KV)

oracle_url  = dbutils.secrets.get(scope="abbvie-kv", key="oracle-jdbc-url")
oracle_user = dbutils.secrets.get(scope="abbvie-kv", key="oracle-username")
oracle_pass = dbutils.secrets.get(scope="abbvie-kv", key="oracle-password")

# Secrets are REDACTED in all Spark logs ✅
# print(oracle_pass) → [REDACTED]

# Read Oracle table using JDBC
df_oracle = spark.read \
    .format("jdbc") \
    .option("url",             oracle_url) \
    .option("dbtable",         "ABBVIE_PROD.PATIENT_MASTER") \
    .option("user",            oracle_user) \
    .option("password",        oracle_pass) \
    .option("driver",          "oracle.jdbc.driver.OracleDriver") \
    .option("numPartitions",   "20") \
    .option("partitionColumn", "PATIENT_ID") \
    .option("lowerBound",      "1") \
    .option("upperBound",      "10000000") \
    .option("fetchsize",       "5000") \
    .load()

print(f"Records read from Oracle: {df_oracle.count()}")
df_oracle.printSchema()
```

---

### Step 4: Transform and Write to Delta
```python
from pyspark.sql.functions import (
    col, current_timestamp, lit, upper, trim, sha2, concat_ws
)

# Transform
df_clean = df_oracle \
    .filter(col("PATIENT_ID").isNotNull()) \
    .withColumn("status",  upper(trim(col("STATUS")))) \
    .withColumn("region",  upper(trim(col("REGION")))) \
    .withColumn("record_hash",
        sha2(concat_ws("||", col("PATIENT_ID"), col("TERRITORY_ID"), col("STATUS")), 256)
    ) \
    .withColumn("_load_ts", current_timestamp()) \
    .withColumnRenamed("PATIENT_ID",   "patient_id") \
    .withColumnRenamed("TERRITORY_ID", "territory_id")

# Write to Delta Silver
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("region") \
    .save("abfss://silver@abbviedatalake.dfs.core.windows.net/patient_master/")
```

---

### Step 5: ADF Linked Service using Key Vault (no-code path)
```json
{
  "name": "LS_Oracle_Prod",
  "type": "OracleLinkedService",
  "properties": {
    "connectionString": "Host=oracle.abbvie.com;Port=1521;Service Name=PRODDB;",
    "password": {
      "type": "AzureKeyVaultSecret",
      "store": { "referenceName": "LS_AbbVie_KeyVault" },
      "secretName": "oracle-password"
    }
  }
}
// ADF fetches secret at runtime using its Managed Identity ✅
// Password NEVER stored in ADF JSON or exported config
```

---

## Q5. Optimization Techniques Used

**Four main levers — always lead with these:**

### 1. Partitioning Strategy
```python
# Problem: full 20 TB table scan on every daily incremental run
# Fix: partition by ingestion date + source system
df.writeTo("iceberg.trusted.patient_events") \
  .partitionedBy("days(event_ts)", "source_system") \
  .append()
# Daily run now touches only today's partition — fraction of the data
```

### 2. Broadcast Join (eliminate shuffle)
```python
from pyspark.sql.functions import broadcast

# Problem: XREF table (small) joining patient events (huge) → full shuffle
# Fix: broadcast the small table → join happens locally on each executor
result = large_patient_df.join(
    broadcast(xref_df),       # xref_df: few thousand rows
    on="source_id",
    how="left"
)
# Before: SortMergeJoin + network shuffle ❌
# After:  BroadcastHashJoin — no shuffle  ✅

# Verify in plan:
result.explain()
# Look for: BroadcastHashJoin ✅   not: SortMergeJoin ❌
```

### 3. Adaptive Query Execution (AQE)
```python
# Handles 3 problems automatically at runtime:
spark.conf.set("spark.sql.adaptive.enabled",                    "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",           "true")   # splits hot partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")   # merges empty partitions

# Skew: some territories had 10x more records → one task ran 10x longer
# AQE splits that oversized partition into smaller sub-tasks automatically
```

### 4. Replace Python UDFs with Built-in Functions
```python
# Problem: Python UDF = row-by-row, JVM → Python serialization per row
# Fix: use Spark built-in functions — stay in JVM, Catalyst-optimized

# Before (slow):
# @udf(StringType())
# def mask_id(x): return x[:-4] + "****"

# After (fast):
from pyspark.sql.functions import concat, substring, lit, length
df.withColumn("masked_id",
    concat(
        substring(col("patient_id"), 1, length(col("patient_id")) - 4),
        lit("****")
    )
)
```

### 5. Tune Shuffle Partitions
```python
# Default 200 is wrong for most datasets
# Too high: 200 empty tasks for 1 GB data = overhead
# Too low: 10 tasks for 50 GB data = huge tasks, slow

# Rule: target 100-200 MB per partition
# 20 GB data / 128 MB target = ~160 partitions
spark.conf.set("spark.sql.shuffle.partitions", "160")
```

### 6. Caching Repeated DataFrames
```python
# Problem: XREF lookup joined 4 times in same pipeline = 4 Iceberg reads
# Fix: cache once, reuse

xref_df = spark.read.format("iceberg").load("iceberg.trusted.xref_master")
xref_df.cache()
xref_df.count()   # trigger cache population

# Now all 4 joins read from memory instead of storage ✅
# Unpersist when done
xref_df.unpersist()
```

---

## Q6. Incremental Load: ADLS Gen2 (CSV) → Delta Table — Step by Step

**Full flow using ADF Tumbling Window + Watermark:**

```
ARCHITECTURE:
ADLS Gen2 (CSV files, daily drops)
       ↓
ADF Tumbling Window Trigger (daily)
       ↓
ADF Pipeline:
  ├── Lookup: read watermark from control table
  ├── Copy: read only new CSV files (filtered by date)
  ├── Databricks Notebook: Bronze CSV → Silver Delta (MERGE INTO)
  └── Script: update watermark
```

---

**Step 1: Control table (tracks what was last processed)**
```sql
-- Azure SQL or Delta table
CREATE TABLE control.incremental_watermarks (
    pipeline_name   VARCHAR(100),
    last_processed  DATETIME,
    last_run_status VARCHAR(20)
);

INSERT INTO control.incremental_watermarks VALUES
('adls_csv_to_delta', '2024-01-01 00:00:00', 'SUCCESS');
```

---

**Step 2: ADF — Tumbling Window Trigger (daily)**
```json
{
  "name": "Daily_CSV_Ingest_Trigger",
  "type": "TumblingWindowTrigger",
  "recurrence": { "frequency": "Day", "interval": 1 },
  "startTime": "2024-01-01T00:00:00Z"
}
// Each day = independent window
// Missed day auto-retried
// @trigger().outputs.windowStartTime → "2024-03-14T00:00:00Z"
// @trigger().outputs.windowEndTime   → "2024-03-15T00:00:00Z"
```

---

**Step 3: ADF Pipeline activities**
```
Activity 1: Lookup_Watermark
  Type: Lookup
  Query: SELECT last_processed FROM control.incremental_watermarks
         WHERE pipeline_name = 'adls_csv_to_delta'
  Output: { "last_processed": "2024-03-13 00:00:00" }

Activity 2: Copy_New_CSV_Files
  Type: Copy
  Source: ADLS Gen2 CSV
    File filter: modifiedDatetimeStart = @{activity('Lookup_Watermark').output.firstRow.last_processed}
                 modifiedDatetimeEnd   = @{trigger().outputs.windowEndTime}
    ← only picks up files modified AFTER last watermark
  Sink: ADLS Gen2 Bronze (Parquet, partitioned by load_date)

Activity 3: Databricks_MERGE_to_Delta
  Type: DatabricksNotebook
  Parameters:
    window_start: @{trigger().outputs.windowStartTime}
    window_end:   @{trigger().outputs.windowEndTime}

Activity 4: Update_Watermark
  Type: Script
  Only runs if Activity 3 SUCCEEDED
  Query: UPDATE control.incremental_watermarks
         SET last_processed = '@{trigger().outputs.windowEndTime}'
         WHERE pipeline_name = 'adls_csv_to_delta'
```

---

**Step 4: Databricks notebook — MERGE INTO Delta**
```python
window_start = dbutils.widgets.get("window_start")
window_end   = dbutils.widgets.get("window_end")

# Read only this window's files from Bronze
df_new = spark.read.parquet(
    f"abfss://bronze@abbviedatalake.dfs.core.windows.net/patient_events/"
    f"load_date={window_start[:10]}/"
)

# Transform
from pyspark.sql.functions import col, current_timestamp, upper, trim, sha2, concat_ws

df_clean = df_new \
    .filter(col("patient_id").isNotNull()) \
    .withColumn("status", upper(trim(col("status")))) \
    .withColumn("record_hash",
        sha2(concat_ws("||", col("patient_id"), col("territory_id")), 256)
    )

# MERGE INTO Delta — upsert (new → insert, changed → update)
df_clean.createOrReplaceTempView("incoming")

spark.sql("""
    MERGE INTO delta.`abfss://silver@abbviedatalake.dfs.core.windows.net/patient_events/`
    AS target
    USING incoming AS source
    ON target.patient_id = source.patient_id

    WHEN MATCHED AND target.record_hash != source.record_hash
        THEN UPDATE SET *

    WHEN NOT MATCHED
        THEN INSERT *
""")

print(f"✅ Incremental load complete: {window_start} → {window_end}")
dbutils.notebook.exit("SUCCESS")
```

---

**Which ADF activities are used — summary:**

| Activity | Purpose |
|---|---|
| `Lookup` | Read watermark from control table |
| `Copy` | Move filtered CSV files → Bronze Parquet |
| `DatabricksNotebook` | Transform + MERGE INTO Delta Silver |
| `Script` | Update watermark after success |
| `If Condition` | On failure → alert via WebActivity |
| `Tumbling Window Trigger` | Track each daily window independently |

---

## Q7. MERGE Statement in SQL and PySpark — Which Layer, How to Implement

**One-liner:**
> MERGE is used at the **Silver layer** — to upsert incoming records into the cleaned table: new records INSERT, changed records UPDATE, deleted records soft-DELETE.

---

### SQL MERGE (Standard / Hive / Spark SQL)
```sql
-- Pattern: MERGE incoming into target
-- New record → INSERT
-- Changed record → UPDATE
-- Record removed from source → soft DELETE

MERGE INTO silver.patient_master AS target
USING (
    -- Deduplicate incoming batch FIRST (keep latest per patient)
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY patient_id
                   ORDER BY source_ts DESC
               ) AS rn
        FROM silver_incoming_staging
    )
    WHERE rn = 1
) AS source
ON target.patient_id = source.patient_id

-- Case 1: exists in both, data changed → UPDATE
WHEN MATCHED AND target.record_hash != source.record_hash THEN
    UPDATE SET
        territory_id  = source.territory_id,
        status        = source.status,
        updated_at    = source.source_ts,
        record_hash   = source.record_hash,
        is_deleted    = false

-- Case 2: new record → INSERT
WHEN NOT MATCHED THEN
    INSERT (patient_id, territory_id, status, created_at,
            updated_at, record_hash, is_deleted)
    VALUES (source.patient_id, source.territory_id, source.status,
            source.source_ts, source.source_ts, source.record_hash, false)

-- Case 3: in target, not in source anymore → soft delete
WHEN NOT MATCHED BY SOURCE AND target.is_deleted = false THEN
    UPDATE SET is_deleted = true, updated_at = current_timestamp();
```

---

### PySpark MERGE (Delta Lake)
```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, sha2, concat_ws, col

# Target Delta table
target = DeltaTable.forPath(
    spark,
    "abfss://silver@abbviedatalake.dfs.core.windows.net/patient_master/"
)

# Incoming source data
source_df = spark.read.format("delta") \
    .load("abfss://bronze@.../patient_master_incoming/") \
    .withColumn("record_hash",
        sha2(concat_ws("||", col("territory_id"), col("status")), 256)
    )

# MERGE
(target.alias("tgt")
 .merge(
     source=source_df.alias("src"),
     condition="tgt.patient_id = src.patient_id"
 )
 .whenMatchedUpdate(
     condition="tgt.record_hash != src.record_hash",
     set={
         "territory_id": "src.territory_id",
         "status":       "src.status",
         "updated_at":   "src.source_ts",
         "record_hash":  "src.record_hash",
         "is_deleted":   lit(False)
     }
 )
 .whenNotMatchedInsert(
     values={
         "patient_id":   "src.patient_id",
         "territory_id": "src.territory_id",
         "status":       "src.status",
         "created_at":   "src.source_ts",
         "updated_at":   "src.source_ts",
         "record_hash":  "src.record_hash",
         "is_deleted":   lit(False)
     }
 )
 .whenNotMatchedBySourceUpdate(
     condition="tgt.is_deleted = false",
     set={"is_deleted": lit(True), "updated_at": current_timestamp()}
 )
 .execute()
)
```

---

**Which layer uses MERGE and why:**

| Layer | Write Mode | Why |
|---|---|---|
| **Raw / Bronze** | Append only | Never overwrite raw — source fidelity |
| **Trusted / Silver** | **MERGE (upsert)** | Handle inserts + updates + deletes from source |
| **Curated / Gold** | Overwrite partition | Full recompute from Silver — aggregates |

---

## Q8. Data Skew — Identify, Resolve

### What is Data Skew?
> When data is unevenly distributed across partitions — some tasks get 10x more data than others. Those heavy tasks become stragglers that delay the entire stage.

---

### How to Identify
```python
# Method 1: Spark UI
# Jobs → Stages → Look at Task Duration
# If one task = 10 min, rest = 1 min → skew on that partition

# Method 2: Check partition sizes in code
df.groupBy(spark_partition_id()).count().orderBy("count", ascending=False).show(20)
# If top partition has 5M rows, others have 50K → severe skew

# Method 3: Check join key distribution
df.groupBy("territory_id").count().orderBy("count", ascending=False).show(10)
# If US territory has 50M rows, others have 5K → join skew
```

---

### Fix 1: Broadcast Join (best for small table)
```python
from pyspark.sql.functions import broadcast

# When one side of the join is small (< 50 MB)
# Broadcast it to all executors — no shuffle for the large table

result = large_patient_df.join(
    broadcast(territory_lookup_df),   # small table → broadcast
    on="territory_id",
    how="left"
)
# No shuffle on large_patient_df ✅
# Verify: result.explain() → look for BroadcastHashJoin
```

---

### Fix 2: AQE — Adaptive Query Execution (automatic)
```python
# Spark 3.x — detects and fixes skew at runtime

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# How it works:
# AQE measures actual partition sizes after shuffle
# If a partition is > skewedPartitionFactor × median → it's skewed
# AQE splits that partition into smaller sub-tasks automatically

# Tune thresholds:
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# partition 5x bigger than median = skewed

spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")
# AND > 256 MB absolute size
```

---

### Fix 3: Salting (manual, for large-large join skew)
```python
# When BOTH tables are large — can't broadcast
# Some keys have millions of rows → skew on those keys

# Step 1: Add a random salt to the large table
import random
from pyspark.sql.functions import rand, floor, concat, lit, col, explode, array

SALT = 10   # number of buckets

patient_salted = patient_df \
    .withColumn("salt", (floor(rand() * SALT)).cast("string")) \
    .withColumn("salted_key", concat(col("territory_id"), lit("_"), col("salt")))

# Step 2: Replicate the other table across all salt values
territory_salted = territory_df \
    .withColumn("salt_array", array([lit(str(i)) for i in range(SALT)])) \
    .withColumn("salt", explode(col("salt_array"))) \
    .withColumn("salted_key", concat(col("territory_id"), lit("_"), col("salt")))

# Step 3: Join on salted key
result = patient_salted.join(
    territory_salted,
    on="salted_key",
    how="left"
).drop("salt", "salted_key", "salt_array")

# Why it works:
# Hot key "US" with 50M rows → split into 10 sub-keys: US_0, US_1...US_9
# Each sub-key has 5M rows → balanced partitions
```

---

**When to use which fix:**

| Scenario | Fix |
|---|---|
| One table is small (< 50 MB) | Broadcast Join |
| Spark 3.x, want automatic fix | AQE skewJoin |
| Both tables large, one key dominates | Salting |

---

## Q9. DENSE_RANK() vs RANK() vs ROW_NUMBER()

**One-liner:**
> All three assign a number to each row ordered within a window. They differ in how they handle **ties** (duplicate values).

---

**Example data:**
```
patient_id | visits
P001       | 10
P002       | 10     ← tie with P001
P003       |  8
P004       |  5
```

**Results side by side:**
```sql
SELECT
    patient_id,
    visits,
    RANK()       OVER (ORDER BY visits DESC) AS rnk,
    DENSE_RANK() OVER (ORDER BY visits DESC) AS dense_rnk,
    ROW_NUMBER() OVER (ORDER BY visits DESC) AS row_num
FROM patient_summary;
```

| patient_id | visits | RANK | DENSE_RANK | ROW_NUMBER |
|---|---|---|---|---|
| P001 | 10 | 1 | 1 | 1 |
| P002 | 10 | 1 | 1 | 2 |
| P003 | 8 | **3** | **2** | 3 |
| P004 | 5 | 4 | 3 | 4 |

---

**Key difference:**

| Function | Ties get same rank? | Skips ranks after tie? | Always unique? |
|---|---|---|---|
| `RANK()` | ✅ Yes (both = 1) | ✅ Yes (skips 2, next = 3) | ❌ No |
| `DENSE_RANK()` | ✅ Yes (both = 1) | ❌ No (next = 2, no gap) | ❌ No |
| `ROW_NUMBER()` | ❌ No (arbitrary 1,2) | ❌ No gaps | ✅ Always unique |

---

**When to use each:**
```
RANK()        → Leaderboard where ties deserve same rank but next is skipped
               "Both P001 and P002 are #1 — next is #3"

DENSE_RANK()  → Compact ranking without gaps
               "Both P001 and P002 are #1 — next is #2"

ROW_NUMBER()  → Deduplication — need exactly one row per group
               "Keep one row per patient_id — doesn't matter which"
```

---

**Deduplication using ROW_NUMBER (most common use in data engineering):**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

w = Window.partitionBy("patient_id").orderBy(col("updated_at").desc())

df_deduped = df \
    .withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .drop("rn")
# Keeps latest record per patient_id ✅
```

---

## Q10. SQL Scenarios: INNER, LEFT, RIGHT JOIN

**Setup:**
```sql
CREATE TABLE orders (
    customer_id INT,
    order_amount DECIMAL(10,2)
);

CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(50)
);

INSERT INTO orders VALUES (1, 500), (2, 300), (4, 200);
-- customer_id 3 has no order
-- order with customer_id 4 has no matching customer

INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
```

---

### INNER JOIN — only matching rows from both
```sql
SELECT c.customer_id, c.customer_name, o.order_amount
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id;
```
| customer_id | customer_name | order_amount |
|---|---|---|
| 1 | Alice | 500 |
| 2 | Bob | 300 |

> Carol (no order) dropped. order with customer_id=4 (no customer) dropped.

---

### LEFT JOIN — all from left (customers), matching from right
```sql
SELECT c.customer_id, c.customer_name, o.order_amount
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;
```
| customer_id | customer_name | order_amount |
|---|---|---|
| 1 | Alice | 500 |
| 2 | Bob | 300 |
| 3 | Carol | **NULL** |

> Carol stays (left table = customers). Order with id=4 dropped (no customer).

---

### RIGHT JOIN — all from right (orders), matching from left
```sql
SELECT c.customer_id, c.customer_name, o.order_amount
FROM customers c
RIGHT JOIN orders o ON c.customer_id = o.customer_id;
```
| customer_id | customer_name | order_amount |
|---|---|---|
| 1 | Alice | 500 |
| 2 | Bob | 300 |
| **NULL** | **NULL** | 200 |

> All orders stay (right table). Order with id=4 stays with NULL customer. Carol dropped.

---

## Q11. Output for INNER, LEFT, RIGHT JOIN on Given Tables

**Input:**
```
Table A     Table B
  1           1
  2           1
  NULL        3
  4           NULL
```

---

### INNER JOIN — only rows where BOTH sides match (NULLs never match)
```sql
SELECT a.val AS a_val, b.val AS b_val
FROM table_a a
INNER JOIN table_b b ON a.val = b.val;
```
| a_val | b_val |
|---|---|
| 1 | 1 |
| 1 | 1 |

> Why?
> - `1` in A matches `1` and `1` in B → 2 rows
> - `2` in A → no match in B (B has 1,1,3,NULL) → dropped
> - `NULL` in A → NULL never equals anything → dropped
> - `4` in A → no match in B → dropped
> - `3` in B → no match in A → dropped
> - `NULL` in B → never matches → dropped

---

### LEFT JOIN — all rows from A, matching from B (NULLs in B for no match)
```sql
SELECT a.val AS a_val, b.val AS b_val
FROM table_a a
LEFT JOIN table_b b ON a.val = b.val;
```
| a_val | b_val |
|---|---|
| 1 | 1 |
| 1 | 1 |
| 2 | NULL |
| NULL | NULL |
| 4 | NULL |

> Why?
> - `1` matches twice → 2 rows
> - `2` no match in B → stays with NULL
> - `NULL` in A → no match (NULL ≠ anything) → stays with NULL
> - `4` no match in B → stays with NULL
> - All 4 rows from A kept, `NULL` in B where no match

---

### RIGHT JOIN — all rows from B, matching from A (NULLs in A for no match)
```sql
SELECT a.val AS a_val, b.val AS b_val
FROM table_a a
RIGHT JOIN table_b b ON a.val = b.val;
```
| a_val | b_val |
|---|---|
| 1 | 1 |
| 1 | 1 |
| NULL | 3 |
| NULL | NULL |

> Why?
> - `1` in B matches `1` in A → 2 rows (B has two 1s)
> - `3` in B → no match in A → stays with NULL in a_val
> - `NULL` in B → never matches → stays with NULL in a_val
> - `2` in A → dropped (right join keeps B, not A unmatched)
> - `4` in A → dropped
> - All 4 rows from B kept, `NULL` in A where no match

---

**Summary table:**

| Join Type | Keeps unmatched A rows? | Keeps unmatched B rows? | NULL handling |
|---|---|---|---|
| INNER | ❌ | ❌ | NULL never matches anything |
| LEFT | ✅ (with NULL for B) | ❌ | NULL in A → stays, no match |
| RIGHT | ❌ | ✅ (with NULL for A) | NULL in B → stays, no match |

> **Key rule:** `NULL = NULL` is **always FALSE** in SQL join conditions. NULL never matches NULL.

---

## 📊 Quick Cheat Sheet

```
Copy Activity flow:    Trigger → IR allocated → Source connected (KV creds)
                       → Data read → Column mapping → Sink written → Output JSON

Incremental pattern:   Lookup watermark → Copy filtered → Notebook MERGE → Update watermark

MERGE layers:          Bronze = APPEND only | Silver = MERGE (upsert) | Gold = OVERWRITE

Skew fixes:            Small table → Broadcast | Auto fix → AQE | Large-large → Salt

Ranking:               RANK = gaps after tie | DENSE_RANK = no gaps | ROW_NUMBER = always unique

JOIN NULLs:            NULL never matches NULL in ON condition
```

---
*Mayuresh Patil | Data Engineer | Cognizant*
