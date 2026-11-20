# ADF Triggers — 25 Q&A + CGI 5 Questions
**Real Project-Based Answers | Interview Script Format**
**Stack: Azure Data Factory | Databricks | ADLS Gen2 | Delta Lake**

---

# PART A — CGI INTERVIEW: 5 QUESTIONS

---

## CGI Q1. How do you optimize a PySpark job that is taking too long?

### Interview Script (say exactly this):
> "In my eCDP project, we had a Silver MERGE job on the PATIENTS table that was taking 45 minutes. I diagnosed it step by step — first checked the Spark UI, saw massive shuffle spill, then fixed it in three steps: added Z-ORDER on the merge key, enabled Low Shuffle Merge, and tuned executor cores to 5. Job came down to 8 minutes."

### Full Answer:

**Step 1 — Diagnose before fixing (most candidates skip this)**

```
Spark UI → Jobs → Find slow stage
Check:
  Shuffle Read/Write size    → if huge → shuffle is the problem
  GC Time > 10% of task time → memory too small
  Task skew (one task 10× longer than others) → data skew problem
  Spill to disk              → executor memory too small
```

**Step 2 — Fix data skew**

```python
# Skew: 1 partition has 80% of data (e.g., region='APAC' has 90% of rows)

# Option A: Salting (add random prefix to distribute skewed key)
from pyspark.sql.functions import concat, lit, floor, rand, col

df_salted = df.withColumn(
    "salted_key",
    concat(col("region"), lit("_"), (floor(rand() * 10)).cast("string"))
)
# Then join on salted_key — distributes one partition into 10

# Option B: AQE (Automatic Query Execution) — Databricks DBR 7.3+
spark.conf.set("spark.sql.adaptive.enabled",                       "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",              "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
# AQE automatically splits skewed partitions at runtime
```

**Step 3 — Fix shuffle partitions**

```python
# Default shuffle.partitions = 200 (too many for small data, too few for large)
# Rule: target 128-256MB per partition after shuffle

# Check shuffle size in Spark UI → Shuffle Write column
# If 10GB shuffle → 10240MB / 200 = 51MB per partition (too small, too many tasks)
# Fix: 10240 / 128 = 80 partitions

spark.conf.set("spark.sql.shuffle.partitions", "80")

# Or use AQE to auto-coalesce:
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "128MB")
```

**Step 4 — Fix MERGE performance on Delta (most critical in prod)**

```python
# Problem: MERGE scans entire Silver table to find matching rows
# Fix 1: Z-ORDER on merge key → data skipping skips 90%+ of files
spark.sql("OPTIMIZE delta.`/mnt/silver/CDB/PATIENTS` ZORDER BY (PATIENT_ID)")

# Fix 2: Low Shuffle Merge — only shuffles affected partitions
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")

# Fix 3: Add partition column to merge condition → partition pruning
merge_condition = """
    target.PATIENT_ID  = source.PATIENT_ID
    AND target.LOAD_MONTH = source.LOAD_MONTH   -- partition pruning
"""
# MERGE now only scans the relevant partition files

# Fix 4: Only update when data actually changed (skip no-op updates)
target.alias("t").merge(source.alias("s"), merge_condition) \
    .whenMatchedUpdate(
        condition="t._row_hash != s._row_hash",   # skip unchanged rows
        set={...}
    ).whenNotMatchedInsertAll().execute()
```

**Step 5 — Executor tuning**

```python
# Formula (always start here):
# executor_cores   = 5          (HDFS throughput sweet spot)
# executors/node   = (node_cores - 1) / 5
# total_executors  = (executors/node × nodes) - 1
# executor_memory  = (node_RAM - 2GB) / executors_per_node

# For Standard_E8ds_v4 (8 cores, 64GB) × 4 nodes:
# executors/node = (8-1)/5 = 1
# total = (1×4) - 1 = 3
# memory = (64-2)/1 = 62GB → cap at 14g for stability

spark.conf.set("spark.executor.cores",          "5")
spark.conf.set("spark.executor.memory",         "14g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
```

**Result in production:**
```
Before optimization: 45 minutes (PATIENTS Silver MERGE, 50M rows)
After optimization:  8 minutes
Improvements:
  Z-ORDER:            file skipping 92% → 40 min → 15 min
  Low Shuffle Merge:  15 min → 10 min
  Executor tuning:    10 min → 8 min
```

---

## CGI Q2. Difference between Delta Lake, Data Lake, and Data Warehouse (with practical use cases)

### Interview Script:
> "I use all three in my project. ADLS Gen2 is our Data Lake — raw Parquet files land here. Delta Lake sits on top of it — same files, but with ACID transactions, MERGE, time travel. Synapse is our Data Warehouse for executive dashboards. The key insight: Delta Lake is not a replacement for a DWH — it's what makes the Data Lake trustworthy enough to serve as a DWH for most use cases."

### Full Answer:

```
Data Lake (ADLS Gen2 / S3):
  What:  Cheap object storage. Stores ANY format (Parquet, CSV, JSON, images).
  When:  Landing zone for raw data. ML training datasets. Archive.
  Pain:  No ACID. No MERGE. Read a file while writing = corrupt read.
         No schema enforcement. Easy to become a "data swamp".
  Cost:  ~$0.02/GB/month

Delta Lake (Delta format ON top of ADLS):
  What:  Open table format (Parquet + _delta_log) adding ACID, MERGE,
         time travel, schema enforcement to your Data Lake files.
  When:  Bronze/Silver/Gold tables. Anywhere you need reliable DML.
  Pain:  Requires Spark/Databricks to read (not raw SQL tools directly).
         Small files problem if not OPTIMIZED regularly.
  Cost:  Same storage as ADLS + compute to run Spark

Data Warehouse (Azure Synapse / Snowflake / Redshift):
  What:  Columnar, MPP (Massively Parallel Processing) database.
         Schema-on-write, SQL-native, optimised for BI queries.
  When:  Executive dashboards. Complex multi-join analytical queries.
         Non-technical users writing SQL. Sub-second query response.
  Pain:  Expensive at scale. Rigid schema. Poor for unstructured data.
  Cost:  ~$2–5/hour for compute
```

**Real project flow (eCDP):**
```
Oracle (source)
   │ ADF Copy (raw Parquet)
   ▼
ADLS Gen2 raw/   ← DATA LAKE (cheap, raw, any format)
   │ Databricks Auto Loader
   ▼
Bronze Delta     ← DELTA LAKE (append-only, ACID, schema tracked)
   │ MERGE
   ▼
Silver Delta     ← DELTA LAKE (clean, deduplicated, SCD2 history)
   │ replaceWhere
   ▼
Gold Delta       ← DELTA LAKE (aggregated, domain marts)
   │ Synapse Link / COPY INTO
   ▼
Azure Synapse    ← DATA WAREHOUSE (BI dashboards, Power BI)
```

**When to use which (interview answer):**

| Scenario | Use |
|---|---|
| Raw file landing zone | Data Lake |
| ML training data | Data Lake |
| ETL pipeline tables (Bronze/Silver/Gold) | Delta Lake |
| Need MERGE / UPSERT | Delta Lake |
| Need time travel / audit | Delta Lake |
| Executive BI dashboards | Data Warehouse |
| Non-engineers writing SQL | Data Warehouse |
| Sub-second query SLA | Data Warehouse |

---

## CGI Q3. Design an end-to-end data pipeline using ADF and Databricks

### Interview Script:
> "In eCDP we built exactly this. ADF is the orchestrator and data mover — it connects to Oracle via Self-Hosted IR, handles watermark-based incremental extraction, and lands Parquet in ADLS Bronze zone. Then ADF triggers Databricks notebooks for Silver and Gold transformations. The key design principle: ADF owns the orchestration, Databricks owns the compute. Never put transformation logic in ADF Dataflows — keep it in Databricks where you can test, version, and optimize it properly."

### Full Answer:

```
ARCHITECTURE:

Oracle (on-prem)
    │ JDBC via Self-Hosted Integration Runtime
    │ Incremental: watermark_column > last_watermark
    ▼
ADF Pipeline: PL_Oracle_to_Bronze
    ├── Lookup: get watermark from control table
    ├── Copy Activity: Oracle → ADLS raw/ (Parquet, snappy)
    └── SP Activity: update watermark after success
    │
    │ ADF triggers Databricks (Execute Notebook Activity)
    ▼
Databricks: 01_bronze_ingestion.py
    ├── Auto Loader (cloudFiles) reads new Parquet from raw/
    ├── Adds audit cols: _ingested_at, _source_file
    └── Appends to Bronze Delta (append-only, never overwrite)
    │
    ▼
Databricks: 02_silver_transform.py
    ├── Incremental read from Bronze (since last Silver run)
    ├── Deduplication: row_number() OVER PARTITION BY key ORDER BY ts DESC
    ├── Quality checks: null PK, future dates → dead-letter
    ├── MERGE into Silver Delta (upsert — handle inserts + updates)
    └── Update Silver watermark
    │
    ▼
Databricks: 03_gold_aggregation.py
    ├── Join Silver tables (patients + encounters + labs)
    ├── Aggregate: daily summaries per patient
    └── replaceWhere: overwrite only today's partition (idempotent)
    │
    ▼
Azure Synapse / Power BI (consumption layer)
```

**Key design decisions I'd explain in interview:**
```
1. Self-Hosted IR for on-prem Oracle — cloud IR can't reach on-prem network
2. Watermark before MERGE — if MERGE fails, watermark stays → safe replay
3. Auto Loader over readStream — tracks files via checkpoint, handles schema drift
4. replaceWhere in Gold — idempotent, re-run same date = same result
5. Dead-letter queue — bad rows quarantined, pipeline continues
6. ADF as orchestrator only — no transformation in ADF (use Databricks for that)
```

---

## CGI Q4. What happens internally when a Spark job is executed?

### Interview Script:
> "When I submit a PySpark job on Databricks, the Driver node parses my code into a DAG of stages. Each stage boundary is a shuffle. Executors run tasks in parallel within each stage. The key thing interviewers want to hear: Spark is LAZY — nothing runs until an action like count() or write(). All transformations before that are just building the DAG. The Catalyst optimizer then rewrites that DAG into the most efficient physical plan."

### Full Answer:

```
Step 1: Driver receives the job
  - Your Python/Scala code runs on the Driver node
  - Driver parses transformations → builds logical plan (DAG)
  - Catalyst optimizer rewrites logical plan → physical plan
  - Physical plan shows: scan, filter, join, shuffle, aggregate steps

Step 2: DAG Scheduler splits into Stages
  - Stage boundary = SHUFFLE (wide transformation)
  - Within a stage: all tasks run on same partition (no data movement)
  - Narrow transformations (filter, map, withColumn) → same stage
  - Wide transformations (groupBy, join, distinct) → new stage

Example DAG for a Silver MERGE:
  Stage 1: Scan Bronze Delta + filter new rows         (narrow)
  Stage 2: row_number() window function                (shuffle → new stage)
  Stage 3: Scan Silver Delta + hash join with source   (shuffle → new stage)
  Stage 4: Write MERGE results to Silver Delta         (narrow)

Step 3: Task Scheduler assigns Tasks to Executors
  - Each partition = 1 task
  - Task runs on an executor (JVM process on worker node)
  - 200 shuffle partitions = 200 tasks after a shuffle stage
  - Executor cores = number of tasks that run in parallel per executor

Step 4: Shuffle (most expensive operation)
  - Wide transformation triggers shuffle
  - Each mapper writes shuffle files to local disk (shuffle write)
  - Reducers fetch relevant partitions from all mappers (shuffle read)
  - Shuffle spill: if shuffle data > executor memory → spills to disk → SLOW

Step 5: Lazy Evaluation
  df.filter(...).groupBy(...).agg(...)   # NO execution yet — just DAG building
  df.filter(...).groupBy(...).agg(...).write.save(...)  # ACTION → triggers execution
  Other actions: count(), collect(), show(), first()
```

**Memory breakdown per executor:**
```
executor.memory = 14g
  ├── Reserved:         300MB (Spark internals)
  ├── User Memory:      3.5g  (your Python objects, UDFs, broadcast vars)
  └── Spark Memory:     10.2g
        ├── Execution:  6.1g  (shuffle, sort, aggregation buffers)
        └── Storage:    4.1g  (cached RDDs/DataFrames, broadcast tables)

executor.memoryOverhead = 2g  (off-heap: Python worker, PySpark overhead)
```

---

## CGI Q5. How do you handle schema changes in production data pipelines?

### Interview Script:
> "In eCDP, Oracle source added a new column CLINICAL_NOTES without telling us. Our pipeline broke because we were writing to a Delta table with strict schema enforcement. After that incident, I implemented three layers: mergeSchema on Bronze to absorb new columns, schema_evolution_mode addNewColumns on Auto Loader, and a schema drift alert that notifies us within 5 minutes of any structural change. For breaking changes — like a column rename or type change — we have a migration runbook."

### Full Answer:

**Three types of schema changes:**
```
1. Additive (non-breaking): new column added to source
2. Type change: VARCHAR(50) → VARCHAR(200) (usually safe) or STRING → INT (breaking)
3. Structural (breaking): column renamed, column removed, table restructured
```

**Layer 1 — Bronze: absorb any change**

```python
# Auto Loader with schema evolution (never break on new columns)
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # absorb new cols
    .option("cloudFiles.schemaLocation", checkpoint_path + "/schema") \
    .load(raw_path) \
    .writeStream \
    .option("mergeSchema", "true")   # Delta also accepts new columns on write
    .start(bronze_path)
```

**Layer 2 — Silver: controlled evolution**

```python
# Silver accepts new columns but blocks type changes
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# In MERGE operation:
target.merge(source, condition) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
# mergeSchema=true → new cols from source automatically added to Silver

# Block breaking changes (type change, column removal):
# Delta's schema enforcement catches this at write time
# → pipeline fails → alert fires → team investigates before data corruption
```

**Layer 3 — Schema drift detection (proactive alerting)**

```python
# Run after every Bronze write — detect drift before Silver breaks
def detect_schema_drift(spark, table_path, table_name, control_db):
    # Get current schema
    current_schema = spark.read.format("delta").load(table_path).schema

    # Get last known schema from control table
    stored_schema_json = spark.sql(f"""
        SELECT schema_json FROM {control_db}.schema_registry
        WHERE table_name = '{table_name}'
        ORDER BY recorded_at DESC LIMIT 1
    """).collect()

    if not stored_schema_json:
        # First run — register schema
        save_schema(current_schema, table_name, control_db)
        return

    from pyspark.sql.types import StructType
    stored_schema = StructType.fromJson(json.loads(stored_schema_json[0][0]))

    current_fields  = {f.name: f.dataType for f in current_schema.fields}
    stored_fields   = {f.name: f.dataType for f in stored_schema.fields}

    new_columns     = set(current_fields) - set(stored_fields)
    removed_columns = set(stored_fields)  - set(current_fields)
    type_changes    = {f: (stored_fields[f], current_fields[f])
                       for f in current_fields
                       if f in stored_fields and current_fields[f] != stored_fields[f]}

    if new_columns or removed_columns or type_changes:
        alert_message = f"""
        SCHEMA DRIFT DETECTED: {table_name}
        New columns:     {new_columns}
        Removed columns: {removed_columns}
        Type changes:    {type_changes}
        """
        # Send Teams/email alert
        send_alert(alert_message)
        # Log to audit table
        log_drift(table_name, new_columns, removed_columns, type_changes, control_db)

        if removed_columns or type_changes:
            raise Exception(f"BREAKING schema change detected: {alert_message}")
        # New columns only → allow pipeline to continue (non-breaking)
```

**Breaking change runbook:**
```
Column renamed (PATIENT_NAME → FULL_NAME):
  Step 1: Add new column FULL_NAME to Silver (keep old PATIENT_NAME)
  Step 2: Update Silver transform: map source FULL_NAME → Silver FULL_NAME
  Step 3: Backfill FULL_NAME from PATIENT_NAME for existing rows
  Step 4: Notify downstream consumers of old column deprecation
  Step 5: Drop PATIENT_NAME after 30-day sunset

Column type change (STRING → INT):
  Step 1: Add new column PATIENT_ID_INT INT
  Step 2: Migrate data: UPDATE Silver SET PATIENT_ID_INT = CAST(PATIENT_ID AS INT)
  Step 3: Validate no nulls / parse errors
  Step 4: Drop old STRING column in next sprint
```

---

---

# PART B — ADF TRIGGERS: 25 QUESTIONS

---

## Q1. What is a Trigger in ADF, and why is it used in real-time projects?

### Interview Script:
> "A trigger is what makes ADF pipelines run automatically without manual intervention. In eCDP, we have three triggers running in production: a Schedule Trigger at 1 AM for Oracle incremental loads, a Storage Event Trigger that fires when Salesforce CSV files land in ADLS, and a Tumbling Window Trigger for hourly aggregations. Without triggers, someone would have to manually click 'Run' every day — not production-grade."

**Definition:** A Trigger defines WHEN an ADF pipeline should execute automatically — based on time, file arrival, or external events.

**Why used in production:**
```
1. Zero manual intervention — pipelines run on schedule automatically
2. SLA enforcement — data guaranteed in Silver by 3 AM for morning reports
3. Event-driven — pipeline starts within seconds of file arrival
4. Backfill — Tumbling Window auto-catches up missed executions
5. Audit trail — every trigger run logged with start time, status, params
```

---

## Q2. What are the different types of triggers in ADF?

| Trigger | Fires When | Key Feature |
|---|---|---|
| **Schedule** | Fixed time/recurrence | Simple, calendar-based |
| **Tumbling Window** | Fixed non-overlapping windows | Auto-backfill, windowStart/End params |
| **Storage Event** | File created/deleted in ADLS | Event-driven, file metadata passed |
| **Custom Event** | Azure Event Grid topic | External system integration |

---

## Q3. Source generates files every day at 6 AM. Which trigger and why?

### Interview Script:
> "Storage Event Trigger, not Schedule. The file sometimes arrives at 6:03, sometimes at 6:15 due to upstream delays. If I use Schedule at 6:05 and file is delayed to 6:15, my pipeline runs on empty. Storage Event fires the moment the file lands — no polling, no wasted runs, handles delays automatically."

```json
{
  "name": "TRG_FileArrival_6AM",
  "type": "BlobEventsTrigger",
  "typeProperties": {
    "blobPathBeginsWith": "/data/landing/oracle/",
    "blobPathEndsWith":   ".parquet",
    "events":             ["Microsoft.Storage.BlobCreated"],
    "ignoreEmptyBlobs":   true
  }
}
// Fires: the moment .parquet file lands in /data/landing/oracle/
// Passes: @triggerBody().fileName, @triggerBody().folderPath to pipeline
```

**Use Schedule Trigger only when:**
- File arrival is 100% guaranteed at exact time
- You must run even if file doesn't arrive (send "no data" alert)
- Multiple upstream files must all arrive first

---

## Q4. Execute ADF pipeline every 30 minutes — how?

```json
{
  "name": "TRG_Every30Min",
  "type": "ScheduleTrigger",
  "typeProperties": {
    "recurrence": {
      "frequency": "Minute",
      "interval":  30,
      "startTime": "2024-01-01T00:00:00",
      "timeZone":  "India Standard Time"
    }
  }
}
// Fires: 00:00, 00:30, 01:00, 01:30 ... every day

// Business hours only (08:00–20:00):
"schedule": {
  "hours":   [8,9,10,11,12,13,14,15,16,17,18,19,20],
  "minutes": [0, 30]
}
```

> **Interview note:** "Schedule Trigger allows concurrent runs. If the pipeline takes 35 min and trigger fires every 30 min, two runs will overlap. For strict no-overlap, I'd use Tumbling Window Trigger with maxConcurrency=1."

---

## Q5. What is a Schedule Trigger? Real-time scenario.

**Definition:** Fires pipeline on calendar-based recurrence (every N minutes/hours/days, or specific days/times). Wall-clock based — fires based on real time.

**Real scenario (eCDP production):**
```
Requirement: Oracle PATIENTS table → Silver must be ready by 3 AM for morning reports

TRG_Daily_Oracle: fires 1:00 AM IST, Mon–Fri
  → ADF copies incremental Oracle rows to ADLS Bronze (1:00–1:30 AM)
  → ADF triggers Databricks: Bronze→Silver MERGE (1:30–2:30 AM)
  → ADF triggers Databricks: Silver→Gold (2:30–2:50 AM)
  → Done by 2:50 AM ✅ — 10 min buffer before 3 AM SLA

Useful pipeline expressions:
  @trigger().scheduledTime  → "2024-10-24T01:00:00"  (when it was supposed to fire)
  @trigger().startTime      → "2024-10-24T01:00:03"  (when it actually fired)
  @formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')  → "2024-10-24" (date only)
```

---

## Q6. What is Tumbling Window Trigger? How is it different from Schedule Trigger?

### Interview Script:
> "Tumbling Window Trigger is for data engineers who need guarantees. Schedule Trigger says 'fire at 1 AM'. Tumbling Window says 'process window 00:00–01:00 and don't start the next window until this one finishes, and if you miss a window, automatically catch up'. In eCDP we used it for hourly transaction aggregations because we couldn't afford to lose a window — the business counts on those hourly numbers."

| Property | Schedule | Tumbling Window |
|---|---|---|
| Missed run | Lost | Auto-backfilled |
| Overlap | Allowed | Blocked (maxConcurrency) |
| windowStart/End params | ❌ | ✅ |
| Multiple pipelines | ✅ | ❌ (1 only) |
| Dependencies | ❌ | ✅ |
| Historical backfill | ❌ | ✅ (set startTime in past) |

```json
{
  "name": "TWT_Hourly_Transactions",
  "type": "TumblingWindowTrigger",
  "typeProperties": {
    "frequency":      "Hour",
    "interval":       1,
    "startTime":      "2024-01-01T00:00:00Z",
    "delay":          "00:10:00",    // 10-min wait after window closes
    "maxConcurrency": 1              // no overlap
  }
}
// windowStart/End FREE parameters inside pipeline:
// @trigger().outputs.windowStartTime → "2024-10-24T02:00:00Z"
// @trigger().outputs.windowEndTime   → "2024-10-24T03:00:00Z"
```

---

## Q7. Business requires processing data continuously in hourly windows. Which trigger?

**Answer: Tumbling Window Trigger**

```
Why TWT over Schedule:
  Problem 1: Overlap — Schedule allows two hourly runs simultaneously
    TWT fix: maxConcurrency = 1 → next window WAITS for previous
  Problem 2: Missed windows — ADF downtime loses 3 hours of data
    TWT fix: auto-backfills in order after recovery
  Problem 3: Precise window queries — "give me exactly 02:00–03:00 data"
    TWT fix: @trigger().outputs.windowStartTime / windowEndTime FREE

Production config (eCDP FinOps hourly processing):
  frequency:      Hour
  interval:       1
  delay:          00:10:00   ← wait 10 min for data to settle
  maxConcurrency: 2          ← allow 2 windows in parallel (faster catch-up)
  retry:          count=3, interval=300s
```

---

## Q8. How to configure dependencies between Tumbling Window Triggers?

**Use case:** Gold TWT must wait for Silver TWT to finish for the SAME window.

```json
// In TWT_Gold — add dependsOn pointing to TWT_Silver:
{
  "name": "TWT_Gold_Summary",
  "type": "TumblingWindowTrigger",
  "typeProperties": {
    "frequency": "Hour",
    "interval":  1,
    "dependsOn": [
      {
        "type": "TumblingWindowTriggerDependencyReference",
        "referenceTrigger": { "referenceName": "TWT_Silver_Patients" },
        "offset": "00:00:00",   // same window
        "size":   "01:00:00"
      }
    ]
  }
}
// Gold window 02:00–03:00 will NOT fire until Silver window 02:00–03:00 SUCCEEDS
// If Silver fails → Gold is blocked → no stale Gold data
```

**Self-dependency (strict sequential windows):**
```json
"dependsOn": [{
  "type":   "SelfDependencyTumblingWindowTriggerReference",
  "offset": "-01:00:00",   // depends on previous window
  "size":   "01:00:00"
}]
// Window 03:00 starts only after window 02:00 completes
```

---

## Q9. What is a Storage Event Trigger in ADF?

**Definition:** Fires pipeline when a file is **created** or **deleted** in Azure Blob Storage or ADLS Gen2, using Azure Event Grid under the hood.

**How it works:**
```
1. ADF registers Event Grid subscription on storage account
2. File created → Event Grid notifies ADF within seconds
3. ADF evaluates path/extension filters
4. If match → pipeline fires with file metadata as parameters
```

**Key outputs available in pipeline:**
```
@triggerBody().fileName      → "patients_20241024.parquet"
@triggerBody().folderPath    → "raw/oracle/CDB/PATIENTS/year=2024/month=10/"
@trigger().startTime         → when pipeline started
```

**Prerequisites:**
- Storage account: ADLS Gen2 or Blob (not Azure Files)
- ADF Managed Identity: Storage Blob Data Contributor on storage
- Event Grid resource provider registered in subscription

---

## Q10. New file arrives in ADLS Gen2 — pipeline should execute automatically. How?

```
Step 1: ADF Studio → Author → Triggers → New → Storage Event Trigger
  Fill:
    Storage account: adlsprodxxx
    Container:       data
    Blob begins with: raw/oracle/CDB/PATIENTS/
    Blob ends with:   .parquet
    Event:           Blob created
    Ignore empty:    Yes

Step 2: Connect trigger to pipeline — pass file info as params:
  file_name:   @triggerBody().fileName
  folder_path: @triggerBody().folderPath

Step 3: Inside pipeline Copy Activity source:
  wildcardFolderPath: @pipeline().parameters.folder_path
  wildcardFileName:   @pipeline().parameters.file_name

Step 4: Publish All → Start trigger → Upload test file → Verify in Monitor
```

```json
// Full trigger JSON:
{
  "name": "TRG_FileArrival_Patients",
  "type": "BlobEventsTrigger",
  "typeProperties": {
    "blobPathBeginsWith": "/data/raw/oracle/CDB/PATIENTS/",
    "blobPathEndsWith":   ".parquet",
    "ignoreEmptyBlobs":   true,
    "events":             ["Microsoft.Storage.BlobCreated"]
  },
  "pipelines": [{
    "pipelineReference": { "referenceName": "PL_Process_Patients" },
    "parameters": {
      "file_name":   "@triggerBody().fileName",
      "folder_path": "@triggerBody().folderPath"
    }
  }]
}
```

---

## Q11. Configure event trigger for a specific folder and file extension.

```json
{
  "typeProperties": {
    "blobPathBeginsWith": "/data/landing/salesforce/",   // specific folder
    "blobPathEndsWith":   ".csv",                        // specific extension
    "ignoreEmptyBlobs":   true,
    "events": ["Microsoft.Storage.BlobCreated"]
  }
}

// Triggers ON:
// ✅ /data/landing/salesforce/contacts.csv
// ✅ /data/landing/salesforce/archive/old.csv  (recursive)
// ❌ /data/landing/oracle/patients.csv         (wrong folder)
// ❌ /data/landing/salesforce/data.json        (wrong extension)
```

> **Interview tip:** "blobPathBeginsWith is case-sensitive and matches the full path including container. Always test with a sample file upload before going to production."

---

## Q12. Pipeline should run only for CSV, not JSON — how?

**Best approach — filter at trigger level:**
```json
{
  "blobPathEndsWith": ".csv"    // JSON files → trigger never fires → no wasted runs
}
```

**Fallback — If Condition inside pipeline (when trigger can't be scoped):**
```json
{
  "name": "If_IsCSV",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@endsWith(pipeline().parameters.file_name, '.csv')",
      "type":  "Expression"
    },
    "ifTrueActivities":  [ /* process CSV */ ],
    "ifFalseActivities": [ /* log skip, exit cleanly */ ]
  }
}
```

---

## Q13. What is a Custom Event Trigger in ADF?

**Definition:** Fires when a **custom event is published to Azure Event Grid Topic** by any external system.

**Use cases:**
```
1. SAP nightly export finishes → publishes event → ADF starts ingestion
2. ML model training done → event → ADF starts scoring pipeline
3. Partner system sends batch-complete signal → event → ADF processes
4. Upstream ADF in another subscription finishes → event → this ADF starts
```

**Publishing from Python:**
```python
from azure.eventgrid import EventGridPublisherClient, EventGridEvent
from azure.core.credentials import AzureKeyCredential

client = EventGridPublisherClient(
    "https://ecdp-events.eastus.eventgrid.azure.net/api/events",
    AzureKeyCredential("your-topic-key")
)
client.send([EventGridEvent(
    event_type="com.ecdp.oracle.export.complete",
    subject="Oracle/CDB/PATIENTS",
    data={"table": "PATIENTS", "row_count": 50000, "export_path": "/data/oracle/"},
    data_version="1.0"
)])
# ADF trigger fires within seconds
```

---

## Q14. Trigger ADF pipeline when event published through Azure Event Grid.

```
Step 1: Create Event Grid Topic:
  az eventgrid topic create --name ecdp-events --resource-group rg-ecdp

Step 2: ADF → Author → Triggers → Custom Event Trigger:
  Topic:              /subscriptions/xxx/.../topics/ecdp-events
  Subject starts with: Oracle/
  Event type:         com.ecdp.oracle.export.complete

Step 3: Pass event data to pipeline:
  Parameters:
    event_data: @triggerBody().event.data
    subject:    @triggerBody().event.subject
    event_time: @triggerBody().event.eventTime

Step 4: Publish All → Start trigger → Publish test event → Verify in Monitor
```

---

## Q15. Difference between Schedule Trigger and Tumbling Window Trigger.

| Property | Schedule Trigger | Tumbling Window |
|---|---|---|
| Missed runs | Lost forever | Auto-backfilled |
| Overlap prevention | ❌ No | ✅ maxConcurrency |
| Window params | ❌ No | ✅ windowStart/End |
| Multi-pipeline | ✅ Yes | ❌ One pipeline |
| Dependencies | ❌ No | ✅ Yes |
| Historical load | ❌ No | ✅ Set past startTime |
| Use case | Simple daily batch | Windowed ETL with SLA |

**When to use Schedule:** Daily batch, simple schedule, missing run is OK.
**When to use TWT:** Hourly windowed queries, cannot afford missed windows, need dependency chain.

---

## Q16. Can multiple triggers execute the same pipeline? Example.

**Yes — multiple triggers can point to the same pipeline.**

```
Pipeline: PL_Oracle_Incremental_Load

TRG_Schedule_1AM      → fires daily at 1 AM (normal daily batch)
TRG_FileArrival_Flash → fires when priority_flag.txt lands (urgent reload)
TRG_Manual_Backfill   → manually started when historical reprocess needed

All three start the same pipeline with different parameters:
  TRG_Schedule:  process_date = yesterday, load_type = INCREMENTAL
  TRG_FileArrival: process_date = today,  load_type = INCREMENTAL
  TRG_Manual:    process_date = "2024-09-01", load_type = FULL_MONTH
```

**Identify which trigger fired inside pipeline:**
```
@pipeline().TriggerType  → "ScheduleTrigger", "BlobEventsTrigger", "Manual"
@pipeline().TriggerName  → "TRG_Schedule_1AM"
```

**Note:** Tumbling Window Trigger → 1 pipeline only. Schedule Trigger → multiple pipelines.

---

## Q17. How to start, stop, and monitor triggers?

**Start:**
```bash
# ADF Studio: Author → Triggers → Click trigger → Start
# CLI:
az datafactory trigger start \
  --resource-group rg-ecdp-prod \
  --factory-name adf-ecdp-prod \
  --trigger-name TRG_Daily_Oracle
```

**Stop:**
```bash
az datafactory trigger stop \
  --resource-group rg-ecdp-prod \
  --factory-name adf-ecdp-prod \
  --trigger-name TRG_Daily_Oracle
# Active runs continue — stop only prevents NEW firings
# MUST stop before editing trigger config
```

**Monitor:**
```
ADF Monitor → Trigger Runs:
  Trigger name, fired time, status (Succeeded/Failed), linked pipeline run

Azure Monitor Log Analytics:
  ADFTriggerRun
  | where TriggerName == "TRG_Daily_Oracle" and Status == "Failed"
  | project TimeGenerated, TriggerName, Status, Error
  | order by TimeGenerated desc

Alert rule: Failed trigger runs > 0 → email/Teams notification
```

---

## Q18. Scheduled pipeline failed yesterday. How to rerun missed execution?

**For Schedule Trigger (no auto-backfill — manual only):**
```
Option 1 — Rerun from Monitor:
  Monitor → Pipeline Runs → Find yesterday's run → Rerun (from failed activity)

Option 2 — Trigger new run with yesterday's date:
  ADF Studio → Pipeline → Trigger Now
  Parameters: process_date = "2024-10-23"

Option 3 — CLI:
  az datafactory pipeline create-run \
    --factory-name adf-ecdp-prod \
    --pipeline-name PL_Oracle_Load \
    --parameters '{"process_date":"2024-10-23"}'
```

**For Tumbling Window (auto-backfills after recovery):**
```
Missed windows auto-queue and run in order after ADF comes back up.
To manually rerun a specific window:
  Monitor → Trigger Runs → Find the failed window → Rerun
```

**Key:** Make all pipelines idempotent — running same date twice = same result.
```
Bronze: Auto Loader checkpoint → safe replay
Silver: MERGE → upsert, not duplicate
Gold:   replaceWhere → overwrites that date's partition
```

---

## Q19. What happens when you modify a trigger that is already running?

```
Rule 1: Cannot edit a STARTED trigger
  → ADF Studio greys out edit button
  → Must STOP the trigger first

Rule 2: Stopping trigger does NOT cancel active runs
  → In-progress pipeline runs complete normally
  → Stop only prevents NEW trigger firings

Rule 3: After editing and restarting:
  → Schedule Trigger: next fire time recalculated from new config
  → TWT: re-reads window config
  → TWT frequency change: must DELETE and recreate (cannot change frequency of existing TWT with runs)

Production change procedure:
  1. Stop trigger (CLI or Studio)
  2. Wait for in-progress runs to complete (Monitor → no "In Progress" runs)
  3. Edit trigger config
  4. Publish All
  5. Start trigger
  6. Verify first fire in Monitor → Trigger Runs
```

---

## Q20. How to pass dynamic parameters from trigger to pipeline?

```json
// Schedule Trigger:
"parameters": {
  "environment":    "prod",
  "trigger_name":   "@trigger().name",
  "trigger_time":   "@trigger().startTime",
  "scheduled_time": "@trigger().scheduledTime",
  "process_date":   "@formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')",
  "yesterday":      "@formatDateTime(addDays(trigger().scheduledTime,-1),'yyyy-MM-dd')"
}

// Tumbling Window Trigger:
"parameters": {
  "window_start": "@trigger().outputs.windowStartTime",
  "window_end":   "@trigger().outputs.windowEndTime",
  "window_date":  "@formatDateTime(trigger().outputs.windowStartTime,'yyyy-MM-dd')"
}

// Storage Event Trigger:
"parameters": {
  "file_name":    "@triggerBody().fileName",
  "folder_path":  "@triggerBody().folderPath",
  "trigger_time": "@trigger().startTime"
}

// Custom Event Trigger:
"parameters": {
  "event_data":   "@triggerBody().event.data",
  "subject":      "@triggerBody().event.subject",
  "event_id":     "@triggerBody().event.id"
}
```

---

## Q21. Pipeline should execute only on weekdays — how?

```json
{
  "name": "TRG_Weekdays_Only",
  "type": "ScheduleTrigger",
  "typeProperties": {
    "recurrence": {
      "frequency": "Week",
      "interval":  1,
      "startTime": "2024-01-01T01:00:00",
      "timeZone":  "India Standard Time",
      "schedule": {
        "weekDays": ["Monday","Tuesday","Wednesday","Thursday","Friday"]
        // Saturday, Sunday omitted → trigger does NOT fire
      }
    }
  }
}
```

**If Condition fallback inside pipeline (extra safety):**
```json
// dayOfWeek: 0=Sunday, 1=Monday, 5=Friday, 6=Saturday
"@and(greater(dayOfWeek(trigger().scheduledTime),0), less(dayOfWeek(trigger().scheduledTime),6))"
// true for Mon–Fri, false for Sat–Sun
```

---

## Q22. Prevent duplicate pipeline executions when multiple files arrive simultaneously?

**Problem:** 5 files arrive at once → 5 trigger fires → 5 concurrent MERGE runs → conflict.

**Solution 1 — Pipeline concurrency limit (simplest):**
```
ADF Studio → Pipeline → Settings → Concurrency: 1
→ 1 run at a time, rest queue → process sequentially → no conflict
```

**Solution 2 — Trigger a marker file, not individual data files:**
```json
// Upstream drops all data files, then drops ONE marker:
{ "blobPathEndsWith": "_LOAD_COMPLETE.flag" }
// 1 trigger fire → 1 pipeline run → processes all files in folder
```

**Solution 3 — ForEach inside single pipeline run:**
```
1 trigger fires → pipeline starts
GetMetadata → list all unprocessed files
Filter → exclude already-processed files (check control table)
ForEach (batchCount=4) → process 4 files in parallel, controlled
Mark each file as processed → idempotent
```

**Solution 4 — Switch to Tumbling Window Trigger:**
```
Instead of event per file, TWT every 15 minutes:
→ Processes all files in the last 15-min window in one run
→ maxConcurrency=1 → no overlap
→ Better for high-volume file arrivals
```

---

## Q23. Difference between Trigger Run and Pipeline Run?

| | Trigger Run | Pipeline Run |
|---|---|---|
| What | Trigger execution | Pipeline execution |
| ID | TriggerRunId | RunId |
| Monitor tab | Monitor → Trigger Runs | Monitor → Pipeline Runs |
| Status | Succeeded = trigger fired OK | Succeeded = pipeline logic OK |
| Can succeed independently? | ✅ Yes | ✅ Yes |

**Key point for interview:**
```
Trigger Run SUCCEEDED + Pipeline Run FAILED = trigger worked, pipeline logic broke
  → Check: which activity failed? What error?

Trigger Run FAILED = trigger itself didn't fire
  → Check: Event Grid config, storage permissions, trigger config

Always check BOTH tabs when troubleshooting.
```

---

## Q24. Tumbling Window failed for multiple windows — troubleshoot and recover.

**Step 1 — Identify all failed windows:**
```
Monitor → Trigger Runs → Filter: TriggerName=TWT_Hourly, Status=Failed
Note: which window start/end times failed
```

**Step 2 — Read error message:**
```
Click failed run → Click pipeline run link → Find failed activity → Read error

Common errors:
  JDBC timeout       → Oracle source unavailable → fix Oracle, then rerun
  OutOfMemory        → data grew, cluster too small → upgrade cluster
  Delta conflict     → two windows ran simultaneously → set maxConcurrency=1
  File not found     → source file moved → check path
```

**Step 3 — Fix root cause, then rerun:**
```
Method 1 — Monitor UI:
  Monitor → Trigger Runs → Click failed window → Rerun
  TWT processes oldest window first → then next → sequential catch-up

Method 2 — Bulk rerun via PowerShell:
  Get failed runs → foreach → Start-AzDataFactoryV2TriggerRun for each

Method 3 — TWT auto-recovery (if retry policy set):
  retryPolicy: count=3, intervalInSeconds=300
  → auto-retries 3× before marking as Failed
```

**Prevention:**
```json
"retryPolicy": { "count": 3, "intervalInSeconds": 300 },
// Also add to activities:
"policy": { "retry": 2, "retryIntervalInSeconds": 60, "timeout": "02:00:00" }
```

---

## Q25. Production: select trigger for daily batch, hourly, and event-based.

### Interview Script:
> "In production I use all three. Daily Oracle batch uses Schedule Trigger — simple, runs at 1 AM, and if we miss a day I manually rerun. Hourly transaction aggregations use Tumbling Window Trigger — I can't afford to miss a window, and I need windowStart/End for precise queries. File-arrival processing uses Storage Event Trigger — fires within seconds, handles upstream delays automatically. The rule I follow: if it's time-based with no catch-up needed → Schedule; if it's windowed with guaranteed processing → Tumbling Window; if it's data-arrival driven → Storage Event."

```
Daily Batch → Schedule Trigger
  TRG_Daily_Oracle: frequency=Day, interval=1, time=01:00 IST, Mon–Fri
  Recovery: manual rerun with process_date param

Hourly Processing → Tumbling Window Trigger
  TWT_Hourly_Txns: frequency=Hour, interval=1, delay=10min, maxConcurrency=2
  Recovery: auto-backfills missed windows in order
  Params: windowStart/End for precise "SELECT WHERE ts BETWEEN start AND end"

Event-based (file arrival) → Storage Event Trigger
  TRG_File_Salesforce: blobPathBeginsWith=/landing/sf/, endsWith=.csv
  Recovery: file just re-dropped → trigger fires again → pipeline reprocesses

Production architecture (eCDP):
  TRG_Daily_Oracle (Schedule)         → PL_Oracle_to_Bronze → Databricks Silver/Gold
  TWT_Hourly_Transactions (TWT)       → PL_Hourly_Aggregation
  TRG_File_Salesforce (Storage Event) → PL_Salesforce_Ingest
  TRG_SAP_Export (Custom Event)       → PL_SAP_Ingest
```

---

## QUICK REFERENCE CHEAT SHEET

```
4 TRIGGER TYPES:
  Schedule      → time-based, simple, misses lost
  Tumbling Wind → windowed, auto-backfill, windowStart/End
  Storage Event → file arrival, seconds latency
  Custom Event  → Azure Event Grid, external systems

KEY EXPRESSIONS:
  @trigger().scheduledTime                       → Schedule: when it was due
  @trigger().outputs.windowStartTime             → TWT: window start
  @trigger().outputs.windowEndTime               → TWT: window end
  @triggerBody().fileName                        → Storage: file name
  @triggerBody().event.data                      → Custom: event payload
  @formatDateTime(trigger().scheduledTime,'yyyy-MM-dd') → date string

RULES:
  Stop trigger before editing (active runs continue after stop)
  TWT: 1 pipeline only | Schedule: multiple pipelines OK
  Concurrency=1 on pipeline → prevents duplicate file-arrival runs
  Idempotent pipelines → safe to rerun any trigger at any time

CHOOSING:
  Simple batch, miss OK           → Schedule Trigger
  Windowed, cannot miss, SLA      → Tumbling Window Trigger
  File arrival, event-driven      → Storage Event Trigger
  External system integration     → Custom Event Trigger
```

---

*End of document — CGI 5 Questions + ADF Triggers 25 Q&A | Real Project | Interview Script*
