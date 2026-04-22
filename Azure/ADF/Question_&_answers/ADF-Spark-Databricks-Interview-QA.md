# Azure Data Engineer Interview — Q&A Guide
**Topics: Azure Data Factory · Spark & Databricks · Data Transformation · Hands-On Practice**

---

## SECTION 1 — Azure Data Factory (ADF)

---

### Q1. How do you connect to on-premises systems and ingest data into Azure Data Lake Storage Gen2?

**Answer:**

To connect ADF to on-premises systems you use the **Self-hosted Integration Runtime (SHIR)**. Think of it as a bridge agent you install inside your company's private network — it lets ADF talk to your on-prem databases (SQL Server, Oracle, SAP etc.) without exposing them to the public internet.

**Step-by-step flow:**

1. **Install SHIR** on a Windows machine inside your on-prem network
2. **Register it** with your ADF instance using an authentication key from the portal
3. **Create a Linked Service** in ADF pointing to your on-prem database — select the SHIR as the connection method
4. **Create a Dataset** on top of that Linked Service (represents your source table)
5. **Create a Linked Service for ADLS Gen2** (the destination)
6. **Build a Copy Activity** in a pipeline — source = on-prem dataset, sink = ADLS Gen2 dataset

**Authentication options for ADLS Gen2:**
- Managed Identity (recommended — no secrets to manage)
- Service Principal + Client Secret
- Account Key

**Example connection string for SQL Server via SHIR:**
```
Server=192.168.1.100;Database=PharmDB;User ID=svc_adf;Password=***;
```

**Key point:** All data transfer happens through the SHIR machine — ADF itself never directly touches your on-prem systems. The SHIR machine pulls data and pushes it to ADLS Gen2 through an outbound HTTPS connection (port 443 only).

---

### Q2. What activities can be used to copy multiple tables from on-prem to ADLS, and how do you orchestrate them?

**Answer:**

**Activities used:**

| Activity | Purpose |
|----------|---------|
| **Copy Activity** | The core worker — copies data from source to sink |
| **Lookup Activity** | Reads a list of table names from a config table or JSON file |
| **ForEach Activity** | Loops over the list and runs Copy Activity for each table |
| **Get Metadata Activity** | Checks if a file or folder exists before copying |
| **If Condition Activity** | Branches the pipeline based on conditions |

**Recommended pattern — Metadata-driven pipeline:**

```
Lookup Activity
  → reads list of tables from a config table:
     [{ "schema": "dbo", "table": "patients" },
      { "schema": "dbo", "table": "drugs"    }]

ForEach Activity (iterates over the list)
  → Copy Activity (parameterised)
       Source: @{item().schema}.@{item().table}
       Sink:   adls://container/raw/@{item().table}/
```

**Why this is better than hardcoded pipelines:**
- Adding a new table = just add one row to the config table. No pipeline changes.
- One parameterised pipeline handles hundreds of tables
- Easy to enable/disable tables without touching pipeline code

**Orchestration options:**
- **Trigger-based** — schedule trigger fires the master pipeline at a set time
- **Tumbling Window Trigger** — for incremental loads with time windows
- **Event-based** — trigger fires when a file arrives in a storage location

---

### Q3. What are the different types of Integration Runtime in ADF?

**Answer:**

Integration Runtime (IR) is the compute infrastructure ADF uses to perform data movement and transformation.

**Three types:**

#### 1. Azure Integration Runtime (Auto-resolve)
- Fully managed by Microsoft — you don't set up anything
- Works for cloud-to-cloud data movement (Azure SQL → ADLS, etc.)
- Auto-selects the nearest Azure region
- Scales automatically — no cluster management

#### 2. Self-hosted Integration Runtime (SHIR)
- You install this agent on your own Windows machine (on-prem or cloud VM)
- Required for: on-premises databases, private network resources, or systems behind a firewall
- Can be installed on multiple machines for high availability and load balancing
- Communicates outbound over HTTPS (port 443) — no inbound firewall rules needed

#### 3. Azure-SSIS Integration Runtime
- A managed cluster of Azure VMs that runs **SQL Server Integration Services (SSIS) packages**
- Used when migrating existing SSIS ETL workloads to the cloud without rewriting them
- You start and stop the cluster to control costs

**Quick comparison:**

| | Azure IR | Self-hosted IR | Azure-SSIS IR |
|---|---|---|---|
| Setup | None | Install agent | Provision cluster |
| Use case | Cloud-to-cloud | On-prem/private | SSIS packages |
| Cost | Pay per DIU | Machine cost | VM cluster cost |
| Managed by | Microsoft | You | Microsoft |

---

### Q4. What are the different trigger types available in ADF?

**Answer:**

ADF has **four trigger types:**

#### 1. Schedule Trigger
Fires at a fixed time and recurrence — like a cron job.
```
Runs every day at 2:00 AM IST
Runs every Monday and Wednesday at 9:00 AM
```
- Multiple pipelines can share one schedule trigger
- One pipeline can have multiple triggers

#### 2. Tumbling Window Trigger
Fires at regular intervals but with a built-in concept of **time windows** — each run processes a specific time slice.
```
Window 1: 2024-01-01 00:00 → 2024-01-01 01:00
Window 2: 2024-01-01 01:00 → 2024-01-01 02:00
```
- Guarantees no overlapping windows
- Can backfill missed runs automatically
- Passes window start and end times to the pipeline as parameters — perfect for incremental loads

#### 3. Storage Event Trigger
Fires when a file is **created or deleted** in Azure Blob Storage or ADLS Gen2.
```
Trigger condition: a file matching *.csv is created in
adls://container/incoming/
```
- Near real-time response (seconds to minutes)
- Useful for event-driven ingestion: "process this file as soon as it arrives"

#### 4. Custom Event Trigger
Fires based on events published to **Azure Event Grid**.
- For advanced event-driven scenarios beyond storage events
- Can respond to any Azure service event (IoT Hub messages, Service Bus, etc.)

---

### Q5. How do you handle and optimize the movement of large volumes of data into ADLS?

**Answer:**

Large-volume data movement requires tuning at multiple levels:

**1. Increase DIU (Data Integration Units)**
DIU is the measure of compute power for a Copy Activity. Default is 4; max is 256.
```
Copy Activity → Settings → Data Integration Units → set to 32 or 64
```
More DIUs = more parallel threads = faster copy.

**2. Enable Parallel Copy**
Controls how many threads read from the source in parallel.
```
Degree of Copy Parallelism: 8–32 (tune based on source capacity)
```

**3. Use Staging (PolyBase/COPY INTO)**
For large loads into Azure Synapse SQL Pools — always stage in Blob/ADLS first, then use PolyBase for bulk load instead of row-by-row inserts.

**4. Partition source reads**
For large database tables, partition the read into multiple ranges:
```
Partition on: order_date
Range: 2024-01-01 to 2024-01-31 (read in 31 daily chunks in parallel)
```

**5. Use binary copy for file-to-file**
If you're just moving files (not transforming), use Binary format — ADF copies raw bytes without parsing. Much faster than CSV or JSON format.

**6. Choose the right file format for ADLS**
- **Parquet** — columnar, compressed, best for analytics workloads
- **Delta/Iceberg** — if downstream processing uses Spark
- Avoid CSV for large volumes — no compression, slow to parse

**7. Network bandwidth**
- SHIR machine needs high network throughput — use a VM with at least 1Gbps NIC
- Co-locate SHIR in the same region as ADLS Gen2 to reduce latency

---

## SECTION 2 — Spark & Databricks

---

### Q6. How do you access and analyze logs in Databricks?

**Answer:**

Databricks has **four places** where you find logs:

#### 1. Cluster Logs
Go to: **Compute → Select Cluster → Logs tab**
- Driver logs (stdout, stderr) — your print statements and errors appear here
- Event logs — cluster start, scale-up, scale-down, termination events
- Init script logs — output from your cluster initialization scripts

#### 2. Spark UI
Go to: **Running cluster → Spark UI** (or from a completed job)
- **Jobs tab** — all Spark actions, their status, duration
- **Stages tab** — each stage's tasks, duration, shuffle read/write bytes
- **Executors tab** — memory usage, GC time, task count per executor
- **SQL tab** — visual query plan for DataFrame SQL operations
- **Storage tab** — cached RDDs and DataFrames

This is your **primary debugging tool** for performance issues.

#### 3. Job Run Logs
Go to: **Workflows → Select Job → Run History → Select a run**
- Full stdout/stderr output
- Task-level status
- Duration per task

#### 4. Log4j / Log Analytics
For structured logging in production:
```python
import logging
logger = logging.getLogger(__name__)
logger.info(f"Processing {record_count} records for date {process_date}")
```
Configure log delivery to:
- **ADLS Gen2** — for long-term retention and querying
- **Azure Monitor / Log Analytics** — for dashboards and alerts
- **Databricks Log Delivery** — configure in workspace settings

**Practical tip:** Always add structured log messages at key pipeline checkpoints — record counts after ingestion, after transformation, after write. This makes debugging much faster.

---

### Q7. If a job is taking longer than expected, how do you debug it and improve performance?

**Answer:**

**Systematic debugging approach — 4 steps:**

#### Step 1: Find the bottleneck
Open **Spark UI → Stages tab**. Sort by Duration descending.
- Which stage takes the longest?
- Are tasks evenly distributed or is one task a straggler?
- How much shuffle read/write is happening?

#### Step 2: Identify the cause

| Symptom | Cause | Fix |
|---------|-------|-----|
| One task takes 10x longer than others | Data skew | Salting, AQE, broadcast join |
| Large shuffle read/write bytes | Too many wide transformations | Reduce groupBy/join stages |
| High GC time in Executors tab | Not enough memory | Increase executor memory or reduce partition size |
| Many small tasks all slow | Too many partitions | coalesce() before write |
| Few tasks, each very slow | Too few partitions | repartition() |
| Stage reads millions of files | Small files problem | Run OPTIMIZE / compaction first |

#### Step 3: Apply fixes

```python
# Enable AQE (Adaptive Query Execution) — Spark 3.x
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Broadcast small tables to avoid shuffle
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_lookup_df), "key")

# Check partition count
df.rdd.getNumPartitions()

# Repartition for even distribution
df = df.repartition(200, "partition_col")
```

#### Step 4: Validate improvement
Re-run and compare Stage duration before and after. Check shuffle bytes reduction in Spark UI.

---

### Q8. How does the VACUUM command work on Delta tables?

**Answer:**

**The problem VACUUM solves:**

Every time you update, delete, or overwrite a Delta table — the old data files are NOT immediately deleted. They stay on disk. Delta Lake keeps them so you can do time travel (query old versions).

Over time, these old files accumulate and waste storage.

**VACUUM removes old files that are no longer needed:**

```sql
-- Default: removes files older than 7 days (168 hours)
VACUUM delta.`/mnt/data/patients`

-- Custom retention: removes files older than 30 days
VACUUM delta.`/mnt/data/patients` RETAIN 720 HOURS

-- Dry run: shows what WOULD be deleted, without actually deleting
VACUUM delta.`/mnt/data/patients` DRY RUN
```

**Important rules:**

1. **Default retention is 7 days** — you cannot go below 7 days without disabling a safety check
2. **After VACUUM, time travel is gone for those versions** — you cannot query snapshots older than your retention period
3. **VACUUM does NOT affect the Delta transaction log** — only data files
4. Run VACUUM as a **scheduled maintenance job** (weekly is common)

**Why the 7-day minimum?**
If a long-running query started before VACUUM and VACUUM deletes files the query needs — the query will fail. The 7-day buffer protects against this.

```sql
-- If you REALLY need to go below 7 days (not recommended in production):
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM delta.`/mnt/data/patients` RETAIN 0 HOURS;
```

---

### Q9. How do you check the number of partitions in a dataset?

**Answer:**

**Method 1: Check partition count on a DataFrame**
```python
df = spark.read.parquet("/mnt/data/patients/")
print(f"Number of partitions: {df.rdd.getNumPartitions()}")
```

**Method 2: Check physical partition folders on ADLS/S3**
```python
# List partition directories
dbutils.fs.ls("/mnt/data/patients/")
# Output:
# /mnt/data/patients/event_date=2024-01-01/
# /mnt/data/patients/event_date=2024-01-02/
# ...
```

**Method 3: Describe a Delta table**
```sql
DESCRIBE DETAIL delta.`/mnt/data/patients`
-- Shows: numFiles, sizeInBytes, partitionColumns
```

**Method 4: Check partition column values**
```sql
SHOW PARTITIONS my_catalog.my_db.patients
```

**Method 5: Via Spark UI**
After running a job → Spark UI → Stages tab → Look at "Number of Tasks" for a stage. Each task processes one partition.

**Rule of thumb for right-sizing partitions:**
- Target **128MB to 256MB per partition file**
- Too small (< 10MB) → small files problem, too many tasks
- Too large (> 1GB) → executors run out of memory, slow GC

---

### Q10. How do you decide when to increase or optimize partitioning?

**Answer:**

**Signs you need MORE partitions (increase):**

| Signal | What it means |
|--------|--------------|
| Spark UI shows only 4–10 tasks for a large dataset | Spark has too few partitions to parallelise |
| Executor memory usage is near 100% | Each partition is too large |
| Tasks take 10+ minutes each | Too much data per task |
| Stage parallelism is low | Not utilising all executor cores |

```python
# Current partition count too low — increase
df = df.repartition(400)  # full shuffle, even distribution
```

**Signs you need FEWER partitions (coalesce):**

| Signal | What it means |
|--------|--------------|
| Output folder has thousands of tiny files | Too many partitions at write time |
| Each output file is < 10MB | Small files problem |
| Downstream queries are slow | Too many files to open per scan |

```python
# Before writing — reduce partition count
df = df.coalesce(10)
df.write.parquet("/mnt/output/")
```

**Signs you need to change PARTITION COLUMNS (repartition by column):**

When queries always filter by a specific column (e.g., `event_date`) — partition by that column so Spark can skip irrelevant partitions entirely.

```python
# Partition by date for time-series data
df.write \
  .partitionBy("event_date") \
  .parquet("/mnt/data/events/")
```

**General guidelines:**

```
Target partition size:     128MB – 256MB per file
Target partition count:    2x to 4x total executor cores
For wide tables (Parquet): lean toward smaller partitions
For streaming:             use trigger intervals to control micro-batch size
```

**Auto-tune with AQE:**
```python
# Let Spark decide optimal partition count after shuffles
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
```

---

## SECTION 3 — Data Transformation Scenario

---

### Q11. Split a comma-separated column into multiple columns

**Input:**
```
col1 | value
1    | a,b,c
2    | d,e,f
```

**Expected Output:**
```
col1 | col2 | col3 | col4
1    | a    | b    | c
2    | d    | e    | f
```

**Solution 1 — PySpark (using split + getItem):**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder.appName("split_demo").getOrCreate()

# Create sample data
data = [(1, "a,b,c"), (2, "d,e,f")]
df = spark.createDataFrame(data, ["col1", "value"])

# Split the comma-separated column
df_result = df.withColumn("col2", split(col("value"), ",").getItem(0)) \
              .withColumn("col3", split(col("value"), ",").getItem(1)) \
              .withColumn("col4", split(col("value"), ",").getItem(2)) \
              .drop("value")

df_result.show()
```

**Output:**
```
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|   1|   a|   b|   c|
|   2|   d|   e|   f|
+----+----+----+----+
```

**Solution 2 — Dynamic split (handles unknown number of values):**
```python
from pyspark.sql.functions import split, col, size
from pyspark.sql.types import ArrayType, StringType

# Split into array first
df_array = df.withColumn("arr", split(col("value"), ","))

# Find max array length dynamically
max_len = df_array.select(size("arr").alias("len")).agg({"len": "max"}).collect()[0][0]

# Dynamically generate columns
for i in range(max_len):
    df_array = df_array.withColumn(f"col{i+2}", col("arr").getItem(i))

df_result = df_array.drop("value", "arr")
df_result.show()
```

**Solution 3 — Pure SQL:**
```sql
SELECT
    col1,
    split(value, ',')[0] AS col2,
    split(value, ',')[1] AS col3,
    split(value, ',')[2] AS col4
FROM input_table;
```

**When to use which:**
- Known, fixed number of values → Solution 1 (cleanest)
- Unknown number of values → Solution 2 (dynamic)
- SQL-only environment → Solution 3

---

## SECTION 4 — Real-World Hands-On Questions

*These go beyond theory — scenario-based questions that test whether you can actually build things.*

---

### HO1. You need to copy 50 tables from an on-prem SQL Server to ADLS Gen2 every night. Design the ADF pipeline.

**Answer:**

**Architecture:**

```
[Config Table in Azure SQL DB]
    table_name | schema | is_active | load_type | watermark_col
    patients   | dbo    | true      | incremental| updated_at
    drugs      | dbo    | true      | full       | NULL
    audit_log  | dbo    | false     | full       | NULL

[ADF Pipeline]
Lookup Activity
  → reads all rows WHERE is_active = true

ForEach Activity (sequential=false, batchCount=5)
  → Copy Activity (parameterised)
       IF load_type = 'full':
           source = SELECT * FROM @{item().schema}.@{item().table_name}
           sink   = adls://raw/@{item().table_name}/full_load/
       IF load_type = 'incremental':
           source = SELECT * FROM @{item().schema}.@{item().table_name}
                    WHERE @{item().watermark_col} > @{pipeline().parameters.last_run}
           sink   = adls://raw/@{item().table_name}/incremental/
  → Store Procedure Activity
       → updates watermark in config table after successful copy
```

**Key design decisions:**
- `batchCount=5` means 5 tables copy in parallel — tune based on source DB capacity
- Config table controls which tables are active — no pipeline changes to add/remove tables
- Watermark stored per table — independent incremental tracking
- On failure — the failed table's watermark is NOT updated, so it re-runs next time

---

### HO2. Your Databricks job runs daily and processes 500GB of Parquet files. After 6 months it is taking 3x longer. What do you investigate and fix?

**Answer:**

**Step 1 — Check file count growth:**
```python
files = dbutils.fs.ls("/mnt/data/events/")
print(f"Total files: {len(files)}")
# If this has grown from 1,000 to 100,000+ — small files problem
```

**Step 2 — Check partition skew:**
```python
df.groupBy("event_date").count().orderBy("count", ascending=False).show(10)
# If one date has 10x more records than others — skew
```

**Step 3 — Run OPTIMIZE on Delta tables:**
```sql
-- Compact small files and Z-order by the most-filtered column
OPTIMIZE events_table ZORDER BY (event_date, region)
```

**Step 4 — Check if old snapshots are bloating storage:**
```sql
VACUUM events_table RETAIN 168 HOURS
```

**Step 5 — Check if AQE is enabled:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**Step 6 — Check cluster sizing vs data growth:**
- Original: 500GB / 8-core cluster
- Now: 500GB × 6 months of daily appends — cluster needs scaling
- Either scale up executors or add more worker nodes

**Expected result:** After OPTIMIZE + VACUUM + AQE — typical improvement is 40–70% reduction in execution time.

---

### HO3. Write a PySpark pipeline that reads from Oracle via JDBC, applies a golden record deduplication, and writes to Delta Lake with upsert.

**Answer:**

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, col, current_timestamp
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("GoldenRecordPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# ── Step 1: Read from Oracle (parallel JDBC) ──────────────────
jdbc_url = "jdbc:oracle:thin:@host:1521/PHARMDB"
jdbc_props = {
    "user": dbutils.secrets.get("scope", "oracle_user"),
    "password": dbutils.secrets.get("scope", "oracle_pwd"),
    "driver": "oracle.jdbc.driver.OracleDriver"
}

df_raw = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT * FROM dbo.patients WHERE updated_at > '2024-01-01') t",
    column="patient_id",
    lowerBound=1,
    upperBound=10000000,
    numPartitions=100,
    properties=jdbc_props
)

# ── Step 2: Deduplication using window function ───────────────
window_spec = Window \
    .partitionBy("patient_id") \
    .orderBy(col("updated_at").desc())

df_deduped = df_raw \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .withColumn("ingested_at", current_timestamp())

# ── Step 3: Upsert into Delta Lake ───────────────────────────
delta_path = "/mnt/trusted/patients"

if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.alias("target").merge(
        df_deduped.alias("source"),
        "target.patient_id = source.patient_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    # First run — create the Delta table
    df_deduped.write \
        .format("delta") \
        .partitionBy("region") \
        .save(delta_path)

print(f"Upsert complete. Records processed: {df_deduped.count()}")
```

---

### HO4. A Databricks job fails intermittently with "Container killed by YARN for exceeding memory limits." How do you fix it?

**Answer:**

This is an **Out of Memory (OOM)** error. The executor is trying to hold more data in memory than it's allowed.

**Diagnosis:**
```python
# Check current memory config
print(spark.conf.get("spark.executor.memory"))
print(spark.conf.get("spark.executor.memoryOverhead"))
print(spark.conf.get("spark.sql.shuffle.partitions"))
```

**Fix 1 — Increase executor memory and overhead:**
```python
spark.conf.set("spark.executor.memory", "8g")         # main heap
spark.conf.set("spark.executor.memoryOverhead", "2g")  # off-heap (for native libs, Python)
```

**Fix 2 — Reduce partition size (most common fix):**
```python
# More partitions = less data per partition = less memory per task
spark.conf.set("spark.sql.shuffle.partitions", "400")  # default is 200
df = df.repartition(400)
```

**Fix 3 — Avoid collecting large DataFrames to driver:**
```python
# WRONG — this pulls everything to the driver, causes OOM
all_data = df.collect()

# RIGHT — process in Spark, only collect aggregates
summary = df.groupBy("region").count().collect()
```

**Fix 4 — Use disk spill for large shuffles:**
```python
spark.conf.set("spark.memory.fraction", "0.8")           # fraction of heap for execution + storage
spark.conf.set("spark.memory.storageFraction", "0.3")    # fraction of above for caching
```

**Fix 5 — Use broadcast for large joins:**
```python
from pyspark.sql.functions import broadcast
# Small lookup table → broadcast to all executors
result = large_df.join(broadcast(lookup_df), "key")
```

---

### HO5. Design an ADF + Databricks pipeline that implements SCD Type 2 (Slowly Changing Dimension).

**Answer:**

**What is SCD Type 2?** When a record changes, you don't overwrite the old record. You close it (set end_date) and insert a new row. This preserves full history.

```
Before update:
patient_id | name      | region | start_date | end_date   | is_current
1001       | John Smith| Mumbai | 2023-01-01 | NULL       | true

After region change to "Pune":
patient_id | name      | region | start_date | end_date   | is_current
1001       | John Smith| Mumbai | 2023-01-01 | 2024-06-01 | false   ← closed
1001       | John Smith| Pune   | 2024-06-01 | NULL       | true    ← new active
```

**PySpark implementation using Delta Lake MERGE:**

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit, sha2, concat_ws

# ── Step 1: Read new/changed records from source ──────────────
df_source = spark.read.jdbc(...)  # today's snapshot from Oracle

# ── Step 2: Compute a hash of all tracked columns ─────────────
df_source = df_source.withColumn(
    "row_hash",
    sha2(concat_ws("||", "name", "region", "email"), 256)
)

# ── Step 3: SCD2 MERGE ────────────────────────────────────────
target = DeltaTable.forPath(spark, "/mnt/dim/patients_scd2")

# Close existing records that changed
target.alias("t").merge(
    df_source.alias("s"),
    "t.patient_id = s.patient_id AND t.is_current = true AND t.row_hash != s.row_hash"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "end_date":   current_date()
}).execute()

# Insert new/changed records as new current rows
df_new = df_source.withColumn("is_current", lit(True)) \
                  .withColumn("start_date", current_date()) \
                  .withColumn("end_date", lit(None).cast("date"))

# Insert only records not already current and unchanged
df_new.createOrReplaceTempView("new_records")

spark.sql("""
    INSERT INTO dim_patients_scd2
    SELECT s.*
    FROM new_records s
    LEFT JOIN dim_patients_scd2 t
        ON s.patient_id = t.patient_id
        AND t.is_current = true
        AND s.row_hash = t.row_hash
    WHERE t.patient_id IS NULL
""")
```

---

### HO6. What happens when you run this code? Predict the output.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

data = [("Alice", 30), ("Bob", None), ("Alice", 25), ("Bob", 40)]
df = spark.createDataFrame(data, ["name", "age"])

result = df.groupBy("name") \
           .agg({"age": "max"}) \
           .filter(col("max(age)") > 25)

result.show()
```

**Answer:**

```
+-----+--------+
| name|max(age)|
+-----+--------+
|Alice|      30|
|  Bob|      40|
+-----+--------+
```

**Explanation step by step:**

1. **Data created:**
   - Alice: ages 30 and 25
   - Bob: ages NULL and 40

2. **groupBy("name").agg(max("age")):**
   - Alice → max(30, 25) = **30**
   - Bob → max(NULL, 40) = **40** ← Spark ignores NULL in aggregations

3. **filter(max(age) > 25):**
   - Alice: 30 > 25 → **keeps**
   - Bob: 40 > 25 → **keeps**
   - Both pass the filter

**Key learning:** Spark's `max()` aggregation **ignores NULL values** — it returns the max of non-null values. This is the same behaviour as SQL's MAX().

---

### HO7. You have a Delta table with 3 years of daily data. A query filtering by a single date takes 45 seconds. How do you make it faster?

**Answer:**

**Step 1 — Check if the table is partitioned:**
```sql
DESCRIBE DETAIL my_catalog.db.events
-- Look at: partitionColumns field
```

If `partitionColumns` is empty → the table has no physical partitioning. Every query scans ALL files.

**Step 2 — Check current file count:**
```sql
DESCRIBE DETAIL my_catalog.db.events
-- Look at: numFiles
-- 3 years × 365 days × multiple daily writes = potentially 100,000+ files
```

**Fix 1 — Recreate the table with partitioning:**
```python
# Read existing table
df = spark.read.format("delta").load("/mnt/data/events")

# Write with partitioning
df.write \
  .format("delta") \
  .partitionBy("event_date") \
  .mode("overwrite") \
  .save("/mnt/data/events_partitioned")
```

Now a query for `event_date = '2024-01-15'` reads **only the files in that partition folder** — skipping 3 years of other data.

**Fix 2 — Run OPTIMIZE + ZORDER:**
```sql
OPTIMIZE events ZORDER BY (event_date, region)
```

This compacts small files AND co-locates data by date within files — even without physical partitioning, queries skip more files.

**Fix 3 — Cache frequently queried data:**
```python
df_recent = spark.read.format("delta") \
    .load("/mnt/data/events") \
    .filter("event_date >= '2024-01-01'")
df_recent.cache()
df_recent.count()  # trigger caching
```

**Expected result:**
- Before: 45 seconds (full scan of 100,000+ files)
- After partitioning: 0.5–2 seconds (scans only today's ~10 files)

---

*End of document — ADF + Spark + Databricks Interview Q&A with Hands-On Practice*
