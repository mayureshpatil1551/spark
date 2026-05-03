# Apache Spark & Databricks — Interview Prep Guide
### Mayuresh Patil | Data Engineer | AbbVie / Cognizant 

---

## SECTION 1 — THEORY QUESTIONS

---

### Q1. What is the architecture of Spark? Explain driver, executors, cluster manager.

**Answer:**

Apache Spark follows a **master-worker architecture** with three core components:

**Driver:**
The Driver is the brain of a Spark application. It runs the `main()` function, creates the `SparkContext`, and converts your transformations into a **DAG (Directed Acyclic Graph)**. The driver breaks the DAG into stages and tasks, and coordinates everything.

> **Real-world context (AbbVie):** When you submitted your PySpark job to ingest Veeva API data into Apache Iceberg tables, the Driver was responsible for parsing your DataFrame transformations, figuring out which partitions to read, and scheduling those tasks across the cluster.

**Executors:**
Executors are worker processes that run on cluster nodes. They execute tasks, store data in memory/disk for caching, and report results back to the Driver. Each Spark application gets its own set of executors.

**Cluster Manager:**
The Cluster Manager allocates resources. Spark supports:
- **YARN** (Hadoop ecosystem)
- **Kubernetes**
- **Databricks-managed** (abstracted for you)
- **Standalone**

```
User Code → SparkContext (Driver)
         → Cluster Manager (request resources)
         → Executors launched on worker nodes
         → Tasks dispatched to executors
         → Results returned to Driver
```

---

### Q2. Difference between RDD vs DataFrame vs Dataset

| Feature | RDD | DataFrame | Dataset |
|---|---|---|---|
| Type Safety | Yes (compile-time) | No | Yes (compile-time) |
| Optimization | Manual | Catalyst Optimizer | Catalyst Optimizer |
| API Level | Low-level | High-level | High-level |
| Language | All | All | Scala/Java only |
| Schema | No | Yes | Yes |
| Performance | Slower | Faster | Faster |

**Real-world context (AbbVie):**
You used **DataFrames** in your PySpark jobs to ingest CSV, TXT, XML from S3 into Iceberg tables. DataFrames gave you:
- Schema inference on raw CSV/JSON from S3
- Easy column transformations (`withColumn`, `select`, `filter`)
- Catalyst optimizer to push down filters before reading from S3

```python
# Example from your S3 → Iceberg pipeline
df = spark.read.option("header", True).csv("s3://abbvie-raw/veeva/patients.csv")
df_cleaned = df.filter(col("status") == "ACTIVE").withColumn("load_ts", current_timestamp())
df_cleaned.writeTo("iceberg.bronze.veeva_patients").append()
```

---

### Q3. Explain Catalyst Optimizer

**Answer:**

The **Catalyst optimizer** transforms your logical query plan into an optimized physical execution plan in four phases:

1. **Analysis** — resolves column names, types, functions against the catalog.
2. **Logical optimization** — applies rule-based optimizations: predicate pushdown, column pruning, constant folding, Boolean simplification.
3. **Physical planning** — generates multiple physical plans and picks the cheapest using a cost model (considers row counts, join strategies).
4. **Code generation** — generates bytecode (via Tungsten's whole-stage code gen) that executes as a tight Java loop.

**How to influence it:**
- Use DataFrames/SQL (not RDDs) — Catalyst only optimizes these
- Collect table statistics: `ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS col1, col2` — improves cost-based join selection
- Use partition pruning — filter on partition columns so Catalyst skips irrelevant files
- Avoid UDFs where possible — they're black boxes to Catalyst and disable predicate pushdown


**Real-world context (AbbVie):**
When you achieved 50% reduction in Spark pipeline execution time, a large part of that was enabling Catalyst to do **predicate pushdown into Iceberg**. Instead of reading all records and then filtering, Catalyst pushed `WHERE` conditions into Iceberg's metadata layer — reading only relevant files.

```python
# Catalyst pushes this filter down to Iceberg file scan level
df = spark.read.format("iceberg").load("iceberg.silver.patient_data") \
    .filter(col("region") == "US") \
    .filter(col("load_date") >= "2024-01-01")
# Result: Only 2 out of 200 Parquet files scanned
```

---

### Q4. What is Tungsten? How does it help in performance?

**Answer:**

**Tungsten** is Spark's execution engine focused on CPU and memory efficiency. It operates below the DataFrame API level.

Key features:
- **Off-heap memory management** — avoids JVM garbage collection pauses by managing memory manually
- **Cache-aware computation** — stores data in a compact binary format that fits CPU cache lines
- **Whole-stage code generation** — fuses multiple operators into a single Java function, eliminating virtual function calls
- **Vectorized execution** — processes data in column batches (like Arrow format)

**Real-world context (AbbVie):**
When you tuned Spark configurations for your Iceberg pipeline, setting `spark.sql.execution.arrow.pyspark.enabled=true` activated Arrow-based columnar transfer between JVM and Python — which is Tungsten in action. This was critical when processing high-volume Veeva API payloads.

---

### Q5. Explain Wide vs Narrow Transformations

**Answer:**

**Narrow Transformation:**
Each input partition contributes to **exactly one output partition**. No data movement across the network. Examples: `map`, `filter`, `select`, `withColumn`, `union`.

**Wide Transformation (shuffle):**
Data from **multiple input partitions** is combined into output partitions. Requires data movement across the network. Examples: `groupBy`, `join`, `distinct`, `repartition`, `orderBy`.

**Real-world context (AbbVie):**
In your Veeva API ingestion pipeline, reading patient records and applying filters (`filter(col("status") == "ACTIVE")`) were narrow transformations — fast and local. But when you joined patient data with territory data across tables, that triggered a **shuffle** — a wide transformation — which you optimized using broadcast joins and partition tuning.

```python
# Narrow — stays local per partition
df_filtered = df.filter(col("country") == "US")  # Narrow

# Wide — shuffles data across network
df_grouped = df.groupBy("territory_id").agg(count("patient_id"))  # Wide
```

---

### Q6. What is a Shuffle? Why is it expensive?

**Answer:**

A **shuffle** is the process of redistributing data across partitions — usually across executor nodes. It's expensive because:

1. **Disk I/O** — shuffle data is written to disk before being sent
2. **Network I/O** — data is serialized and transferred over the network
3. **Serialization/Deserialization** — CPU-intensive packing/unpacking of data
4. **Blocking** — next stage can't start until all shuffle data is ready (barrier synchronization)

**Real-world context (AbbVie):**
When you joined your Veeva API output (large) with reference/lookup tables (small), doing a naive join would have shuffled the large dataset. Instead, you used **broadcast join** for small lookup tables, eliminating the shuffle entirely and contributing to your 50% processing time reduction.

```python
# BAD — causes shuffle of both tables
result = large_df.join(small_ref_df, "territory_id")

# GOOD — broadcasts small table, no shuffle for large_df
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_ref_df), "territory_id")
```

---

### Q7. What is Lazy Evaluation?

**Answer:**

Lazy evaluation means Spark does **not execute transformations immediately** when you call them. It builds an execution plan (DAG) and only executes when an **action** is called.

- **Transformations** (lazy): `select`, `filter`, `join`, `groupBy`, `withColumn`
- **Actions** (trigger execution): `show()`, `collect()`, `count()`, `write()`, `save()`

**Why it's powerful:**
Spark can **optimize the entire plan** before execution — combining steps, pushing down filters, eliminating unnecessary reads.

**Real-world context (AbbVie):**
In your S3 → Iceberg ingestion pipeline, you chained multiple transformations (read → filter → cast → rename → deduplicate) before writing to Iceberg. Spark optimized the entire chain as one execution plan, applying predicate pushdown at the S3 read level — never loading records it would have filtered later anyway.

```python
# All of this is LAZY — nothing executes yet
df = spark.read.parquet("s3://abbvie-raw/smartsheet/")
df = df.filter(col("active") == True)
df = df.withColumn("load_date", current_date())
df = df.dropDuplicates(["record_id"])

# THIS triggers execution of the entire chain
df.writeTo("iceberg.silver.smartsheet_data").append()  # ACTION
```

---

### Q8. What are Jobs, Stages, and Tasks?

**Answer:**

- **Job**: Created for each **action** (e.g., `count()`, `write()`). One action = one job.
- **Stage**: A job is split into stages at **shuffle boundaries** (wide transformations). Within a stage, all operations are narrow (no network transfer).
- **Task**: The smallest unit of work. One task runs on **one partition** in one executor. If a stage has 200 partitions → 200 tasks.

**Real-world context (AbbVie):**
In your Veeva API pipeline:
- **Job 1**: Write silver Iceberg table
  - **Stage 1**: Read S3, filter, cast columns (narrow) — 100 tasks (100 partitions)
  - **Stage 2** (after shuffle): GroupBy deduplication — 200 tasks
  - **Stage 3**: Write to Iceberg — 200 tasks

You monitored this in the **Spark UI** to identify which stages were taking longest and tuned partition counts accordingly.

---

### Q9. What is Broadcast Join? When Should You Use It?

**Answer:**

A **broadcast join** sends a copy of the smaller table to **every executor**, so the join happens locally without shuffling the large table.

**When to use:**
- One table is **small** (typically < 10 MB by default, configurable via `spark.sql.autoBroadcastJoinThreshold`)
- Joining a large fact table with a small dimension/lookup table

**When NOT to use:**
- Both tables are large
- The small table is too big to fit in executor memory

**Real-world context (AbbVie):**
When joining your patient data (millions of rows from Veeva API) with a territory lookup table (few hundred rows), broadcast join eliminated the shuffle of the patient table entirely.

```python
from pyspark.sql.functions import broadcast

# Patient data: 50M rows | Territory lookup: 300 rows
df_result = patient_df.join(
    broadcast(territory_lookup_df),
    on="territory_id",
    how="left"
)

# Or configure threshold globally
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50MB
```

---

### Q10. What is Delta Lake? Explain ACID in Delta.

**Answer:**

**Delta Lake** is an open-source storage layer that brings **ACID transactions** to data lakes (Parquet files on cloud storage). It uses a **transaction log (_delta_log)** to track all changes.

**ACID in Delta:**
- **Atomicity** — a write either fully completes or fully fails. No partial writes.
- **Consistency** — schema enforcement ensures data always matches the defined schema.
- **Isolation** — concurrent readers and writers don't interfere. Readers see a consistent snapshot.
- **Durability** — committed changes are permanent, stored in the transaction log.

**Real-world context:**
While you primarily used **Apache Iceberg** at AbbVie, the same ACID principles apply. For example, when your multithreaded API ingestion loaded data concurrently from Veeva and Ravex APIs, Iceberg's ACID guarantees ensured no reader saw partial writes from concurrent threads.

```python
# Delta MERGE — ACID transaction
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/mnt/silver/patients")

delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.patient_id = source.patient_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
# Either FULL merge completes or nothing changes — Atomicity
```

---

### Q11. What is Z-Ordering? When Should You Apply It?

**Answer:**

**Z-Ordering** is a **co-location technique** that sorts data so that related values are stored in the same files. It uses a space-filling curve (Z-curve) to map multi-dimensional data into a 1D order, keeping values close together physically.

**Benefits:**
- Reduces the number of files Spark needs to scan for filtered queries
- Especially useful for high-cardinality columns used in WHERE filters

**When to apply:**
- Columns frequently used in **WHERE / JOIN / GROUP BY** clauses
- Columns with **high cardinality** (e.g., patient_id, territory_id)
- **After OPTIMIZE** command

**When NOT to use:**
- Columns already used for **partitioning** (redundant)
- Columns rarely used in filters

**Real-world context (AbbVie):**
On your Iceberg tables, you'd apply sorting/clustering on `patient_id` and `load_date` since most downstream queries filter by these.

```sql
-- Delta Lake
OPTIMIZE silver.patient_data ZORDER BY (patient_id, load_date);

-- Equivalent in Iceberg (sort order at write time)
-- Set sort order when creating table
-- Iceberg: define sort order
CREATE TABLE silver.patient_data (
    patient_id BIGINT,
    load_date DATE,
    name STRING
)
USING iceberg
PARTITIONED BY (load_date)
WITH WRITE ORDERED BY (patient_id, load_date);

```

---

### Q12. What is OPTIMIZE in Delta?

**Answer:**

`OPTIMIZE` is a Delta Lake command that **compacts small files** into larger files. Over time, streaming ingestion or frequent small writes create many small Parquet files ("small file problem"), which degrades read performance.

```sql
OPTIMIZE delta.`/mnt/silver/veeva_patients`;

-- With Z-Ordering
OPTIMIZE delta.`/mnt/silver/veeva_patients` ZORDER BY (patient_id);
```

**Real-world context (AbbVie):**
Your incremental API ingestion jobs (Veeva, Ravex, Smartsheet) ran frequently, each writing small batches. Without OPTIMIZE, queries on these tables would scan thousands of tiny files. Running OPTIMIZE periodically (via a scheduled Databricks job or ADF trigger) compacts these into efficient 1 GB files.

---

### Q13. What is VACUUM in Delta? What Does Retention Threshold Mean?

**Answer:**

`VACUUM` removes **old data files** no longer referenced by the Delta transaction log. Delta retains old file versions for **time travel**, but they consume storage indefinitely unless vacuumed.

**Retention threshold** (default: 7 days / 168 hours):
- Files older than the threshold are deleted
- You cannot time-travel beyond the retention window after vacuum

```sql
-- Default: removes files older than 7 days
VACUUM delta.`/mnt/silver/patients`;

-- Custom retention (minimum 168 hours enforced by default)
VACUUM delta.`/mnt/silver/patients` RETAIN 240 HOURS;

-- Check what would be deleted (dry run)
VACUUM delta.`/mnt/silver/patients` RETAIN 168 HOURS DRY RUN;
```

**Real-world context (AbbVie):**
For your compliance-sensitive AbbVie pipelines, you'd be careful not to vacuum beyond audit requirements. If regulatory audit required 30-day data history, you'd set retention to 720 hours minimum.

---

### Q14. Schema Enforcement vs Schema Evolution in Delta

**Answer:**

**Schema Enforcement (default ON):**
Delta rejects writes that don't match the table's current schema. This prevents accidental data corruption.

```python
# This FAILS if new_column doesn't exist in target table
df_with_extra_col.write.format("delta").mode("append").save("/mnt/silver/patients")
# → AnalysisException: A schema mismatch detected
```

**Schema Evolution (opt-in):**
Delta automatically adds new columns when you enable `mergeSchema`.

```python
# This SUCCEEDS — new columns are added to the table schema
df_with_extra_col.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/mnt/silver/patients")
```

**Real-world context (AbbVie):**
When Veeva API added new fields to their response payload, your ingestion job needed to handle this gracefully. You used schema evolution (`mergeSchema=true`) for Bronze layer writes (raw intake), but enforced strict schema in Silver layer to protect downstream consumers.

---

### Q15. What is Checkpointing in Spark?

**Answer:**

**Checkpointing** saves the RDD/DataFrame to a **reliable distributed storage** (HDFS/S3), breaking the lineage chain. This is used to:
- Prevent recomputation of complex/long lineage
- Enable **fault tolerance** in iterative algorithms
- Used in **Spark Streaming** to save state

```python
# Set checkpoint directory
spark.sparkContext.setCheckpointDir("s3://abbvie-checkpoints/spark/")

# Checkpoint a DataFrame (breaks lineage)
df_complex = df.groupBy("patient_id").agg(...)  # Long chain
df_complex.checkpoint()  # Saved to S3, lineage broken
```

**Streaming checkpoint (Structured Streaming):**
```python
query = df_stream.writeStream \
    .format("iceberg") \
    .option("checkpointLocation", "s3://abbvie-checkpoints/streaming/") \
    .start("iceberg.silver.streaming_events")
```

---

### Q16. Caching vs Checkpointing vs Persistence

| Feature | Cache | Persist | Checkpoint |
|---|---|---|---|
| Storage | Memory (default) | Memory+Disk (configurable) | Disk (reliable storage) |
| Lineage | Preserved | Preserved | **BROKEN** |
| Fault tolerant | No (recomputed) | Partial | Yes |
| Use case | Reuse in same job | Large DFs reused | Long lineage / streaming |
| Location | Executor memory | Executor memory/disk | HDFS / S3 |

```python
from pyspark import StorageLevel

# Cache — memory only
df.cache()

# Persist — memory + disk spill
df.persist(StorageLevel.MEMORY_AND_DISK)

# Checkpoint — reliable distributed storage
spark.sparkContext.setCheckpointDir("s3://abbvie-checkpoints/")
df.checkpoint()
```

**Real-world context (AbbVie):**
You cached the territory lookup DataFrame that was joined with multiple large API result sets in the same job — avoiding re-reading the lookup table repeatedly.

---

### Q17. What Does Adaptive Query Execution (AQE) Do?

**Answer:**

**AQE** (introduced in Spark 3.0) dynamically **re-optimizes query plans at runtime** based on actual data statistics collected during execution.

Key features:
1. **Dynamically coalesces shuffle partitions** — merges small post-shuffle partitions into larger ones
2. **Converts sort-merge joins to broadcast joins** — if one side turns out to be small after filtering
3. **Skew join optimization** — splits skewed partitions into smaller sub-tasks

```python
# Enable AQE (default ON in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Real-world context (AbbVie):**
When ingesting Veeva data where some territories had 10x more patients than others (data skew), AQE's skew join optimization automatically split those hot partitions, preventing a few tasks from running 10x longer than others — a key contributor to your 50% processing time improvement.

---

### Q18. What is Cluster Mode vs Client Mode?

**Answer:**

**Client Mode:**
- Driver runs on the **machine that submitted the job** (local machine or edge node)
- Driver is outside the cluster
- Useful for interactive sessions, notebooks, debugging
- **Risk**: if client machine goes down, job fails

**Cluster Mode:**
- Driver runs **inside the cluster** (on one of the worker nodes)
- Job submission machine can disconnect after submission
- Preferred for **production batch jobs**
- More fault tolerant

**Real-world context (AbbVie):**
- Your **Databricks notebooks** used Client Mode — the driver ran on the cluster driver node but was tied to your notebook session
- Your **scheduled production jobs** (ADF-triggered Databricks jobs) ran in Cluster Mode — the driver was managed by Databricks, independent of any user session

---

### Q19. How Does Spark Handle Faults Internally?

**Answer:**

Spark handles faults at multiple levels:

1. **Task failure**: Automatically **re-runs failed tasks** (default: 4 retry attempts). If a task fails on bad data, Spark retries on a different executor.
2. **Executor failure**: Cluster Manager launches a new executor. Tasks from the failed executor are rescheduled.
3. **Driver failure**: The entire application fails (in Client Mode). In Cluster Mode, some cluster managers support driver restart.
4. **Lineage-based recovery**: If a partition is lost, Spark **recomputes it** by tracing back through the DAG lineage.

```python
# Configure task retries
spark.conf.set("spark.task.maxFailures", "4")

# For streaming — exactly-once with checkpoints
query = stream_df.writeStream \
    .option("checkpointLocation", "s3://abbvie-checkpoints/") \
    .start()
```

**Real-world context (AbbVie):**
When you did root cause analysis on connectivity and file transfer issues, you were essentially handling cases where Spark's automatic retry hit its limit — requiring you to investigate why tasks were consistently failing (network timeouts to Oracle, S3 throttling, etc.).

---

### Q20. What is Dynamic Partition Pruning?

**Answer:**

**Dynamic Partition Pruning (DPP)** is an optimization where Spark uses the result of one side of a join (typically a small dimension table) to **filter partitions** in the large fact table at runtime — before reading them.

Without DPP: Spark reads all partitions of the large table, then joins.
With DPP: Spark first evaluates the dimension filter, uses those values to prune which partitions of the fact table to scan.

```python
# Enable DPP
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Example: Only load_dates matching the filtered territory will be scanned
result = patient_df.join(
    territory_df.filter(col("region") == "US"),
    on="territory_id"
)
# DPP: patient_df partitions for non-US territories are SKIPPED entirely
```

**Real-world context (AbbVie):**
Your Iceberg tables were partitioned by `load_date` and `region`. When a downstream query filtered on `region = "US"`, DPP ensured Spark only opened the relevant Iceberg manifest files and Parquet data files — critical for your 20 TB dataset performance.

---

## SECTION 2 — PRACTICAL / HANDS-ON

---

### P1. Read Nested JSON and Flatten It

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.appName("AbbVie-Veeva-Flatten").getOrCreate()

# Real-world: Veeva API response with nested patient + visits structure
json_df = spark.read.option("multiLine", True).json("s3://abbvie-raw/veeva/patients.json")

# Schema example:
# root
#  |-- patient_id: string
#  |-- demographics: struct
#  |    |-- age: int
#  |    |-- gender: string
#  |-- visits: array
#  |    |-- element: struct
#  |    |    |-- visit_date: string
#  |    |    |-- diagnosis_code: string

# Flatten struct columns
df_flat = json_df.select(
    col("patient_id"),
    col("demographics.age").alias("age"),
    col("demographics.gender").alias("gender"),
    col("visits")
)

# Explode array column (one row per visit)
df_exploded = df_flat.withColumn("visit", explode(col("visits"))) \
    .select(
        col("patient_id"),
        col("age"),
        col("gender"),
        col("visit.visit_date"),
        col("visit.diagnosis_code")
    )

df_exploded.show(5)

# Write to Iceberg Bronze layer
df_exploded.writeTo("iceberg.bronze.veeva_patient_visits").append()
```

---

### P2. Optimize Joins Between Two Large Tables

```python
from pyspark.sql.functions import broadcast, col
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "52428800")  # 50MB
    .getOrCreate()

# Scenario: Join 50M patient records with 300-row territory lookup
patient_df = spark.read.format("iceberg").load("iceberg.silver.patients")
territory_df = spark.read.format("iceberg").load("iceberg.silver.territories")  # Small

# Strategy 1: Broadcast small table
result = patient_df.join(broadcast(territory_df), on="territory_id", how="left")

# Strategy 2: For two large tables — partition alignment
# Re-partition both on the join key to reduce shuffle
patient_repartitioned = patient_df.repartition(200, col("territory_id"))
events_repartitioned = events_df.repartition(200, col("territory_id"))
result_large = patient_repartitioned.join(events_repartitioned, "territory_id")

# Strategy 3: Handle skew — salt the join key
from pyspark.sql.functions import concat, lit, rand, floor

SALT = 10
# Add salt to large table
patient_salted = patient_df.withColumn("salt", (floor(rand() * SALT)).cast("string")) \
    .withColumn("salted_key", concat(col("territory_id"), lit("_"), col("salt")))

# Replicate small table across salt values
from pyspark.sql.functions import explode, array
territory_salted = territory_df.withColumn(
    "salt_array", array([lit(str(i)) for i in range(SALT)])
).withColumn("salt", explode(col("salt_array"))) \
 .withColumn("salted_key", concat(col("territory_id"), lit("_"), col("salt")))

result_skew = patient_salted.join(territory_salted, "salted_key", "left")
```

---

### P3. PySpark Job to Ingest Data from Oracle to Delta/Iceberg

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

spark = SparkSession.builder \
    .appName("AbbVie-Oracle-to-Iceberg") \
    .config("spark.jars", "/opt/ojdbc8.jar") \
    .getOrCreate()

# Oracle JDBC connection (use Key Vault for credentials in production)
ORACLE_URL = "jdbc:oracle:thin:@//oracle-host:1521/ABBVIE_DB"
ORACLE_USER = dbutils.secrets.get("abbvie-kv", "oracle-user")
ORACLE_PASS = dbutils.secrets.get("abbvie-kv", "oracle-pass")

# Full load
def full_load(table_name: str, iceberg_table: str):
    df = spark.read.format("jdbc") \
        .option("url", ORACLE_URL) \
        .option("dbtable", table_name) \
        .option("user", ORACLE_USER) \
        .option("password", ORACLE_PASS) \
        .option("numPartitions", 20) \
        .option("partitionColumn", "PATIENT_ID") \
        .option("lowerBound", 1) \
        .option("upperBound", 10000000) \
        .load()
    
    df = df.withColumn("load_timestamp", current_timestamp()) \
           .withColumn("load_type", lit("FULL"))
    
    df.writeTo(iceberg_table).overwritePartitions()

# Incremental load using watermark
def incremental_load(table_name: str, iceberg_table: str, last_run_ts: str):
    query = f"(SELECT * FROM {table_name} WHERE UPDATED_AT > TO_TIMESTAMP('{last_run_ts}','YYYY-MM-DD HH24:MI:SS')) t"
    
    df = spark.read.format("jdbc") \
        .option("url", ORACLE_URL) \
        .option("dbtable", query) \
        .option("user", ORACLE_USER) \
        .option("password", ORACLE_PASS) \
        .load()
    
    df = df.withColumn("load_timestamp", current_timestamp())
    df.writeTo(iceberg_table).append()

# Execute
full_load("ABBVIE.PATIENT_MASTER", "iceberg.bronze.patient_master")
incremental_load("ABBVIE.PATIENT_EVENTS", "iceberg.bronze.patient_events", "2024-01-01 00:00:00")
```

---

### P4. Write a PySpark UDF and Explain Why UDFs Are Expensive

```python
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re

# Scenario: Mask PHI (Patient Health Information) for GDPR compliance at AbbVie

# EXPENSIVE WAY — Python UDF
@udf(returnType=StringType())
def mask_patient_id_udf(patient_id: str) -> str:
    """Mask last 4 digits of patient ID for compliance"""
    if patient_id and len(patient_id) > 4:
        return patient_id[:-4] + "****"
    return "****"

df_masked = patient_df.withColumn("masked_id", mask_patient_id_udf(col("patient_id")))

# WHY UDFs ARE EXPENSIVE:
# 1. Serialization: Data serialized from JVM → Python process per row
# 2. Row-by-row: Python UDFs are not vectorized — process one row at a time
# 3. No Catalyst optimization: Spark cannot optimize inside UDF logic
# 4. GIL contention in Python: Global Interpreter Lock limits parallelism

# BETTER WAY — Use built-in Spark SQL functions (stays in JVM)
from pyspark.sql.functions import regexp_replace, substring, concat, lit

df_masked_efficient = patient_df.withColumn(
    "masked_id",
    concat(
        substring(col("patient_id"), 1, col("patient_id").cast("string").length - 4),
        lit("****")
    )
)

# BEST when UDF is unavoidable — Pandas UDF (vectorized)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def mask_patient_id_pandas_udf(series: pd.Series) -> pd.Series:
    return series.apply(lambda x: x[:-4] + "****" if x and len(x) > 4 else "****")

df_masked_pandas = patient_df.withColumn("masked_id", mask_patient_id_pandas_udf(col("patient_id")))
# Pandas UDF: data passed as Arrow batches — much faster than row-by-row Python UDF
```

---

### P5. Identify Performance Optimizations for a Slow Query

```python
# SLOW QUERY — typical issues you'd see in AbbVie pipelines

# Problem 1: Reading entire table when only need recent data (no partition pruning)
# BAD:
df = spark.read.format("iceberg").load("iceberg.silver.patient_events")
df_recent = df.filter(col("event_date") >= "2024-01-01")  # Reads ALL files first

# FIX: Ensure partition column is event_date, filter pushes to Iceberg scan
# Also enable: spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled","true")

# Problem 2: Too many small files → too many tasks
df.rdd.getNumPartitions()  # Check: if 5000 partitions for 10GB = bad

# FIX: Coalesce before write
df.coalesce(50).writeTo("iceberg.silver.patient_events").append()

# Problem 3: Data skew — one partition 100x larger than others
# Check skew via Spark UI: look for one task taking 10x longer
# FIX: Use AQE skew join or salt the join key (see P2 above)

# Problem 4: Repeated reads — cache intermediates
territory_lookup = spark.read.format("iceberg").load("iceberg.silver.territories")
territory_lookup.cache()  # Used in 5 subsequent joins — read once, reuse
territory_lookup.count()  # Trigger cache population

# Problem 5: UDFs — replace with built-in functions (see P4)

# Problem 6: Inefficient joins — check execution plan
df.join(other_df, "id").explain(True)  # Look for SortMergeJoin → should be BroadcastHashJoin

# Problem 7: Spark configs to tune
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default 200, tune to data size
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

### P6. Implement SCD Type 2 in PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (current_timestamp, lit, col, 
                                    when, sha2, concat_ws, to_date)
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("AbbVie-SCD2").getOrCreate()

# Scenario: Track history of patient territory assignments at AbbVie
# patient_id can change territory → keep history

# Current Silver table (SCD2 target)
# Schema: patient_id, territory_id, region, effective_from, effective_to, is_current, record_hash

# Incoming new/changed records from source
source_df = spark.read.format("iceberg").load("iceberg.bronze.patient_territory_latest")

# Add hash of business columns to detect changes
source_df = source_df.withColumn(
    "record_hash",
    sha2(concat_ws("||", col("territory_id"), col("region"), col("sub_region")), 256)
)

target_table = DeltaTable.forName(spark, "silver.patient_territory_history")
target_df = target_table.toDF().filter(col("is_current") == True)

# Join to find changed records
joined = source_df.alias("src").join(
    target_df.alias("tgt"),
    on="patient_id",
    how="left"
)

# Records that changed (hash mismatch)
changed_records = joined.filter(
    (col("tgt.patient_id").isNull()) |  # New records
    (col("src.record_hash") != col("tgt.record_hash"))  # Changed records
)

# Step 1: Expire old records (set is_current=False, effective_to=today)
records_to_expire = changed_records.filter(col("tgt.patient_id").isNotNull()) \
    .select(col("tgt.patient_id"), col("tgt.effective_from"))

target_table.alias("tgt").merge(
    records_to_expire.alias("exp"),
    "tgt.patient_id = exp.patient_id AND tgt.effective_from = exp.effective_from AND tgt.is_current = true"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "effective_to": current_timestamp()
}).execute()

# Step 2: Insert new/changed records as current
new_records = changed_records.select(
    col("src.patient_id"),
    col("src.territory_id"),
    col("src.region"),
    current_timestamp().alias("effective_from"),
    lit("9999-12-31").cast("timestamp").alias("effective_to"),
    lit(True).alias("is_current"),
    col("src.record_hash")
)

new_records.writeTo("silver.patient_territory_history").append()
```

---

### P7. Read XML into Spark and Load into Hive/Iceberg Tables

data format -->
<Transactions>
  <Transaction TransactionID="T1" Date="2024-01-01">
    <Amount Currency="USD">100.5</Amount>
    <Vendor>
      <VendorID>V101</VendorID>
      <VendorName>ABC Corp</VendorName>
    </Vendor>
    <LineItems>
      <LineItem>
        <ItemCode>I1</ItemCode>
        <Description>Item 1</Description>
        <Amount>50.25</Amount>
      </LineItem>
    </LineItems>
  </Transaction>
</Transactions>


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("AbbVie-XML-to-Iceberg") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0") \
    .enableHiveSupport() \
    .getOrCreate()

# Scenario: Your FinOps PoC — reading financial XML data from S3

xml_schema = StructType([

    # Attributes (_ prefix in spark-xml)
    StructField("_TransactionID", StringType(), True),
    StructField("_Date", StringType(), True),

    # Amount struct (value + attribute)
    StructField("Amount", StructType([
        StructField("_VALUE", DoubleType(), True),
        StructField("_Currency", StringType(), True)
    ]), True),

    # Vendor struct
    StructField("Vendor", StructType([
        StructField("VendorID", StringType(), True),
        StructField("VendorName", StringType(), True)
    ]), True),

    # LineItems array
    StructField("LineItems", StructType([
        StructField("LineItem", ArrayType(
            StructType([
                StructField("ItemCode", StringType(), True),
                StructField("Description", StringType(), True),
                StructField("Amount", DoubleType(), True)
            ])
        ), True)
    ]), True)

])
# Read XML file from S3
xml_df = spark.read \
    .format("xml") \
    .option("rowTag", "Transaction") \
    .option("rootTag", "Transactions") \
    .schema(xml_schema) \
    .load("s3://abbvie-raw/finops/transactions/*.xml")

xml_df.printSchema()
xml_df.show(5, truncate=False)

# Flatten nested XML structure
from pyspark.sql.functions import explode

df_flat = xml_df.select(
    col("_TransactionID").alias("transaction_id"),
    col("_Date").alias("transaction_date"),
    col("Amount._VALUE").alias("amount"),
    col("Amount._Currency").alias("currency"),
    col("Vendor.VendorID").alias("vendor_id"),
    col("Vendor.VendorName").alias("vendor_name"),
    col("LineItems.LineItem").alias("line_items")
)

# Explode line items array
df_exploded = df_flat.withColumn("line_item", explode("line_items")) \
    .select(
        col("transaction_id"),
        col("transaction_date"),
        col("amount"),
        col("currency"),
        col("vendor_id"),
        col("vendor_name"),
        col("line_item.ItemCode").alias("item_code"),
        col("line_item.Description").alias("description"),
        col("line_item.Amount").alias("line_amount"),
        current_timestamp().alias("load_timestamp")
    )

# Load into Hive table
df_exploded.write \
    .format("hive") \
    .mode("append") \
    .partitionBy("transaction_date") \
    .saveAsTable("finops.bronze_transactions")

# OR load into Iceberg table
df_exploded.writeTo("iceberg.bronze.finops_transactions") \
    .partitionedBy("transaction_date") \
    .append()
```

---

### P8. MERGE INTO Delta Table

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.appName("AbbVie-MERGE").getOrCreate()

# Scenario: Upsert Veeva API patient records into Silver Delta table
# New records → INSERT
# Changed records → UPDATE
# Deleted records (soft delete) → UPDATE is_deleted = True

# Incoming batch from Bronze
source_df = spark.read.format("iceberg").load("iceberg.bronze.veeva_patients_latest") \
    .withColumn("batch_ts", current_timestamp())

# Target Silver table
target = DeltaTable.forName(spark, "silver.patient_master")

# MERGE operation
target.alias("tgt").merge(
    source_df.alias("src"),
    condition="tgt.patient_id = src.patient_id"
).whenMatchedUpdate(
    condition="tgt.record_hash != src.record_hash",  # Only update if data changed
    set={
        "territory_id": "src.territory_id",
        "status": "src.status",
        "updated_at": "src.batch_ts",
        "record_hash": "src.record_hash"
    }
).whenNotMatchedInsert(
    values={
        "patient_id": "src.patient_id",
        "territory_id": "src.territory_id",
        "status": "src.status",
        "created_at": "src.batch_ts",
        "updated_at": "src.batch_ts",
        "record_hash": "src.record_hash",
        "is_deleted": "false"
    }
).whenNotMatchedBySourceUpdate(  # Records in target not in source = deleted
    set={"is_deleted": lit(True), "updated_at": current_timestamp()}
).execute()

print(f"Merge completed. History: ")
target.history(1).show(truncate=False)
```

---

### P9. Medallion Architecture Pipeline (Bronze → Silver → Gold)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, current_timestamp, lit, 
                                    sha2, concat_ws, when, trim, upper)

spark = SparkSession.builder.appName("AbbVie-Medallion").getOrCreate()

# ============================================================
# BRONZE LAYER — Raw ingestion, no transformation
# ============================================================
def bronze_ingest(source_path: str, source_system: str, table_name: str):
    """Ingest raw data as-is into Bronze Iceberg table"""
    df_raw = spark.read.option("header", True).csv(source_path)
    
    # Only add metadata columns — no business transformations
    df_bronze = df_raw \
        .withColumn("_source_system", lit(source_system)) \
        .withColumn("_ingest_timestamp", current_timestamp()) \
        .withColumn("_source_file", lit(source_path)) \
        .withColumn("_batch_id", lit(f"{source_system}_{current_timestamp()}"))
    
    df_bronze.writeTo(f"iceberg.bronze.{table_name}") \
        .option("write.metadata.compression-codec", "gzip") \
        .append()
    
    print(f"Bronze: {df_bronze.count()} records ingested from {source_system}")


# ============================================================
# SILVER LAYER — Cleansed, deduplicated, typed
# ============================================================
def silver_transform(bronze_table: str, silver_table: str):
    """Transform Bronze → Silver with DQ checks and deduplication"""
    df = spark.read.format("iceberg").load(f"iceberg.bronze.{bronze_table}")
    
    # Data Quality: Remove nulls on mandatory fields
    dq_fail = df.filter(col("patient_id").isNull() | col("territory_id").isNull())
    dq_fail.writeTo("iceberg.dq.failed_records").append()  # Quarantine bad records
    df_clean = df.filter(col("patient_id").isNotNull() & col("territory_id").isNotNull())
    
    # Standardize: uppercase, trim whitespace
    df_clean = df_clean \
        .withColumn("status", upper(trim(col("status")))) \
        .withColumn("region", upper(trim(col("region"))))
    
    # Deduplicate: keep latest record per patient_id
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window = Window.partitionBy("patient_id").orderBy(col("_ingest_timestamp").desc())
    df_deduped = df_clean.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")
    
    # Add record hash for change detection
    df_silver = df_deduped.withColumn(
        "record_hash",
        sha2(concat_ws("||", col("patient_id"), col("territory_id"), col("status")), 256)
    ).withColumn("silver_load_ts", current_timestamp())
    
    df_silver.writeTo(f"iceberg.silver.{silver_table}") \
        .partitionedBy("region") \
        .overwritePartitions()
    
    print(f"Silver: {df_silver.count()} clean records | {dq_fail.count()} quarantined")


# ============================================================
# GOLD LAYER — Business aggregates for analytics
# ============================================================
def gold_aggregate(silver_table: str, gold_table: str):
    """Build Gold layer aggregates for business reporting"""
    df = spark.read.format("iceberg").load(f"iceberg.silver.{silver_table}")
    
    # Aggregate: Patient count per territory per region (for Tableau/PowerBI)
    df_gold = df.groupBy("region", "territory_id") \
        .agg(
            count("patient_id").alias("total_patients"),
            sum(when(col("status") == "ACTIVE", 1).otherwise(0)).alias("active_patients"),
            max("silver_load_ts").alias("last_updated")
        )
    
    df_gold.writeTo(f"iceberg.gold.{gold_table}") \
        .partitionedBy("region") \
        .overwritePartitions()


# Execute pipeline
bronze_ingest("s3://abbvie-raw/veeva/patients/", "VEEVA", "veeva_patients_raw")
silver_transform("veeva_patients_raw", "patient_master")
gold_aggregate("patient_master", "patient_territory_summary")
```

---

*Document prepared for Mayuresh Uttam Patil — Data Engineer Interview Preparation*  
*Tailored to AbbVie / Cognizant project experience with Apache Iceberg, PySpark, and Azure*
