# Data Engineering Interview: Complete Answer Guide
## Databricks | Spark | Delta Lake | Azure | SQL | Kafka | Iceberg

---

# SECTION 1: DATABRICKS & SPARK
 
## Theory Questions

### 1. Spark Architecture: Driver, Executors, Cluster Manager

**Answer:**

Apache Spark follows a master-slave architecture with three core components:

**Driver Program:**
- The JVM process that runs the `main()` function of your application
- Hosts the `SparkContext` / `SparkSession`
- Responsible for converting user code into a DAG (Directed Acyclic Graph) of stages and tasks
- Coordinates with the cluster manager to acquire resources
- Schedules tasks on executors and monitors their execution
- Collects results from executors

**Cluster Manager:**
- Acts as the resource broker between the driver and the worker nodes
- Options: YARN, Kubernetes, Mesos, or Spark Standalone
- In Databricks, the cluster manager is abstracted — Databricks manages its own cluster lifecycle on top of cloud VMs (Azure VM Scale Sets / AWS EC2)
- Handles executor allocation, scaling, and termination

**Executors:**
- JVM processes running on worker nodes
- Each executor has a fixed number of CPU cores and RAM
- Executes tasks assigned by the driver
- Stores intermediate shuffle data and cached partitions
- Reports task status and results back to the driver

**Real Project Context:**  
In our eCDP healthcare pipeline on Databricks, we tuned executor configuration:
```
spark.executor.cores = 4
spark.executor.memory = 16g
spark.executor.memoryOverhead = 2g
```
We ran on a 10-node cluster (Standard_DS4_v2) with auto-scaling between 4–15 nodes, which helped during peak ingestion windows when 300+ SQL scripts fired concurrently.

**Flow:**
```
User Code → SparkSession → DAG Scheduler → Task Scheduler
         → Cluster Manager → Executors (Tasks) → Results
```

---

### 2. RDD vs DataFrame vs Dataset

| Feature | RDD | DataFrame | Dataset |
|--------|-----|-----------|---------|
| Type Safety | Yes (compile-time) | No (runtime) | Yes (compile-time) |
| Optimization | Manual | Catalyst + Tungsten | Catalyst + Tungsten |
| Language | Scala, Java, Python, R | All | Scala, Java only |
| Schema | No | Yes | Yes |
| API | Functional (map/filter) | SQL-like | Both |
| Performance | Slowest (no optimizer) | Fast | Fast |

**RDD (Resilient Distributed Dataset):**
- Low-level, immutable distributed collection of objects
- No built-in schema — you manually serialize/deserialize
- Use when: custom serialization, working with unstructured data, or needing fine-grained control

**DataFrame:**
- Distributed collection of `Row` objects with a named schema
- Optimized via Catalyst Optimizer and Tungsten engine
- Python/R DataFrames don't have type safety — column type errors surface at runtime
- Most common in production pipelines

**Dataset:**
- Strongly-typed DataFrame (Scala/Java only)
- Type-safe transformations like `map`, `flatMap`, `filter` on typed objects
- Not available in PySpark — PySpark's `DataFrame` is effectively `Dataset[Row]`

**Real Project Context:**  
In our Databricks eCDP project, we exclusively used DataFrames because all transformations were applied in PySpark. We only dropped to RDD level once — when parsing a malformed XML structure that Spark's `from_xml` couldn't handle and needed custom Python-level row parsing.

---

### 3. Catalyst Optimizer

**Answer:**

Catalyst is Spark's extensible query optimization framework that transforms logical query plans into optimal physical execution plans through 4 phases:

**Phase 1 — Analysis:**
- Resolves attribute names and types using the catalog
- Validates column references and functions
- Example: `df.select("patient_id")` → verifies `patient_id` exists in schema

**Phase 2 — Logical Optimization:**
- Applies rule-based transformations to the logical plan
- Key optimizations:
  - **Predicate Pushdown**: moves `WHERE` filters as close to the data source as possible (e.g., pushed into Parquet/Delta file reads)
  - **Constant Folding**: evaluates `1 + 1` → `2` at planning time
  - **Column Pruning**: removes unused columns before reading data

**Phase 3 — Physical Planning:**
- Converts the logical plan into multiple physical plans
- Uses a cost model (estimated row counts, data sizes) to pick the best plan
- Decides join strategies: BroadcastHashJoin, SortMergeJoin, ShuffleHashJoin

**Phase 4 — Code Generation:**
- Tungsten's whole-stage code generation compiles the physical plan into Java bytecode
- Eliminates virtual function dispatch overhead

**Real Project Context:**  
In our eCDP project, Catalyst's predicate pushdown was critical. Our Delta tables had 3 years of patient data (~2TB per table). Queries filtered on `event_date`. Once we ensured `event_date` was the partition column AND filter conditions were in the right format (date literals, not string casts), Catalyst pushed the filter down to file-level, reducing scan from 2TB to ~50GB — a 40x improvement.

---

### 4. Tungsten — Performance Optimization

**Answer:**

Tungsten is Spark's execution engine focused on CPU and memory efficiency, operating below the Catalyst layer.

**Key Features:**

**1. Off-Heap Memory Management:**
- Manages memory directly using `sun.misc.Unsafe` instead of JVM heap
- Avoids JVM garbage collection pauses that can cause executor timeouts
- Binary format stores data in compact row/columnar layout

**2. Cache-Aware Computation:**
- Designs algorithms and data structures to maximize CPU cache hits
- Uses cache-friendly iteration patterns (sequential memory access vs random)

**3. Whole-Stage Code Generation:**
- Instead of interpreting the plan row-by-row (Volcano model), Tungsten compiles the entire stage into a single tight Java function
- Eliminates virtual function call overhead per row

**Real Project Context:**  
In the eCDP project, we processed 300M+ rows of clinical event data. Enabling Tungsten (on by default in Spark 2.x+) with whole-stage codegen (`spark.sql.codegen.wholeStage=true`) reduced processing time by ~30% versus legacy Spark 1.x behavior. We also set `spark.sql.execution.arrow.pyspark.enabled=true` to leverage Arrow for pandas↔Spark conversions.

---

### 5. Wide vs Narrow Transformations

**Answer:**

**Narrow Transformations:**
- Each input partition contributes to only ONE output partition
- No data movement across the network
- Can be pipelined (executed in the same stage)
- Examples: `map`, `filter`, `flatMap`, `select`, `withColumn`, `union`

**Wide Transformations (Shuffle):**
- Input partitions contribute to MULTIPLE output partitions
- Require data to be moved across the network (shuffle)
- Create stage boundaries in the DAG
- Examples: `groupBy`, `join`, `distinct`, `repartition`, `orderBy`, `reduceByKey`

```
Narrow:  [P1] → [P1']    [P2] → [P2']    (no cross-partition communication)
Wide:    [P1] ──────────→ [P1', P2', P3'] (all-to-all communication)
         [P2] ──────────→ 
         [P3] ──────────→
```

**Real Project Context:**  
In our Silver layer transformation, we had a `groupBy("patient_id").agg(...)` on 500M rows. This wide transformation caused a massive shuffle. We optimized it by:
1. Pre-filtering data before the groupBy (reducing input from 500M → 120M rows)
2. Increasing shuffle partitions to 400: `spark.sql.shuffle.partitions=400`
3. Using AQE to dynamically coalesce small partitions post-shuffle

---

### 6. What is a Shuffle? Why is it Expensive?

**Answer:**

A shuffle is the process of redistributing data across partitions — usually across the network between executors — triggered by wide transformations.

**What happens during a shuffle:**

1. **Map Phase**: Each executor writes its output data to local disk, partitioned by the target partition key (hash of group-by or join key)
2. **Shuffle Write**: Data is serialized and written to shuffle files on local disk of each executor
3. **Shuffle Read**: Each executor reads its required partitions from all other executors over the network
4. **Merge/Sort Phase**: Incoming data is merged/sorted for the aggregation or join

**Why it's expensive:**
- **Disk I/O**: Data is spilled to disk (serialize + write + read)
- **Network I/O**: Data moves across executors (can be GBs to TBs)
- **Serialization/Deserialization overhead**: Data must be serialized for transfer
- **Synchronization**: Stage boundary — all tasks in the previous stage must complete before the next stage can begin
- **Memory pressure**: Large shuffles can cause OOM errors or spill to disk

**Real Project Context:**  
Our biggest shuffle issue was in the eCDP project's SCD Type 2 implementation. The `join` between the incoming CDC data (~50M rows) and the existing dimension table (~200M rows) caused a 45-minute shuffle. We solved it by:
- Broadcasting the incoming CDC batch (it was ~2GB after filtering) using `broadcast()`
- Pre-sorting both DataFrames on `patient_id` to enable SortMergeJoin more efficiently
- Using AQE's skew join optimization to handle patients with 10,000+ records

---

### 7. Lazy Evaluation

**Answer:**

Lazy evaluation means Spark does NOT execute transformations immediately when called. Instead, it builds a logical plan (DAG) and only executes when an **action** is called.

**Transformations (lazy):** `select`, `filter`, `groupBy`, `join`, `withColumn`, `map`  
**Actions (trigger execution):** `count()`, `show()`, `collect()`, `write`, `save()`, `take()`

**Why lazy evaluation is beneficial:**
1. **Optimization opportunity**: Catalyst can see the full pipeline and optimize (e.g., combine multiple filters, push predicates down)
2. **Avoids unnecessary computation**: If you filter before a join, Spark can optimize the order
3. **Fault tolerance**: Lineage graph allows recomputation of lost partitions

```python
# Nothing executes here — just builds a DAG
df = spark.read.parquet("/delta/patients")
df_filtered = df.filter(df.status == "ACTIVE")
df_joined = df_filtered.join(df_visits, "patient_id")

# Execution happens HERE
df_joined.write.format("delta").save("/delta/silver/patients")
```

**Real Project Context:**  
In our eCDP Silver layer, we chained 15+ transformations before the final write. Lazy evaluation meant Catalyst could see all transformations holistically. Predicate pushdown from the final `filter` was pushed all the way back to the initial Delta table read, avoiding reading 70% of the data. If transformations executed eagerly, each step would have materialized intermediate results.

---

### 8. Jobs, Stages, Tasks

**Answer:**

**Job:**
- Created every time an action is called (e.g., `.write()`, `.count()`)
- Represents the full computation from source to sink
- A single Spark application can have many jobs

**Stage:**
- A job is split into stages at shuffle boundaries (wide transformations)
- All transformations within a stage are narrow — can be pipelined without network transfer
- Stages within a job are executed sequentially (next stage waits for previous to complete)

**Task:**
- The unit of execution — one task per partition per stage
- A stage with 200 partitions = 200 parallel tasks
- Each task runs on a single executor core, processing one partition

```
Job (triggered by .write())
  └── Stage 1: Read + Filter + Select (narrow)   → 200 tasks (200 partitions)
  └── Stage 2: GroupBy Shuffle                    → 400 tasks (400 shuffle partitions)
  └── Stage 3: Write to Delta                     → 200 tasks
```

**Real Project Context:**  
In the eCDP pipeline, our most complex job had 7 stages. We monitored this in the Spark UI. Stage 4 (the SCD Type 2 merge join) was consistently the bottleneck — taking 40 minutes out of a 65-minute total. By optimizing the join strategy (broadcast), we reduced Stage 4 from 40 → 8 minutes.

---

### 9. Broadcast Join

**Answer:**

A broadcast join sends a copy of the smaller DataFrame to every executor, so the join happens locally without any shuffle of the larger DataFrame.

**When to use:**
- When one table is small enough to fit in executor memory (default threshold: 10MB, configurable up to ~2GB)
- `spark.sql.autoBroadcastJoinThreshold` (default: 10485760 bytes = 10MB)

**How it works:**
1. Driver collects the small DataFrame
2. Broadcasts it to all executors via BlockManager
3. Each executor performs a local hash join against its partition of the large DataFrame
4. No shuffle of the large table

```python
from pyspark.sql.functions import broadcast

# Explicit broadcast
result = large_df.join(broadcast(small_lookup_df), "patient_id", "left")

# Or increase threshold for auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 209715200)  # 200MB
```

**When NOT to use:**
- Small table is still too large (>2GB) — OOM on executors
- Join key has massive skew — consider salting instead
- Dynamic data whose size is unknown at planning time (use AQE's adaptive broadcast)

**Real Project Context:**  
In eCDP, our `dim_provider` table was 800MB after filtering active records. Default broadcast threshold (10MB) wouldn't catch it. We explicitly broadcast it using `broadcast()` when joining against 500M fact records. This eliminated a 25-minute SortMergeJoin shuffle, replacing it with a 3-minute BroadcastHashJoin.

---

### 10. Delta Lake and ACID Transactions

**Answer:**

Delta Lake is an open-source storage layer that brings ACID transactions, scalable metadata handling, and data versioning to data lakes (Parquet files on cloud storage).

**ACID in Delta Lake:**

**Atomicity:**
- All writes either fully succeed or fully fail — no partial writes
- Delta uses a `_delta_log/` transaction log — entries are written atomically
- If a job crashes mid-write, the uncommitted files are ignored

**Consistency:**
- Schema enforcement prevents writing data that violates the table schema
- Constraints (NOT NULL, CHECK) maintain data integrity

**Isolation:**
- Delta uses Optimistic Concurrency Control (OCC)
- Multiple concurrent writers can operate; conflicts are detected and resolved at commit time
- Snapshot isolation: readers always see a consistent snapshot

**Durability:**
- Once a transaction is committed to `_delta_log/`, it's durable
- The log is stored on cloud object storage (ADLS, S3) which provides high durability (11 nines)

**Delta Transaction Log (`_delta_log/`):**
- Every commit creates a new JSON file (e.g., `00000000000000000001.json`)
- Contains: added files, removed files, metadata changes
- Every 10 commits → a Parquet checkpoint file is created for faster log replay

```
/delta/patients/
├── _delta_log/
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   ├── 00000000000000000010.checkpoint.parquet
├── part-00000-*.parquet
├── part-00001-*.parquet
```

**Real Project Context:**  
In eCDP, we ran concurrent writes from 3 ADF pipelines into the same Delta Bronze table. Without Delta's OCC, we would have had file-level conflicts. Delta's transaction log serialized the commits, and only once did we get a `ConcurrentAppendException` which we handled with retry logic.

---

### 11. Z-Ordering

**Answer:**

Z-Ordering is a data skipping technique that co-locates related data within the same set of files, improving query performance for high-cardinality columns that aren't partition columns.

**How it works:**
- Uses a Z-order (Morton) space-filling curve to map multi-dimensional data into 1D ordering
- Files contain data that is locally clustered by the Z-order columns
- Delta's statistics (min/max per file) allow the engine to skip files that don't contain matching data

```sql
OPTIMIZE delta.`/delta/silver/clinical_events`
ZORDER BY (patient_id, event_date);
```

**When to use Z-Ordering:**
- Column has high cardinality (many unique values) — not suitable for partitioning
- Column is frequently used in `WHERE` filters or join conditions
- You can Z-ORDER by 1-4 columns (diminishing returns beyond that)

**When NOT to use:**
- Low cardinality columns → use partitioning instead
- Columns never used in filters

**Real Project Context:**  
In eCDP, `patient_id` had 20M unique values — partitioning by it would create 20M tiny files (small file problem). Instead, we partitioned by `year_month` (low cardinality) and Z-ORDERed by `patient_id`. Queries filtering `WHERE patient_id = 'X'` went from scanning 800 files to 3–5 files — a 160x reduction in file scans.

---

### 12. OPTIMIZE in Delta

**Answer:**

`OPTIMIZE` compacts small Parquet files in a Delta table into larger files (target: 1GB per file), improving read performance.

```sql
-- Basic optimize
OPTIMIZE delta.`/delta/silver/patients`;

-- With Z-ordering
OPTIMIZE delta.`/delta/silver/clinical_events`
ZORDER BY (patient_id, event_date);

-- Partition-level optimize (more targeted)
OPTIMIZE delta.`/delta/silver/clinical_events`
WHERE event_date >= '2024-01-01';
```

**Why small files are a problem:**
- Object storage (ADLS/S3) has per-file overhead for listing and opening files
- Spark creates one task per file — thousands of tiny files = thousands of tiny tasks = scheduling overhead

**OPTIMIZE behavior:**
- Reads all small files in a partition, merges, writes new larger files
- Adds new file entries and removes old file entries in `_delta_log/`
- Old files are NOT deleted immediately — use `VACUUM` to clean up

**Real Project Context:**  
Our Bronze layer ingested data every 15 minutes from ADF, creating ~96 small files per day per partition. After a week, queries on 30 days of data were scanning 2,880 files. We scheduled `OPTIMIZE` as a nightly Databricks job at 02:00 AM. Post-OPTIMIZE, the same query scanned 30–45 files (1 per partition). Read time dropped from 12 minutes to 90 seconds.

---

### 13. VACUUM in Delta

**Answer:**

`VACUUM` removes old data files from a Delta table that are no longer referenced by the transaction log, freeing up storage.

```sql
-- Default: removes files older than 7 days (168 hours)
VACUUM delta.`/delta/silver/patients`;

-- Custom retention (minimum 7 days enforced by default)
VACUUM delta.`/delta/silver/patients` RETAIN 336 HOURS;  -- 14 days

-- Dry run (see what would be deleted without deleting)
VACUUM delta.`/delta/silver/patients` DRY RUN;
```

**Retention Threshold:**
- Default: 7 days (168 hours)
- Files modified more recently than the threshold are KEPT (safe for time travel)
- Files older than threshold → eligible for deletion

**Safety Warning:**
- If you have long-running Spark jobs that reference old snapshots (e.g., streaming checkpoints), vacuum can delete files those jobs still need → causing `FileNotFoundException`
- Never reduce retention below the longest-running query/job duration

**Time Travel:**
```sql
-- Only works if VACUUM hasn't removed the old files
SELECT * FROM delta.`/delta/silver/patients` VERSION AS OF 5;
SELECT * FROM delta.`/delta/silver/patients` TIMESTAMP AS OF '2024-01-15';
```

**Real Project Context:**  
In eCDP, we retained 30 days for time travel (regulatory audit requirement). We set:
```sql
ALTER TABLE patients SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days');
VACUUM patients RETAIN 720 HOURS;
```
This added ~15% storage overhead but met our compliance requirement to reconstruct any point-in-time snapshot.

---

### 14. Schema Enforcement vs Schema Evolution in Delta

**Answer:**

**Schema Enforcement (default):**
- Delta rejects any write that doesn't match the existing table schema
- Prevents accidental schema corruption
- Throws `AnalysisException` if new columns appear or column types mismatch

```python
# This will FAIL if 'new_column' doesn't exist in Delta schema
df_with_new_column.write.format("delta").mode("append").save("/delta/patients")
# AnalysisException: A schema mismatch detected...
```

**Schema Evolution:**
- Allows new columns to be automatically added to the Delta schema
- Enable with `mergeSchema=true` or `autoMerge.enabled=true`

```python
# Append with new columns → auto-adds them to schema
df_with_new_column.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/delta/patients")

# Or globally
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

**What schema evolution handles:**
- Adding new columns ✅
- Widening types (int → long, float → double) ✅
- Changing incompatible types (string → int) ❌ (requires explicit `CAST`)

**Real Project Context:**  
In eCDP, source system teams occasionally added new columns to their Oracle tables. We enabled `mergeSchema=true` in our Bronze ingestion job so new columns were automatically propagated without breaking the pipeline. However, in the Silver layer, we enforced strict schema to catch unexpected upstream changes. This two-tier approach (permissive Bronze, strict Silver) was key to our pipeline reliability.

---

### 15. Checkpointing in Spark

**Answer:**

Checkpointing saves the materialized state of an RDD/DataFrame to a reliable storage (HDFS, ADLS), cutting the lineage graph to avoid recomputation chains that grow too long.

**Types:**

**Reliable Checkpointing:**
```python
spark.sparkContext.setCheckpointDir("abfss://container@storage.dfs.core.windows.net/checkpoints")
df.checkpoint()  # Materializes to ADLS, breaks lineage
```

**Local Checkpointing (faster, less reliable):**
```python
df.localCheckpoint()  # Materializes to executor local disk
```

**Streaming Checkpointing:**
```python
df.writeStream \
    .option("checkpointLocation", "abfss://container@storage.dfs.core.windows.net/stream_checkpoints") \
    .start()
```
- Stores streaming offsets, state, and committed batches
- Enables recovery after restart — resumes from last committed offset

**When to use checkpointing:**
- Very long lineage chains (iterative algorithms like ML training)
- Structured Streaming — mandatory for exactly-once semantics
- Expensive intermediate results that are reused multiple times

**Real Project Context:**  
In our Kafka→Delta streaming pipeline, checkpointing was mandatory. When a Databricks cluster restarted after a node failure, the streaming job resumed from the last checkpointed Kafka offset, ensuring no records were missed or duplicated.

---

### 16. Caching vs Checkpointing vs Persistence

| Feature | Cache | Persist | Checkpoint |
|--------|-------|---------|------------|
| Storage | Memory (default) | Configurable | ADLS/HDFS |
| Lineage | Preserved | Preserved | **Broken** |
| Fault tolerant | No (recomputed) | Partial | Yes |
| Performance | Fastest | Configurable | Slowest (disk I/O) |
| Use case | Reused DataFrames | Large data | Long lineage / streaming |

**Cache:** `df.cache()` — shorthand for `persist(StorageLevel.MEMORY_AND_DISK)`

**Persist with storage levels:**
```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)    # Default cache
df.persist(StorageLevel.MEMORY_ONLY)         # Fastest, OOM risk
df.persist(StorageLevel.DISK_ONLY)           # Slow, fault tolerant
df.persist(StorageLevel.MEMORY_AND_DISK_SER) # Serialized (less memory)
df.persist(StorageLevel.OFF_HEAP)            # Tungsten off-heap
```

**Real Project Context:**  
In our Gold layer, we joined a `dim_patient` DataFrame 8 times across different metric calculations. Without caching, it was re-read from Delta and re-computed 8 times. We added `dim_patient_df.cache()` and called `.count()` once to force materialization before the loop. This reduced the Gold layer job from 45 minutes to 12 minutes.

---

### 17. Adaptive Query Execution (AQE)

**Answer:**

AQE is a Spark 3.0+ feature that re-optimizes query plans at runtime using actual statistics collected during execution, rather than relying purely on pre-execution estimates.

**Enable:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Default in Spark 3.2+
```

**Three Key Features:**

**1. Coalescing Post-Shuffle Partitions:**
- After a shuffle, AQE combines small partitions into fewer, larger ones
- Avoids the "10,000 small partitions" problem that adds scheduling overhead

```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

**2. Converting Sort-Merge Join to Broadcast Join:**
- If AQE discovers (at runtime) that one side of a join is small enough, it converts from SortMergeJoin → BroadcastHashJoin automatically
- Useful when statistics were stale or unavailable at planning time

**3. Skew Join Optimization:**
- Detects partition skew (one partition significantly larger than others)
- Splits skewed partitions into sub-tasks to parallelize processing

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

**Real Project Context:**  
In eCDP, some `patient_id` values had 50,000+ records (chronic patients) while most had <10. This caused severe skew in our `groupBy("patient_id")` operations. AQE's skew join split those hot partitions into sub-tasks, reducing stragglers from 40 minutes to under 5 minutes and cutting overall job time by 35%.

---

### 18. Cluster Mode vs Client Mode

**Answer:**

| Feature | Client Mode | Cluster Mode |
|--------|------------|--------------|
| Driver location | **Client machine** (local) | **One of the worker nodes** |
| Use case | Interactive (notebooks, spark-shell) | Production batch jobs |
| Network dependency | High (driver must stay connected) | Low |
| Log access | Local | Remote (cluster logs) |
| Databricks notebooks | Always client mode (driver = notebook) | N/A (Databricks manages this) |

**Client Mode:**
- Driver runs on the machine that submitted the job (your laptop, ADF compute, etc.)
- All executor→driver communication goes over network to the submitting machine
- If client machine dies → job fails
- Good for development/debugging

**Cluster Mode:**
- Driver runs inside the cluster on one of the worker nodes
- No dependency on the submitting machine after submission
- Better for production — submit and disconnect
- `spark-submit --deploy-mode cluster ...`

**Real Project Context:**  
In Databricks, notebooks always run in client mode (driver = notebook kernel). For production jobs in our eCDP pipeline, we used Databricks Workflows (Jobs) which internally run in cluster mode — the driver is spawned on the cluster, independent of the submitting service (ADF). This was critical because our ADF pipeline had a 6-hour timeout, and some Spark jobs ran for 3+ hours.

---

### 19. Spark Fault Tolerance

**Answer:**

Spark handles faults through a combination of **RDD lineage**, **task retries**, and **speculation execution**.

**Task-Level Faults:**
- If a task fails, the scheduler retries it on a different executor (up to `spark.task.maxFailures=4` by default)
- Logs the failure and redistributes the task

**Executor-Level Faults:**
- If an executor dies, the driver detects the lost heartbeat
- Any tasks running on that executor are rescheduled
- Shuffle data on the dead executor's disk must be recomputed (no replication by default)
- In Databricks: failed nodes are replaced automatically by the cluster autoscaler

**Stage-Level Recovery:**
- If shuffle data is lost (executor died after shuffle write), the upstream stage is **re-executed** to regenerate the shuffle files

**RDD Lineage (DAG):**
- Spark maintains the full lineage of transformations
- If a partition is lost, it can be **recomputed** from its parent partitions
- Checkpointing cuts lineage to avoid recomputing the full chain

**Speculative Execution:**
```python
spark.conf.set("spark.speculation", "true")
```
- If a task runs significantly slower than peers (possible hardware issue), Spark launches a speculative copy on another executor
- First to finish wins, the other is killed

**Real Project Context:**  
In eCDP, we had occasional Azure spot instance preemptions that would kill executors mid-job. With `spark.task.maxFailures=8` and Databricks' auto-replacement of nodes, most jobs recovered transparently. Only twice did we need to manually restart — when the driver node itself was preempted (we moved the driver to an on-demand instance to solve this).

---

### 20. Dynamic Partition Pruning (DPP)

**Answer:**

Dynamic Partition Pruning is a Spark 3.0+ optimization that filters the partition scan of a large fact table based on values from a smaller dimension table — at runtime.

**How it works:**
1. Spark executes the filter on the dimension table first (e.g., `WHERE region = 'APAC'`)
2. Collects the matching keys (e.g., `store_id IN [101, 202, 303]`)
3. Broadcasts this filter as a "dynamic filter" to the fact table scan
4. Fact table only reads partitions containing those keys

```python
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Example: fact table partitioned by store_id
result = fact_sales.join(dim_store.filter(dim_store.region == "APAC"), "store_id")
# DPP: only reads fact partitions where store_id matches APAC stores
```

**Requirements:**
- The join key must be a partition column of the fact table
- The dimension table filter must reduce the join key space significantly

**Real Project Context:**  
In our eCDP Gold layer, the `clinical_events` table (2TB) was partitioned by `facility_id`. When generating regional reports, we joined with `dim_facility` filtered to a specific region. DPP pruned ~80% of `clinical_events` partitions, reducing the scan from 2TB to 400GB, cutting report generation from 35 minutes to 7 minutes.

---

## Practical / Hands-on Questions

### 1. Read Nested JSON and Flatten It

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer, from_json
from pyspark.sql.types import *

spark = SparkSession.builder.appName("FlattenJSON").getOrCreate()

# Sample nested JSON:
# {"patient_id": "P001", "name": "John", 
#  "address": {"city": "Mumbai", "zip": "400001"},
#  "visits": [{"date": "2024-01-01", "diagnosis": "flu"}, 
#              {"date": "2024-02-01", "diagnosis": "cold"}]}

df = spark.read.option("multiline", "true").json("/data/patients.json")

# Flatten struct fields
df_flat = df.select(
    col("patient_id"),
    col("name"),
    col("address.city").alias("city"),
    col("address.zip").alias("zip"),
    explode_outer(col("visits")).alias("visit")  # explode_outer preserves nulls
)

# Flatten the exploded struct
df_final = df_flat.select(
    col("patient_id"),
    col("name"),
    col("city"),
    col("zip"),
    col("visit.date").alias("visit_date"),
    col("visit.diagnosis").alias("diagnosis")
)

df_final.show()

# For deeply nested / dynamic schemas, use recursive flattening:
def flatten_df(df):
    """Recursively flatten all struct fields"""
    from pyspark.sql.types import StructType
    
    complex_fields = {
        field.name: field.dataType 
        for field in df.schema.fields 
        if isinstance(field.dataType, StructType)
    }
    
    while complex_fields:
        col_name = list(complex_fields.keys())[0]
        expanded = [
            col(f"{col_name}.{sub_field.name}").alias(f"{col_name}_{sub_field.name}")
            for sub_field in complex_fields[col_name]
        ]
        df = df.select(
            [col(c) for c in df.columns if c != col_name] + expanded
        )
        complex_fields = {
            field.name: field.dataType 
            for field in df.schema.fields 
            if isinstance(field.dataType, StructType)
        }
    return df

df_fully_flat = flatten_df(df)
df_fully_flat.write.format("delta").mode("overwrite").save("/delta/bronze/patients_flat")
```

---

### 2. Optimized Joins Between Two Large Tables

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "400") \
    .getOrCreate()

# ---- Strategy 1: Broadcast Join (small table < 200MB) ----
large_fact = spark.read.format("delta").load("/delta/silver/clinical_events")  # 2TB
small_dim = spark.read.format("delta").load("/delta/silver/dim_provider")      # 800MB

# Force broadcast for slightly larger tables
result = large_fact.join(broadcast(small_dim), "provider_id", "left")

# ---- Strategy 2: Partition co-location (SortMergeJoin optimization) ----
# Pre-bucket/sort both tables on join key to avoid full shuffle
large_df = spark.read.format("delta").load("/delta/silver/fact_claims")
other_large_df = spark.read.format("delta").load("/delta/silver/fact_encounters")

# Repartition both on join key with same number of partitions
large_df_repartitioned = large_df.repartition(400, "patient_id")
other_large_repartitioned = other_large_df.repartition(400, "patient_id")

result = large_df_repartitioned.join(other_large_repartitioned, "patient_id", "inner")

# ---- Strategy 3: Filter before join ----
# Always push filters BEFORE joins
filtered_events = large_fact.filter(col("event_date") >= "2024-01-01")
result = filtered_events.join(broadcast(small_dim), "provider_id", "left")

# ---- Strategy 4: Handle Skew with Salting ----
from pyspark.sql.functions import rand, floor, concat_ws, lit

SALT_FACTOR = 50

# Salt the large (skewed) DataFrame
large_df_salted = large_df.withColumn(
    "salted_key", 
    concat_ws("_", col("patient_id"), (floor(rand() * SALT_FACTOR)).cast("string"))
)

# Explode the small DataFrame to match all salt values
from pyspark.sql.functions import array, explode as sql_explode
small_df_salted = small_dim.withColumn(
    "salt_array", array([lit(i) for i in range(SALT_FACTOR)])
).withColumn("salt", sql_explode(col("salt_array"))) \
 .withColumn("salted_key", concat_ws("_", col("patient_id"), col("salt").cast("string"))) \
 .drop("salt_array", "salt")

result_skew_handled = large_df_salted.join(small_df_salted, "salted_key", "left")
```

---

### 3. PySpark Job: Oracle to Delta Ingestion

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Oracle_To_Delta_Ingestion") \
    .config("spark.jars", "/dbfs/jars/ojdbc8.jar") \
    .getOrCreate()

# Oracle JDBC connection config (secrets from Azure Key Vault via Databricks Secret Scope)
oracle_host = dbutils.secrets.get("ecdp-scope", "oracle-host")
oracle_port = dbutils.secrets.get("ecdp-scope", "oracle-port")
oracle_sid  = dbutils.secrets.get("ecdp-scope", "oracle-sid")
oracle_user = dbutils.secrets.get("ecdp-scope", "oracle-user")
oracle_pass = dbutils.secrets.get("ecdp-scope", "oracle-password")

jdbc_url = f"jdbc:oracle:thin:@{oracle_host}:{oracle_port}:{oracle_sid}"

# Incremental load: read last watermark from control table
watermark_df = spark.read.format("delta").load("/delta/meta/watermarks")
last_watermark = watermark_df.filter(
    watermark_df.table_name == "CLINICAL_EVENTS"
).select("last_extracted_at").first()[0]

logger.info(f"Extracting records updated after: {last_watermark}")

# Read from Oracle with parallelism using partitionColumn
oracle_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"""
        (SELECT * FROM SCHEMA.CLINICAL_EVENTS 
         WHERE LAST_UPDATED_DT > TO_DATE('{last_watermark}', 'YYYY-MM-DD HH24:MI:SS')
        ) T
    """) \
    .option("user", oracle_user) \
    .option("password", oracle_pass) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("partitionColumn", "PATIENT_ID_NUMERIC") \
    .option("lowerBound", "1") \
    .option("upperBound", "50000000") \
    .option("numPartitions", "50") \
    .option("fetchsize", "10000") \
    .load()

# Add audit columns
oracle_df_with_audit = oracle_df \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit("ORACLE_ECDP"))

# Write to Bronze Delta
oracle_df_with_audit.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("YEAR_MONTH") \
    .save("/delta/bronze/clinical_events")

# Update watermark
new_watermark = oracle_df.agg({"LAST_UPDATED_DT": "max"}).first()[0]

spark.createDataFrame([{
    "table_name": "CLINICAL_EVENTS",
    "last_extracted_at": str(new_watermark)
}]).write.format("delta").mode("overwrite") \
   .option("replaceWhere", "table_name = 'CLINICAL_EVENTS'") \
   .save("/delta/meta/watermarks")

logger.info(f"Ingested {oracle_df.count()} records. New watermark: {new_watermark}")
```

---

### 4. PySpark UDF — Why They Are Expensive

```python
from pyspark.sql.functions import udf, col, when, regexp_replace
from pyspark.sql.types import StringType, IntegerType
import re

# ---------- UDF Approach (SLOW) ----------
@udf(returnType=StringType())
def clean_phone_udf(phone):
    """Remove all non-numeric characters from phone number"""
    if phone is None:
        return None
    return re.sub(r'[^0-9]', '', str(phone))

df_with_udf = df.withColumn("clean_phone", clean_phone_udf(col("phone_number")))

# ---------- Why UDFs Are Expensive ----------
# 1. Serialization overhead: 
#    Spark serializes each Row to Python objects, calls the Python function,
#    then deserializes back to JVM → row-by-row Python↔JVM boundary crossing
#
# 2. No Catalyst optimization:
#    Catalyst treats UDFs as black boxes — cannot push predicates through,
#    cannot optimize away, cannot use vectorized execution
#
# 3. Python GIL: 
#    Python's Global Interpreter Lock limits true parallelism within each executor
#
# Benchmark: UDF approach ~10x slower than native Spark functions

# ---------- Better: Native Spark SQL Functions (FAST) ----------
df_native = df.withColumn(
    "clean_phone", 
    regexp_replace(col("phone_number"), r"[^0-9]", "")
)

# ---------- If UDF is unavoidable: Use Pandas UDF (vectorized) ----------
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def clean_phone_pandas_udf(phone_series: pd.Series) -> pd.Series:
    """Vectorized UDF using Apache Arrow — 10-50x faster than regular UDF"""
    return phone_series.str.replace(r'[^0-9]', '', regex=True)

df_fast = df.withColumn("clean_phone", clean_phone_pandas_udf(col("phone_number")))

# ---------- Pandas UDF for complex aggregations ----------
@pandas_udf("double")
def weighted_avg_udf(values: pd.Series, weights: pd.Series) -> float:
    return (values * weights).sum() / weights.sum()

result = df.groupBy("department").agg(
    weighted_avg_udf(col("salary"), col("weight")).alias("weighted_avg_salary")
)
```

---

### 5. Identify Performance Optimizations for a Slow Query

```python
# ---- Diagnostic Approach ----

# Step 1: Check the Spark UI
# - Jobs tab: which job is slow?
# - Stages tab: which stage is the bottleneck?
# - Tasks tab: are there stragglers? (data skew indicator)
# - SQL tab: view the physical plan — look for SortMergeJoin when you expected BroadcastHashJoin

# Step 2: Inspect the query plan
slow_df.explain(mode="extended")
# Look for:
# - Exchange (shuffle) operations — reduce these
# - FileScan reading large amount of data — add partition filters
# - CartesianProduct — NEVER want this, means join keys missing

# Step 3: Check partition distribution (detect skew)
slow_df.groupBy(spark_partition_id()).count().orderBy("count", ascending=False).show(20)

# ---- Common Fixes ----

# Fix 1: Add missing filter to enable partition pruning
# BAD (reads all partitions):
bad_query = events_df.filter(col("year_month").cast("string") == "2024-01")
# GOOD (partition filter works directly):
good_query = events_df.filter(col("year_month") == "2024-01")

# Fix 2: Cache repeatedly used DataFrames
dim_patient = spark.read.format("delta").load("/delta/silver/dim_patient")
dim_patient.cache()
dim_patient.count()  # Materialize cache

# Fix 3: Increase shuffle partitions for large data
spark.conf.set("spark.sql.shuffle.partitions", "800")

# Fix 4: Enable AQE for dynamic optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Fix 5: Handle skew with repartitioning
# Check for skew:
events_df.groupBy("patient_id").count() \
    .orderBy("count", ascending=False).show(5)
# If top values have 1000x more rows → salt the key

# Fix 6: Avoid UDFs — replace with native functions
# Fix 7: Broadcast small dimension tables
# Fix 8: Push down filters before joins

# Fix 9: Check for unnecessary wide transformations
# BAD:
df.orderBy("patient_id")  # Full sort = expensive shuffle
# GOOD (if only need top N):
df.orderBy("patient_id").limit(1000)  # Spark optimizes this
```

---

### 6. SCD Type 2 in PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws,
    when, to_date
)
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("SCD2").getOrCreate()

# Existing SCD2 dimension table (Delta)
dim_table_path = "/delta/silver/dim_patient"

# New incoming data (daily CDC extract)
incoming_df = spark.read.format("delta").load("/delta/bronze/patient_cdc_today")

# Add hash of business columns to detect changes
business_cols = ["first_name", "last_name", "dob", "address", "phone", "email"]

incoming_with_hash = incoming_df.withColumn(
    "row_hash",
    sha2(concat_ws("||", *[col(c).cast("string") for c in business_cols]), 256)
)

# Load existing dimension
dim_df = spark.read.format("delta").load(dim_table_path)
active_dim = dim_df.filter(col("is_current") == True)

# Detect changed records (existing patients with different hash)
changed_records = incoming_with_hash.alias("src").join(
    active_dim.alias("tgt"),
    col("src.patient_id") == col("tgt.patient_id"),
    "inner"
).filter(col("src.row_hash") != col("tgt.row_hash")) \
 .select(col("src.*"))

# New records (patient_id not in dimension)
new_records = incoming_with_hash.alias("src").join(
    active_dim.select("patient_id").alias("tgt"),
    col("src.patient_id") == col("tgt.patient_id"),
    "left_anti"
)

# Records to insert (new + changed)
records_to_insert = new_records.union(changed_records) \
    .withColumn("effective_start_date", current_timestamp()) \
    .withColumn("effective_end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))

# Expire changed records (set is_current=False, effective_end_date=now)
delta_table = DeltaTable.forPath(spark, dim_table_path)

delta_table.alias("tgt").merge(
    changed_records.alias("src"),
    "tgt.patient_id = src.patient_id AND tgt.is_current = true"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "effective_end_date": current_timestamp()
}).execute()

# Insert new and changed records
records_to_insert.write \
    .format("delta") \
    .mode("append") \
    .save(dim_table_path)

print(f"SCD2 update complete. New: {new_records.count()}, Changed: {changed_records.count()}")
```

---

### 9. Merge Into Delta Table (UPSERT)

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Source: incoming data
source_df = spark.read.format("delta").load("/delta/bronze/patient_updates")

# Target: existing Delta table
delta_table = DeltaTable.forPath(spark, "/delta/silver/dim_patient")

# MERGE (Upsert: update if exists, insert if new, optionally delete)
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.patient_id = source.patient_id"
).whenMatchedUpdate(set={
    "first_name":       "source.first_name",
    "last_name":        "source.last_name",
    "email":            "source.email",
    "last_updated_at":  "source.last_updated_at",
    "_merge_timestamp": current_timestamp()
}).whenNotMatchedInsert(values={
    "patient_id":       "source.patient_id",
    "first_name":       "source.first_name",
    "last_name":        "source.last_name",
    "email":            "source.email",
    "last_updated_at":  "source.last_updated_at",
    "_merge_timestamp": current_timestamp()
}).whenNotMatchedBySourceDelete()  # Delete records not in source (optional)
  .execute()
```

---

### 10. Medallion Architecture Pipeline (Bronze → Silver → Gold)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, trim, upper, to_date,
    count, when, isnan, isnull, sha2, concat_ws
)
from delta.tables import DeltaTable
import logging

spark = SparkSession.builder.appName("MedallionPipeline").getOrCreate()
logger = logging.getLogger(__name__)

BASE_PATH = "abfss://data@ecpdadls.dfs.core.windows.net"

# ============================================================
# BRONZE LAYER: Raw ingestion — as-is from source
# ============================================================
def ingest_to_bronze(source_path, table_name):
    logger.info(f"[BRONZE] Ingesting {table_name}")
    
    raw_df = spark.read \
        .option("mergeSchema", "true") \
        .format("json") \
        .load(source_path)
    
    bronze_df = raw_df \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_file", col("_metadata.file_path")) \
        .withColumn("_source_system", lit("ORACLE_ECDP"))
    
    bronze_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("year_month") \
        .save(f"{BASE_PATH}/bronze/{table_name}")
    
    logger.info(f"[BRONZE] {table_name}: {bronze_df.count()} records ingested")

# ============================================================
# SILVER LAYER: Cleansed, validated, standardized
# ============================================================
def transform_to_silver(table_name):
    logger.info(f"[SILVER] Processing {table_name}")
    
    bronze_df = spark.read.format("delta").load(f"{BASE_PATH}/bronze/{table_name}")
    
    # Data quality checks
    total_count = bronze_df.count()
    null_patient_id = bronze_df.filter(col("patient_id").isNull()).count()
    
    if null_patient_id / total_count > 0.05:
        raise ValueError(f"DQ FAIL: {null_patient_id/total_count:.1%} null patient_ids (threshold: 5%)")
    
    silver_df = bronze_df \
        .filter(col("patient_id").isNotNull()) \
        .withColumn("first_name", trim(upper(col("first_name")))) \
        .withColumn("last_name",  trim(upper(col("last_name")))) \
        .withColumn("dob", to_date(col("dob"), "yyyy-MM-dd")) \
        .withColumn("email", trim(col("email"))) \
        .withColumn("_silver_timestamp", current_timestamp()) \
        .dropDuplicates(["patient_id", "event_date"])
    
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("year_month") \
        .save(f"{BASE_PATH}/silver/{table_name}")
    
    # Run OPTIMIZE after large writes
    spark.sql(f"""
        OPTIMIZE delta.`{BASE_PATH}/silver/{table_name}`
        ZORDER BY (patient_id, event_date)
    """)
    
    logger.info(f"[SILVER] {table_name} complete")

# ============================================================
# GOLD LAYER: Business-ready aggregated metrics
# ============================================================
def build_gold_patient_summary():
    logger.info("[GOLD] Building patient summary")
    
    events_df = spark.read.format("delta").load(f"{BASE_PATH}/silver/clinical_events")
    patients_df = spark.read.format("delta").load(f"{BASE_PATH}/silver/dim_patient")
    
    # Cache shared dimensions
    patients_df.cache()
    
    gold_df = events_df \
        .groupBy("patient_id", "year_month") \
        .agg(
            count("*").alias("total_events"),
            count(when(col("event_type") == "ADMISSION", 1)).alias("admissions"),
            count(when(col("event_type") == "DISCHARGE", 1)).alias("discharges")
        ) \
        .join(patients_df.select("patient_id", "first_name", "last_name", "region"), 
              "patient_id", "left") \
        .withColumn("_gold_timestamp", current_timestamp())
    
    gold_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{BASE_PATH}/gold/patient_monthly_summary")
    
    patients_df.unpersist()
    logger.info("[GOLD] Patient summary complete")

# Run pipeline
ingest_to_bronze("/raw/clinical_events/*.json", "clinical_events")
transform_to_silver("clinical_events")
build_gold_patient_summary()
```

---

# SECTION 2: DATA MODELING & ARCHITECTURE

## Theory Questions

### 1. Data Lake vs Data Warehouse vs Lakehouse

| Feature | Data Lake | Data Warehouse | Lakehouse |
|---------|-----------|----------------|-----------|
| Data format | Raw (Parquet, JSON, CSV) | Proprietary (columnar) | Open (Delta, Iceberg) |
| Schema | Schema-on-read | Schema-on-write | Both |
| Data types | Structured + Unstructured | Structured only | Structured + Semi |
| ACID | No | Yes | Yes (Delta/Iceberg) |
| Cost | Low (object storage) | High | Low-Medium |
| Performance | Variable | High | High (with optimization) |
| Example | ADLS, S3 | Snowflake, Redshift | Databricks, AWS Lake Formation |

**Real Project Context:**  
In eCDP, we implemented a Lakehouse on Databricks + ADLS Gen2 with Delta Lake. This gave us Data Warehouse-grade ACID and query performance at Data Lake-grade cost. We avoided Snowflake because 20TB of unstructured clinical notes couldn't be stored economically in a DW.

---

### 2. Star Schema vs Snowflake Schema

**Star Schema:**
- Fact table at center, surrounded by denormalized dimension tables
- Each dimension is a flat table (no further normalization)
- Simple joins (1 level), fast query performance
- More storage (denormalized = redundancy)
- Best for: analytical queries, BI tools, OLAP

**Snowflake Schema:**
- Dimension tables are further normalized into sub-dimension tables
- Reduces redundancy/storage
- More complex joins (2+ levels)
- Slightly slower queries (more joins)
- Best for: storage-sensitive environments, complex hierarchical dimensions

**Real Project Context:**  
In eCDP Gold layer, we used Star Schema:
- Fact: `fact_clinical_events` (patient events)
- Dims: `dim_patient`, `dim_provider`, `dim_facility`, `dim_date`, `dim_diagnosis`
Simple star schema allowed PowerBI reports to run via DirectQuery with <5 second response times.

---

### 4. Surrogate Key

A surrogate key is a system-generated, meaningless unique identifier assigned to each record in a dimension table, separate from the natural business key.

**Why use surrogate keys:**
1. Handles NULL natural keys (source data might lack a key)
2. Manages SCD Type 2 — multiple rows for same business entity with different surrogate keys
3. Consistent identifier even if natural key changes (e.g., patient SSN correction)
4. Faster joins (integer vs string comparison)
5. Source system independence

```python
from pyspark.sql.functions import monotonically_increasing_id, sha2, concat_ws

# Option 1: Auto-increment (monotonically_increasing_id)
df_with_sk = df.withColumn("patient_sk", monotonically_increasing_id())

# Option 2: Hash-based surrogate key (deterministic, good for SCD2)
df_with_sk = df.withColumn(
    "patient_sk",
    sha2(concat_ws("||", col("patient_id"), col("effective_start_date").cast("string")), 256)
)
```

---

### 7. Partitioning Strategy in Delta

**Partitioning Guidelines:**

1. **Cardinality**: Partition by low-cardinality columns (10–1000 unique values)
   - Good: `year_month`, `region`, `status`, `event_type`
   - Bad: `patient_id` (millions of unique values → millions of tiny files)

2. **Query pattern**: Partition by columns in your most common `WHERE` clauses

3. **Partition size**: Aim for 1GB per partition (to align with Delta file sizes)
   - Too small → small file problem, overhead in listing files
   - Too large → less pruning benefit, slower individual writes

4. **Date partitioning**: Almost always partition time-series data by date

```python
# Good partitioning
df.write.format("delta") \
    .partitionBy("year_month") \     # Low cardinality
    .save("/delta/clinical_events")

# Then Z-ORDER high-cardinality filter columns
spark.sql("OPTIMIZE delta.`/delta/clinical_events` ZORDER BY (patient_id, provider_id)")
```

**Real Project Context:**  
In eCDP, we partitioned `clinical_events` by `year_month` (36 unique values for 3 years). This gave us 36 clean partitions, each ~55GB. For queries filtering on `patient_id` (within a month), Z-ordering on `patient_id` within each partition reduced file scan by 95%.

---

# SECTION 3: AZURE / CLOUD / CI-CD

## Theory Questions

### 1. ADF Pipelines, Triggers, Linked Services

**Pipelines:**
- Logical grouping of activities (Copy Data, Databricks Notebook, Stored Procedure, etc.)
- Support conditional execution, loops (ForEach), error handling

**Triggers:**
- **Schedule Trigger**: Runs pipeline at fixed times (cron-like)
- **Tumbling Window Trigger**: For time-sliced processing with dependency management
- **Event-based Trigger**: Fires when a file arrives in ADLS/Blob Storage
- **Manual Trigger**: On-demand execution

**Linked Services:**
- Connection definitions for external services (Oracle, ADLS, Databricks, Key Vault)
- Credentials stored in Azure Key Vault — linked service references the secret

**Real Project Context:**  
In eCDP, we used an Event-based trigger on ADLS: when Oracle pump files landed in `/raw/incoming/`, ADF triggered the Bronze ingestion pipeline. For scheduled daily jobs (SCD2 refresh), we used Schedule triggers at 01:00 AM IST. Linked services connected to Oracle via self-hosted Integration Runtime in the on-premise network.

---

### 2. Delta Live Tables (DLT)

Delta Live Tables is a declarative ETL framework on Databricks that manages:
- Pipeline orchestration
- Data quality enforcement (Expectations)
- Automatic schema inference
- Error handling and retry
- Auto-scaling

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

# Bronze table
@dlt.table(name="bronze_clinical_events", comment="Raw clinical events from Oracle")
def bronze_clinical_events():
    return (spark.read.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("/raw/clinical_events/"))

# Silver table with quality expectations
@dlt.table(name="silver_clinical_events")
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_date", "event_date >= '2000-01-01'")
def silver_clinical_events():
    return dlt.read("bronze_clinical_events") \
        .withColumn("event_date", col("event_date").cast("date")) \
        .withColumn("_silver_ts", current_timestamp())
```

---

### 7. CI/CD for Data Engineering

**Our CI/CD Setup (Databricks + GitHub Actions):**

```yaml
# .github/workflows/deploy_databricks.yml
name: Deploy to Databricks

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with: { python-version: '3.10' }
      - name: Install dependencies
        run: pip install pyspark pytest chispa
      - name: Run unit tests
        run: pytest tests/ -v

  deploy:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      - name: Deploy notebooks to Databricks
        uses: databricks/run-notebook@v1
        with:
          databricks-host: ${{ secrets.DATABRICKS_HOST }}
          databricks-token: ${{ secrets.DATABRICKS_TOKEN }}
          notebook-path: /Repos/prod/ecdp-pipelines
      - name: Deploy Databricks Jobs
        run: |
          databricks jobs reset --job-id $JOB_ID \
            --json @jobs/clinical_events_job.json
```

---

# SECTION 4: SQL & QUERY OPTIMIZATION

## Practical Questions

### 1. Rank Customers Based on Purchases

```sql
-- Window function to rank customers
SELECT 
    customer_id,
    customer_name,
    total_purchases,
    purchase_amount,
    RANK()        OVER (PARTITION BY region ORDER BY total_purchases DESC) AS rank_in_region,
    DENSE_RANK()  OVER (ORDER BY total_purchases DESC)                     AS overall_dense_rank,
    ROW_NUMBER()  OVER (ORDER BY purchase_amount DESC)                     AS row_num,
    NTILE(4)      OVER (ORDER BY total_purchases DESC)                     AS quartile,
    ROUND(
      100.0 * purchase_amount / SUM(purchase_amount) OVER (), 2
    ) AS pct_of_total
FROM (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.region,
        COUNT(o.order_id)      AS total_purchases,
        SUM(o.order_amount)    AS purchase_amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= DATEADD(year, -1, CURRENT_DATE)
    GROUP BY c.customer_id, c.customer_name, c.region
) ranked_customers
ORDER BY overall_dense_rank;
```

---

### 2. Optimize Slow SQL with Joins on Big Tables

```sql
-- ORIGINAL SLOW QUERY (problematic patterns highlighted)
SELECT p.patient_name, e.event_type, e.event_date, pr.provider_name
FROM clinical_events e
JOIN patients p ON UPPER(e.patient_id) = UPPER(p.patient_id)   -- Function on join key! Prevents index use
JOIN providers pr ON e.provider_id = pr.provider_id
WHERE YEAR(e.event_date) = 2024                                  -- Function on date! Prevents partition pruning
ORDER BY e.event_date;                                           -- Full sort on massive result set

-- OPTIMIZED QUERY
SELECT /*+ BROADCAST(pr) */                 -- Hint to broadcast small providers table
    p.patient_name, e.event_type, e.event_date, pr.provider_name
FROM clinical_events e                      -- Large partitioned table: scan only 2024 partition
JOIN patients p ON e.patient_id = p.patient_id   -- No function wrapping join key
JOIN providers pr ON e.provider_id = pr.provider_id
WHERE e.event_date >= '2024-01-01'          -- Range filter instead of YEAR() function
  AND e.event_date < '2025-01-01'           -- Enables partition pruning
ORDER BY e.event_date
LIMIT 10000;                                -- Avoid sorting billions of rows

-- Additional optimizations:
-- 1. Ensure patient_id is VARCHAR (not CHAR) consistently — CHAR pads with spaces → mismatch
-- 2. Run ANALYZE TABLE to update statistics for cost-based optimizer
-- 3. Add Z-ORDER on (patient_id, event_date) in Delta
-- 4. Cache providers table if queried repeatedly
```

---

# SECTION 5: DATA GOVERNANCE & QUALITY

## Scenario Questions

### 1. DQ Checks in Bronze → Silver Layer

```python
from pyspark.sql.functions import col, count, when, isNull, to_date, regexp_extract
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

class DataQualityChecker:
    
    def __init__(self, df: DataFrame, table_name: str):
        self.df = df
        self.table_name = table_name
        self.total_count = df.count()
        self.issues = []
    
    def check_null_rate(self, col_name: str, threshold: float = 0.05):
        """Fail if null rate exceeds threshold"""
        null_count = self.df.filter(col(col_name).isNull()).count()
        null_rate = null_count / self.total_count
        if null_rate > threshold:
            self.issues.append(f"NULL rate for {col_name}: {null_rate:.2%} exceeds {threshold:.2%}")
        logger.info(f"[DQ] {col_name} null rate: {null_rate:.2%}")
        return self
    
    def check_referential_integrity(self, col_name: str, ref_df: DataFrame, ref_col: str):
        """Check all values exist in reference table"""
        orphan_count = self.df.join(
            ref_df.select(ref_col), 
            self.df[col_name] == ref_df[ref_col], 
            "left_anti"
        ).count()
        if orphan_count > 0:
            self.issues.append(f"Referential integrity: {orphan_count} orphan {col_name} values")
        return self
    
    def check_date_range(self, col_name: str, min_date: str, max_date: str):
        """Check dates are within valid range"""
        invalid_count = self.df.filter(
            (col(col_name) < min_date) | (col(col_name) > max_date)
        ).count()
        if invalid_count > 0:
            self.issues.append(f"Date range violation: {invalid_count} records for {col_name}")
        return self
    
    def check_duplicates(self, key_cols: list):
        """Check for duplicate primary keys"""
        dup_count = self.df.groupBy(key_cols).count().filter(col("count") > 1).count()
        if dup_count > 0:
            self.issues.append(f"Duplicate keys: {dup_count} duplicated combinations of {key_cols}")
        return self
    
    def validate(self, fail_on_error: bool = True):
        """Run all checks and report"""
        if self.issues:
            report = f"\n[DQ REPORT] {self.table_name}:\n" + "\n".join(f"  ❌ {i}" for i in self.issues)
            if fail_on_error:
                raise ValueError(report)
            else:
                logger.warning(report)
        else:
            logger.info(f"[DQ] {self.table_name}: All checks passed ✅")

# Usage in Silver pipeline
dim_provider = spark.read.format("delta").load("/delta/silver/dim_provider")

(DataQualityChecker(silver_df, "clinical_events")
    .check_null_rate("patient_id", threshold=0.0)     # Zero tolerance for PK
    .check_null_rate("event_date", threshold=0.01)    # 1% tolerance
    .check_referential_integrity("provider_id", dim_provider, "provider_id")
    .check_date_range("event_date", "2000-01-01", "2025-12-31")
    .check_duplicates(["patient_id", "event_date", "event_type"])
    .validate(fail_on_error=True)
)
```

---

# SECTION 6: APACHE ICEBERG / DELTA / LAKEHOUSE

## Theory Questions

### 1. Delta vs Iceberg vs Hudi

| Feature | Delta Lake | Apache Iceberg | Apache Hudi |
|---------|-----------|----------------|-------------|
| Origin | Databricks | Netflix | Uber |
| ACID | Yes | Yes | Yes |
| Time Travel | Yes | Yes | Yes |
| Schema Evolution | Yes | Advanced | Yes |
| Partitioning | Physical | Hidden/Physical | Physical |
| Engine Support | Spark, Trino, Flink | Spark, Flink, Trino, Presto, Hive | Spark, Flink |
| Metadata | JSON transaction log | Metadata tree (JSON+Avro) | Timeline (commits) |
| Compaction | OPTIMIZE | REWRITE_DATA_FILES | COMPACT |
| Primary Use | Databricks ecosystem | Multi-engine lakehouse | Streaming upserts |

**Hidden Partitioning (Iceberg advantage):**
- Iceberg partitions are not exposed as physical directory structure
- Query predicates on source columns (e.g., `event_date`) are automatically translated to partition filters
- Eliminates the "wrong partition format" bug common in Hive/Delta

**Real Project Context:**  
We chose Iceberg in eCDP for the 20TB dataset because:
1. Multi-engine requirement: Spark for ingestion + Trino for BI queries + Flink for streaming
2. Hidden partitioning avoided the partition evolution pain we had in Hive
3. Iceberg's metadata tree scales better than Delta's flat JSON log for tables with billions of files

---

## Scenario Questions

### 1. How Did You Migrate 20 TB to Iceberg?

**Migration Strategy:**

```python
# Phase 1: Create Iceberg table with same schema
spark.sql("""
    CREATE TABLE iceberg_catalog.ecdp.clinical_events (
        patient_id      STRING,
        event_date      DATE,
        event_type      STRING,
        provider_id     STRING,
        facility_id     STRING,
        diagnosis_code  STRING,
        _ingestion_ts   TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (months(event_date))
    TBLPROPERTIES (
        'write.target-file-size-bytes' = '1073741824',
        'write.parquet.compression-codec' = 'snappy'
    )
""")

# Phase 2: Migrate in date-based batches (avoid one giant job)
from datetime import datetime, timedelta

start_date = datetime(2021, 1, 1)
end_date = datetime(2023, 12, 31)
batch_months = []

current = start_date
while current <= end_date:
    batch_months.append(current.strftime("%Y-%m"))
    current += timedelta(days=32)
    current = current.replace(day=1)

for year_month in batch_months:
    print(f"Migrating {year_month}...")
    
    batch_df = spark.read.format("delta") \
        .load("/delta/silver/clinical_events") \
        .filter(f"year_month = '{year_month}'")
    
    batch_df.writeTo("iceberg_catalog.ecdp.clinical_events") \
        .option("write.spark.fanout.enabled", "true") \
        .append()
    
    print(f"  ✅ {year_month}: {batch_df.count()} records migrated")

# Phase 3: Validate row counts match
delta_count = spark.read.format("delta").load("/delta/silver/clinical_events").count()
iceberg_count = spark.read.table("iceberg_catalog.ecdp.clinical_events").count()
assert delta_count == iceberg_count, f"Count mismatch: Delta={delta_count}, Iceberg={iceberg_count}"
print(f"✅ Migration validated: {iceberg_count:,} records")
```

**Challenges and Solutions:**
1. **Schema inconsistencies**: Source had mixed date formats → standardized in pre-migration transformation
2. **Long migration time**: Parallelized by month — ran 6 concurrent month migrations
3. **Validation**: Row count + hash-based spot checks on 1% sample
4. **Cutover**: Dual-write period (write to both Delta and Iceberg) for 1 week before cutover

---

# SECTION 7: KAFKA / REAL-TIME

## Practical Questions

### 1. Micro-batch Ingestion: Kafka → Delta

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, year, month
)
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToDelta") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Define schema for Kafka message value
event_schema = StructType([
    StructField("patient_id",    StringType(),    True),
    StructField("event_type",    StringType(),    True),
    StructField("event_date",    StringType(),    True),
    StructField("provider_id",   StringType(),    True),
    StructField("facility_id",   StringType(),    True),
    StructField("diagnosis_code",StringType(),    True),
    StructField("metadata",      MapType(StringType(), StringType()), True)
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "clinical_events_topic") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 100000) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .load()

# Parse Kafka message
parsed_df = kafka_df.select(
    col("offset"),
    col("partition").alias("kafka_partition"),
    col("timestamp").alias("kafka_timestamp"),
    from_json(col("value").cast("string"), event_schema).alias("data")
).select(
    col("data.*"),
    col("kafka_timestamp"),
    col("kafka_partition"),
    col("offset").alias("kafka_offset"),
    current_timestamp().alias("_ingestion_ts")
)

# Write to Delta in micro-batches
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime="2 minutes") \  # Micro-batch every 2 minutes
    .option("checkpointLocation", "abfss://checkpoints@ecpdadls.dfs.core.windows.net/kafka_events") \
    .option("mergeSchema", "true") \
    .partitionBy("event_date") \
    .start("abfss://data@ecpdadls.dfs.core.windows.net/bronze/streaming_events")

query.awaitTermination()
```

---

# SECTION 8: PROJECT DEEP DIVE

## eCDP Project Questions

### 1. Biggest Technical Challenge

**Challenge: 300 Dynamic SQL Scripts Optimization**

The legacy system had 300+ SQL scripts in Oracle PL/SQL, each with hardcoded date ranges, table names, and connection strings. Running them sequentially took 15+ hours per batch.

**Solution:**

```python
# Metadata-driven framework to parallelize SQL execution
import concurrent.futures
from dataclasses import dataclass
from typing import List

@dataclass
class SQLScript:
    script_id: str
    sql_template: str
    priority: int
    dependencies: List[str]
    target_table: str
    
class DynamicSQLEngine:
    
    def __init__(self, spark):
        self.spark = spark
        self.completed = set()
        
    def render_template(self, template: str, params: dict) -> str:
        """Replace placeholders with dynamic values"""
        for key, value in params.items():
            template = template.replace(f"{{{{  {key}  }}}}", str(value))
        return template
    
    def execute_script(self, script: SQLScript, params: dict) -> dict:
        """Execute a single rendered SQL and write to Delta"""
        try:
            rendered_sql = self.render_template(script.sql_template, params)
            result_df = self.spark.sql(rendered_sql)
            
            result_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(f"/delta/silver/{script.target_table}")
            
            return {"script_id": script.script_id, "status": "SUCCESS", 
                    "rows": result_df.count()}
        except Exception as e:
            return {"script_id": script.script_id, "status": "FAILED", "error": str(e)}
    
    def execute_parallel(self, scripts: List[SQLScript], params: dict, max_workers: int = 10):
        """Execute independent scripts in parallel"""
        # Group by priority (lower priority = run first)
        from collections import defaultdict
        priority_groups = defaultdict(list)
        for s in scripts:
            priority_groups[s.priority].append(s)
        
        results = []
        for priority in sorted(priority_groups.keys()):
            group = priority_groups[priority]
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self.execute_script, script, params): script
                    for script in group
                }
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    results.append(result)
                    self.completed.add(result["script_id"])
        return results

# Result: 300 scripts ran in 2.5 hours (vs 15+ hours sequential)
# Parallel groups of 10–15 scripts executing simultaneously
```

---

### 3. How Did You Reduce Batch Processing Time by 50%?

**Key Optimizations Applied:**

1. **Eliminated sequential script execution** → Parallel execution framework (above)
2. **Broadcast joins**: Replaced 8 SortMergeJoins with BroadcastHashJoins for dim tables < 1GB
3. **Predicate pushdown**: Fixed filter conditions to use partition columns directly
4. **Z-ordering**: Applied Z-ORDER BY (patient_id) on 5 highest-queried Silver tables
5. **AQE**: Enabled adaptive skew join handling for patient_id-based aggregations
6. **Caching**: Cached 3 dimension tables used across 50+ scripts
7. **OPTIMIZE**: Scheduled nightly file compaction to reduce scan overhead
8. **Partition tuning**: Adjusted shuffle partitions from default 200 → 600 based on data volume

**Measurement:**
- Before: 15.5 hours end-to-end batch
- After: 7.5 hours (52% reduction)
- Further improvements planned: vectorized UDFs, Photon engine on Databricks

---

### 5. How Did You Avoid 15+ Hours Downtime?

**Root Issue:** Schema migration of a 2TB production table required dropping and recreating columns — traditional approach would lock the table for 4–6 hours.

**Solution: Blue-Green Table Strategy**

```python
# Step 1: Create new "green" table with desired schema (while "blue" is still serving reads)
spark.sql("""
    CREATE TABLE delta.`/delta/silver/clinical_events_v2`
    DEEP CLONE delta.`/delta/silver/clinical_events`
""")

# Step 2: Apply schema changes to green table (no downtime on blue)
spark.sql("""
    ALTER TABLE delta.`/delta/silver/clinical_events_v2`
    ADD COLUMN new_column STRING
""")

# Step 3: Backfill new column in green table
spark.sql("""
    UPDATE delta.`/delta/silver/clinical_events_v2`
    SET new_column = derive_value(old_column)
""")

# Step 4: Validate green table
green_count = spark.read.format("delta").load("/delta/silver/clinical_events_v2").count()
blue_count  = spark.read.format("delta").load("/delta/silver/clinical_events").count()
assert green_count == blue_count

# Step 5: Atomic swap (update downstream jobs to point to v2)
# This is a metadata-only operation — near instantaneous
spark.sql("""
    ALTER TABLE delta.`/delta/silver/clinical_events`
    SET LOCATION '/delta/silver/clinical_events_v2'
""")

# Result: <30 minutes total downtime (just the metadata swap)
# vs 4-6 hours for in-place migration
```

---

## FinOps PoC Questions

### 1. XML → Hive Pipeline

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, trim
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("XMLToHive") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.16.0") \
    .enableHiveSupport() \
    .getOrCreate()

# Read XML using spark-xml library
xml_df = spark.read \
    .format("xml") \
    .option("rootTag", "FinancialReports") \
    .option("rowTag", "Report") \
    .option("attributePrefix", "_attr_") \
    .option("inferSchema", "true") \
    .load("/raw/finops_reports/*.xml")

# Flatten nested XML structure
flattened_df = xml_df.select(
    col("_attr_report_id").alias("report_id"),
    col("_attr_fiscal_year").alias("fiscal_year"),
    col("Header.Department").alias("department"),
    col("Header.CostCenter").alias("cost_center"),
    explode(col("LineItems.Item")).alias("item")
).select(
    col("report_id"),
    col("fiscal_year"),
    col("department"),
    col("cost_center"),
    col("item._attr_item_id").alias("item_id"),
    col("item.Category").alias("category"),
    col("item.Amount").cast("decimal(18,2)").alias("amount"),
    col("item.Currency").alias("currency")
)

# Write to Hive (ORC format for Hive compatibility)
flattened_df.write \
    .mode("overwrite") \
    .format("orc") \
    .saveAsTable("finops_db.financial_line_items")

# Register as external Hive table
spark.sql("""
    CREATE TABLE IF NOT EXISTS finops_db.financial_line_items_ext
    LIKE finops_db.financial_line_items
    LOCATION '/warehouse/finops/financial_line_items'
""")

print("✅ XML → Hive pipeline complete")
print(f"   Records: {flattened_df.count():,}")
```

### 2. How Did You Complete an 8-Week Job in 2.5 Weeks?

**Key factors:**

1. **Reused proven patterns**: Applied the same XML→Hive pattern from internal documentation instead of building from scratch
2. **Parallel workstreams**: While XML parsing was running, simultaneously built the Hive schema and validation scripts
3. **Automated testing**: Wrote Python unit tests using `chispa` for DataFrame equality — caught schema issues early
4. **Metadata-driven design**: Single configurable framework handled all 47 XML file types instead of 47 separate scripts
5. **Early stakeholder alignment**: Schema sign-off in week 1 prevented scope creep
6. **Daily delivery**: Shipped working components every day (not waiting for "done" milestone)

**Technical shortcuts that were sound:**
- Used `inferSchema=true` for initial prototype (refined with explicit schema for production)
- Leveraged Databricks Auto Loader for file detection instead of custom polling
- Used Delta as intermediate format before final Hive write (easier to debug and reprocess)

---

*End of Interview Preparation Guide*
*Prepared for Senior Data Engineer (Databricks/Azure) Role*
