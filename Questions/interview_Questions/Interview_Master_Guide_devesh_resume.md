# 🎯 Data Engineer Interview Master Guide — Mayuresh Patil
> **Personalized to your resume | AbbVie/Cognizant | 3+ Years PySpark, Iceberg, Azure**
> **Target: Senior/Mid-level Data Engineer Roles**

---

## Table of Contents
1. [Section 1 — Databricks & Spark (Core Focus)](#section-1--databricks--spark-core-focus)
2. [Section 2 — Data Modeling & Architecture](#section-2--data-modeling--architecture)
3. [Section 3 — Azure / Cloud / CI-CD](#section-3--azure--cloud--ci-cd)
4. [Section 4 — SQL & Query Optimization](#section-4--sql--query-optimization)
5. [Section 5 — Data Governance & Quality](#section-5--data-governance--quality)
6. [Section 6 — Apache Iceberg / Delta / Lakehouse](#section-6--apache-iceberg--delta--lakehouse)
7. [Section 7 — Kafka / Real-Time](#section-7--kafka--real-time)
8. [Section 8 — Project Deep Dive (Resume-Based)](#section-8--project-deep-dive-resume-based)
9. [Practical Coding Answers](#section-9--practical-coding-answers)

---

# SECTION 1 — Databricks & Spark (Core Focus)

---

## Theory Questions

---

### Q1. What is the architecture of Spark? Explain Driver, Executors, Cluster Manager.

**Answer:**

Spark follows a **Master-Worker architecture** with three core components:

```
┌──────────────────────────────────────────────────────────┐
│                    CLUSTER MANAGER                        │
│         (YARN / Kubernetes / Standalone / Databricks)     │
│   Allocates resources, manages executor lifecycle        │
└─────────────────────┬────────────────────────────────────┘
                      │ allocates
         ┌────────────▼────────────┐
         │       DRIVER NODE        │
         │  - Runs main() / your   │
         │    PySpark code         │
         │  - SparkContext         │
         │  - DAG Scheduler        │
         │  - Task Scheduler       │
         │  - Catalyst Optimizer   │
         └────────────┬────────────┘
                      │ sends tasks
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │Executor 0│  │Executor 1│  │Executor 2│
  │ - Cores  │  │ - Cores  │  │ - Cores  │
  │ - Memory │  │ - Memory │  │ - Memory │
  │ - Cache  │  │ - Cache  │  │ - Cache  │
  └──────────┘  └──────────┘  └──────────┘
```

**Driver:** The brain of the Spark application. It runs your PySpark code, creates the SparkContext, builds the DAG, applies the Catalyst Optimizer, and schedules tasks on executors. In Databricks, the driver is the cluster's master node.

**Executors:** Worker JVM processes launched on worker nodes. Each executor runs tasks assigned by the driver, stores cached data, and reports status back. Executors are long-lived for the duration of the application.

**Cluster Manager:** An external service that manages resources across machines. In Databricks, this is managed automatically. On-premise options include YARN, Kubernetes, or Spark's built-in standalone manager.

**Your resume connection:** In your AbbVie role, you tuned Spark configurations (executor memory, cores, parallelism) to achieve 50% reduction in processing time — this directly involves understanding executor resource allocation.

---

### Q2. Difference between RDD vs DataFrame vs Dataset.

**Answer:**

| Aspect | RDD | DataFrame | Dataset |
|---|---|---|---|
| API Level | Low-level | High-level | High-level |
| Type Safety | Yes (compile-time) | No (runtime errors) | Yes (compile-time) |
| Schema | No schema | Yes — enforced | Yes — enforced |
| Optimization | None (manual) | Catalyst Optimizer | Catalyst Optimizer |
| Language | Java, Scala, Python | All | Java, Scala only |
| Performance | Slowest | Fast | Fast |
| Serialization | Java/Kryo | Tungsten binary | Tungsten binary |
| Use in PySpark | Available | Primary | Not available (Python) |

**When to use RDD (still relevant):**
- Fine-grained control over physical data distribution
- Operations not expressible in DataFrame API
- Working with unstructured data (text files, binary)

**In practice (your AbbVie experience):** You work primarily with DataFrames in PySpark, which gives you Catalyst optimization + Tungsten memory management automatically. RDDs would be used for low-level operations like custom partitioners.

```python
# RDD — manual, no optimization
rdd = sc.parallelize([1, 2, 3]).map(lambda x: x * 2)

# DataFrame — optimized by Catalyst
df = spark.createDataFrame([(1,), (2,), (3,)], ["value"]) \
          .withColumn("doubled", F.col("value") * 2)
```

---

### Q3. Explain Catalyst Optimizer.

**Answer:**
Catalyst is Spark's **query optimization framework** — it transforms your logical query into the most efficient physical execution plan.

**Four Phases:**
1. **Parsing** → Your code/SQL becomes an Unresolved Logical Plan (AST)
2. **Analysis** → Catalog resolves column names, types → Resolved Logical Plan
3. **Logical Optimization** → Rule-based transformations:
   - Filter Pushdown (move filters closer to source)
   - Projection Pushdown (read only needed columns)
   - Constant Folding (evaluate `1+1` at plan time)
   - Predicate Simplification
4. **Physical Planning** → Multiple physical plans generated, CBO picks cheapest

**Key Catalyst rules that matter in your work:**
- **Predicate pushdown into Parquet/Iceberg** → skips row groups without reading them
- **Join reordering** → joins smaller tables first to minimize intermediate data
- **Broadcast detection** → auto-detects small tables for BroadcastHashJoin

---

### Q4. What is Tungsten? How does it help in performance?

**Answer:**
Tungsten is Spark's **physical execution engine** introduced in Spark 1.4. It operates below the DataFrame API level and provides three key optimizations:

**1. Off-Heap Memory Management**
- Manages memory directly in native memory (outside JVM heap)
- Avoids Java GC (Garbage Collection) pauses — a major source of Spark slowdowns
- Uses `sun.misc.Unsafe` for direct memory access
- Stores data in compact binary format (not Java objects)

**2. Cache-Aware Computation**
- Algorithms and data structures designed for CPU cache locality
- Sort and hash operations optimized to minimize cache misses
- Significant CPU efficiency improvement

**3. Whole-Stage Code Generation**
- Compiles a series of operators into a single optimized JVM bytecode function
- Eliminates virtual function calls between operators
- Visible in physical plan as `*(1) Filter`, `*(2) HashAggregate` — the `*` means Tungsten codegen is active

**Practical impact:** Tungsten is why DataFrames are 10-100x faster than RDDs — you get Tungsten automatically by using DataFrames.

---

### Q5. Explain wide vs narrow transformations.

*(Covered in depth in Guide 13 — summary here)*

**Narrow:** `filter`, `withColumn`, `select`, `coalesce` — no shuffle, partition works independently, single stage.

**Wide:** `groupBy`, `join` (SMJ), `repartition`, `distinct`, `orderBy` — shuffle required, stage boundary created, network-intensive.

**Your resume connection:** Every time you optimized Spark pipelines at AbbVie (50% reduction), a key lever was minimizing wide transformations — reducing shuffle by using broadcast joins where applicable and tuning partition counts.

---

### Q6. What is a shuffle? Why is it expensive?

**Answer:**
A shuffle is the **redistribution of data across all executors** over the network. It happens during wide transformations.

**Why expensive:**
1. **Disk I/O** — shuffle data is written to disk on the mapper side before being read on the reducer side (even with in-memory shuffle, spills happen)
2. **Network transfer** — all executors communicate with all others (all-to-all transfer)
3. **Serialization/Deserialization** — data is serialized to send over network, deserialized on receipt
4. **Sort** — Sort-Merge Join requires sorting after shuffle

**A shuffle involves:**
```
Map Phase (write):     Reduce Phase (read):
  Executor 0           Executor 0 reads its partitions from ALL mappers
  Executor 1    ───►   Executor 1 reads its partitions from ALL mappers
  Executor 2           Executor 2 reads its partitions from ALL mappers
  (write shuffle files)  (network transfer)
```

**Cost you've seen at AbbVie:** Large shuffles in your Iceberg pipelines (during joins on `cust_id`, `api_record_id`, etc.) would show up as high `Shuffle Read/Write` in Spark UI — reducing these via broadcast joins or partition tuning is exactly what gave you the 50% improvement.

---

### Q7. What is lazy evaluation?

**Answer:**
Spark is **lazy** — transformations do not execute immediately. Spark builds a **DAG (Directed Acyclic Graph)** of all transformations when you call them, but actual computation only starts when an **action** is triggered.

```python
# None of these run yet:
df = spark.read.parquet("s3://data/")    # No read
df2 = df.filter(F.col("age") > 25)       # No filter
df3 = df2.groupBy("city").count()         # No groupBy

# THIS triggers execution of all the above:
df3.show()   # ← Action → DAG executes
```

**Benefits:**
- Catalyst can see the entire pipeline and optimize globally (e.g., push filters before joins)
- Avoids computing intermediate results that might be discarded
- Enables pipelining of narrow transformations into single stages

**Actions that trigger execution:** `show()`, `count()`, `collect()`, `write()`, `save()`, `take()`

---

### Q8. What are Jobs, Stages, Tasks?

**Answer:**

```
Spark Application
└── Job (triggered by one Action)
    └── Stage (group of tasks with no shuffle between them)
        └── Task (one unit of work on one partition)
```

**Job:** Created every time an **action** is called (`count()`, `show()`, `write()`). One action = one job.

**Stage:** A group of transformations that can run without a shuffle between them. Every wide transformation (shuffle) creates a new stage boundary. A job with `filter → groupBy → join` has 3 stages (filter stage, groupBy stage, join stage).

**Task:** The smallest unit of execution. One task processes **one partition**. If you have 200 partitions, a stage has 200 tasks. Tasks run in parallel across executor cores.

**Real connection:** When you saw your AbbVie jobs taking long, you'd look at Spark UI → Jobs → Stages → Tasks. A stage with 1 task running while 199 finish = data skew. That's what you'd fix with salting or broadcast joins.

---

### Q9. What is broadcast join? When should you use it?

*(Covered in depth in Guide 11 — summary)*

**Broadcast Join:** The entire small table is sent to every executor. The large table is never shuffled. Each executor joins its local chunk against the local copy of the small table.

**Use when:** One table is small enough to fit in executor memory (< autoBroadcastJoinThreshold, default 10 MB — tune up to 100 MB+).

**Immune to data skew** — large table is not repartitioned by join key.

```python
df_large.join(F.broadcast(df_small), "key", "inner")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)  # 100 MB
```

**Your resume connection:** In your API ingestion pipelines at AbbVie, reference/lookup tables (product codes, facility IDs from Veeva/Ravex) would be perfect candidates for broadcast joins.

---

### Q10. What is Delta Lake? Explain ACID in Delta.

**Answer:**
Delta Lake is an **open-source storage layer** that adds ACID transactions, schema enforcement, time travel, and DML (UPDATE/DELETE/MERGE) to data lakes (S3, ADLS, HDFS).

**ACID in Delta:**

| Property | How Delta Implements It |
|---|---|
| **Atomicity** | All changes in a transaction either fully commit or fully roll back. Delta uses the `_delta_log` (transaction log) — either the commit entry is written or it isn't. |
| **Consistency** | Schema enforcement ensures every write matches the table schema. Invalid writes are rejected before data is written. |
| **Isolation** | Optimistic concurrency control — multiple writers can proceed; conflicts are detected at commit time. Readers always see a consistent snapshot. |
| **Durability** | Once committed to the `_delta_log`, changes are permanent — even if the cluster crashes. The log is the source of truth. |

**Delta's `_delta_log` mechanism:**
```
s3://my-delta-table/
  _delta_log/
    00000000000000000000.json   ← Commit 0: initial write
    00000000000000000001.json   ← Commit 1: MERGE INTO
    00000000000000000002.json   ← Commit 2: OPTIMIZE
    00000000000000000010.checkpoint.parquet  ← Checkpoint (every 10 commits)
  part-00000-xxx.parquet
  part-00001-xxx.parquet
```

Every operation (write, update, delete, optimize) writes a new JSON entry to `_delta_log`. Reading the log gives the current state of the table.

---

### Q11. What is Z-Ordering? When should you apply it?

**Answer:**
Z-Ordering is a **multi-dimensional data clustering technique** used in Delta Lake and Iceberg to co-locate related data within Parquet files. It improves query performance by enabling Parquet's min/max statistics to skip more files.

**How it works:**
Regular partitioning clusters by one column. Z-ordering uses a space-filling curve (Z-curve) to interleave the sort orders of multiple columns — so data with similar values across multiple columns ends up in the same files.

```sql
OPTIMIZE my_delta_table
ZORDER BY (customer_id, event_date);
```

**When to apply:**
- You have high-cardinality columns that are frequently filtered (e.g., `customer_id`, `product_id`)
- Columns are too high-cardinality to partition on (would create millions of tiny directories)
- Queries filter on 2–3 columns together frequently

**When NOT to apply:**
- Low cardinality columns that are already partition columns
- Columns never used in filter conditions
- Very small tables (overhead isn't worth it)

**Your resume connection:** For your 20 TB Iceberg dataset at AbbVie, Z-ordering on `cust_id` or `api_record_id` would help skip files when API-ingested records are queried by these IDs.

---

### Q12. What is OPTIMIZE in Delta?

**Answer:**
`OPTIMIZE` is a Delta Lake maintenance command that **compacts many small Parquet files into fewer larger files** (typically targeting 1 GB files).

```sql
OPTIMIZE my_delta_table;                      -- Compact all files
OPTIMIZE my_delta_table WHERE date = '2024-01'; -- Compact specific partition
OPTIMIZE my_delta_table ZORDER BY (col1, col2); -- Compact + Z-order
```

**Why needed:** Spark writes create many small files (one per task per partition). Queries reading thousands of tiny files have high overhead (file open/close, metadata listing). OPTIMIZE merges them into fewer, larger files that are faster to scan.

**Bin-packing:** OPTIMIZE uses a bin-packing algorithm to group small files into target-size bins without sorting (unless ZORDER is specified).

**Trigger strategy in production:**
```python
# Databricks — schedule OPTIMIZE after heavy ingestion
spark.sql("OPTIMIZE schema.table ZORDER BY (cust_id, event_date)")

# Or use Delta's Auto Optimize (Databricks feature)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

---

### Q13. What is VACUUM in Delta? What does retention threshold mean?

**Answer:**
`VACUUM` removes **old Parquet files that are no longer referenced** by the `_delta_log`. These orphaned files accumulate from updates, deletes, and compaction.

```sql
VACUUM my_delta_table;                     -- Default: 7-day retention
VACUUM my_delta_table RETAIN 168 HOURS;   -- Explicit 7-day retention
VACUUM my_delta_table RETAIN 0 HOURS;     -- ⚠️ Dangerous — loses time travel
```

**Retention Threshold:**
Delta retains data files for at least `delta.deletedFileRetentionDuration` (default: 7 days / 168 hours). This period enables **time travel** — querying historical versions of the table.

```sql
-- Time travel requires files to still exist
SELECT * FROM my_table VERSION AS OF 5;
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-15';
```

If you VACUUM with 0-hour retention, old versions are permanently lost — time travel becomes impossible for those versions.

**Production recommendation:**
- Keep default 7-day retention for most tables
- For compliance tables (GDPR audit trails): extend to 30+ days
- Schedule VACUUM weekly after OPTIMIZE

```python
# Safe VACUUM
spark.sql("VACUUM schema.table RETAIN 168 HOURS")  # 7 days

# Check what would be deleted (dry run)
spark.sql("VACUUM schema.table RETAIN 168 HOURS DRY RUN")
```

---

### Q14. How does schema enforcement vs schema evolution work in Delta?

**Answer:**

**Schema Enforcement (default):**
Delta rejects writes that don't match the existing table schema. It protects data quality by preventing accidental schema changes.

```python
# If table has columns [id, name, age]
# Writing a DF with [id, name, age, email] → FAILS with AnalysisException
df_new.write.format("delta").mode("append").save("s3://my-delta-table/")
# AnalysisException: A schema mismatch detected when writing to the Delta table
```

**Schema Evolution:**
When you want Delta to **automatically update** the table schema to accommodate new columns:

```python
# Appending with new columns — Delta adds new columns to table schema
df_new.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("s3://my-delta-table/")

# For overwrite with schema change
df_new.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save("s3://my-delta-table/")
```

**What schema evolution handles:**
- ✅ Adding new columns
- ✅ Widening data types (INT → LONG)
- ❌ Removing columns (use overwriteSchema)
- ❌ Renaming columns (Delta doesn't rename — must use overwriteSchema)

**Your resume connection:** In your API ingestion pipelines from Veeva/Ravex/Smartsheet, API response schemas may evolve over time. Using `mergeSchema=true` protects your Iceberg/Delta pipelines from failing when new fields appear.

---

### Q15. What is checkpointing in Spark?

**Answer:**
Checkpointing **breaks the lineage** of an RDD/DataFrame by materializing it to stable storage (HDFS, S3, DBFS). Unlike caching (in-memory), checkpointed data persists to disk and survives executor failures.

**Two types:**

**1. RDD Checkpointing:**
```python
sc.setCheckpointDir("s3://checkpoints/")
rdd.checkpoint()   # Saves to S3; lineage cleared
rdd.count()        # Triggers checkpoint write
```

**2. Streaming Checkpointing (Structured Streaming):**
```python
df_stream.writeStream \
    .option("checkpointLocation", "s3://checkpoints/streaming/") \
    .start()
# Saves: offsets, state, progress — enables exactly-once semantics
```

**Why checkpoint vs cache:**
- Cache: in-memory, lost on executor failure → recomputed from lineage
- Checkpoint: on disk, survives failures → no recomputation needed
- Use checkpoint for: long lineage chains (iterative ML), streaming jobs, long-running pipelines where executor failure is likely

---

### Q16. Explain caching vs checkpointing vs persistence.

**Answer:**

| Feature | `cache()` | `persist(StorageLevel)` | `checkpoint()` |
|---|---|---|---|
| Storage | Memory (default: MEMORY_AND_DISK) | Configurable (memory/disk/both) | Stable storage (HDFS/S3) |
| Lineage | Preserved | Preserved | **Broken** (truncated) |
| Survives executor failure | Partial (recomputes) | Partial | Yes — reads from disk |
| Lazy | Yes | Yes | Yes |
| Use case | Reused DataFrames | Fine-grained storage control | Long lineage, streaming |
| Unpersist | `df.unpersist()` | `df.unpersist()` | Cannot unpersist |

*(See Guide 09 for full deep dive on Storage Levels)*

---

### Q17. What does Adaptive Query Execution (AQE) do?

*(Covered in depth in Guide 11 — summary)*

AQE re-optimizes the query plan **at runtime** using actual execution statistics (real partition sizes, real row counts) after each shuffle stage. Three key features:
1. **Dynamic partition coalescing** — merges empty/tiny partitions
2. **Join strategy switching** — SMJ → BroadcastHashJoin if small side is discovered at runtime
3. **Skew join handling** — auto-splits skewed partitions

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

---

### Q18. What is Cluster Mode vs Client Mode?

**Answer:**

| Aspect | Client Mode | Cluster Mode |
|---|---|---|
| Driver location | On the machine submitting the job | On a worker node inside the cluster |
| Use case | Interactive (Jupyter, Databricks notebooks) | Production batch jobs |
| Network | Driver must maintain connection to cluster | Driver runs inside cluster (no external connection needed) |
| If client disconnects | Job fails | Job continues |
| Logs | Printed to local terminal | Must retrieve from cluster logs |
| Databricks | Default for interactive notebooks | Used for Jobs/Workflows |

```bash
# Client mode (default for spark-submit from local machine)
spark-submit --master yarn --deploy-mode client my_job.py

# Cluster mode (production — driver runs on cluster)
spark-submit --master yarn --deploy-mode cluster my_job.py
```

**Your context:** In Databricks (your AbbVie environment), interactive notebooks use client mode by default. Your production ADF-triggered Databricks jobs run in cluster mode.

---

### Q19. How does Spark handle faults internally?

**Answer:**

**Task Failure:**
- Spark retries failed tasks automatically (default: 4 attempts via `spark.task.maxFailures`)
- The DAG lineage allows recomputing lost partitions from the original data source
- If a partition is cached/checkpointed, only that partition is recomputed from cache

**Executor Failure:**
- Cluster Manager detects executor loss (heartbeat timeout)
- Tasks running on that executor are re-scheduled on surviving executors
- Cached data on the failed executor is lost → recomputed from lineage
- Shuffle files lost → the entire shuffle stage is re-run

**Driver Failure:**
- Job fails — driver is the single point of failure
- Mitigation: Structured Streaming checkpointing (resumes from last checkpoint)
- Databricks: Driver restart policies available for Jobs

**Lineage as fault tolerance:**
```
Source → filter → join → groupBy → [Partition 3 lost]
                                   ↑
Spark recomputes only Partition 3 by re-running:
Source(partition 3) → filter → join → groupBy → Partition 3 recovered
```

This is why RDDs/DataFrames track their full lineage — it's the recovery mechanism.

---

### Q20. What is Dynamic Partition Pruning?

*(Covered in depth in Guide 12 — summary)*

DPP allows Spark to use runtime filter values from a small table to prune partitions of a large partitioned table before scanning it. Instead of reading all partitions, Spark reads only the partitions whose values match the small table's filter results.

**Requires:** Large table must be physically partitioned; join/filter on the partition column; inner/semi join.

**Confirm in plan:** Look for `dynamicpruningexpression` in `PartitionFilters`.

---

# SECTION 2 — Data Modeling & Architecture

---

### Q1. Data Lake vs Data Warehouse vs Lakehouse?

| Aspect | Data Lake | Data Warehouse | Lakehouse |
|---|---|---|---|
| Data format | Raw (any format) | Structured only | Raw + structured |
| Schema | Schema-on-read | Schema-on-write | Schema-on-write (enforced) |
| ACID | No | Yes | Yes (Delta/Iceberg) |
| Cost | Low (object storage) | High (proprietary) | Low (object storage) |
| Use case | ML, exploration | BI, reporting | Both |
| Examples | S3 + Parquet | Snowflake, Redshift | Delta Lake, Iceberg |
| Your stack | S3 + Iceberg | Oracle | S3 + Iceberg (Lakehouse) |

**Your answer:** "At AbbVie, we implemented a Lakehouse architecture using Apache Iceberg on S3. This gave us the cost efficiency of a data lake with ACID transactions, schema enforcement, and time travel — properties traditionally only available in data warehouses."

---

### Q2. Star Schema vs Snowflake Schema?

**Star Schema:**
```
         [Date Dim]
              |
[Customer] — [FACT TABLE] — [Product]
              |
         [Location]
```
- Dimension tables directly connected to fact table
- Denormalized dimensions — faster reads, more storage
- Fewer joins — better for OLAP queries

**Snowflake Schema:**
```
[City] → [Location] → [FACT] → [Category] → [Product]
```
- Normalized dimensions — sub-dimensions broken out
- More joins — slower reads, less storage
- Better for storage efficiency

**Your recommendation:** Star schema for analytics (fewer joins = faster). Snowflake when storage is a constraint or dimensions have shared sub-dimensions.

---

### Q3. What is Dimensional Modeling?

Dimensional modeling is a data modeling technique for analytical databases that organizes data into **facts** and **dimensions**.

- **Fact Table:** Measurable business events (sales, clicks, transactions). Contains metrics (revenue, quantity) and foreign keys to dimensions.
- **Dimension Table:** Descriptive context (who, what, when, where). Contains attributes (customer name, product category, date).

**Your AbbVie context:** In the eCDP (enterprise Customer Data Platform) project, the fact table would be patient interactions/events, and dimensions would be patient, facility, HCP, date.

---

### Q4. What is a Surrogate Key?

A **surrogate key** is a system-generated, meaningless primary key (usually an integer or UUID) used instead of a natural/business key.

```python
# Adding surrogate key in PySpark
from pyspark.sql.functions import monotonically_increasing_id, md5, concat_ws

# Method 1: Monotonically increasing ID (not globally unique across runs)
df.withColumn("surrogate_key", monotonically_increasing_id())

# Method 2: Hash-based (stable across runs — preferred for SCD Type 2)
df.withColumn("surrogate_key", md5(concat_ws("||", "natural_key_col1", "natural_key_col2")))
```

**Why surrogate keys:** Natural keys can change (customer email changes), be nullable, or be composite — making them poor join candidates. Surrogate keys are stable, indexed, and join-efficient.

---

### Q5. OLTP vs OLAP?

| Aspect | OLTP | OLAP |
|---|---|---|
| Purpose | Transactional operations | Analytics/reporting |
| Query type | Short, frequent, row-level | Long, complex, aggregations |
| Data volume | Small (per transaction) | Large (historical) |
| Schema | Normalized (3NF) | Denormalized (star/snowflake) |
| Examples | Oracle, MySQL, PostgreSQL | Snowflake, Redshift, BigQuery |
| Your usage | Oracle (source in your pipelines) | Iceberg + Spark (your target) |

---

### Q6. How do you decide which column to partition on?

**Answer (key interview question):**

**Good partition column characteristics:**
- **Low to medium cardinality** (10–10,000 distinct values) — avoid millions of tiny directories
- **Frequently used in WHERE clauses** — so partition pruning triggers
- **Even distribution** — avoid skewed partitions (one date with 90% of data)
- **Monotonically growing** — `date`, `year_month` are ideal for time-series data

```python
# Good: partition by date (low cardinality, always filtered)
df.write.partitionBy("event_date").parquet("s3://data/events/")

# Bad: partition by user_id (too high cardinality → millions of folders)
df.write.partitionBy("user_id").parquet("s3://data/events/")  # ❌
```

**Your AbbVie context:** Your CSV/TXT/XML files from S3 would be partitioned by `load_date` or `source_system` — columns that are always filtered in downstream queries.

---

### Q7. What happens if your partition column has high cardinality?

**Answer:**
High cardinality partitioning creates the **small file problem**:
- Millions of partition directories → massive metadata overhead
- Each partition has very few files → Spark launches one task per tiny file
- Metastore/catalog becomes a bottleneck (listing millions of directories)
- S3 LIST operations become expensive

**Solutions:**
- **Reduce cardinality:** Instead of `user_id` (millions), use `date` or `date + region`
- **Hierarchical partitioning:** `year=2024/month=01/` instead of `date=2024-01-15/`
- **Z-ordering instead:** Use Z-ORDER BY for high-cardinality columns within coarser partitions
- **Bucket by the high-cardinality column:** `bucketBy(200, "user_id")` in Hive-style tables

---

# SECTION 3 — Azure / Cloud / CI-CD

---

### Q1. What are ADF Pipelines? Triggers? Linked Services?

**ADF (Azure Data Factory)** is Azure's cloud ETL/orchestration service.

**Pipelines:** A logical grouping of activities (Copy Data, Databricks Notebook, Stored Procedure, Web Activity, etc.) that together perform a data engineering task.

**Triggers:**
- **Schedule Trigger:** Runs pipeline on a cron schedule
- **Tumbling Window:** Fixed-size, non-overlapping time windows — useful for backfilling
- **Event-Based:** Fires when a file arrives in ADLS (blob created/deleted event)
- **Manual:** On-demand execution

**Linked Services:** Connection definitions (like JDBC connection strings or API endpoints). They are ADF's equivalent of connection managers — they define HOW to connect to a data store (S3, Oracle, Databricks, REST API).

**Your experience:** Your AbbVie role involves ADF triggers for ingestion workflows from Veeva/Ravex APIs and S3 file-based ingestion — you'd use Event-Based triggers for file arrivals and Schedule Triggers for API polling.

---

### Q2. Difference between Data Factory vs Databricks Workflows?

| Aspect | Azure Data Factory | Databricks Workflows |
|---|---|---|
| Orchestration | GUI + JSON pipelines | Code-first (YAML/UI) |
| Compute | External (Databricks, SQL, etc.) | Databricks clusters |
| Transformations | Limited (no native Spark) | Full PySpark / SQL / DBT |
| Monitoring | ADF Monitor UI | Databricks Jobs UI |
| CI/CD | ARM templates / Git | YAML + Databricks CLI |
| Best for | Cross-system orchestration | Databricks-native pipelines |
| Your stack | ADF triggers Databricks notebooks | Databricks Workflows for complex Spark jobs |

**Common pattern:** ADF handles ingestion triggers and copies data from source to ADLS. Databricks Workflows handle the transformation (Bronze → Silver → Gold medallion).

---

### Q3. What is CI/CD? What tools have you used?

**Answer:**
CI/CD (Continuous Integration / Continuous Deployment) automates the build, test, and deployment of code to ensure reliable, repeatable releases.

**Your stack (from resume):**
- **Version Control:** Git + GitHub
- **CI/CD tools:** GitHub Actions (Databricks deployment)

**Databricks CI/CD pattern:**
```yaml
# .github/workflows/deploy.yml
name: Deploy to Databricks
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install Databricks CLI
        run: pip install databricks-cli
      - name: Deploy notebooks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks workspace import_dir ./notebooks /Shared/production --overwrite
      - name: Update job config
        run: |
          databricks jobs reset --job-id 12345 --json-file job_config.json
```

**Key practices:**
- Keep secrets in GitHub Secrets (never in code) + Azure Key Vault
- Branch strategy: `dev` → `staging` → `main` (with PR reviews)
- Unit tests for PySpark (pytest + pyspark testing framework)

---

# SECTION 4 — SQL & Query Optimization

---

### Q1. Difference between Window Functions vs Aggregate Functions?

| Aspect | Aggregate Functions | Window Functions |
|---|---|---|
| Rows returned | One row per group | One row per input row |
| Syntax | `GROUP BY` | `OVER (PARTITION BY ... ORDER BY ...)` |
| Access to individual rows | No | Yes |
| Use case | Totals, counts | Rankings, running totals, LAG/LEAD |

```sql
-- Aggregate: collapses rows
SELECT city, COUNT(*) as cnt FROM customers GROUP BY city;

-- Window: preserves rows
SELECT city, customer_id,
       COUNT(*) OVER (PARTITION BY city) as city_count,
       RANK() OVER (PARTITION BY city ORDER BY revenue DESC) as rank
FROM customers;
```

---

### Q2. Write SQL to rank customers based on purchases.

```sql
-- Rank customers by total purchases within each region
SELECT
    customer_id,
    customer_name,
    region,
    total_purchases,
    RANK() OVER (PARTITION BY region ORDER BY total_purchases DESC) AS purchase_rank,
    DENSE_RANK() OVER (PARTITION BY region ORDER BY total_purchases DESC) AS dense_rank,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_purchases DESC) AS row_num
FROM (
    SELECT
        c.customer_id,
        c.customer_name,
        c.region,
        SUM(o.purchase_amount) AS total_purchases
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name, c.region
) ranked_customers
ORDER BY region, purchase_rank;
```

**RANK vs DENSE_RANK vs ROW_NUMBER:**
- `RANK()`: 1, 2, 2, 4 (skips after tie)
- `DENSE_RANK()`: 1, 2, 2, 3 (no skip)
- `ROW_NUMBER()`: 1, 2, 3, 4 (unique, no ties)

---

### Q3. Convert correlated subquery into a JOIN.

```sql
-- Correlated subquery (slow — runs once per row)
SELECT customer_id, name
FROM customers c
WHERE total_spend > (
    SELECT AVG(total_spend) FROM customers WHERE region = c.region
);

-- Optimized: JOIN with pre-aggregated subquery
SELECT c.customer_id, c.name
FROM customers c
JOIN (
    SELECT region, AVG(total_spend) AS avg_spend
    FROM customers
    GROUP BY region
) regional_avg
ON c.region = regional_avg.region
WHERE c.total_spend > regional_avg.avg_spend;
```

---

# SECTION 5 — Data Governance & Quality

---

### Q1. Common DQ Checks (Null, Referential, etc.)

```python
from pyspark.sql import functions as F

def run_dq_checks(df, table_name):
    results = {}

    # Null checks
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        results[f"null_{col}"] = null_count

    # Duplicate check
    total = df.count()
    distinct = df.dropDuplicates(["primary_key_col"]).count()
    results["duplicate_count"] = total - distinct

    # Referential integrity check
    # results["orphan_records"] = df.join(dim_table, "key", "left_anti").count()

    # Range/business rule checks
    results["negative_amount"] = df.filter(F.col("amount") < 0).count()
    results["future_dates"]    = df.filter(F.col("event_date") > F.current_date()).count()

    # Schema check
    results["expected_columns"] = all(c in df.columns for c in ["id", "name", "date"])

    for check, result in results.items():
        status = "❌ FAIL" if (isinstance(result, int) and result > 0) or result == False else "✅ PASS"
        print(f"{status} | {check}: {result}")

    return results
```

---

### Q2. If CDC ingestion duplicates records, how do you fix it?

**Answer:**
CDC (Change Data Capture) can produce duplicates when:
- The same event is published multiple times to the message queue
- Network retries cause re-delivery
- Exactly-once semantics aren't configured

**Fix using Delta MERGE (upsert):**
```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://data/delta/target/")

# CDC stream has: operation (INSERT/UPDATE/DELETE), record_key, updated_at
df_cdc = spark.read.parquet("s3://data/cdc/incoming/")

# De-duplicate CDC events first (keep latest per key)
from pyspark.sql.window import Window
window = Window.partitionBy("record_key").orderBy(F.desc("updated_at"))
df_cdc_deduped = df_cdc.withColumn("rn", F.row_number().over(window)) \
                        .filter(F.col("rn") == 1).drop("rn")

# Merge into target
delta_table.alias("target").merge(
    df_cdc_deduped.alias("source"),
    "target.record_key = source.record_key"
).whenMatchedUpdate(set={
    "target.col1": "source.col1",
    "target.updated_at": "source.updated_at"
}).whenNotMatchedInsertAll().execute()
```

---

# SECTION 6 — Apache Iceberg / Delta / Lakehouse

---

### Q1. Delta vs Iceberg vs Hudi

| Feature | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| Creator | Databricks | Netflix | Uber |
| Metadata | `_delta_log` (JSON+Parquet) | Manifest + snapshot files | Timeline (.commit files) |
| ACID | Yes | Yes | Yes |
| Time Travel | Yes | Yes | Yes |
| Schema Evolution | Yes | Yes (advanced) | Yes |
| Hidden Partitioning | No | **Yes** (key differentiator) | No |
| Engine support | Spark, Trino, Flink | Spark, Flink, Trino, Hive, Presto | Spark, Flink |
| Partition Evolution | Limited | **Yes** (change without rewrite) | Limited |
| Your stack | Azure/Databricks context | AbbVie production | Less common |

**Why your team chose Iceberg at AbbVie:**
"Iceberg's hidden partitioning meant our query layer didn't need to know partition layouts — filters on `event_date` automatically benefited from partition pruning without hardcoding partition columns into every query. Partition evolution also let us change strategies without migrating 20 TB of data."

---

### Q2. How does Iceberg Handle Metadata?

**Answer:**
Iceberg uses a **hierarchical metadata** structure:

```
Iceberg Table
└── Current Metadata Pointer (metadata/v3.metadata.json)
    └── Snapshot (latest commit)
        └── Manifest List (lists all manifest files)
            ├── Manifest File 1 (tracks data files + statistics)
            │   ├── data/part-00001.parquet
            │   └── data/part-00002.parquet
            └── Manifest File 2
                ├── data/part-00003.parquet
                └── data/part-00004.parquet
```

**Key advantages over Hive-style:**
- **File-level statistics** (min/max per column per file) → Iceberg can skip files without listing directories
- **Snapshot isolation** → readers always see a consistent snapshot
- **No directory listing** → O(1) metadata lookup vs O(n) S3 LIST operations
- **Partition evolution** → change partition spec without rewriting data

---

### Q3. Explain Iceberg Partitioning & Hidden Partitions.

**Answer:**

**Traditional (Hive-style) partitioning:**
```sql
-- Writer must write to correct directory
INSERT INTO table PARTITION (event_date='2024-01-15') ...

-- Reader must know partition column name
SELECT * FROM table WHERE event_date = '2024-01-15';
-- If you write WHERE date = '2024-01-15' → full scan! (column name mismatch)
```

**Iceberg Hidden Partitioning:**
```sql
-- Define partition transforms, not just columns
CREATE TABLE events (
    event_id BIGINT,
    user_id STRING,
    event_ts TIMESTAMP,
    amount DOUBLE
) USING iceberg
PARTITIONED BY (days(event_ts));   -- Hidden transform: extract date from timestamp
```

Now Spark automatically:
- Derives the partition value from `event_ts` at write time
- Applies partition pruning from any filter on `event_ts` (e.g., `WHERE event_ts BETWEEN ...`)
- No need to create a separate `event_date` column

**Partition transforms available:**
- `identity(col)` — standard value partitioning
- `year(col)`, `month(col)`, `day(col)`, `hour(col)` — time-based
- `bucket(N, col)` — hash bucket partitioning
- `truncate(L, col)` — string prefix partitioning

**Partition Evolution:**
```sql
-- Change from daily to hourly partitioning (no data rewrite!)
ALTER TABLE events REPLACE PARTITION FIELD days(event_ts) WITH hours(event_ts);
-- New files use hourly partitioning; old files use daily — both work seamlessly
```

---

# SECTION 7 — Kafka / Real-Time

---

### Q1. How does Kafka Work Internally?

**Answer:**

```
Producer → [Topic: user-events]
              ├── Partition 0: [offset 0][offset 1][offset 2]...
              ├── Partition 1: [offset 0][offset 1][offset 2]...
              └── Partition 2: [offset 0][offset 1][offset 2]...
                                    ↑
                              Consumer Group A
                              (each partition consumed by one consumer)
```

**Key concepts:**
- **Topic:** Named stream of records (like a table in a database)
- **Partition:** Ordered, immutable sequence of records. Parallelism unit — more partitions = more consumer parallelism
- **Offset:** Unique ID of a record within a partition. Consumers track offsets to know where they left off
- **Consumer Group:** A group of consumers sharing partition consumption. Each partition assigned to exactly one consumer in the group — enables load balancing
- **Replication:** Partitions replicated across brokers for fault tolerance

---

### Q2. Write a micro-batch ingestion job: Kafka → Delta.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder \
    .appName("KafkaToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# Define schema of the Kafka message value
event_schema = StructType([
    StructField("event_id",   StringType(), True),
    StructField("user_id",    StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount",     LongType(),   True),
    StructField("event_ts",   StringType(), True)
])

# Read from Kafka (streaming)
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "user-events")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON value
df_parsed = (
    df_kafka
    .select(
        F.from_json(F.col("value").cast("string"), event_schema).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition"),
        F.col("offset")
    )
    .select("data.*", "kafka_timestamp", "partition", "offset")
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("load_date", F.to_date(F.current_timestamp()))
)

# Write to Delta (micro-batch)
def write_to_delta(batch_df, batch_id):
    """Upsert micro-batch to Delta — handles duplicates via MERGE."""
    from delta.tables import DeltaTable

    target_path = "s3://data/delta/user_events/"

    if DeltaTable.isDeltaTable(spark, target_path):
        dt = DeltaTable.forPath(spark, target_path)
        dt.alias("target").merge(
            batch_df.alias("source"),
            "target.event_id = source.event_id"
        ).whenNotMatchedInsertAll().execute()
    else:
        batch_df.write.format("delta") \
            .partitionBy("load_date") \
            .mode("overwrite") \
            .save(target_path)

query = (
    df_parsed.writeStream
    .foreachBatch(write_to_delta)
    .option("checkpointLocation", "s3://checkpoints/user_events/")
    .trigger(processingTime="60 seconds")   # micro-batch every 60s
    .start()
)

query.awaitTermination()
```

---

# SECTION 8 — Project Deep Dive (Resume-Based)

---

### Q1. What was the biggest technical challenge at AbbVie and how did you solve it?

**Suggested Answer (tailored to your resume):**

"One of the biggest challenges was building robust API ingestion pipelines for Veeva and Ravex APIs that had rate limits and unpredictable response times. Initially, sequential API calls were taking several hours to complete data extraction.

I analyzed the API's rate limit structure and identified that calls to different entity endpoints were independent. I implemented **multithreaded parallel API calls using Python's `concurrent.futures.ThreadPoolExecutor`**, where I grouped API calls by entity type and executed them in parallel batches, respecting rate limits per batch.

This reduced ingestion time significantly — enough to earn the Emerging Talent Award in April 2025. I also implemented robust error handling with exponential backoff for rate limit responses (HTTP 429), and tracked execution using Modak Nabu for auditability."

```python
# Multithreaded API ingestion pattern you implemented
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time

def fetch_entity(entity_id, api_config):
    """Fetch one entity from API with retry logic."""
    for attempt in range(3):
        try:
            resp = requests.get(
                f"{api_config['base_url']}/entity/{entity_id}",
                headers=api_config['headers'],
                timeout=30
            )
            if resp.status_code == 429:  # Rate limited
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            resp.raise_for_status()
            return entity_id, resp.json()
        except Exception as e:
            if attempt == 2:
                return entity_id, None

def parallel_api_ingestion(entity_ids, api_config, max_workers=10):
    """Parallel API calls with thread pool."""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_entity, eid, api_config): eid
                   for eid in entity_ids}
        for future in as_completed(futures):
            entity_id, data = future.result()
            if data:
                results[entity_id] = data
    return results
```

---

### Q2. How did you reduce batch processing time by 50%?

**Suggested Answer:**

"The 50% reduction came from a combination of targeted optimizations rather than a single change:

**1. Partition tuning:** Original pipelines used default 200 shuffle partitions, but our typical datasets had ~20 distinct keys after groupBy. AQE coalesced them, but I also set `spark.sql.shuffle.partitions` appropriately per job type based on data volume.

**2. Broadcast joins:** Several pipelines joined large raw data files against small reference tables (product codes, facility IDs). These reference tables were well under 100 MB — I replaced Sort-Merge Joins with broadcast joins, eliminating entire shuffle stages.

**3. Query plan analysis:** Used `explain('formatted')` to identify redundant shuffles. Found cases where data was being repartitioned unnecessarily before a join that would shuffle it anyway.

**4. Iceberg partition design:** Aligned partition columns with the most common filter patterns in downstream queries, ensuring partition pruning triggered consistently.

**5. Spark configuration:** Tuned `spark.executor.memory`, `spark.executor.cores`, and `spark.memory.fraction` based on actual job memory profiles from Spark UI."

---

### Q3. Why Iceberg? Why not Delta?

**Suggested Answer:**

"The decision to use Apache Iceberg at AbbVie came down to three factors:

**1. Engine flexibility:** Our ecosystem uses Spark for ingestion but Trino/Presto for ad-hoc analytics queries. Delta Lake has strong Spark support but limited Trino/Presto compatibility. Iceberg has first-class support across all engines we use.

**2. Hidden partitioning:** Our queries filter on timestamps, but the raw ingestion data from APIs contains timestamps at different granularities. Iceberg's hidden partition transforms (`days(event_ts)`, `hours(event_ts)`) let us define logical partitioning without adding physical partition columns to the data — making queries simpler and partition pruning automatic.

**3. Partition evolution:** As data volumes grew, we needed to change partition granularity without rewriting 20 TB of data. Iceberg's partition evolution allowed us to change from monthly to daily partitioning for hot tables without data migration.

That said, Delta Lake would have been equally valid in a pure Databricks environment — it's excellent for that stack."

---

### Q4. Explain the 300 SQL dynamic scripts optimization (July 2023 – Aug 2024 role).

**Suggested Answer:**

"In my earlier role on the AbbVie eCDP project, infrastructure provisioning required manually writing 300+ SQL configuration scripts to set up tables, views, and stored procedures across environments (dev, test, prod). This process took 15+ hours per week and was error-prone.

I automated this using **Python's Jinja2 templating and dynamic SQL generation**:

```python
from jinja2 import Template

# Template for table creation
table_template = Template("""
CREATE TABLE IF NOT EXISTS {{ schema }}.{{ table_name }} (
    {% for col in columns %}
    {{ col.name }} {{ col.type }}{% if not loop.last %},{% endif %}
    {% endfor %}
)
PARTITIONED BY ({{ partition_col }})
STORED AS PARQUET
LOCATION '{{ location }}';
""")

# Config-driven generation
table_configs = load_yaml("table_configs.yaml")  # 300 table definitions
for config in table_configs:
    sql = table_template.render(**config)
    execute_sql(sql, environment=target_env)
```

This eliminated 15+ hours/week of manual work, removed copy-paste errors, and standardized environment configurations — ensuring dev/test/prod were always in sync."

---

# SECTION 9 — Practical Coding Answers

---

### P1. Read nested JSON and flatten it.

```python
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Sample nested JSON:
# {"user_id": "U1", "profile": {"name": "John", "age": 30},
#  "orders": [{"order_id": "O1", "amount": 100}, {"order_id": "O2", "amount": 200}]}

df_raw = spark.read.option("multiLine", "true").json("s3://data/users.json")
df_raw.printSchema()

# Flatten struct fields
df_flat = df_raw \
    .withColumn("name", F.col("profile.name")) \
    .withColumn("age",  F.col("profile.age")) \
    .drop("profile")

# Explode array column
df_exploded = df_flat \
    .withColumn("order", F.explode(F.col("orders"))) \
    .withColumn("order_id", F.col("order.order_id")) \
    .withColumn("amount",   F.col("order.amount")) \
    .drop("order", "orders")

df_exploded.show(truncate=False)
```

---

### P2. Implement SCD Type 2 in PySpark using Delta.

```python
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def apply_scd2(spark, source_df, target_path, key_col, track_cols):
    """
    Apply SCD Type 2 to a Delta table.
    - Closes existing records when tracked columns change
    - Inserts new records with current=True, end_date=NULL
    """
    today = F.current_date()

    # Prepare source with SCD2 metadata
    df_source = source_df \
        .withColumn("effective_date", today) \
        .withColumn("expiry_date",    F.lit(None).cast("date")) \
        .withColumn("is_current",     F.lit(True))

    if not DeltaTable.isDeltaTable(spark, target_path):
        df_source.write.format("delta").mode("overwrite").save(target_path)
        return

    delta_table = DeltaTable.forPath(spark, target_path)
    df_target   = delta_table.toDF().filter(F.col("is_current") == True)

    # Detect changed records
    change_condition = " OR ".join([f"src.{c} != tgt.{c}" for c in track_cols])
    df_changed = df_source.alias("src") \
        .join(df_target.alias("tgt"), key_col, "inner") \
        .filter(F.expr(change_condition)) \
        .select(F.col(f"src.{key_col}"))

    # Close existing records for changed keys
    delta_table.alias("tgt").merge(
        df_changed.alias("src"),
        f"tgt.{key_col} = src.{key_col} AND tgt.is_current = true"
    ).whenMatchedUpdate(set={
        "is_current":  "false",
        "expiry_date": str(today)
    }).execute()

    # Insert new versions for changed + new records
    df_new = df_source.alias("src") \
        .join(df_target.alias("tgt"), key_col, "left_anti") \
        .union(
            df_source.join(df_changed, key_col, "inner")
        )

    df_new.write.format("delta").mode("append").save(target_path)

# Usage
apply_scd2(
    spark, df_customers_new,
    target_path="s3://data/delta/dim_customers/",
    key_col="customer_id",
    track_cols=["email", "address", "phone"]
)
```

---

### P3. Write code to merge data into Delta table using MERGE INTO.

```python
from delta.tables import DeltaTable
import pyspark.sql.functions as F

# Source: incoming batch (may have inserts, updates, deletes)
df_source = spark.read.parquet("s3://staging/daily_batch/")

# Target: Delta table
delta_table = DeltaTable.forPath(spark, "s3://data/delta/customers/")

(
    delta_table.alias("target")
    .merge(
        df_source.alias("source"),
        "target.customer_id = source.customer_id"
    )
    # UPDATE when record exists and has changed
    .whenMatchedUpdate(
        condition="source.updated_at > target.updated_at",
        set={
            "target.name":       "source.name",
            "target.email":      "source.email",
            "target.updated_at": "source.updated_at"
        }
    )
    # INSERT when record doesn't exist
    .whenNotMatchedInsert(
        values={
            "target.customer_id": "source.customer_id",
            "target.name":        "source.name",
            "target.email":       "source.email",
            "target.updated_at":  "source.updated_at"
        }
    )
    # OPTIONAL: DELETE records flagged as deleted in source
    .whenMatchedDelete(
        condition="source.operation = 'DELETE'"
    )
    .execute()
)
```

---

### P4. Write a PySpark UDF and explain why UDFs are expensive.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

# UDF definition
def classify_age(age):
    if age is None:
        return "unknown"
    elif age < 18:
        return "minor"
    elif age <= 35:
        return "young_adult"
    elif age <= 60:
        return "adult"
    else:
        return "senior"

classify_age_udf = udf(classify_age, StringType())

# Usage
df_result = df.withColumn("age_group", classify_age_udf(F.col("age")))

# ─── BETTER ALTERNATIVE: Use native Spark functions (no UDF needed) ───
df_result = df.withColumn(
    "age_group",
    F.when(F.col("age").isNull(), "unknown")
     .when(F.col("age") < 18,  "minor")
     .when(F.col("age") <= 35, "young_adult")
     .when(F.col("age") <= 60, "adult")
     .otherwise("senior")
)
```

**Why UDFs are expensive:**
1. **JVM ↔ Python serialization:** For Python UDFs, data must be serialized from JVM (Spark's executor) to Python process (Pickle format), processed, then serialized back → 2 serialization round trips per row
2. **No Catalyst optimization:** Catalyst treats UDFs as black boxes — cannot push filters inside, cannot optimize across UDF boundaries
3. **No Tungsten vectorization:** UDFs run row-by-row in Python, not in vectorized Tungsten execution
4. **Python GIL:** Python's Global Interpreter Lock limits parallelism within the UDF process

**Alternatives:**
- Native Spark SQL functions (always prefer first)
- Pandas UDFs (`@pandas_udf`) — vectorized, process entire batches, much faster
- Scala UDFs registered in SparkContext (if Scala is available)

---

### P5. Create a Medallion Architecture Pipeline (Bronze → Silver → Gold).

```python
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("MedallionPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

BASE_PATH = "s3://data/medallion"

# ═══════════════════════════════════════════════
# BRONZE LAYER — Raw ingestion, no transformation
# Schema-on-read, full history preserved
# ═══════════════════════════════════════════════
def ingest_bronze(source_path, table_name):
    df_raw = spark.read.option("header", "true").csv(source_path)
    df_bronze = df_raw \
        .withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("source_file", F.input_file_name()) \
        .withColumn("load_date", F.to_date(F.current_timestamp()))

    df_bronze.write.format("delta") \
        .mode("append") \
        .partitionBy("load_date") \
        .save(f"{BASE_PATH}/bronze/{table_name}")
    print(f"✅ Bronze ingested: {df_bronze.count()} rows")

# ═══════════════════════════════════════════════
# SILVER LAYER — Cleaned, validated, conformed
# Deduplicated, null-handled, typed correctly
# ═══════════════════════════════════════════════
def process_silver(bronze_table, silver_table, key_col):
    df_bronze = spark.read.format("delta").load(f"{BASE_PATH}/bronze/{bronze_table}")

    # 1. Remove duplicates (keep latest)
    from pyspark.sql.window import Window
    window = Window.partitionBy(key_col).orderBy(F.desc("ingestion_timestamp"))
    df_deduped = df_bronze \
        .withColumn("rn", F.row_number().over(window)) \
        .filter(F.col("rn") == 1).drop("rn")

    # 2. Data quality checks
    df_clean = df_deduped \
        .filter(F.col(key_col).isNotNull()) \
        .filter(F.col("amount") >= 0)

    # 3. Type casting and enrichment
    df_silver = df_clean \
        .withColumn("amount",     F.col("amount").cast("double")) \
        .withColumn("event_date", F.to_date("event_date_str", "yyyy-MM-dd")) \
        .withColumn("silver_processed_at", F.current_timestamp()) \
        .drop("source_file", "ingestion_timestamp")

    # Upsert to Silver Delta
    silver_path = f"{BASE_PATH}/silver/{silver_table}"
    if DeltaTable.isDeltaTable(spark, silver_path):
        DeltaTable.forPath(spark, silver_path).alias("t") \
            .merge(df_silver.alias("s"), f"t.{key_col} = s.{key_col}") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df_silver.write.format("delta").mode("overwrite").save(silver_path)
    print(f"✅ Silver processed: {df_silver.count()} rows")

# ═══════════════════════════════════════════════
# GOLD LAYER — Business-level aggregations
# Optimized for consumption by BI/ML teams
# ═══════════════════════════════════════════════
def build_gold(silver_table, gold_table):
    df_silver = spark.read.format("delta").load(f"{BASE_PATH}/silver/{silver_table}")

    # Business aggregation
    df_gold = df_silver \
        .groupBy("customer_id", "event_date") \
        .agg(
            F.sum("amount").alias("daily_revenue"),
            F.count("*").alias("transaction_count"),
            F.avg("amount").alias("avg_transaction"),
            F.max("amount").alias("max_transaction")
        ) \
        .withColumn("gold_built_at", F.current_timestamp())

    df_gold.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .save(f"{BASE_PATH}/gold/{gold_table}")

    # OPTIMIZE the gold table for fast reads
    spark.sql(f"OPTIMIZE delta.`{BASE_PATH}/gold/{gold_table}` ZORDER BY (customer_id)")
    print(f"✅ Gold built: {df_gold.count()} rows")

# ═══════════════════════════════════════════════
# RUN PIPELINE
# ═══════════════════════════════════════════════
ingest_bronze("s3://raw/transactions/2024-01-15/", "transactions")
process_silver("transactions", "transactions_clean", key_col="transaction_id")
build_gold("transactions_clean", "daily_revenue_summary")
```

---

## 🎯 Final Interview Tips — Personalized for You

### Strengths to Highlight Confidently
1. **Iceberg expertise** — Most candidates know Delta. Your Iceberg experience (hidden partitions, partition evolution, 20 TB scale) is a real differentiator.
2. **Multithreaded API ingestion** — Shows Python depth beyond PySpark.
3. **Certifications** — Databricks Certified + Azure Data Fundamentals + AWS CCP = well-rounded cloud knowledge.
4. **Production impact** — 50% pipeline reduction, 15+ hours/week saved — always quantify.

### How to Answer "Tell Me About Yourself"
*"I'm a Data Engineer at Cognizant, working on the AbbVie eCDP project for about 2 years. My core work involves building and optimizing PySpark pipelines on Apache Iceberg, handling both batch file ingestion from S3 and complex API integrations from systems like Veeva and Ravex. I recently reduced pipeline execution time by 50% through Spark tuning, partition optimization, and query plan analysis. I hold Databricks, Azure, and AWS certifications, and I'm looking to take on more complex data engineering challenges at scale."*

### Questions to Ask the Interviewer
- "What does the data stack look like — are you on Delta or Iceberg?"
- "How is the team structured between data engineering and data science?"
- "What's the biggest current performance challenge in your pipelines?"
- "Do you follow medallion architecture or a different layering strategy?"

---

*Last Updated: April 2026 | Tailored for Mayuresh Patil | AbbVie/Cognizant → Next Role*


C:\Users\patil\Downloads\Interview_Master_Guide_Mayuresh_Patil.md
