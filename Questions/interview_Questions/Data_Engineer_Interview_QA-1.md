# Data Engineer Interview Q&A — Complete Reference
**PySpark · Spark Internals · Airflow · Optimization · Coding**
*Prepared for Mayuresh Patil | April 2026*

---

## SECTION 1 · Handling Large Volume of Data

### Q1. How do you handle large volumes of data? What problems arise and how do you resolve them?

Large data volumes introduce three core challenges: memory pressure, slow I/O, and unequal data distribution (skew).

| Problem | Root Cause | Resolution |
|---|---|---|
| Out-of-Memory (OOM) | Executors exceed heap limits → job fails | Tune `spark.executor.memory`, use `persist(DISK_AND_MEMORY)`, avoid `collect()` on large DFs |
| Data Skewness | One partition holds most data → one task runs forever | Salting, AQE skew-join optimization, repartition by a more distributed key |
| Excessive Shuffling | Wide transforms (groupBy, join) move data across network → slow | Broadcast joins for small tables, partition pruning, avoid unnecessary wide transforms |
| Slow I/O | Many small files or uncompressed data | Use Parquet/ORC, coalesce small files, enable partition pruning |
| Driver Bottleneck | `collect()` on huge data → driver OOM | Never bring large data to driver; use distributed writes `df.write.parquet(...)` |

---

## SECTION 2 · Data Skewness

### Q2. What is data skewness? How do you detect and fix it?

**Definition:** Data skewness occurs when data is not evenly distributed across partitions. One or a few partitions contain significantly more data than others, causing those tasks to run much longer — this is the "straggler" problem.

**The problem with data skewness:**
- One task processes 80% of data while others are idle
- Job completion time = slowest task → overall pipeline is delayed
- Can cause OOM on the overloaded executor
- Wastes cluster resources (other executors sit idle)

**Detection:**
```python
# Check key distribution
df.groupBy("key").count().orderBy("count", ascending=False).show(10)

# Check partition sizes
df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()

# Spark UI → Stages tab: look for one task 10x longer than median
```

**Fix 1 — Salting (most common for joins):**
```python
import pyspark.sql.functions as F

SALT = 10  # number of buckets

# Add random salt to the skewed side
skewed_df = skewed_df.withColumn("salt_key",
    F.concat(F.col("key"), F.lit("_"), (F.rand() * SALT).cast("int").cast("string")))

# Explode salt on the small (lookup) side
small_df = small_df.withColumn("salt_key",
    F.explode(F.array([F.concat(F.col("key"), F.lit(f"_{i}")) for i in range(SALT)])))

result = skewed_df.join(small_df, "salt_key")
```

**Fix 2 — AQE (Spark 3.x, zero code change):**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# AQE auto-splits skewed partitions at runtime
```

**Fix 3 — Repartition by finer grain key:**
```python
df = df.repartition(200, "country", "city")  # finer than country alone
```

**Fix 4 — Skew hints (Spark 3.2+):**
```python
df.hint("skew", "key")
```

**Fix 5 — Filter + union (isolate the skewed key):**
```python
# Process the dominant key separately
us_df    = df.filter(F.col("country") == "US")
other_df = df.filter(F.col("country") != "US")

us_result    = us_df.repartition(50).join(lookup_df, "country")
other_result = other_df.join(broadcast(lookup_df), "country")

result = us_result.union(other_result)
```

### Q3. How to distribute data equally across partitions?

```python
# Option 1: repartition (full shuffle, balanced)
df = df.repartition(200)

# Option 2: repartition by column (hash-based, balanced per key)
df = df.repartition(200, "store_id")

# Option 3: repartition by range (good for range queries)
df = df.repartitionByRange(200, "date")

# Option 4: coalesce (reduces partitions, no full shuffle — use when shrinking)
df = df.coalesce(50)
```

---

## SECTION 3 · df.groupBy("userId").count() Explained

### Q4. Explain df.groupBy("userId").count() — what happens internally?

This is a **wide transformation** (requires a shuffle). Spark executes it in two phases:

1. **Phase 1 – Map-side partial aggregation:** Each executor counts `userId` occurrences locally (partial counts), reducing shuffle data volume.
2. **Phase 2 – Shuffle + reduce:** Partial counts are shuffled by `userId` hash to the same reducer, which sums them to produce the final count.

```python
df.groupBy("userId").count()
# Equivalent SQL: SELECT userId, COUNT(*) FROM table GROUP BY userId

# Internally:
# 1. HashAggregate (partial) — on each partition
# 2. Exchange (shuffle by userId hashcode)
# 3. HashAggregate (final) — merge partial counts

# Performance tip: pre-partition to avoid shuffle
df.repartition("userId").groupBy("userId").count()
```

> **Note:** `groupBy` triggers a shuffle → expensive. Enable AQE to auto-coalesce small post-shuffle partitions.

---

## SECTION 4 · Google Cloud Dataproc

### Q5. Tell about the Dataproc service.

Google Cloud Dataproc is a **fully managed cloud service** for running Apache Spark, Hadoop, Hive, and Pig workloads.

**Key characteristics:**
- Fast cluster creation (< 90 seconds) vs hours for on-prem clusters
- **Pay-per-second billing** — spin up for a job, tear down immediately
- **Separation of storage and compute:** data lives in GCS, clusters are ephemeral
- Native integration with BigQuery, Cloud Storage, Pub/Sub, Cloud Logging
- Supports autoscaling, preemptible VMs (cost savings), optional components (Jupyter, Zeppelin)
- **Dataproc Serverless:** submit Spark jobs without managing clusters at all

```bash
# Submit a PySpark job via gcloud CLI
gcloud dataproc jobs submit pyspark gs://my-bucket/jobs/etl.py \
  --cluster=my-cluster --region=us-central1 \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
```

> **Azure equivalent:** Azure HDInsight or Azure Databricks (your Cognizant stack).

---

## SECTION 5 · Apache Airflow & Job Scheduling

### Q6. What is Apache Airflow?

Apache Airflow is an open-source **workflow orchestration platform**. Pipelines are defined as DAGs (Directed Acyclic Graphs) in Python.

**Key components:**
- **Scheduler** — monitors DAGs, triggers task instances when dependencies are met
- **Executor** — determines how tasks run (LocalExecutor, CeleryExecutor, KubernetesExecutor)
- **Worker** — where actual task code executes
- **Metadata DB** — stores DAG definitions, task states, run history (usually PostgreSQL)
- **Webserver** — UI to monitor, trigger, and debug DAG runs

### Q7. Explain scheduling of jobs/tasks in Airflow.

**Key concepts:**
- `execution_date` — the logical date of a DAG run (not the actual run time)
- `DAG Run` — an instance of a DAG for a specific `execution_date`
- **Task dependencies** — `>>` / `<<` operators or `set_upstream()` / `set_downstream()`
- **Catchup** — if `True`, Airflow backfills all missed runs since `start_date`
- **Retries** — configure `retries` and `retry_delay` per task

### Q8. Write an Airflow DAG: daily at 2am, GCS CSV → aggregate revenue → write JSON. Retries=2, idempotent.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json, csv
from google.cloud import storage

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def aggregate_revenue(ds, **kwargs):
    """Read CSV from GCS, compute total revenue per product."""
    client  = storage.Client()
    bucket  = client.bucket("my-bucket")
    blob    = bucket.blob(f"sales/{ds}/sales.csv")
    content = blob.download_as_text().splitlines()
    reader  = csv.DictReader(content)

    revenue = {}
    for row in reader:
        product = row["product"]
        revenue[product] = revenue.get(product, 0) + float(row["revenue"])

    # Push to XCom so next task can use it
    kwargs["ti"].xcom_push(key="revenue", value=revenue)

def write_result(ds, **kwargs):
    """Write aggregated result to GCS as JSON (idempotent — overwrites same key)."""
    revenue = kwargs["ti"].xcom_pull(key="revenue", task_ids="aggregate_revenue")
    client  = storage.Client()
    bucket  = client.bucket("my-bucket")
    blob    = bucket.blob(f"sales_agg/{ds}/result.json")
    # Overwriting is idempotent — re-running for same ds gives same result
    blob.upload_from_string(json.dumps(revenue), content_type="application/json")

with DAG(
    dag_id="daily_sales_aggregation",
    default_args=default_args,
    description="Aggregate daily sales revenue per product",
    schedule_interval="0 2 * * *",   # 2 AM daily
    start_date=days_ago(1),
    catchup=False,                   # don't backfill historical runs
    tags=["sales", "gcs"],
) as dag:

    t1 = PythonOperator(
        task_id="aggregate_revenue",
        python_callable=aggregate_revenue,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="write_result",
        python_callable=write_result,
        provide_context=True,
    )

    t1 >> t2   # t2 runs only after t1 succeeds
```

> **Idempotency:** Uploading to the same GCS path for the same `ds` overwrites the file — re-running for the same date is always safe.

---

## SECTION 6 · Spark Optimization Techniques

### Q9. What are all the key Spark optimization techniques?

| Technique | Description |
|---|---|
| **Broadcast Join** | Replicate small table to every executor → zero shuffle for that side |
| **AQE (Adaptive Query Execution)** | Spark 3.x re-plans at runtime: coalesces partitions, handles skew, switches to broadcast join |
| **Partition Pruning** | Partition data by filter columns; Spark skips irrelevant directories |
| **Predicate Pushdown** | Filters pushed into Parquet/Delta reader — only needed rows read |
| **Columnar Format (Parquet/ORC)** | Read only required columns; better compression; vectorized reads |
| **Coalesce vs Repartition** | `coalesce()` reduces partitions without full shuffle; `repartition()` does full shuffle for balanced output |
| **Caching / Persistence** | `df.cache()` or `df.persist()` to avoid recomputing repeated DFs |
| **Salting for Skew** | Add random salt to skewed keys to distribute across more partitions |
| **Avoid UDFs** | Python UDFs break vectorization; prefer built-in Spark SQL functions or Pandas UDFs |
| **Dynamic Resource Allocation** | `spark.dynamicAllocation.enabled=true` — adds/removes executors based on workload |

### Q10. Broadcast join vs Sort-Merge Join — what is the drawback of broadcast join?

```python
# Broadcast join — small table replicated to all executors
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# Sort-Merge Join — both sides sorted then merged (default for large tables)
result = large_df.join(small_df, "key")  # Spark chooses SMJ automatically
```

**Broadcast Join DRAWBACKS:**
- If the "small" table is not actually small, broadcasting copies it to EVERY executor → driver and executor OOM risk
- Default threshold is 10 MB; raising it blindly causes memory issues
- Not suitable when both sides are large

**Sort-Merge Join:**
- Requires shuffle and sort on both sides
- Scales to any data size — no OOM risk
- Higher latency due to shuffle

### Q11. How do you reduce shuffling?

```python
# 1. Broadcast join for small tables
result = large_df.join(broadcast(small_df), "key")

# 2. Pre-partition on join keys (co-located data needs no shuffle)
df.repartition(200, "key").write.partitionBy("key").parquet("path/")

# 3. Bucket tables in Spark SQL (pre-sorted on disk)
df.write.bucketBy(64, "key").sortBy("key").saveAsTable("bucketed_table")

# 4. Enable AQE (auto-coalesces small post-shuffle partitions)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 5. Chain filters before any groupBy/join
df.filter(col("country") == "IN").filter(col("year") == 2024).groupBy("city").count()
```

### Q12. Dynamic Allocation in Spark

```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "5")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
# Spark adds executors when tasks are pending; removes idle ones
```

**How it works:**
- If tasks are waiting in queue → Spark requests more executors from YARN/K8s
- If executors are idle for `executorIdleTimeout` → they are released
- Saves cost in cloud environments — you only pay for what you use

---

## SECTION 7 · AQE, Memory & Lazy Evaluation

### Q13. Have you faced OOM issues? What does AQE do?

**OOM typically happens when:**
- A single partition is too large (data skew)
- Executor memory is misconfigured
- `collect()` is called on large data → driver OOM
- Too many cached DFs fill up memory
- Broadcast of a table that's actually large

**Adaptive Query Execution (AQE) — Spark 3.0+:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

**What AQE does:**
- **Coalesces** too-many small post-shuffle partitions into fewer larger ones (reduces task overhead)
- **Splits** skewed partitions into multiple smaller tasks automatically
- **Converts** sort-merge join to broadcast join at runtime if one side turns out small

### Q14. How does OOM / Memory issue happen in depth?

**Spark Memory Model:**
```
Executor JVM Heap
├── Reserved Memory       (300 MB fixed)
├── Spark Memory (60%)
│   ├── Execution Memory  (shuffles, joins, sorts, aggregations)
│   └── Storage Memory    (cached RDDs/DFs)
└── User Memory (40%)     (UDFs, non-Spark objects)
```

**Causes of OOM:**
- **Execution OOM:** Partition too large for available execution memory (data skew, too few partitions)
- **Storage OOM:** Caching too many large DFs — eviction spills to disk but extreme cases fail
- **Driver OOM:** `df.collect()`, `toPandas()`, `show(n)` with large n — brings all data to driver
- **Broadcast OOM:** Broadcasting a table larger than driver/executor can hold

**Fixes:**
```python
# Tune memory
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.memory.fraction", "0.8")       # 80% to Spark
spark.conf.set("spark.memory.storageFraction", "0.3") # 30% of that to storage

# Increase parallelism to reduce per-partition size
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Spill to disk instead of failing
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")
```

### Q15. What is Lazy Evaluation in Spark?

Spark builds a logical plan when you call **transformations** (`map`, `filter`, `groupBy`, `join`) but does NOT execute anything. Execution is triggered only by **actions** (`show`, `count`, `collect`, `write`).

**Why it's powerful:**
- Catalyst optimizer views the entire pipeline before running a single task
- Applies predicate pushdown, join reordering, column pruning across the whole DAG
- Avoids intermediate materializations

```python
df = spark.read.parquet("s3://...")           # no execution
df2 = df.filter(col("date") > "2024-01-01")  # no execution
df3 = df2.groupBy("country").count()          # no execution
df3.show()  # ← THIS triggers execution of the entire chain
```

### Q16. What happens if df.show() is called, file deleted, then df.show() again?

- **If NOT cached:** Spark re-reads from source for every action. After deletion → second `df.show()` throws `AnalysisException / FileNotFoundException`
- **If cached (`df.cache()`):** First action populates cache. Second `df.show()` succeeds from in-memory cache

```python
df = spark.read.parquet("s3://bucket/data/")
df.cache()       # mark for caching
df.show()        # triggers read + caches in memory
# delete the file now
df.show()        # ✓ works — served from cache
```

---

## SECTION 8 · Compression vs Compaction

### Q17. What is the difference between Compression and Compaction?

| Aspect | Compression | Compaction |
|---|---|---|
| **What** | Reduces file size by encoding data | Merges many small files into fewer large files |
| **Goal** | Save storage & I/O bandwidth | Eliminate the "small file problem" |
| **When applied** | At write time | Post-ingestion maintenance job |
| **Spark/Delta** | `df.write.option("compression","snappy").parquet(...)` | `OPTIMIZE table` (Delta) / `coalesce()` |
| **Impact on reads** | Faster reads (less data scanned) | Faster reads (less metadata overhead, fewer open file handles) |

**Parquet compression codecs:**
- `snappy` — default, fast compression/decompression, moderate ratio
- `gzip` — slow compression, best ratio, CPU-intensive
- `zstd` — balanced, recommended for Spark 3.x (better than snappy ratio, faster than gzip)
- `lz4` — fastest decompression, low ratio
- `brotli` — very high ratio, slow (less common in Spark)

```python
# Compression at write time
df.write.option("compression", "snappy").parquet("s3://path/")
df.write.option("compression", "zstd").parquet("s3://path/")

# Compaction in Delta Lake
spark.sql("OPTIMIZE my_delta_table")
spark.sql("OPTIMIZE my_delta_table ZORDER BY (date, store_id)")

# Compaction in plain Spark
df.coalesce(10).write.parquet("s3://compacted-path/")
```

---

## SECTION 9 · SQL Optimizer (Catalyst)

### Q18. How does the Spark SQL Optimizer (Catalyst) work?

```
SQL / DataFrame API
       ↓
1. Parsing          → Unresolved Logical Plan
       ↓
2. Analysis         → Resolved Logical Plan (schema, column names validated)
       ↓
3. Logical Optimization → Optimized Logical Plan
   • Predicate pushdown
   • Constant folding
   • Column pruning
   • Join reordering
       ↓
4. Physical Planning → Physical Plans (multiple strategies)
   • Choose join strategy (broadcast vs SMJ vs hash join)
   • Cost-based optimizer selects best plan
       ↓
5. Code Generation (Tungsten)
   → JVM bytecode for expression evaluation
   → Avoids interpreter overhead, uses vectorized CPU instructions
```

---

## SECTION 10 · Java/Scala Comfort & Pipeline Understanding

### Q19. Are you comfortable switching to Scala?

*"My primary language is Python/PySpark. Scala has a steeper learning curve but since Spark is written in Scala, Scala APIs are first-class and often faster (no Python-JVM bridge overhead for transformations). I am comfortable reading Scala Spark code, understanding the logic, and performing optimizations at the Spark configuration and query plan level — which is language-agnostic. Given time, I can ramp up on Scala syntax, especially since the Spark DataFrame API is nearly identical across both languages."*

**PySpark vs Scala Spark — same logic, different syntax:**
```python
# PySpark
df.filter(col("country") == "IN").groupBy("city").count()
```
```scala
// Scala Spark
df.filter(col("country") === "IN").groupBy("city").count()
```

### Q20. Entire pipeline is built in Java in SAMS — how will you understand and optimize it without knowing Java?

**Answer strategy:**
1. **Read the query/execution plans** — `df.explain(true)` or Spark UI → same regardless of language. Plans are language-agnostic.
2. **Understand the DAG** — Spark UI shows stages, shuffles, and task times — observable in the UI, not the code.
3. **Configuration optimizations** — tuning `spark.conf` settings, memory, AQE, partitions — all external to Java code.
4. **Identify bottlenecks** — Spark UI Stage tab: which stage is slow? Which task is a straggler? This is UI-driven, not code-driven.
5. **Read Java like pseudocode** — Java Spark code is verbose but highly readable; DataFrame API calls map 1:1 to PySpark.
6. **Collaborate** — work with Java developers for code changes; own the infra/config/pipeline optimization layer.

---

## SECTION 11 · Coding Questions

### Q21. Employee salary — current, previous, salary difference

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

emp_data = [
    (1, "Amit",  "2023-01-01", 50000),
    (1, "Amit",  "2023-04-01", 55000),
    (1, "Amit",  "2023-07-01", 60000),
    (2, "Neha",  "2023-02-01", 40000),
    (2, "Neha",  "2023-06-01", 45000),
]
schema = ["emp_id", "emp_name", "effective_date", "salary"]
df = spark.createDataFrame(emp_data, schema)

# Window: partition by employee, order by date ascending
w = Window.partitionBy("emp_id").orderBy("effective_date")

result = (df
    .withColumn("prev_salary", F.lag("salary", 1).over(w))
    .withColumn("salary_diff", F.col("salary") - F.col("prev_salary"))
)

result.select("emp_id", "emp_name", "effective_date",
              "salary", "prev_salary", "salary_diff").show()

# Output:
# +------+--------+--------------+------+-----------+-----------+
# |emp_id|emp_name|effective_date|salary|prev_salary|salary_diff|
# +------+--------+--------------+------+-----------+-----------+
# |     1|    Amit|    2023-01-01| 50000|       null|       null|
# |     1|    Amit|    2023-04-01| 55000|      50000|       5000|
# |     1|    Amit|    2023-07-01| 60000|      55000|       5000|
# |     2|    Neha|    2023-02-01| 40000|       null|       null|
# |     2|    Neha|    2023-06-01| 45000|      40000|       5000|
# +------+--------+--------------+------+-----------+-----------+
```

### Q22. Top 3 users per day by event count using window functions

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# df has columns: user_id, event_time, event_type

result = (
    df
    .withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date", "user_id")
    .agg(F.count("*").alias("event_count"))
    .withColumn("rank",
        F.dense_rank().over(
            Window.partitionBy("event_date").orderBy(F.desc("event_count"))
        )
    )
    .filter(F.col("rank") <= 3)
    .orderBy("event_date", "rank")
)
result.show()
```

### Q23. Daily revenue growth per store — lag, lead, partitioning

**Data:**
```
Store A, 2023-01-01, 1000
Store A, 2023-01-02, 1200
Store A, 2023-01-03, 1100
Store A, 2023-01-01,  500   ← duplicate date! must aggregate first
Store A, 2023-01-02,  700
```

**Expected Output:** Store | Date | Revenue | Prev Day Revenue | Next Day Revenue | Daily Growth

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

data = [
    ("Store A", "2023-01-01", 1000),
    ("Store A", "2023-01-02", 1200),
    ("Store A", "2023-01-03", 1100),
    ("Store A", "2023-01-01",  500),
    ("Store A", "2023-01-02",  700),
]
df = spark.createDataFrame(data, ["store", "date", "revenue"])

# Step 1: Aggregate duplicate (store, date) combinations first
df_agg = df.groupBy("store", "date").agg(F.sum("revenue").alias("revenue"))

# Step 2: Window — partition by store, order by date
w = Window.partitionBy("store").orderBy("date")

# Step 3: Apply lag and lead
result = (
    df_agg
    .withColumn("prev_day_revenue", F.lag("revenue", 1).over(w))
    .withColumn("next_day_revenue", F.lead("revenue", 1).over(w))
    .withColumn("daily_growth",     F.col("revenue") - F.col("prev_day_revenue"))
)

result.select("store", "date", "revenue",
              "prev_day_revenue", "next_day_revenue", "daily_growth").show()

# Output:
# +-------+----------+-------+----------------+----------------+------------+
# |  store|      date|revenue|prev_day_revenue|next_day_revenue|daily_growth|
# +-------+----------+-------+----------------+----------------+------------+
# |Store A|2023-01-01|   1500|            null|            1900|        null|
# |Store A|2023-01-02|   1900|            1500|            1100|         400|
# |Store A|2023-01-03|   1100|            1900|            null|        -800|
# +-------+----------+-------+----------------+----------------+------------+
```

**How to partition for this problem:**
```python
# Partition by store so lag/lead only looks within the same store's dates
w = Window.partitionBy("store").orderBy("date")
# partitionBy("store") → each store's data stays in the same partition boundary
# orderBy("date")      → within that partition, rows sorted by date for lag/lead to work correctly
```

---

## SECTION 12 · Parquet Advantages

### Q24. What are the advantages of Parquet over other file formats for Spark?

| Feature | Parquet | CSV | JSON |
|---|---|---|---|
| **Storage model** | Columnar | Row-based | Row-based |
| **Read efficiency** | Read only needed columns | Must read all columns | Must read all columns |
| **Compression** | Per-column dictionary + RLE | Basic | Basic |
| **Schema** | Embedded in file | External required | Inferred (slow) |
| **Splittable** | Yes (row groups) | Depends on compression | Depends |
| **Predicate pushdown** | Yes (row group stats) | No | No |
| **Vectorized reads** | Yes (Spark native) | No | No |

**Key advantages:**
- **Columnar storage:** reads only the columns needed — avoids full row scans
- **Predicate pushdown:** filters pushed into file reader; unneeded row groups skipped using min/max statistics
- **Dictionary encoding + RLE compression** per column → much smaller files than CSV/JSON
- **Schema embedded** → no schema mismatch issues at read time
- **Splittable** → each row group can be read by a different Spark task in parallel
- **Vectorized batch readers** → Spark reads Parquet faster than row-by-row

---

## SECTION 13 · Batch vs Real-Time Jobs

### Q25. Have you worked on batch jobs or real-time jobs?

**Batch Processing:**
- Process bounded datasets on a schedule (daily/hourly)
- Your Cognizant eCDP work (full loads, incremental loads triggered by ADF) = batch
- Optimizes for **throughput** — process as much data as fast as possible
- Tools: Spark, ADF, Airflow

**Real-Time / Streaming:**
- Spark Structured Streaming, Kafka consumers, micro-batch or continuous mode
- Optimizes for **latency** — process events as they arrive
- More complex: state management, watermarking, exactly-once semantics, checkpointing

**Key differences:**
```python
# Batch
df = spark.read.parquet("s3://path/")
df.write.parquet("s3://output/")

# Streaming
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "...") \
    .option("subscribe", "topic") \
    .load()

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://checkpoints/") \
    .start()
```

---

## SECTION 14 · DataFrame SQL

### Q26. Have you worked on DataFrame SQL?

Yes. Spark supports two equivalent APIs — DataFrame API and SQL API:

```python
# Register as temp view first
df.createOrReplaceTempView("sales")

# DataFrame API
result = df.filter(col("date") > "2024-01-01") \
           .groupBy("store") \
           .agg(F.sum("revenue").alias("total_revenue")) \
           .orderBy(F.desc("total_revenue"))

# Equivalent SQL API — same execution plan
result = spark.sql("""
    SELECT store, SUM(revenue) AS total_revenue
    FROM sales
    WHERE date > '2024-01-01'
    GROUP BY store
    ORDER BY total_revenue DESC
""")
# Both produce identical execution plans — Catalyst treats them the same
```

---

*End of Document — Mayuresh Patil | Data Engineer Interview Preparation | April 2026*
