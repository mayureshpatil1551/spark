# 09 — Cache, Persist & Storage Levels in Apache Spark
> **Target Audience:** Data Engineers with 3–4 years of experience | Interview + Real-World Prep

---

## Table of Contents
1. [Concept Deep Dive](#1-concept-deep-dive)
2. [Lazy Evaluation & How Caching Helps](#2-lazy-evaluation--how-caching-helps)
3. [cache() vs persist()](#3-cache-vs-persist)
4. [Storage Levels — Detailed Breakdown](#4-storage-levels--detailed-breakdown)
5. [Serialized vs Deserialized](#5-serialized-vs-deserialized)
6. [Code Examples](#6-code-examples)
7. [Spark UI — What to Look For](#7-spark-ui--what-to-look-for)
8. [Interview Questions & Answers](#8-interview-questions--answers)
9. [Real-World Project Scenarios](#9-real-world-project-scenarios)
10. [Quick Reference Cheat Sheet](#10-quick-reference-cheat-sheet)

---

## 1. Concept Deep Dive

### What is Caching in Spark?

Caching is a **performance optimization mechanism** that stores intermediate DataFrame/Dataset/RDD results in memory, disk, or both — so that Spark does **not recompute** them every time an action is triggered.

```
Source → Filter → Join → GroupBy → [CACHE HERE] → Action 1
                                                 ↘ Action 2
                                                 ↘ Action 3
```

Without caching, each of the three actions above re-executes the entire chain from the source. With caching, the chain runs **once**, and Actions 2 & 3 read directly from the cache.

### When Should You Cache?

| Situation | Cache? |
|---|---|
| Same DataFrame used in 2+ downstream actions | ✅ Yes |
| Expensive transformations (joins, aggregations, UDFs) | ✅ Yes |
| Iterative ML pipelines (multiple passes over data) | ✅ Yes |
| Simple ETL read → transform → write (single action) | ❌ No — adds overhead |
| Very large data with insufficient memory | ⚠️ Use MEMORY_AND_DISK or DISK_ONLY |
| Streaming workloads | ❌ Generally avoid |

### What Happens Internally When You Cache?

1. You call `df.cache()` — Spark **marks** the DataFrame for caching (nothing is stored yet).
2. The **first action** (e.g., `show()`, `count()`) triggers computation and stores results in the cache.
3. Every **subsequent action** on that DataFrame reads from the **InMemoryTableScan** instead of recomputing.
4. Cached data stays alive until the Spark session ends, or you explicitly call `unpersist()`.

> **Key insight:** `cache()` is lazy — it does not store data at the moment of the call. Storage happens at the first action.

---

## 2. Lazy Evaluation & How Caching Helps

### Spark's Lazy Evaluation Model

Spark builds a **DAG (Directed Acyclic Graph)** of transformations, called the **lineage**. No computation happens until an **action** is triggered.

```
Transformations (lazy):    filter() → withColumn() → join() → groupBy()
                                                                    ↓
Action (eager):                                                  count()  ← computation starts here
```

### The Problem Without Caching

```python
df_base = df_customers.filter(...).withColumn(...).join(...)

df_base.count()   # Spark runs: filter → withColumn → join → count
df_base.show()    # Spark re-runs: filter → withColumn → join → show  ← DUPLICATE WORK
df_base.write...  # Spark re-runs: filter → withColumn → join → write ← DUPLICATE WORK
```

Every action **re-triggers the full lineage from the source**. This means:
- Repeated file reads / network calls
- Repeated shuffle operations (the most expensive part)
- Higher compute cost and time

### The Solution With Caching

```python
df_base = df_customers.filter(...).withColumn(...).join(...)
df_base.cache()

df_base.count()   # Runs full lineage + stores in cache
df_base.show()    # Reads from InMemoryTableScan ← FAST
df_base.write...  # Reads from InMemoryTableScan ← FAST
```

---

## 3. cache() vs persist()

| Feature | `cache()` | `persist()` |
|---|---|---|
| Storage Level | Fixed: `MEMORY_AND_DISK_DESER` | Configurable via `StorageLevel` |
| Custom Storage Level | ❌ Not supported | ✅ Supported |
| Syntax | `df.cache()` | `df.persist(StorageLevel.MEMORY_ONLY)` |
| Laziness | ✅ Lazy (triggers on first action) | ✅ Lazy |
| Unpersist | `df.unpersist()` | `df.unpersist()` |
| Use Case | Quick caching with sensible defaults | Fine-grained control over storage |

```python
from pyspark import StorageLevel

# cache() — simple, uses default storage level
df_base.cache()

# persist() — full control
df_base.persist(StorageLevel.MEMORY_ONLY)
df_base.persist(StorageLevel.MEMORY_AND_DISK)
df_base.persist(StorageLevel.DISK_ONLY)

# Always unpersist when done to free resources
df_base.unpersist()
```

> **Rule of thumb:** Use `cache()` for most cases. Use `persist()` when you need to control memory/disk trade-offs based on data size and cluster resources.

---

## 4. Storage Levels — Detailed Breakdown

### All Storage Levels (Spark 3.x)

| Storage Level | In Memory | On Disk | Serialized | Replicated | Description |
|---|---|---|---|---|---|
| `MEMORY_ONLY` | ✅ | ❌ | ❌ | ❌ | Fastest; fails if memory is insufficient |
| `MEMORY_ONLY_2` | ✅ | ❌ | ❌ | 2x | Resilient; replicates across 2 executors |
| `MEMORY_ONLY_SER` | ✅ | ❌ | ✅ | ❌ | Memory-efficient; CPU overhead for serialization |
| `MEMORY_AND_DISK` | ✅ | ✅ (spill) | ❌ | ❌ | Spills to disk when memory is full |
| `MEMORY_AND_DISK_2` | ✅ | ✅ (spill) | ❌ | 2x | Replicated version of above |
| `MEMORY_AND_DISK_DESER` | ✅ | ✅ (spill) | ❌ | ❌ | **Default for cache()** — deserialized in memory |
| `MEMORY_AND_DISK_SER` | ✅ | ✅ (spill) | ✅ | ❌ | Serialized; saves memory |
| `DISK_ONLY` | ❌ | ✅ | ✅ | ❌ | Lowest memory footprint; slowest access |
| `DISK_ONLY_2` | ❌ | ✅ | ✅ | 2x | Replicated disk storage |
| `DISK_ONLY_3` | ❌ | ✅ | ✅ | 3x | Triple replicated disk storage |
| `OFF_HEAP` | ✅ (off-heap) | ❌ | ✅ | ❌ | Stores outside JVM heap; avoids GC pressure |

### Decision Guide — Which Storage Level to Choose?

```
Is your data small enough to fit in memory?
    ├── YES → MEMORY_ONLY (fastest)
    │         or MEMORY_ONLY_SER (if memory is tight)
    │
    └── NO  → Will data exceed executor memory?
                  ├── YES → MEMORY_AND_DISK (safe default)
                  │         or MEMORY_AND_DISK_SER (memory-tight clusters)
                  │
                  └── DISK_ONLY (for archival/rarely accessed data)

Do you need fault tolerance (executor failures)?
    └── YES → Add _2 suffix for replication (e.g., MEMORY_ONLY_2)
```

---

## 5. Serialized vs Deserialized

### Deserialized (DESER)
- Data is stored as **JVM objects** (Java/Scala objects in memory)
- **Fast to read** — no conversion needed when Spark accesses data
- **Memory intensive** — JVM objects have object overhead (headers, pointers)
- Example: A string "Boston" stored as a Java String object with ~40 bytes overhead

### Serialized (SER)
- Data is stored as **compact byte arrays** (using Kryo or Java serialization)
- **Slower to read** — must deserialize before use (CPU overhead)
- **Memory efficient** — raw bytes are much smaller than JVM objects
- Example: A string "Boston" stored as 6 bytes in a byte array

### Memory Comparison Example

```
DataFrame with 1 million rows:
  Deserialized (JVM objects): ~2 GB in memory
  Serialized (Kryo):          ~600 MB in memory
```

> **Best Practice:** Use Kryo serializer for serialized storage levels:
> ```python
> spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
> ```

---

## 6. Code Examples

### Example 1 — Basic cache() Usage

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("CachingDemo").getOrCreate()

df_customers = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Build transformation chain
df_base = (
    df_customers
    .filter(F.col("city") == "boston")
    .withColumn(
        "customer_group",
        F.when(F.col("age").between(20, 30), F.lit("young"))
         .when(F.col("age").between(31, 50), F.lit("mid"))
         .when(F.col("age") > 51,            F.lit("old"))
         .otherwise(F.lit("kid"))
    )
    .select("cust_id", "name", "age", "gender", "birthday", "zip", "city", "customer_group")
)

# Cache before multiple actions
df_base.cache()

# First action — computes AND stores in cache
df_base.show(5, truncate=False)

# Second action — reads from InMemoryTableScan (no recomputation)
print(f"Total Boston customers: {df_base.count()}")

# Third action — still from cache
df_base.groupBy("customer_group").count().show()

# Free cache when done
df_base.unpersist()

spark.stop()
```

### Example 2 — persist() with Custom Storage Level

```python
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("PersistDemo").getOrCreate()

df_base = (
    df_customers
    .filter(F.col("city") == "boston")
    .withColumn("customer_group", F.lit("sample"))
)

# Persist with specific storage level
df_base.persist(StorageLevel.MEMORY_AND_DISK)

# Build derived DataFrames from the cached base
df2 = (
    df_base
    .withColumn("test_column_1", F.lit("test_column_1"))
    .withColumn("birth_year", F.split("birthday", "/").getItem(2))
)

df3 = (
    df_base
    .groupBy("customer_group")
    .agg(F.count("*").alias("cnt"), F.avg("age").alias("avg_age"))
)

df2.show(5, truncate=False)
df3.show()

# Unpersist to release memory/disk
df_base.unpersist()

spark.stop()
```

### Example 3 — Checking Cache Status Programmatically

```python
# Check if a DataFrame is cached
print(df_base.is_cached)           # True or False

# Check storage level
print(df_base.storageLevel)        # StorageLevel(True, True, False, True, 1)

# List all cached RDDs in the Spark context
for rdd_info in spark.sparkContext._jsc.getPersistentRDDs().values():
    print(rdd_info)
```

---

## 7. Spark UI — What to Look For

When you cache a DataFrame, the **Spark UI** gives you visibility into what's happening.

### Storage Tab
- Navigate to: `http://<driver-host>:4040/storage`
- Cached DataFrames appear here with:
  - Fraction cached (e.g., 100% means all partitions fit in memory)
  - Memory used
  - Disk used
  - Storage level applied

### SQL/DAG Tab
- **Without cache:** Each job shows full DAG from source (FileScan → Filter → Join → ...)
- **With cache:** Subsequent jobs show `InMemoryTableScan` at the start — this confirms Spark is reading from cache, not recomputing

### Key Metrics to Watch
| Metric | Without Cache | With Cache |
|---|---|---|
| Job duration | High (full recomputation) | Low (reads from cache) |
| Shuffle read/write | High | Near zero for cached stages |
| Stage count | Same stages repeated | Only new stages after cache point |
| `InMemoryTableScan` in DAG | ❌ Absent | ✅ Present |

---

## 8. Interview Questions & Answers

---

### Q1. What is the difference between `cache()` and `persist()` in Spark?

**Answer:**
Both are used to store intermediate DataFrames to avoid recomputation, but they differ in flexibility.

`cache()` is a shorthand for `persist()` with the default storage level `MEMORY_AND_DISK_DESER`. You cannot change its storage level.

`persist()` accepts a `StorageLevel` parameter, giving you full control — you can choose memory-only, disk-only, serialized, replicated, or combinations thereof.

```python
df.cache()                                  # Fixed: MEMORY_AND_DISK_DESER
df.persist(StorageLevel.MEMORY_ONLY)        # Custom storage level
df.persist(StorageLevel.DISK_ONLY_2)        # Disk + 2x replication
```

Use `cache()` for quick wins; use `persist()` when you need to fine-tune based on data size or cluster constraints.

---

### Q2. Is `cache()` eager or lazy? When does data actually get stored?

**Answer:**
`cache()` is **lazy**. Calling `df.cache()` only marks the DataFrame as "to be cached" — it does not trigger any computation or storage.

The actual caching happens at the **first action** (e.g., `count()`, `show()`, `collect()`) triggered on that DataFrame. At that point, Spark:
1. Computes the full transformation chain
2. Executes the action
3. Stores the result in the cache

All subsequent actions read from `InMemoryTableScan` in the DAG.

---

### Q3. What happens if the cached data doesn't fit in memory?

**Answer:**
The behavior depends on the storage level:

- **`MEMORY_ONLY`:** Spark drops partitions that don't fit. If a dropped partition is needed, Spark **recomputes it from the lineage** on the fly. No error is thrown.
- **`MEMORY_AND_DISK`:** Spark **spills excess partitions to disk**. Slower than memory but no recomputation.
- **`DISK_ONLY`:** All partitions go to disk. Highest latency but guaranteed persistence.

For production pipelines where data size is unpredictable, `MEMORY_AND_DISK` is the safest default.

---

### Q4. When should you NOT use caching?

**Answer:**
Caching is not always beneficial. Avoid it when:

1. **DataFrame is used only once** — caching adds serialization/storage overhead with no benefit.
2. **Data is too large for memory and disk** — it can cause excessive disk I/O and slow things down.
3. **Transformations are cheap** — if the computation is a simple filter or projection, recomputing is faster than caching.
4. **Streaming workloads** — micro-batch DataFrames are typically processed and discarded; caching rarely helps.
5. **You forget to `unpersist()`** — stale caches consume executor memory, starving other jobs.

---

### Q5. What is the difference between MEMORY_ONLY and MEMORY_AND_DISK?

**Answer:**

| Aspect | MEMORY_ONLY | MEMORY_AND_DISK |
|---|---|---|
| Storage location | RAM only | RAM + disk overflow |
| If memory is full | Partitions dropped; recomputed later | Partitions spill to disk |
| Speed | Fastest | Medium |
| Risk | Recomputation overhead | Disk I/O overhead |
| Use case | Small DataFrames that fit in RAM | Medium/large DataFrames |

Choose `MEMORY_ONLY` when data comfortably fits in executor memory. Choose `MEMORY_AND_DISK` when you want guaranteed caching without risk of full recomputation.

---

### Q6. What is the significance of the `_2` suffix in storage levels (e.g., MEMORY_ONLY_2)?

**Answer:**
The `_2` suffix means the data is **replicated across 2 executor nodes**. This provides **fault tolerance** — if one executor fails, Spark can read the cached data from the replica instead of recomputing.

Use replicated storage levels when:
- You have long-running jobs where executor failures are a real risk
- Recomputing the cached DataFrame is extremely expensive
- You're on a large cluster where node failures are statistically likely

The trade-off is **double the memory/disk consumption**.

---

### Q7. How does caching interact with Spark's DAG and Catalyst Optimizer?

**Answer:**
When a DataFrame is cached, Spark inserts an `InMemoryRelation` node into the logical plan. The **Catalyst Optimizer** is aware of this node and treats it as a boundary — it does not push optimizations (like predicate pushdown) past the cache boundary.

This means:
- Filters applied **before** `cache()` are pushed down to the source
- Filters applied **after** `cache()` are applied on the cached data in memory

This is why it's important to apply all your filters and projections **before** caching — to reduce the size of data stored in cache.

```python
# Good — filter before cache (smaller cached dataset)
df.filter("city = 'boston'").select("id", "name").cache()

# Bad — filters after cache (full dataset is cached first)
df.cache().filter("city = 'boston'")
```

---

### Q8. What is OFF_HEAP storage and when would you use it?

**Answer:**
`OFF_HEAP` stores serialized data **outside the JVM heap**, in native memory managed by Spark (using Project Tungsten's memory management).

Benefits:
- Avoids **JVM Garbage Collection (GC) pressure** — GC pauses can stall Spark tasks
- Better memory utilization for large datasets

Configuration required:
```python
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")

df.persist(StorageLevel.OFF_HEAP)
```

Use it when you're experiencing **long GC pauses** in your Spark UI and have large datasets to cache.

---

### Q9. How do you programmatically verify if a DataFrame is cached?

**Answer:**
```python
# Check cache status
print(df_base.is_cached)       # Returns True/False

# Check storage level details
print(df_base.storageLevel)
# Output: StorageLevel(disk, memory, deserialized, 1 replicas)
```

You can also verify in the **Spark UI → Storage tab**, which shows all cached RDDs/DataFrames with their storage level, partition count, memory used, and disk used.

---

### Q10. What happens when you call `unpersist()`? Is it immediate?

**Answer:**
`unpersist()` removes the DataFrame from the cache and frees the associated memory/disk storage.

By default, `unpersist()` is **asynchronous** — it schedules the removal but may not free memory immediately.

To force synchronous removal:
```python
df_base.unpersist(blocking=True)   # Waits until memory is actually freed
```

Always call `unpersist()` when:
- You're done with a cached DataFrame
- Before caching a new, large DataFrame
- At the end of a long pipeline to avoid memory pressure on subsequent jobs

---

## 9. Real-World Project Scenarios

---

### Scenario 1 — Multi-Branch ETL Pipeline (Most Common)

**Problem:**
You are building an ETL pipeline that reads a large customer transaction table (500 GB), applies a complex set of enrichment transformations (joins with 3 lookup tables, UDF-based scoring), and then needs to write **3 separate output datasets**: one for analytics, one for ML feature engineering, and one for a compliance report.

**Without Cache — What Goes Wrong:**
Each of the 3 `write()` actions re-reads and re-processes the full 500 GB, re-runs all joins, re-applies all UDFs = **3x compute cost**.

**Solution:**
```python
from pyspark import StorageLevel
import pyspark.sql.functions as F

# Step 1: Build the expensive shared base
df_transactions = spark.read.parquet("s3://data-lake/transactions/")
df_customers    = spark.read.parquet("s3://data-lake/customers/")
df_products     = spark.read.parquet("s3://data-lake/products/")
df_risk_scores  = spark.read.parquet("s3://data-lake/risk_scores/")

df_enriched = (
    df_transactions
    .join(df_customers,   "customer_id", "left")
    .join(df_products,    "product_id",  "left")
    .join(df_risk_scores, "customer_id", "left")
    .withColumn("txn_month",   F.date_format("txn_date", "yyyy-MM"))
    .withColumn("risk_bucket", F.when(F.col("score") > 0.8, "high")
                                .when(F.col("score") > 0.5, "medium")
                                .otherwise("low"))
    .filter(F.col("status") == "completed")
)

# Step 2: Cache — all 3 writes benefit
df_enriched.persist(StorageLevel.MEMORY_AND_DISK)
df_enriched.count()  # Trigger caching (materialize the cache)

# Step 3: 3 writes — each reads from cache, not from source
df_enriched.select("customer_id", "txn_month", "revenue") \
           .write.parquet("s3://output/analytics/")

df_enriched.select("customer_id", "risk_bucket", "score", "product_id") \
           .write.parquet("s3://output/ml_features/")

df_enriched.select("customer_id", "txn_id", "amount", "risk_bucket") \
           .filter(F.col("risk_bucket") == "high") \
           .write.parquet("s3://output/compliance/")

# Step 4: Release
df_enriched.unpersist()
```

**Result:** 3x reduction in data read, shuffle, and UDF execution time.

---

### Scenario 2 — Iterative Aggregation Dashboard

**Problem:**
You are building a daily executive dashboard. The same base fact table needs to be aggregated at 4 different granularities: daily, weekly, monthly, and by region. All 4 aggregations feed into a reporting database.

**Solution:**
```python
df_sales = (
    spark.read.parquet("s3://data-lake/sales/date=2024-*/")
    .filter(F.col("is_valid") == True)
    .withColumn("week",  F.weekofyear("sale_date"))
    .withColumn("month", F.month("sale_date"))
    .withColumn("year",  F.year("sale_date"))
)

# Cache before 4 aggregations
df_sales.cache()
df_sales.count()  # Materialize cache

# 4 aggregations — all from cache
daily_agg   = df_sales.groupBy("sale_date", "region") \
                      .agg(F.sum("revenue").alias("daily_revenue"))

weekly_agg  = df_sales.groupBy("year", "week", "region") \
                      .agg(F.sum("revenue").alias("weekly_revenue"))

monthly_agg = df_sales.groupBy("year", "month", "region") \
                      .agg(F.sum("revenue").alias("monthly_revenue"))

region_agg  = df_sales.groupBy("region") \
                      .agg(F.sum("revenue").alias("total_revenue"),
                           F.avg("revenue").alias("avg_revenue"),
                           F.count("*").alias("txn_count"))

# Write all 4 — cache prevents re-reading the source 4 times
daily_agg.write.mode("overwrite").parquet("s3://reports/daily/")
weekly_agg.write.mode("overwrite").parquet("s3://reports/weekly/")
monthly_agg.write.mode("overwrite").parquet("s3://reports/monthly/")
region_agg.write.mode("overwrite").parquet("s3://reports/region/")

df_sales.unpersist()
```

---

### Scenario 3 — Memory-Constrained Cluster with Large Dataset

**Problem:**
You have a 200 GB DataFrame on a cluster with only 80 GB of total executor memory. Using `MEMORY_ONLY` causes partitions to be dropped and recomputed. You need consistent performance.

**Solution — Use MEMORY_AND_DISK_SER:**
```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

df_large = (
    spark.read.parquet("s3://data-lake/large-table/")
    .filter(F.col("region") == "APAC")
    .withColumn("enriched_col", some_complex_udf(F.col("raw_col")))
)

# Serialized + spill to disk = compact storage + no dropped partitions
df_large.persist(StorageLevel.MEMORY_AND_DISK_SER)
df_large.count()

# Use normally — Spark handles memory/disk tiering automatically
df_large.groupBy("product").agg(F.sum("revenue")).show()
df_large.write.parquet("s3://output/apac_enriched/")

df_large.unpersist()
```

**Why `MEMORY_AND_DISK_SER`:**
- Serialized format reduces 200 GB → ~60 GB (Kryo compression effect)
- 60 GB fits in 80 GB cluster memory — no disk spill needed in practice
- Even if some partitions spill to disk, they are serialized (compact) on disk too

---

### Scenario 4 — Cache Invalidation Bug (Common Pitfall)

**Problem:**
A junior engineer cached a DataFrame, made changes to it after caching, and couldn't understand why the old data was showing up.

```python
# BUG — classic cache invalidation mistake
df = spark.read.parquet("s3://input/")
df.cache()
df.count()  # Cache is now populated

# Engineer then modifies df... but Spark DataFrames are IMMUTABLE
# This creates a NEW DataFrame, not modifying the cached one
df = df.filter(F.col("status") == "active")   # df is now a NEW object
df.show()   # This re-reads from source, NOT from cache!
            # Because the cached object was the OLD df reference
```

**Fix — Cache after transformations:**
```python
df_raw = spark.read.parquet("s3://input/")

# Apply ALL transformations first, THEN cache
df_clean = df_raw.filter(F.col("status") == "active") \
                 .withColumn("processed_date", F.current_date())

df_clean.cache()
df_clean.count()   # Now the FINAL DataFrame is cached

df_clean.show()    # Reads from cache ✅
df_clean.unpersist()
```

**Lesson:** Always cache the **final form** of the DataFrame you intend to reuse. DataFrames in Spark are immutable — transformations always return new objects.

---

### Scenario 5 — Caching in a Spark Streaming Micro-batch (Reference Data Pattern)

**Problem:**
You have a Spark Structured Streaming job that processes IoT events. Each micro-batch needs to join against a **device metadata lookup table** (static, 10 MB). Without caching, this small table is re-read from S3 on every micro-batch.

**Solution — Cache the static reference data:**
```python
# Cache the small static lookup table once at startup
df_device_meta = spark.read.parquet("s3://reference/device_metadata/")
df_device_meta.cache()
df_device_meta.count()  # Materialize into memory

# Streaming query — df_device_meta reads from cache on every micro-batch
df_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "iot-events")
    .load()
)

def process_batch(batch_df, batch_id):
    enriched = batch_df.join(df_device_meta, "device_id", "left")
    enriched.write.mode("append").parquet("s3://output/enriched-events/")

df_stream.writeStream \
         .foreachBatch(process_batch) \
         .option("checkpointLocation", "s3://checkpoints/iot/") \
         .start() \
         .awaitTermination()

# Note: unpersist() when the stream is stopped
df_device_meta.unpersist()
```

**Result:** The 10 MB metadata table is read once from S3 and served from RAM for thousands of micro-batches — eliminating thousands of S3 API calls.

---

## 10. Quick Reference Cheat Sheet

```
┌─────────────────────────────────────────────────────────────────────┐
│                  CACHING QUICK REFERENCE                            │
├─────────────────┬───────────────────────────────────────────────────┤
│ cache()         │ Default: MEMORY_AND_DISK_DESER, no config option  │
│ persist(level)  │ Full control over storage level                    │
│ unpersist()     │ Async removal from cache                          │
│ unpersist(True) │ Synchronous / blocking removal                    │
│ is_cached       │ Boolean — check if DataFrame is marked for cache  │
│ storageLevel    │ Returns the StorageLevel object                   │
├─────────────────┼───────────────────────────────────────────────────┤
│ MEMORY_ONLY     │ Fastest, RAM only, drops on overflow              │
│ MEMORY_AND_DISK │ Safe default, spills to disk on overflow          │
│ DISK_ONLY       │ Low memory, slowest access                        │
│ _2 suffix       │ 2x replication for fault tolerance                │
│ _SER suffix     │ Serialized: less memory, more CPU                 │
│ OFF_HEAP        │ Outside JVM heap, avoids GC pauses                │
├─────────────────┼───────────────────────────────────────────────────┤
│ DESER           │ JVM objects — fast read, high memory              │
│ SER             │ Byte arrays — slow read (deserialize), low memory │
└─────────────────┴───────────────────────────────────────────────────┘

When to Cache:           When NOT to Cache:
✅ Used 2+ times          ❌ Single-use DataFrames
✅ Expensive transforms   ❌ Very cheap transforms
✅ Multiple actions       ❌ Streaming micro-batches (usually)
✅ Iterative algorithms   ❌ Insufficient cluster memory
✅ Static reference data  ❌ Forgotten unpersist() → memory leak
```

---

*Last Updated: 2026 | PySpark 3.x | For Data Engineering Interview Prep*
