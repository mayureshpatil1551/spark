# Apache Spark – Memory Management
> **Level:** 3.5 Years Data Engineering | **Stack:** Azure Databricks, PySpark, Delta Lake

---

## Table of Contents
1. [Executor Memory Layout – Full Picture](#1-executor-memory-layout--full-picture)
2. [Unified Memory – Deep Dive](#2-unified-memory--deep-dive)
3. [Off-Heap Memory – When & Why](#3-off-heap-memory--when--why)
4. [Key Spark Memory Configs – Cheat Sheet](#4-key-spark-memory-configs--cheat-sheet)
5. [How YARN Allocates Memory](#5-how-yarn-allocates-memory)
6. [Interview Q&A – Tiered](#6-interview-qa--tiered)
7. [Real-World Scenarios & Solutions](#7-real-world-scenarios--solutions)

---

## 1. Executor Memory Layout – Full Picture

When you set `spark.executor.memory = 10GB`, you're only defining the **on-heap JVM memory**. The actual memory consumed by an executor on the cluster is larger.

```
┌─────────────────────────────────────────────────────────────┐
│                  YARN Container  (~11GB+)                   │
│                                                             │
│  ┌──────────────────────────────────┐                       │
│  │       On-Heap Memory (10GB)      │  spark.executor.memory│
│  │                                  │                       │
│  │  ┌────────────────────────────┐  │                       │
│  │  │   Reserved Memory (300MB)  │  │  Spark internals      │
│  │  └────────────────────────────┘  │                       │
│  │                                  │                       │
│  │  ┌────────────────────────────┐  │                       │
│  │  │   User Memory (~3.7GB)     │  │  UDFs, objects, vars  │
│  │  └────────────────────────────┘  │                       │
│  │                                  │                       │
│  │  ┌────────────────────────────┐  │                       │
│  │  │   Unified Memory (6GB)     │  │  fraction = 0.6       │
│  │  │  ┌──────────┬───────────┐  │  │                       │
│  │  │  │Execution │  Storage  │  │  │                       │
│  │  │  │  (3GB)   │  (3GB)    │  │  │                       │
│  │  │  └──────────┴───────────┘  │  │                       │
│  │  └────────────────────────────┘  │                       │
│  └──────────────────────────────────┘                       │
│                                                             │
│  ┌──────────────────────────────────┐                       │
│  │     Overhead Memory (~1GB)       │  10% of exec memory   │
│  └──────────────────────────────────┘                       │
│                                                             │
│  ┌──────────────────────────────────┐                       │
│  │     Off-Heap Memory (optional)   │  Managed by OS        │
│  └──────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────┘
```

### Breaking Down Each Section

| Region | Size (10GB example) | Config | Purpose |
|---|---|---|---|
| Reserved Memory | 300 MB (fixed) | Hardcoded | Spark framework itself |
| User Memory | ~3.7 GB | `(1 - fraction) × (total - 300MB)` | UDFs, custom data structures |
| Unified Memory | 6 GB | `spark.memory.fraction = 0.6` | Execution + Storage (shared) |
| Overhead Memory | ~1 GB | `spark.executor.memoryOverhead` | Off-JVM OS-level ops |
| Off-Heap Memory | Configurable | `spark.memory.offheap.size` | GC-free execution/storage |

---

## 2. Unified Memory – Deep Dive

Introduced in **Spark 1.6**, Unified Memory replaced the old static split (StaticMemoryManager). The key insight: **Execution and Storage share a pool and can borrow from each other.**

### Execution Memory
Used for:
- **Joins** (sort-merge join, hash join buffers)
- **Shuffles** (shuffle write/read buffers)
- **Sorting & Aggregations** (GroupBy, OrderBy)
- **Window functions**

### Storage Memory
Used for:
- **Cached RDDs and DataFrames** (`.cache()`, `.persist()`)
- **Broadcast variables** (small lookup tables sent to all executors)
- **Unrolling serialized blocks**

### Dynamic Borrowing – The Critical Rule

```
If Execution memory is full  → it can evict Storage blocks (spill to disk)
If Storage memory is full    → it can borrow free Execution memory
BUT: Storage CANNOT evict Execution memory (Execution has priority)
```

**Why?** Execution memory holds *in-progress* shuffle/join state — evicting it would break the task. Storage holds cached data that can be recomputed from lineage.

### Practical Implication

```python
# BAD: Caching a DF you won't reuse wastes Storage memory
df = spark.read.parquet("/path/to/data")
df.cache()                    # occupies Storage memory
df.filter("year = 2024").show()  # used once, then ignored

# GOOD: Only cache if you use the DF multiple times
df_filtered = spark.read.parquet("/path").filter("year = 2024")
df_filtered.cache()           # cache AFTER filtering (smaller footprint)
df_filtered.groupBy("region").count().show()
df_filtered.groupBy("product").sum("revenue").show()
df_filtered.unpersist()       # always release after you're done
```

---

## 3. Off-Heap Memory – When & Why

### The GC Problem (Why Off-Heap Exists)

When the JVM heap fills up, the **Garbage Collector (GC)** kicks in:

```
JVM Heap Full
     ↓
GC Cycle Starts → "Stop-the-World" pause
     ↓
All executor threads PAUSE (tasks stop)
     ↓
GC cleans dead objects
     ↓
Executor resumes — but time was lost
```

In large Spark jobs, GC pauses can cause:
- Task timeout failures
- Slow stage completion
- Executor heartbeat loss → "Executor lost" errors

### Off-Heap: The Solution

Off-heap memory lives **outside the JVM**, managed directly by the OS (via `sun.misc.Unsafe`).

| Property | On-Heap | Off-Heap |
|---|---|---|
| Managed by | JVM (GC) | OS / Developer |
| GC pressure | Yes — causes pauses | No GC |
| Speed | Faster (cache-friendly) | Slightly slower |
| Safety | Auto garbage collected | Manual — risk of memory leaks |
| Use case | Default | Large caches, high GC pressure jobs |

### When to Enable Off-Heap

Enable off-heap when you observe:
- Long GC pauses in Spark UI (`Event Timeline` → long yellow GC bars)
- `java.lang.OutOfMemoryError: GC overhead limit exceeded`
- Jobs with very large cached DataFrames

```python
# Enable off-heap in SparkSession config
spark = SparkSession.builder \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \  # 10-20% of executor memory
    .getOrCreate()
```

---

## 4. Key Spark Memory Configs – Cheat Sheet

```python
# ── Core Executor Memory ──────────────────────────────────────
spark.executor.memory          = "10g"   # On-heap JVM memory
spark.executor.memoryOverhead  = "1g"    # Off-JVM overhead (default: max(10%, 384MB))

# ── Unified Memory Split ──────────────────────────────────────
spark.memory.fraction          = 0.6    # Unified memory = 60% of (heap - 300MB)
spark.memory.storageFraction   = 0.5    # Storage = 50% of Unified memory

# ── Off-Heap ─────────────────────────────────────────────────
spark.memory.offHeap.enabled   = true
spark.memory.offHeap.size      = "2g"   # Recommended: 10-20% of executor memory

# ── Quick Math for 10GB executor ─────────────────────────────
# Usable heap       = 10GB - 300MB        = 9.7 GB
# Unified memory    = 9.7 × 0.6           = 5.82 GB (~6GB)
# Storage memory    = 5.82 × 0.5          = 2.91 GB (~3GB)
# Execution memory  = 5.82 × 0.5          = 2.91 GB (~3GB)
# User memory       = 9.7 × 0.4           = 3.88 GB (~3.7GB)
```

---

## 5. How YARN Allocates Memory

This is a common interview trap — knowing what YARN *actually* gives you vs. what you configure.

```
Developer sets:   spark.executor.memory = 10GB
                  spark.executor.memoryOverhead = 1GB (auto-calculated)

YARN allocates:   10GB + 1GB = 11GB container

If off-heap:      YARN allocates: 10GB + 1GB + offHeap.size
```

> **Key Point:** If YARN's container limit is lower than what Spark requests, the executor will be killed. Always check `yarn.scheduler.maximum-allocation-mb` on your cluster.

---

## 6. Interview Q&A – Tiered

---

### 🟢 Basic Level

**Q1. What is the difference between `spark.executor.memory` and the total memory YARN allocates?**

**A:** `spark.executor.memory` sets only the **on-heap JVM memory**. YARN allocates additional **overhead memory** (`spark.executor.memoryOverhead`, default ~10% or min 384MB) for OS-level operations, Python process memory (in PySpark), and internal Netty buffers. So for `spark.executor.memory = 10GB`, YARN gives ~11GB.

---

**Q2. What is Reserved Memory in Spark and why is it hardcoded at 300MB?**

**A:** Reserved memory is set aside for Spark's own internal objects and metadata — things like the SparkContext, task metadata, and internal data structures. It is **hardcoded at 300MB** and not configurable. It's subtracted first before any fraction-based calculation. Without this buffer, Spark's internal operations could starve under memory pressure.

---

**Q3. What types of operations use Execution Memory?**

**A:** Joins (sort-merge, hash), shuffle read/write buffers, sort operations, aggregations (GroupBy), and window functions. These are *in-progress* computations that require temporary scratch space.

---

### 🟡 Medium Level

**Q4. Can Storage Memory and Execution Memory borrow from each other? What are the rules?**

**A:** Yes — this is the core of Unified Memory Management. 
- **Execution can evict Storage blocks** if it needs more space (cached blocks spill to disk).  
- **Storage can borrow free (unused) Execution memory**.  
- **Storage cannot evict in-use Execution memory** because that would break active shuffle/join tasks.  
The split defined by `spark.memory.storageFraction` is a *soft* boundary, not a hard wall.

---

**Q5. You have a Spark job that caches a 5GB DataFrame but your Storage memory is only 3GB. What happens?**

**A:** Spark uses an **LRU (Least Recently Used)** eviction policy. It will cache as many partitions as fit in the 3GB storage pool. The remaining partitions are either:
- Not cached (recomputed on next access), or  
- Spilled to disk if `persist(StorageLevel.MEMORY_AND_DISK)` is used.  
To reduce the footprint, you can use `.persist(StorageLevel.MEMORY_AND_DISK_SER)` which serializes data before caching, reducing size by ~2-3x.

---

**Q6. What is User Memory and what can go wrong if it fills up?**

**A:** User Memory stores developer-defined objects — Python/Scala variables, UDF closures, broadcast variable metadata, and custom accumulators. If a UDF holds a large lookup dictionary or you create large in-memory collections in your code, this region can fill up. When User Memory is exhausted, you get `OutOfMemoryError` not from Spark's memory manager but from the JVM itself — harder to debug because Spark's UI won't directly surface it.

---

### 🔴 Advanced Level

**Q7. When would you choose off-heap memory, and what are its risks?**

**A:** Off-heap is ideal when:
1. You see **frequent GC pauses** (>1s) in the Spark UI event timeline
2. Jobs with **large cached DataFrames** are causing GC overhead errors
3. You're processing **long-running streaming jobs** where GC pauses cause processing delays

Risks:
- **Memory leaks** — off-heap is not GC'd; improper usage in custom serializers or native code can silently leak memory
- **Debugging difficulty** — OOM in off-heap doesn't produce the usual JVM heap dumps
- **Slightly slower** than on-heap due to serialization overhead when crossing the JVM boundary

---

**Q8. A job runs fine with 5 executors but fails with "ExecutorLostFailure" when scaled to 20 executors. Memory is suspected. How do you diagnose?**

**A:** Systematic diagnosis steps:
1. **Check Spark UI → Executors tab** — look at GC time column. If GC time > 10% of task time, memory pressure is the cause.
2. **Check overhead memory** — with PySpark, Python worker processes consume overhead memory. 20 executors × higher Python memory = overhead exhausted → YARN kills containers.
3. **Check YARN logs** for "Container killed by YARN for exceeding memory limits" — this confirms overhead, not heap.
4. **Fix:** Increase `spark.executor.memoryOverhead` to 15-20% of executor memory, or reduce `spark.executor.cores` to lower per-executor Python process memory.

---

**Q9. Explain what happens step-by-step when a sort-merge join runs out of Execution Memory.**

**A:**
1. Sort-merge join starts building sort buffers in Execution Memory
2. Execution Memory fills up → Spark checks if it can evict Storage blocks → evicts if available
3. If still not enough → **spill to disk** (Spark's ExternalSorter writes sorted partitions to local disk)
4. Final merge reads spilled partitions back from disk + remaining in-memory partitions
5. Result: job completes but with **much higher latency** due to disk I/O

This is why reducing data size before a join (push-down filters, broadcast small tables) is always preferred over relying on spill.

---

## 7. Real-World Scenarios & Solutions

---

### 🏗️ Scenario 1 – GC Pressure on a Large Delta Lake Pipeline

**Context:** You're running a daily ETL pipeline on Azure Databricks that reads 50GB of Delta Lake data, applies complex transformations, and writes to a Gold layer. The job takes 3 hours but should take ~45 minutes. Spark UI shows GC time at 35% of total task time.

**Root Cause Analysis:**
- Executor memory: 8GB, with 6GB unified — large intermediate DataFrames are filling the heap
- Frequent GC cycles are pausing tasks for seconds at a time
- No off-heap configured

**Solution:**

```python
spark = SparkSession.builder \
    .appName("GoldLayerETL") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.memoryOverhead", "1g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \     # moved large caches here
    .config("spark.memory.fraction", "0.7") \         # increase unified pool
    .config("spark.memory.storageFraction", "0.3") \  # less storage, more execution
    .getOrCreate()

# Also: unpersist DataFrames as soon as you're done with them
bronze_df = spark.read.format("delta").load("/bronze/sales")
silver_df = bronze_df.filter(...).withColumn(...)
silver_df.cache()

# ... multiple operations on silver_df ...

silver_df.unpersist()   # free memory before gold layer processing
gold_df = silver_df.groupBy(...).agg(...)
gold_df.write.format("delta").save("/gold/sales_summary")
```

**Result:** GC time dropped to ~4%, job completed in 50 minutes.

---

### 🏗️ Scenario 2 – Broadcast Join vs. Sort-Merge Join Memory Decision

**Context:** In your MDM Golden Record pipeline, you join a 200GB transactions table with a 150MB master entity lookup table. The job is hitting memory limits during the join phase.

**Problem:** Spark is defaulting to a sort-merge join, requiring both sides to be sorted and shuffled — consuming massive Execution Memory.

**Solution: Force a Broadcast Join**

```python
from pyspark.sql.functions import broadcast

# The lookup table is small enough to broadcast (< 200MB threshold)
entity_lookup = spark.read.format("delta").load("/master/entities")  # 150MB

transactions = spark.read.format("delta").load("/transactions")      # 200GB

# Broadcast the small table to all executors — eliminates shuffle entirely
result = transactions.join(
    broadcast(entity_lookup),
    on="entity_id",
    how="left"
)

# Config: increase broadcast threshold if needed
# spark.sql.autoBroadcastJoinThreshold = 209715200  # 200MB (default is 10MB)
```

**Why this helps memory:** Broadcast join stores the 150MB table in **Storage Memory** on each executor. There is no shuffle, so **Execution Memory is freed** for other operations. The 200GB table never needs to be sorted or exchanged.

---

### 🏗️ Scenario 3 – Apache Iceberg Migration with Memory Tuning

**Context:** You're migrating a large Hive table (~500GB, 10,000+ partitions) to Apache Iceberg. The migration job keeps failing with `java.lang.OutOfMemoryError` during the metadata write phase.

**Root Cause:** Iceberg's metadata operations (snapshot creation, manifest file writes) are handled in the **driver** and **User Memory** of executors. With 10,000+ partition metadata objects, User Memory is exhausted.

**Solution:**

```python
# 1. Increase driver memory for metadata operations
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \           # driver handles Iceberg metadata
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.memoryOverhead", "2g") \ # overhead for Iceberg writers
    .config("spark.memory.fraction", "0.6") \
    .getOrCreate()

# 2. Migrate in batches by partition to reduce per-job memory pressure
partition_years = [2020, 2021, 2022, 2023, 2024]

for year in partition_years:
    hive_batch = spark.read.table("hive_db.large_table").filter(f"year = {year}")
    
    hive_batch.writeTo("iceberg_catalog.iceberg_db.large_table") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .append()
    
    # Explicitly clear the plan cache between batches
    spark.catalog.clearCache()
    print(f"Year {year} migrated successfully.")

# 3. After migration: run Iceberg maintenance to compact metadata
spark.sql("CALL iceberg_catalog.system.rewrite_manifests('iceberg_db.large_table')")
```

---

### 🏗️ Scenario 4 – Diagnosing OOM in a PySpark UDF

**Context:** A pipeline using a PySpark UDF (Python function applied row-by-row) is failing with executor OOM errors. The UDF loads a 500MB ML model per call.

**Root Cause:** Each executor spawns a Python worker process. The ML model (500MB) is loaded **into Python process memory (overhead memory)** for every task. With 4 cores per executor, that's 4 Python workers × 500MB = 2GB of overhead memory, but `memoryOverhead` was set to the default 384MB.

**Solution:**

```python
# Option 1: Broadcast the model, load once per executor using a Singleton pattern
import pickle
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Broadcast the serialized model
model_bytes = sc.broadcast(pickle.dumps(loaded_model))

def predict(features):
    import pickle
    # Model is deserialized once and reused within the same Python worker
    if not hasattr(predict, "_model"):
        predict._model = pickle.loads(model_bytes.value)
    return float(predict._model.predict([features])[0])

predict_udf = udf(predict, DoubleType())

# Option 2: Increase memoryOverhead to account for Python workers
# spark.executor.memoryOverhead = 3g   (4 workers × ~700MB each)

# Option 3 (Best): Use Pandas UDFs (vectorized) — processes batches, not rows
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(DoubleType())
def predict_pandas(features: pd.Series) -> pd.Series:
    model = pickle.loads(model_bytes.value)
    return pd.Series(model.predict(features.tolist()))

df.withColumn("prediction", predict_pandas("features_col"))
```

---

## Quick Reference – Memory Troubleshooting Checklist

| Symptom | Likely Cause | Fix |
|---|---|---|
| `GC overhead limit exceeded` | JVM heap full | Enable off-heap, increase `spark.memory.fraction` |
| `Container killed by YARN` | Overhead exceeded | Increase `spark.executor.memoryOverhead` |
| Executor Lost / Heartbeat timeout | GC pause too long | Off-heap, reduce executor cores |
| Tasks spilling to disk | Execution memory too small | Increase `spark.executor.memory`, reduce join size |
| OOM in UDF | Python worker memory in overhead | Increase overhead, use Pandas UDF |
| Broadcast join OOM on driver | Small table still too big | Increase `spark.driver.memory` |

---

*Next Topic: Spark Shuffle, Partitioning & Data Skew* 🚀
