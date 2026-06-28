# Databricks Cluster & Compute — Interview Q&A

---

## CATEGORY 1 — Cluster Types (Always Asked First)

**Q: What are the types of clusters in Databricks?**

| Cluster Type | When to Use | Who Controls It |
|---|---|---|
| **All-Purpose Cluster** | Interactive notebooks, development, ad-hoc queries | You start/stop manually |
| **Job Cluster** | Production pipeline runs — created fresh per job, terminated after | ADF / Databricks Jobs auto-manages |
| **SQL Warehouse** | Databricks SQL, BI dashboards (Power BI, Tableau) | Serverless or managed |

> **Interview tip:** Always say — *"In production we use Job Clusters, not All-Purpose, because Job Clusters are ephemeral — they spin up clean per run and terminate automatically, so no stale state and no idle cost."*

---

**Q: What is the difference between All-Purpose and Job Cluster in production?**

```
All-Purpose Cluster:
  - Persistent, always running
  - Shared across multiple notebooks
  - Risk: stale SparkContext, library conflicts
  - Cost: Charged even when idle
  - Use: Development only

Job Cluster:
  - Created fresh when job starts
  - Destroyed when job ends
  - Isolated: clean environment every run
  - Cost: Pay only for job duration
  - Use: Production pipelines (ADF triggers this)
```

---

## CATEGORY 2 — Driver vs Executor (Core Concept)

**Q: Explain Driver node vs Executor node. What does each do?**

```
YOUR SPARK CLUSTER:

┌─────────────────────────────────┐
│         DRIVER NODE             │
│                                 │
│  - Runs your main Python code   │
│  - Creates SparkContext         │
│  - Builds Execution Plan (DAG)  │
│  - Schedules Tasks to Executors │
│  - Collects results back        │
│  - Hosts Spark UI (port 4040)   │
└────────────┬────────────────────┘
             │ distributes tasks
    ┌────────┴──────────┐
    ▼                   ▼
┌──────────┐      ┌──────────┐
│EXECUTOR 1│      │EXECUTOR 2│
│          │      │          │
│ Core 1   │      │ Core 1   │
│ Core 2   │      │ Core 2   │
│ Core 3   │      │ Core 3   │
│          │      │          │
│ 10GB RAM │      │ 10GB RAM │
└──────────┘      └──────────┘
  processes          processes
  partition 1-5      partition 6-10
```

**Driver:**
- Brain of the cluster
- Does NOT process data partitions
- Holds the final collected results
- If Driver OOMs → entire job fails

**Executor:**
- Worker nodes — actually process data
- Each executor has N cores and M GB RAM
- Each core processes one partition at a time
- If one executor fails → Spark retries on another

---

## CATEGORY 3 — How to Size Driver and Executors

**Q: How do you decide Driver node size?**

```python
# Driver sizing rules:

# Small Driver (4-8 GB) when:
# - No .collect() calls
# - No large broadcast joins
# - Pipeline just reads, transforms, writes

# Large Driver (16-32 GB) when:
# - You use df.collect() — brings ALL data to driver
# - Large broadcast variables
# - Complex DAG with many stages
# - Running many concurrent notebooks on same cluster

# Rule of thumb:
Driver memory = 2x the size of your largest collect() result
```

**In Databricks UI:**
- Cluster → Edit → Driver type → choose node type
- Example: `Standard_DS3_v2` = 14 GB RAM, 4 cores (small — good for dev)
- Example: `Standard_DS5_v2` = 56 GB RAM, 16 cores (large — good for heavy orchestration)

---

**Q: How do you decide number of Executors and their size?**

```
Formula:

Step 1: Know your data size
  - Dataset = 200 GB

Step 2: Target partition size = 128-200 MB
  - Number of partitions = 200 GB / 128 MB = ~1600 partitions

Step 3: Each core processes 1 partition at a time
  - If you want to process all partitions in parallel: 1600 cores
  - But that's too expensive — aim for 2-3 waves
  - Target: 600 cores total → 3 waves of 1600 partitions

Step 4: Choose executor size
  - Rule: 5 cores per executor (sweet spot)
  - Too few cores (1-2): executor overhead high
  - Too many cores (16+): HDFS throughput bottleneck, GC pressure
  - 5 cores per executor = proven production standard

Step 5: Calculate executor count
  - 600 total cores / 5 cores per executor = 120 executors

Step 6: Memory per executor
  - Rule: 4 GB per core
  - 5 cores × 4 GB = 20 GB per executor
  - But reserve 1 GB for OS overhead
  - Effective memory = 19 GB per executor
```

---

**Q: What is the 5-core rule? Why not use all cores in one big executor?**

```
Why 5 cores is the sweet spot:

1 core per executor   → Too much overhead (JVM, GC per executor is wasteful)
5 cores per executor  → Good balance of parallelism and memory sharing
16+ cores per executor→ 
    - HDFS can only serve 3-5 parallel reads efficiently
    - Too many threads → GC pauses → job slows down
    - One slow task blocks all 16 cores (stragglers)

Example Node: 16 cores, 64 GB RAM

Option A: 1 executor with 15 cores, 63 GB
  → GC hell, HDFS bottleneck, one failure kills everything

Option B: 3 executors × 5 cores, 21 GB each  ✅
  → Better GC, 3x fault tolerance, HDFS happy
  → (1 core reserved for OS per node)
```

---

## CATEGORY 4 — Spark Memory Model (Deep Dive)

**Q: How is executor memory divided internally?**

```
Total Executor Memory = 20 GB (example)

├── Reserved Memory: 300 MB (fixed — Spark internals)
│
└── Usable Memory: 19.7 GB
    │
    ├── Spark Memory (spark.memory.fraction = 0.6 default)
    │   = 11.8 GB
    │   │
    │   ├── Execution Memory (75% of Spark Memory)
    │   │   = 8.85 GB
    │   │   Used for: shuffles, joins, aggregations, sorts
    │   │
    │   └── Storage Memory (25% of Spark Memory)  
    │       = 2.95 GB
    │       Used for: caching (.cache(), .persist())
    │       ← Can borrow from Execution if idle
    │
    └── User Memory (1 - 0.6 = 0.4 = 40%)
        = 7.88 GB
        Used for: your UDFs, Python objects, broadcast vars
```

**Q: What happens when Execution Memory is full?**

```
Shuffle data > Execution Memory
→ Spark spills to disk
→ Performance degrades (disk I/O is 100x slower)
→ Job doesn't fail but becomes VERY slow

Fix: Either increase executor memory OR reduce partition size
     so each task needs less memory
```

---

## CATEGORY 5 — Cluster Configuration in Databricks UI

**Q: Walk me through how you configure a cluster in Databricks for production.**

```
Databricks → Compute → Create Cluster → Fill:

1. Policy: Unrestricted (or your org's custom policy)

2. Cluster mode:
   - Single Node    → dev/testing only (driver = executor)
   - Standard       → production (separate driver + N workers)
   - High Concurrency→ multiple users sharing (SQL workloads)

3. Databricks Runtime:
   - 13.3 LTS  → stable, long-term support, use for production
   - ML Runtime → if you need MLflow, sklearn, TensorFlow
   - Never use non-LTS in production — it gets deprecated

4. Driver type:
   - Standard_DS4_v2 → 28 GB, 8 cores (typical)

5. Worker type:
   - Standard_DS4_v2 → 28 GB, 8 cores (typical)

6. Number of workers:
   - Fixed: e.g. 10 workers (predictable cost)
   - Autoscale: min 2, max 20 (cost-efficient but slower startup)

7. Auto termination:
   - All-Purpose: 30-60 min (ALWAYS set this)
   - Job Cluster: not needed (terminates after job)

8. Advanced Options → Spark Config:
   spark.sql.shuffle.partitions 400
   spark.databricks.delta.optimizeWrite.enabled true
   spark.databricks.delta.autoCompact.enabled true
```

---

## CATEGORY 6 — Spark Config Parameters

**Q: What Spark configurations do you set in production and why?**

```python
# In Databricks cluster Spark Config tab or notebook:

# 1. Shuffle partitions (default 200 — often wrong)
spark.conf.set("spark.sql.shuffle.partitions", "400")
# Rule: 2-3x number of cores in your cluster
# 200 GB data / 128 MB = 1600 → set to 1600 for big jobs

# 2. Adaptive Query Execution (AQE) — ALWAYS enable
spark.conf.set("spark.sql.adaptive.enabled", "true")
# AQE auto-adjusts shuffle partitions, fixes skew at runtime

# 3. Dynamic Partition Pruning
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
# Skips reading irrelevant partitions in joins

# 4. Delta optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
# Coalesces small files before writing (avoids small file problem)

spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
# Auto-compacts Delta files after writes

# 5. Broadcast join threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
# Tables smaller than 10 MB are auto-broadcast (no shuffle needed)
```

---

## CATEGORY 7 — Common Interview Scenarios

**Q: Your Spark job is running but very slow. How do you diagnose?**

```
Step 1: Open Spark UI (cluster → Spark UI tab)
  → Look at Stages: which stage is taking longest?

Step 2: Check for Data Skew
  → Stages tab → Task Duration: is one task taking 10x longer than others?
  → Fix: salting, repartition, AQE skew hint

Step 3: Check Shuffle Size
  → If shuffle read/write is huge → increase shuffle partitions
  → Or check for unnecessary wide transformations

Step 4: Check GC Time
  → If GC time > 10% of task time → executor memory too low
  → Increase executor memory or reduce data per executor

Step 5: Check Spill
  → If "Spill (disk)" shows large numbers → execution memory too low
  → Increase spark.memory.fraction or executor memory
```

---

**Q: What is the difference between repartition() and coalesce()?**

```python
# repartition(n) — Full shuffle
df.repartition(200)
# - Triggers a full shuffle (expensive)
# - Can INCREASE or DECREASE partition count
# - Results in perfectly equal partition sizes
# - Use: before a wide transformation (join, groupBy)

# coalesce(n) — No shuffle
df.coalesce(10)
# - No shuffle — just merges existing partitions
# - Can ONLY DECREASE partition count
# - Partitions may be unequal
# - Use: before writing to reduce output files

# Production example:
df_large = df.repartition(400)           # Before join — balance data
df_final.coalesce(10).write.parquet(...) # Before write — reduce files
```

---

**Q: What is OOM (Out of Memory) error and how do you fix it?**

```
Two types:

1. Driver OOM:
   Error: "java.lang.OutOfMemoryError" on driver
   Cause: df.collect() on huge dataset, large broadcast var
   Fix: 
     - Avoid collect() — use show(), write() instead
     - Increase driver memory
     - Use df.limit(1000).collect() for debugging

2. Executor OOM:
   Error: "ExecutorLostFailure" or "Container killed by YARN"
   Cause: Partition too large for executor memory
   Fix:
     - Repartition to smaller partitions: df.repartition(800)
     - Increase executor memory
     - Increase spark.memory.fraction
     - Check for data skew (one partition has 10GB, others have 100MB)
```

---

## Quick Cheat Sheet for Interview

| Question | Answer |
|---|---|
| Cores per executor? | **5 cores** (sweet spot) |
| Memory per core? | **4 GB per core** |
| Default shuffle partitions? | **200** — always tune this |
| Shuffle partitions rule? | **2-3x total cores** |
| Production cluster type? | **Job Cluster** (not All-Purpose) |
| Runtime for production? | **LTS version** (e.g. 13.3 LTS) |
| AQE — what does it do? | **Auto-tunes shuffle, fixes skew at runtime** |
| Driver OOM cause? | **collect() on large data** |
| Executor OOM cause? | **Partition too large / data skew** |
| repartition vs coalesce? | **repartition = shuffle, coalesce = no shuffle** |

---

Want me to go deeper on any specific one — like **AQE internals**, **data skew handling**, or **Photon engine** which interviewers are now asking about?
