# Apache Spark – Execution Tuning (Executor Sizing)
> **Level:** 3.5 Years Data Engineering | **Stack:** Azure Databricks, PySpark, Delta Lake

---

## Table of Contents
1. [What Is an Executor?](#1-what-is-an-executor)
2. [The Three Sizing Strategies](#2-the-three-sizing-strategies)
3. [Fat Executor – Deep Dive](#3-fat-executor--deep-dive)
4. [Thin Executor – Deep Dive](#4-thin-executor--deep-dive)
5. [Rules for Optimal Executor Sizing](#5-rules-for-optimal-executor-sizing)
6. [Worked Example 1 – 5 Node Cluster](#6-worked-example-1--5-node-cluster)
7. [Worked Example 2 – 3 Node Cluster](#7-worked-example-2--3-node-cluster)
8. [Why Data Size Doesn't Directly Drive Executor Sizing](#8-why-data-size-doesnt-directly-drive-executor-sizing)
9. [Interview Q&A – Tiered](#9-interview-qa--tiered)
10. [Real-World Scenarios & Solutions](#10-real-world-scenarios--solutions)

---

## 1. What Is an Executor?

An **Executor** is a JVM process launched on a **worker node** in a Spark cluster. It is responsible for:
- Running **tasks** assigned by the Driver
- Storing data in memory (caching/persistence)
- Writing shuffle output to local disk

```
Spark Cluster
├── Driver (orchestrates, schedules tasks)
└── Worker Node 1
│   ├── Executor 1  (cores + RAM)
│   │   ├── Task Thread 1
│   │   ├── Task Thread 2
│   │   └── Task Thread N  (one per core)
└── Worker Node 2
    └── Executor 2  (cores + RAM)
```

> **Key Rule:** One executor core = one task running in parallel.  
> So an executor with 5 cores runs **5 tasks simultaneously**.

### Node vs Executor vs Task

| Unit | What it is | Analogy |
|---|---|---|
| Node | Physical/virtual machine | Office building floor |
| Executor | JVM process on a node | A team on that floor |
| Core | Thread inside an executor | An individual worker |
| Task | Unit of work on one partition | One assignment for one worker |

---

## 2. The Three Sizing Strategies

Given a cluster of **5 nodes × 12 cores × 48 GB RAM**:

> First, subtract **1 core + 1 GB per node** for OS/Hadoop/YARN daemons.  
> Usable per node: **11 cores, 47 GB RAM**

```
┌────────────────────────────────────────────────────────────────────┐
│              5 Nodes × 11 Cores × 47 GB  (usable)                 │
├──────────────────┬──────────────────────┬──────────────────────────┤
│   Fat Executor   │   Thin Executor      │   Optimal Executor       │
│  1 per node      │  11 per node         │  ~2 per node             │
│  11 core / 47GB  │  1 core / 4GB        │  5 core / 20GB           │
└──────────────────┴──────────────────────┴──────────────────────────┘
```

---

## 3. Fat Executor – Deep Dive

### Configuration
```
Per Node:  1 executor  ×  11 cores  ×  47 GB RAM
Cluster:   5 executors ×  11 cores  ×  47 GB RAM

spark-submit:
  --num-executors  5
  --executor-cores 11
  --executor-memory 47g
```

### Why Fat Executors Seem Attractive
- All 11 cores share one large memory pool — useful for operations requiring large broadcast tables
- Fewer executor JVM processes → lower per-process overhead

### The Real Problems

#### 1. HDFS Throughput Degradation
HDFS clients on each executor use **multiple threads** to read/write data. With 11 concurrent threads per executor, HDFS I/O contention becomes a bottleneck. The community recommendation (and Databricks guidance) is **3–5 cores per executor** for optimal HDFS throughput.

#### 2. GC Pressure at Scale
With 47 GB in a single JVM heap, GC pauses are long and unpredictable. A GC cycle on an 11-core executor pauses **all 11 tasks simultaneously** — compared to a smaller executor where only 5 tasks are affected.

#### 3. Fault Tolerance Risk
If a 47GB executor fails, all data cached on it is lost and all 11 in-progress tasks must be restarted. This is expensive to recover from.

#### 4. Resource Underutilization
If your job's tasks don't all use memory equally, a 47GB pool may sit largely idle while you've blocked those resources from other jobs on the cluster.

### Summary Table

| | Fat Executor |
|---|---|
| ✅ Task-level parallelism | High (11 tasks at once per node) |
| ✅ Data locality | Good (large local cache) |
| ❌ GC performance | Poor (large heap = long GC pauses) |
| ❌ HDFS throughput | Poor (>5 HDFS client threads is counterproductive) |
| ❌ Fault tolerance | Poor (expensive to restart) |
| ❌ Resource efficiency | Poor (idle memory blocks cluster resources) |

---

## 4. Thin Executor – Deep Dive

### Configuration
```
Per Node:  11 executors × 1 core × 4 GB RAM
Cluster:   55 executors × 1 core × 4 GB RAM

spark-submit:
  --num-executors  55
  --executor-cores 1
  --executor-memory 4g
```

### Why Thin Executors Seem Attractive
- More executors = more executor-level parallelism
- Smaller unit of failure — one executor going down loses only 1 task

### The Real Problems

#### 1. No Multi-Threading Benefit
With 1 core, there is **no parallelism within an executor**. Features like broadcast variables and cached DataFrames **cannot be shared** between tasks within the same executor (because there's only one task per executor). You lose the memory-sharing advantage of multi-threaded executors.

#### 2. High Network Traffic & Shuffle Cost
With 55 executors each holding tiny memory, shuffle data is fragmented across many executors. This increases network traffic during shuffle read phases.

#### 3. JVM Overhead × 55
Every executor is a separate JVM process. 55 JVMs = 55 sets of JVM startup overhead, 55 separate GC cycles to manage, and 55 heartbeat connections to the Driver.

#### 4. Reduced Data Locality
With only 4GB per executor, each executor can hold fewer partitions locally. HDFS reads are more likely to be **remote reads** (rack-aware hops) rather than local reads.

### Summary Table

| | Thin Executor |
|---|---|
| ✅ Executor-level parallelism | High (55 executors) |
| ✅ Fault tolerance | Good (small unit of failure) |
| ❌ Memory sharing | None (1 task per executor) |
| ❌ Network traffic | High (fragmented shuffle) |
| ❌ Data locality | Poor (small memory = fewer local partitions) |
| ❌ JVM process overhead | High (55 JVM instances) |

---

## 5. Rules for Optimal Executor Sizing

Follow these **4 rules in order** every time:

```
Rule 1: Reserve 1 core + 1 GB per node for OS/Hadoop/YARN daemons

Rule 2: Reserve resources for YARN ApplicationMaster (AM)
        → 1 executor worth of resources (not needed for fat executor approach)
        → Typically: 1 core + 1 GB RAM at the cluster level

Rule 3: Use 3–5 cores per executor
        → Balances HDFS throughput, GC pressure, and parallelism
        → 5 cores is the most common sweet spot

Rule 4: Executor memory you specify EXCLUDES overhead memory
        → memory_overhead = max(384MB, 10% of executor memory)
        → Subtract overhead BEFORE specifying --executor-memory
        → i.e., --executor-memory = (allocated_memory_per_executor - overhead)
```

---

## 6. Worked Example 1 – 5 Node Cluster

### Given
```
5 nodes × 12 cores × 48 GB RAM
```

### Step 1 – Reserve for OS/Hadoop/YARN (per node)
```
12 - 1 = 11 cores per node
48 - 1 = 47 GB per node
```

### Step 2 – Cluster-Level Totals
```
Total cores  = 11 × 5 = 55
Total memory = 47 × 5 = 235 GB
```

### Step 3 – Reserve for YARN Application Master
```
Total cores  = 55 - 1 = 54
Total memory = 235 - 1 = 234 GB
```

### Step 4 – Apply the 5-Core Rule
```
Executors = 54 / 5 = 10 executors  (floor)
```

### Step 5 – Memory Per Executor (before overhead)
```
Raw memory per executor = 234 / 10 = 23.4 GB ≈ 23 GB
```

### Step 6 – Subtract Overhead
```
Overhead = max(384MB, 10% of 23GB) = max(384MB, 2.3GB) = 2.3 GB
Executor memory = 23 - 2.3 ≈ 20 GB
```

### Final spark-submit
```bash
spark-submit \
  --num-executors  10 \
  --executor-cores 5  \
  --executor-memory 20g
```

### Memory per Core Check
```
Memory per executor = 20 GB / 5 cores = 4 GB per core
Default partition size = 128 MB
→ Each core can comfortably process partitions up to 4 GB
→ A 10 GB file = 80 partitions × 128 MB — well within capacity
```

---

## 7. Worked Example 2 – 3 Node Cluster

### Given
```
3 nodes × 16 cores × 48 GB RAM
```

### Step 1 – Reserve for OS/Hadoop/YARN (per node)
```
16 - 1 = 15 cores
48 - 1 = 47 GB
```

### Step 2 – Cluster-Level Totals
```
Total cores  = 15 × 3 = 45
Total memory = 47 × 3 = 141 GB
```

### Step 3 – Reserve for YARN ApplicationMaster
```
Total cores  = 45 - 1 = 44
Total memory = 141 - 1 = 140 GB
```

### Step 4 – Apply the 4-Core Rule (fits better here)
```
Executors = 44 / 4 = 11 executors
```

### Step 5 – Memory Per Executor (before overhead)
```
Raw memory per executor = 140 / 11 ≈ 12 GB
```

### Step 6 – Subtract Overhead
```
Overhead = max(384MB, 10% of 12GB) = max(384MB, 1.2GB) = 1.2 GB ≈ 1 GB
Executor memory = 12 - 1 = 11 GB
```

### Final spark-submit
```bash
spark-submit \
  --num-executors  11 \
  --executor-cores 4  \
  --executor-memory 11g
```

---

## 8. Why Data Size Doesn't Directly Drive Executor Sizing

This is a common misconception. Executor sizing is about **memory per core**, not total data size.

```
The correct mental model:

  1 core  processes  1 partition  at a time
  
  So you need: memory per core ≥ size of 1 partition

  Default partition size = 128 MB
  In Example 1: memory per core = 4 GB
  → You have 32× headroom per partition — plenty of room
```

Data size matters for **number of partitions** and **parallelism planning**, not raw executor memory sizing.

```python
# How Spark partitions a 10 GB file (default 128 MB block size):
# 10 GB = 10 × 1024 MB = 10240 MB
# Partitions = 10240 / 128 = 80 partitions

# With 10 executors × 5 cores = 50 parallel tasks
# 80 partitions / 50 parallel slots = ~2 waves of processing
# This is fine — no need to resize executors for data volume
```

If partitions are too large (causing OOM), you tune:
```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "67108864")  # 64 MB
# or
df.repartition(200)
```

---

## 9. Interview Q&A – Tiered

---

### 🟢 Basic Level

**Q1. What is an executor in Spark, and what does executor-cores mean?**

**A:** An executor is a JVM process on a worker node that runs tasks and stores cached data. `executor-cores` defines how many tasks can run **in parallel within a single executor**. If you set `--executor-cores 5`, that executor runs 5 tasks simultaneously, each on one partition.

---

**Q2. Why do we subtract 1 core and 1 GB per node before sizing executors?**

**A:** Every node runs OS processes and Hadoop/YARN daemons (NodeManager, DataNode, etc.) that need CPU and memory to function. If Spark claims all resources, these daemons get starved, causing node instability, YARN heartbeat failures, and potential executor losses. It's standard practice to leave at least 1 core and 1 GB per node.

---

**Q3. What is the YARN ApplicationMaster (AM) and why does it need resources?**

**A:** The YARN AM is a process responsible for negotiating resources with the ResourceManager and managing the Spark Driver's lifecycle on YARN. It runs on one of the nodes and needs a small portion of cluster resources (typically 1 core + 1 GB). If you don't account for it, YARN may fail to launch the AM, causing the job to not start at all.

---

### 🟡 Medium Level

**Q4. How do you decide on executor sizing when given cluster specs? Walk me through your process.**

**A:** I follow 4 steps:
1. Subtract 1 core + 1 GB per node for OS/YARN daemons
2. Subtract 1 core + 1 GB at cluster level for YARN AM
3. Decide cores per executor (I target 4–5) and divide total cores to get executor count
4. Divide total remaining memory by executor count, then subtract overhead memory (`max(384MB, 10% of raw memory)`) to get `--executor-memory`

For a 5-node cluster with 12 cores and 48 GB each: the result is 10 executors, 5 cores, 20 GB.

---

**Q5. Why is the 3–5 core recommendation important for HDFS-based clusters?**

**A:** Each executor core spawns an **HDFS client thread** for reading/writing data. HDFS performs best with around 3–5 concurrent client threads per executor. Beyond 5 threads, HDFS I/O becomes contended — multiple threads compete for the same HDFS blocks, disk bandwidth, and network slots, actually reducing throughput. This is the primary reason fat executors (11 cores) underperform in HDFS-based environments.

---

**Q6. Why is `--executor-memory` not the same as the memory YARN allocates?**

**A:** `--executor-memory` defines only the **on-heap JVM memory**. YARN actually allocates `executor-memory + memoryOverhead`. The overhead (`max(384MB, 10% of executor memory)`) is used for off-JVM processes: Python worker processes in PySpark, Netty network buffers, and OS-level memory. If you set `--executor-memory 20g`, YARN actually reserves ~22 GB for that executor container.

---

### 🔴 Advanced Level

**Q7. On Databricks, you don't use spark-submit with --num-executors. How does executor sizing work there?**

**A:** Databricks uses **cluster configurations** in the UI or via the Jobs API (`num_workers`, `node_type_id`). The key difference:
- In YARN mode, you size executors manually via spark-submit flags
- In Databricks, you choose a **node type** (e.g., `Standard_DS3_v2` — 4 cores, 14 GB) and Databricks auto-configures one executor per worker node with cores/memory pre-set to the node's specs (minus Databricks runtime overhead)
- You can override with `spark.executor.cores` and `spark.executor.memory` in the cluster's Spark config section
- For auto-scaling clusters, Databricks dynamically adds/removes workers — here you focus on ensuring partition count scales with worker count (target 2–4 partitions per core)

---

**Q8. A job runs fine on your cluster, but after adding more data (3× volume), it starts hitting OOM errors. The cluster specs haven't changed. How do you approach this?**

**A:** More data usually means more/larger partitions. OOM is typically not a sizing issue but a **partition skew or partition size** issue:
1. **Check partition sizes** in Spark UI → Stage → Tasks — if some tasks process 10× more data than others, you have skew
2. **Check shuffle partitions** — `spark.sql.shuffle.partitions` defaults to 200. With 3× data, you may need 400–600
3. **Check broadcast joins** — if a table grew beyond `autoBroadcastJoinThreshold` (10 MB default), Spark stops broadcasting it and falls back to a sort-merge join, causing sudden large Execution Memory spikes
4. **Fix:** Increase shuffle partitions, salting for skew, adjust broadcast threshold — not necessarily more executor memory

---

**Q9. You're running a PySpark job on a 10-node cluster with 8 cores and 32 GB per node. Size the optimal executors and justify each decision.**

**A:**
```
Step 1 – Per node usable:
  Cores:  8 - 1 = 7
  Memory: 32 - 1 = 31 GB

Step 2 – Cluster totals:
  Cores:  7 × 10 = 70
  Memory: 31 × 10 = 310 GB

Step 3 – Subtract for YARN AM:
  Cores:  70 - 1 = 69
  Memory: 310 - 1 = 309 GB

Step 4 – 5 cores per executor (PySpark: keep 5 for HDFS + GC balance):
  Executors = 69 / 5 = 13 executors

Step 5 – Memory per executor (raw):
  309 / 13 = 23.8 GB ≈ 23 GB

Step 6 – Subtract overhead:
  Overhead = max(384MB, 10% × 23) = 2.3 GB
  For PySpark, I'd add extra overhead buffer (Python workers): bump to 3 GB
  Executor memory = 23 - 3 = 20 GB

Final:
  --num-executors  13
  --executor-cores 5
  --executor-memory 20g
  spark.executor.memoryOverhead = 3g  ← explicit for PySpark
```

---

## 10. Real-World Scenarios & Solutions

---

### 🏗️ Scenario 1 – ETL Job Fails with "Container Killed by YARN"

**Context:** You submit a PySpark job on a 10-node YARN cluster (12 cores, 48 GB each). You used a fat executor config (1 executor per node, 11 cores, 47 GB). Two hours in, YARN kills executor containers.

**Root Cause:**
- You set `--executor-memory 47g`
- YARN calculates container size as `47 + overhead = 47 + 4.7 ≈ 52 GB`
- Node's total RAM is 48 GB → YARN kills the container because it exceeds physical memory
- Additionally: PySpark Python worker processes consume additional overhead memory — not accounted for

**Fix:**
```bash
# Corrected sizing (leaving 1 GB for OS + accounting for overhead)
# Usable = 47 GB per node
# Overhead = max(384MB, 10% of executor memory)
# Target executor memory = 40g → overhead = 4g → YARN allocates 44g (fits in 47g)

spark-submit \
  --num-executors 10 \
  --executor-cores 5 \
  --executor-memory 18g \
  --conf spark.executor.memoryOverhead=2g
```

---

### 🏗️ Scenario 2 – Sizing Executors for a Delta Lake Pipeline on Azure Databricks

**Context:** You're running a daily Databricks job that processes 200 GB of Delta Lake data with joins, aggregations, and a Gold layer write. The cluster has 8 worker nodes of type `Standard_DS4_v2` (8 vCores, 28 GB RAM each).

**Sizing Calculation:**
```
Per node:
  Cores:  8 - 1 = 7   (1 for Databricks runtime)
  Memory: 28 - 2 = 26 GB  (Databricks reserves ~2 GB)

Cluster (8 workers):
  Total cores  = 7 × 8 = 56
  Total memory = 26 × 8 = 208 GB

Executor sizing (Databricks auto-assigns 1 executor per worker):
  Cores per executor = 7
  Memory per executor = 26 GB
  Overhead (auto) = max(384MB, 10% × 26) ≈ 2.6 GB

Spark config overrides in Databricks cluster UI:
  spark.executor.cores = 5              ← reduce from 7 to stay HDFS-safe
  spark.executor.memory = 20g
  spark.executor.memoryOverhead = 3g    ← extra buffer for Delta Lake writers
  spark.sql.shuffle.partitions = 400    ← tuned for 200 GB data
```

---

### 🏗️ Scenario 3 – Apache Iceberg Migration: Right-Sizing Executors

**Context:** You are migrating a 500 GB Hive table (10,000+ partitions) to Iceberg. The job runs fine with thin executors for small test batches but OOMs when run against the full dataset.

**Root Cause:** Thin executors (1 core, 4 GB) cannot hold even a single 128 MB partition in Execution Memory after accounting for Unified Memory fractions — especially during the shuffle-heavy rewrite phase of Iceberg migration.

```
Thin executor memory: 4 GB
Unified memory (60%): 2.4 GB
Execution memory (50% of unified): 1.2 GB
→ Sort/shuffle buffers for Iceberg manifest compaction have only 1.2 GB → spill → OOM on 500GB
```

**Fix — Switch to Optimal Executor Sizing:**
```bash
spark-submit \
  --num-executors  20 \
  --executor-cores 5  \
  --executor-memory 20g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.memory.fraction=0.7 \        # more room for Iceberg rewrite execution
  --conf spark.sql.shuffle.partitions=1000  # handle 500 GB × many partitions

# Run migration in year-based batches
for year in 2020 2021 2022 2023 2024; do
    spark.sql(f"""
        INSERT INTO iceberg_catalog.db.target_table
        SELECT * FROM hive_metastore.db.source_table
        WHERE year = {year}
    """)
done
```

---

### 🏗️ Scenario 4 – Dynamic Allocation vs Fixed Executors

**Context:** Your team debates whether to use fixed `--num-executors` or Spark's dynamic allocation for a shared Databricks cluster serving multiple jobs.

**Fixed Executors — When to use:**
- Predictable, single-job clusters
- Streaming jobs (need consistent resource availability)
- When you've tuned a specific sizing that you know works

**Dynamic Allocation — When to use:**
- Shared clusters where multiple jobs run concurrently
- Jobs with variable data volumes (month-end vs daily)
- Cost optimization on cloud clusters

```python
# Enable Dynamic Allocation (Azure Databricks)
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "5")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "120s")  # release idle executors after 2 min

# Note: still apply the per-executor core/memory sizing rules
# Dynamic allocation scales QUANTITY, not size of individual executors
```

---

## Quick Reference – Executor Sizing Cheat Sheet

```
CLUSTER INPUTS:         N nodes × C cores × R GB RAM

Step 1 (per node):      cores = C-1      memory = R-1 GB
Step 2 (cluster):       total_cores = (C-1)×N      total_mem = (R-1)×N
Step 3 (YARN AM):       total_cores -= 1            total_mem -= 1
Step 4 (executor):      executor_count = total_cores / 5  (use 3-5 cores)
Step 5 (raw memory):    raw_mem = total_mem / executor_count
Step 6 (overhead):      overhead = max(384MB, 0.1×raw_mem)
                        executor_memory = raw_mem - overhead

OUTPUT:
  --num-executors   executor_count
  --executor-cores  5
  --executor-memory executor_memory
```

| Approach | Cores/Executor | Memory/Executor | Executors (5 node, 11c, 47GB) | Use When |
|---|---|---|---|---|
| Fat | 11 | 47 GB | 5 | ❌ Rarely — poor GC, poor HDFS |
| Thin | 1 | 4 GB | 55 | ❌ Rarely — no sharing, high overhead |
| Optimal | 4–5 | 20 GB | 10 | ✅ Almost always |

---

*Next Topic: Spark Shuffle, Partitioning & Data Skew* 🚀
