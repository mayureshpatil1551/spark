# Spark Cluster Configuration — Complete Interview Guide
## executor-memory · executor-cores · driver-memory · num-executors
### For Data Sizes: 10GB · 1GB · 100MB

---

## ⚠️ READ THIS FIRST — The Golden Formula

Before anything else, memorise this. Interviewers LOVE asking it.

```
Total Cluster RAM needed  =  Data Size  ×  3  to  4
(because Spark needs RAM for data + intermediate + overhead + OS)

executor-memory           =  RAM per worker node  −  overhead  −  OS
num-executors             =  Total cores available  /  executor-cores
executor-cores            =  5  (sweet spot — always start here)
driver-memory             =  1GB to 4GB  (unless collecting large data)
```

---

## TABLE OF CONTENTS

1. [What is a Spark Cluster? — The Physical Picture](#1-what-is-a-spark-cluster)
2. [executor-memory — Deep Dive](#2-executor-memory)
3. [executor-cores — Deep Dive](#3-executor-cores)
4. [num-executors — Deep Dive](#4-num-executors)
5. [driver-memory — Deep Dive](#5-driver-memory)
6. [Memory Internals — How Spark Uses RAM](#6-memory-internals)
7. [Configuration for 10GB Data](#7-configuration-for-10gb-data)
8. [Configuration for 1GB Data](#8-configuration-for-1gb-data)
9. [Configuration for 100MB Data](#9-configuration-for-100mb-data)
10. [How to Pass Configuration](#10-how-to-pass-configuration)
11. [Common Mistakes and Fixes](#11-common-mistakes-and-fixes)
12. [Interview Q&A — Real Questions](#12-interview-qa)

---

## 1. What is a Spark Cluster? — The Physical Picture

Before tuning any config, you must understand the physical setup. Interviewers always check this first.

```
┌─────────────────────────────────────────────────────────────────┐
│                        SPARK CLUSTER                           │
│                                                                 │
│  ┌─────────────────┐                                           │
│  │   DRIVER NODE   │  ← Your SparkSession lives here          │
│  │                 │    Runs main() / notebook code            │
│  │  driver-memory  │    Coordinates all executors             │
│  │  driver-cores   │    Collects results from executors        │
│  └────────┬────────┘                                           │
│           │  sends tasks to executors                          │
│           │                                                     │
│    ┌──────┴──────┬───────────────┬───────────────┐            │
│    ▼             ▼               ▼               ▼            │
│ ┌──────┐      ┌──────┐       ┌──────┐        ┌──────┐        │
│ │WORKER│      │WORKER│       │WORKER│        │WORKER│        │
│ │ NODE │      │ NODE │       │ NODE │        │ NODE │        │
│ │      │      │      │       │      │        │      │        │
│ │[EXE1]│      │[EXE2]│       │[EXE3]│        │[EXE4]│        │
│ │[EXE2]│      │      │       │      │        │      │        │
│ └──────┘      └──────┘       └──────┘        └──────┘        │
│                                                                 │
│  Each Worker Node = one physical/virtual machine               │
│  Each Executor    = one JVM process on a worker node           │
│  Each Executor has: its own RAM + its own CPU cores            │
└─────────────────────────────────────────────────────────────────┘
```

### Key Vocabulary

| Term | What it is | Lives on |
|------|-----------|----------|
| **Driver** | The brain — runs your code, creates SparkContext, plans the job | Driver node |
| **Executor** | The muscle — runs tasks, stores cached data | Worker nodes |
| **Task** | One unit of work — processes one partition | Runs inside executor |
| **Partition** | One chunk of your data | Stored in executor memory |
| **Stage** | Group of tasks that can run without a shuffle | Multiple executors |
| **Job** | One action (count, save) — broken into stages | Whole cluster |

### What Actually Happens When You Run a Query

```
You call df.count()
         │
         ▼
1. Driver creates a DAG (plan)
2. Driver splits data into partitions
3. Driver sends Task 1 → Executor 1 (processes partition 1)
   Driver sends Task 2 → Executor 2 (processes partition 2)
   Driver sends Task 3 → Executor 1 (processes partition 3) ← reuses executor
   ...
4. Each executor runs its task, returns result to driver
5. Driver aggregates results → returns count to you
```

---

## 2. executor-memory

### What it is

`executor-memory` is the amount of RAM allocated to **each executor JVM process**.

This is the most important configuration. Getting this wrong causes:
- OOM (Out of Memory) errors if too low
- Wasted resources and more GC pauses if too high

### What is included in executor-memory

When you set `--executor-memory 4g`, Spark uses that 4GB for:

```
┌─────────────────────────────────────────────────────┐
│              4GB executor-memory                    │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │         Unified Memory (default 60%)        │   │
│  │              = 4 × 0.6 = 2.4 GB            │   │
│  │                                             │   │
│  │  ┌───────────────────┬─────────────────┐   │   │
│  │  │  Execution Memory │ Storage Memory  │   │   │
│  │  │   (shuffles,      │  (cached RDDs,  │   │   │
│  │  │    sorts, joins,  │   DataFrames,   │   │   │
│  │  │    aggregations)  │   broadcast)    │   │   │
│  │  │                   │                 │   │   │
│  │  │  Can borrow from  │  Can borrow     │   │   │
│  │  │  storage memory   │  from execution │   │   │
│  │  └───────────────────┴─────────────────┘   │   │
│  └─────────────────────────────────────────────┘   │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │         Reserved Memory = 300MB (fixed)     │   │
│  │         (Spark internal objects)            │   │
│  └─────────────────────────────────────────────┘   │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │         User Memory (remaining ~38%)        │   │
│  │         (your custom data structures,       │   │
│  │          UDFs, accumulators)                │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### The memoryOverhead — Most Candidates Miss This!

When YARN or Kubernetes allocates memory to an executor, it allocates:

```
Total container memory = executor-memory + memoryOverhead

memoryOverhead = max(executor-memory × 0.10, 384MB)
```

**Example:**
```
executor-memory = 4g
memoryOverhead  = max(4096 × 0.10, 384) = max(409.6, 384) = 410MB

Total container = 4096 + 410 = ~4.5 GB requested from cluster
```

**Why does overhead matter?**
The overhead is for:
- JVM off-heap memory (Java NIO buffers)
- Python worker processes (when using PySpark/UDFs)
- Network buffers
- OS and JVM startup overhead

**If your PySpark UDFs use lots of memory, increase overhead:**
```python
spark.conf.set("spark.executor.memoryOverhead", "1g")
```

### Rule of thumb for executor-memory

```
executor-memory  =  (Node RAM - OS RAM) / executors per node

OS needs ~1-2 GB
Node RAM = 16GB
Executors per node = 2
executor-memory = (16 - 2) / 2 = 7GB → set to 6g (leave some buffer)
```

---

## 3. executor-cores

### What it is

`executor-cores` is the number of CPU cores allocated to **each executor**.

This controls how many tasks an executor can run **simultaneously** (parallelism within one executor).

### The Parallelism Math

```
One executor with 5 cores can run 5 tasks at the same time.

If you have:
  10 executors × 5 cores = 50 tasks running simultaneously = 50 partitions processed at once
```

### Why 5 is the Magic Number

This is a famous interview answer. Always say **5** and explain why:

```
Too few cores (e.g., 1 or 2):
  → Executor underutilizes the node's CPU
  → Many executors needed → more JVM overhead → more GC

Too many cores (e.g., 10-15):
  → All cores share the same executor RAM
  → Each task gets very little RAM
  → Spill to disk, OOM errors
  → HDFS throughput degrades
    (HDFS client has throughput problems with >5 concurrent threads)

5 cores — the sweet spot:
  → Good parallelism within executor
  → Each task gets enough RAM
  → HDFS throughput stays optimal
  → YARN container scheduling is efficient
```

### What happens with different core counts

```python
# Cluster: 4 worker nodes, each with 16 cores, 64GB RAM

# Bad: 1 core per executor
executor-cores = 1
# You get: many executors, each with tiny RAM slices
# 4 nodes × 16 cores = 64 executors × (64/16)GB = 4GB each
# Problem: 64 JVMs to manage, high JVM overhead

# Bad: 15 cores per executor
executor-cores = 15
# You get: 4 nodes × 1 executor each (15 cores per executor)
# 4 × 48GB each
# Problem: HDFS throughput degrades, huge GC pauses

# Good: 5 cores per executor  ← THE ANSWER
executor-cores = 5
# You get: 4 nodes × 3 executors each = 12 executors
# Each executor: 5 cores, ~20GB RAM (minus OS)
# PERFECT balance
```

---

## 4. num-executors

### What it is

`num-executors` is the **total number of executor processes** across the entire cluster.

### Static vs Dynamic Allocation

**Static allocation (you set it manually):**
```
--num-executors 10
```

**Dynamic allocation (Spark adjusts automatically — recommended):**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "5")
```

With dynamic allocation, Spark:
- Starts with `initialExecutors`
- Scales up when tasks are waiting (data is large)
- Scales down when executors are idle (query is light)

### Calculating num-executors

```
Total worker cores in cluster     = num_worker_nodes × cores_per_node
Cores reserved for OS per node    = 1 core
Usable cores per node             = cores_per_node - 1

Executors per node                = (usable cores per node) / executor-cores
Total executors                   = executors_per_node × num_worker_nodes - 1
                                                                          ↑
                               Leave 1 executor for ApplicationMaster (YARN)
```

### Full Example Calculation

```
Cluster:
  10 worker nodes
  16 cores per node
  64 GB RAM per node

Step 1: Executor cores = 5 (always start here)

Step 2: Executors per node
  Usable cores = 16 - 1 (OS) = 15
  Executors per node = 15 / 5 = 3

Step 3: num-executors
  Total = (3 × 10) - 1 (ApplicationMaster) = 29

Step 4: executor-memory
  RAM per executor = (64GB - 1GB OS) / 3 executors per node = 21GB
  Leave 10% for overhead: 21 × 0.9 = ~18GB
  Set executor-memory = 18g

Step 5: Verify
  Total RAM used  = 29 executors × (18 + 1.8 overhead) = ~574GB
  Total available = 10 × 63GB = 630GB  ← fine, within limits ✅

Final config:
  --num-executors     29
  --executor-cores    5
  --executor-memory   18g
```

---

## 5. driver-memory

### What it is

`driver-memory` is the RAM allocated to the **Driver JVM process**.

The driver runs your application code and coordinates executors. It is NOT where data processing happens — that's the executors.

### What the Driver Actually Does (and needs RAM for)

```
┌─────────────────────────────────────────────────────┐
│                  DRIVER TASKS                       │
│                                                     │
│  1. Runs your main program / notebook               │
│     → SparkSession, spark.read(), df transforms     │
│                                                     │
│  2. Creates the DAG (query plan)                    │
│     → Catalyst optimizer, physical plan             │
│                                                     │
│  3. Schedules tasks on executors                    │
│     → TaskScheduler, sends task descriptions        │
│                                                     │
│  4. Collects results from executors                 │
│     → df.collect() pulls ALL data to driver!        │
│     → df.show() pulls 20 rows                       │
│                                                     │
│  5. Holds broadcast variables                       │
│     → broadcast(small_df) → stored in driver first  │
│                                                     │
│  6. Holds accumulators                              │
└─────────────────────────────────────────────────────┘
```

### When to increase driver-memory

```
Default 1g is fine for:
  ✅ Most ETL jobs where results are written to storage
  ✅ Jobs that don't collect() large datasets
  ✅ Standard notebook execution

Increase to 2g–4g when:
  ⚠️  Using df.collect() on medium-sized results
  ⚠️  Broadcasting multiple large lookup tables
  ⚠️  Complex query planning (many joins, many CTEs)

Increase to 8g–16g when:
  ⚠️  Collecting large results to driver (avoid this!)
  ⚠️  Driver-side Pandas operations on large data
  ⚠️  Many large broadcast variables

NEVER do this (causes Driver OOM):
  ❌  df.collect() on a 10GB DataFrame
  ❌  Converting huge Spark DF to Pandas: df.toPandas()
```

### Driver memory rules

```python
# Rule 1: Never collect large DataFrames
rows = df.collect()      # ❌ pulls everything to driver RAM
rows = df.limit(1000).collect()  # ✅ cap it first

# Rule 2: Use show() not collect() for debugging
df.show(20)              # ✅ only 20 rows to driver

# Rule 3: Write results, don't collect
df.write.parquet("/mnt/output/")  # ✅ executors write directly
```

---

## 6. Memory Internals — How Spark Uses RAM

### The Unified Memory Model (Spark 1.6+)

```
Total executor-memory = 4GB

┌─────────────────────────────────────────────────┐
│  Reserved Memory = 300MB (fixed, not configurable)│
│  For Spark's own internal objects                 │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  Usable Memory = 4GB - 300MB = 3.7GB            │
└─────────────────────────────────────────────────┘
                    │
           split by spark.memory.fraction (default 0.6)
                    │
         ┌──────────┴──────────────┐
         ▼                         ▼
┌─────────────────┐       ┌───────────────────┐
│  Unified Memory │       │   User Memory     │
│  = 3.7 × 0.6   │       │  = 3.7 × 0.4     │
│  = 2.22 GB      │       │  = 1.48 GB        │
│                 │       │                   │
│  Split between: │       │  Your custom data │
│  Execution &    │       │  structures,UDFs, │
│  Storage memory │       │  accumulators     │
└────────┬────────┘       └───────────────────┘
         │
  split by spark.memory.storageFraction (default 0.5)
         │
    ┌────┴─────┐
    ▼          ▼
┌────────┐  ┌────────┐
│Execute │  │Storage │
│ Memory │  │ Memory │
│ 1.11GB │  │ 1.11GB │
│        │  │        │
│Shuffles│  │ Cache  │
│ Sorts  │  │persist │
│  Joins │  │brdcast │
│  Aggs  │  │        │
└────────┘  └────────┘
     ↕ CAN BORROW FROM EACH OTHER
  (dynamic — unified model)
```

### Key Config Parameters for Memory

```python
# How much of heap goes to Spark (rest is user memory)
spark.conf.set("spark.memory.fraction", "0.6")        # default

# Within Spark memory, how much is for storage (rest is execution)
spark.conf.set("spark.memory.storageFraction", "0.5") # default

# Off-heap memory (outside JVM — for Tungsten, Arrow)
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")

# Container overhead (10% of executor-memory, min 384MB)
spark.conf.set("spark.executor.memoryOverhead", "512m")
```

### What Spills to Disk

When execution memory is full during a shuffle or sort, Spark **spills** intermediate data to disk. This is:
- Much slower (disk I/O vs RAM)
- But doesn't crash the job (unlike OOM)

```
Signs of spilling (check Spark UI → Stages → Shuffle Spill):
  "Shuffle Spill (Memory)" — data that was in RAM before spill
  "Shuffle Spill (Disk)"   — data written to disk

If spill is large → increase executor-memory or reduce partition size
```

---

## 7. Configuration for 10GB Data

### Cluster Assumption
```
Cluster: 5 worker nodes, 16 cores each, 64GB RAM each
Data size: 10GB compressed Parquet (expands to ~30-40GB in memory)
```

### The Thinking Process (Say This in Interview)

```
Step 1: How much RAM do I need?
  10GB Parquet compressed
  Parquet compression ratio ≈ 3-4x
  In-memory size ≈ 30-40GB

  Spark needs:
    Input data:           ~35GB
    Shuffle intermediates: ~35GB (can be same size as input)
    Output/cache:          ~35GB (if caching)
  Total headroom needed: 70-100GB across all executors

Step 2: Pick executor-cores = 5 (always)

Step 3: Executors per node
  Usable cores = 16 - 1 = 15
  Executors per node = 15 / 5 = 3

Step 4: num-executors
  Total = (3 × 5 nodes) - 1 = 14

Step 5: executor-memory
  RAM per executor = (64 - 2) / 3 = ~20GB
  Set = 18g (leave some buffer)
  Overhead = 18 × 0.10 = 1.8g → ~20g per container

Step 6: Verify total RAM
  14 executors × 20g container = 280GB across cluster
  Total cluster RAM = 5 × 64 = 320GB
  Utilisation = 87.5% ← acceptable ✅

Step 7: driver-memory
  10GB job, writing to storage, no large collects
  driver-memory = 2g (standard, slightly more than default for query planning)

Step 8: Partition count
  Recommended partition size = 128-256MB
  In-memory data = ~35GB
  Partitions = 35000MB / 128MB = ~273
  Set: spark.sql.shuffle.partitions = 300
```

### Final Configuration for 10GB

```python
spark = SparkSession.builder \
    .appName("eCDP_10GB_Pipeline") \
    .config("spark.executor.instances",    "14") \
    .config("spark.executor.cores",        "5") \
    .config("spark.executor.memory",       "18g") \
    .config("spark.executor.memoryOverhead","2g") \
    .config("spark.driver.memory",         "2g") \
    .config("spark.driver.cores",          "2") \
    .config("spark.sql.shuffle.partitions","300") \
    .config("spark.sql.adaptive.enabled",  "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

```bash
# spark-submit equivalent
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 14 \
  --executor-cores 5 \
  --executor-memory 18g \
  --conf spark.executor.memoryOverhead=2g \
  --driver-memory 2g \
  --driver-cores 2 \
  --conf spark.sql.shuffle.partitions=300 \
  --conf spark.sql.adaptive.enabled=true \
  my_job.py
```

### Tuning Checklist for 10GB

```
✅ executor-memory=18g  → gives ~10.8GB unified memory per executor
✅ 14 executors × 5 cores = 70 parallel tasks
✅ 70 tasks × ~514MB per task = ~35GB processed simultaneously
✅ AQE enabled → handles skew automatically
✅ 300 shuffle partitions → ~117MB per partition after shuffle
✅ memoryOverhead=2g → handles PySpark subprocess + network buffers
```

---

## 8. Configuration for 1GB Data

### The Thinking Process

```
Step 1: Data size
  1GB compressed Parquet → ~3-4GB in memory (much smaller job)
  
Step 2: Don't over-provision!
  Common mistake: using the same large config for small data
  Over-provisioning wastes cluster resources and can cause issues
  (e.g., YARN can't fit an over-sized container)

Step 3: Smaller cluster needed
  In-memory data = ~3-4GB
  Spark working memory = ~8GB total should be sufficient

Step 4: Config
  executor-cores = 5 (keep this constant)
  executor-memory = 4g (3 executors on 1 node easily handles this)
  num-executors  = 3 (one node worth of executors)
  driver-memory  = 1g (default is fine)

Step 5: Partitions
  4GB / 128MB = ~31 partitions
  spark.sql.shuffle.partitions = 50 (small but not default 200)
```

### Final Configuration for 1GB

```python
spark = SparkSession.builder \
    .appName("Small_1GB_Job") \
    .config("spark.executor.instances",    "3") \
    .config("spark.executor.cores",        "5") \
    .config("spark.executor.memory",       "4g") \
    .config("spark.executor.memoryOverhead","512m") \
    .config("spark.driver.memory",         "1g") \
    .config("spark.sql.shuffle.partitions","50") \
    .config("spark.sql.adaptive.enabled",  "true") \
    .getOrCreate()
```

```bash
spark-submit \
  --num-executors 3 \
  --executor-cores 5 \
  --executor-memory 4g \
  --conf spark.executor.memoryOverhead=512m \
  --driver-memory 1g \
  --conf spark.sql.shuffle.partitions=50 \
  my_small_job.py
```

### Why NOT use the same big config for 1GB?

```
If you use 14 executors × 18g for 1GB data:

Problem 1: Resource waste
  14 × 18g = 252GB RAM requested for a 1GB job
  Other jobs starved of resources on shared cluster

Problem 2: Too many partitions → too many tiny tasks
  200 partitions (default) for 1GB = 5MB per partition
  5MB per task is tiny → massive task scheduling overhead
  → thousands of tiny tasks = slower than fewer bigger tasks

Problem 3: Executor startup overhead
  Launching 14 JVMs takes 30-60 seconds
  For a 1GB job that runs in 2 minutes, that's 25-50% overhead

Correct approach: right-size for the data
```

---

## 9. Configuration for 100MB Data

### The Thinking Process

```
Step 1: Data size
  100MB compressed → ~300-400MB in memory

Step 2: Is Spark even the right tool?
  For 100MB: Pandas would be faster!
  But in a production pipeline where this is one step among many,
  you still use Spark. Just configure it tiny.

Step 3: Minimal config
  1 executor is enough
  executor-cores = 2 (no need for 5)
  executor-memory = 2g (400MB data × 4 safety = 1.6GB → 2g is fine)
  driver-memory = 1g (default)
  shuffle.partitions = 10 (or even 5)

Step 4: Consider local mode
  For development/testing, local mode is fastest for tiny data:
  spark.master = "local[4]"  → 4 threads, no YARN overhead
```

### Final Configuration for 100MB

```python
# Production — still using cluster
spark = SparkSession.builder \
    .appName("Tiny_100MB_Job") \
    .config("spark.executor.instances",    "1") \
    .config("spark.executor.cores",        "2") \
    .config("spark.executor.memory",       "2g") \
    .config("spark.executor.memoryOverhead","512m") \
    .config("spark.driver.memory",         "1g") \
    .config("spark.sql.shuffle.partitions","10") \
    .config("spark.sql.adaptive.enabled",  "true") \
    .getOrCreate()

# Development/Testing — local mode (fastest for small data)
spark = SparkSession.builder \
    .appName("Tiny_100MB_Dev") \
    .master("local[4]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()
```

### The Default shuffle.partitions Problem for Small Data

```
spark.sql.shuffle.partitions = 200  (default)

For 100MB data:
  100MB / 200 partitions = 0.5MB per partition ← way too small!
  200 tiny tasks → massive scheduling overhead
  Each task launches JVM, reads 0.5MB, closes → wasteful

Fix: set shuffle.partitions based on data size
  Rule: target 128MB per partition
  100MB / 128MB = 1 partition (minimum)
  Set to 10 (with AQE it'll coalesce anyway)

With AQE (Adaptive Query Execution):
  spark.sql.adaptive.coalescePartitions.enabled = true
  Spark automatically merges small shuffle partitions
  → Set 200, AQE reduces to 10 automatically ✅
```

---

## 10. How to Pass Configuration

### Method 1 — SparkSession (in code)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.executor.instances",       "10") \
    .config("spark.executor.cores",           "5") \
    .config("spark.executor.memory",          "8g") \
    .config("spark.executor.memoryOverhead",  "1g") \
    .config("spark.driver.memory",            "4g") \
    .config("spark.driver.cores",             "2") \
    .config("spark.sql.shuffle.partitions",   "200") \
    .config("spark.sql.adaptive.enabled",     "true") \
    .config("spark.dynamicAllocation.enabled","false") \
    .getOrCreate()
```

### Method 2 — spark-submit (CLI)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "MyApp" \
  --num-executors 10 \
  --executor-cores 5 \
  --executor-memory 8g \
  --driver-memory 4g \
  --conf "spark.executor.memoryOverhead=1g" \
  --conf "spark.sql.shuffle.partitions=200" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.dynamicAllocation.enabled=false" \
  --py-files utils.zip \
  my_job.py
```

### Method 3 — spark-defaults.conf (cluster-wide defaults)

```properties
# /etc/spark/conf/spark-defaults.conf
spark.executor.memory          8g
spark.executor.cores           5
spark.driver.memory            4g
spark.sql.adaptive.enabled     true
spark.sql.shuffle.partitions   200
```

These are the defaults for ALL jobs. Individual jobs can override them.

### Method 4 — At Runtime (limited use)

```python
# Some configs can be changed after SparkSession created
spark.conf.set("spark.sql.shuffle.partitions", "100")

# But executor memory/cores CANNOT be changed after session started
# These are fixed at session creation time
```

### Method 5 — Databricks Cluster Configuration (UI)

```
Databricks UI → Compute → Edit Cluster → Advanced Options → Spark Config

spark.executor.memory          8g
spark.driver.memory            4g
spark.sql.shuffle.partitions   200
spark.sql.adaptive.enabled     true

# Note: In Databricks, num-executors is controlled by:
# Min workers / Max workers (for autoscaling clusters)
# or Fixed workers (for fixed clusters)
```

---

## 11. Common Mistakes and Fixes

### Mistake 1 — OOM (Out of Memory) Error

```
Error: ExecutorLostFailure (executor 3 exited caused by one of the 
       stages): Container killed by YARN for exceeding memory limits.
       16.5 GB of 16 GB physical memory used.

Root Cause: executor-memory + overhead > container limit

Fix:
  # If executor-memory = 15g
  # YARN sees: 15g + (15 × 0.10) = 16.5g → over the 16g limit!
  
  # Option A: Reduce executor-memory
  executor-memory = 13g
  
  # Option B: Increase container limit
  spark.conf.set("spark.executor.memoryOverhead", "512m")
  # Now: 15g + 512m = 15.5g → fits in 16g ✅
```

### Mistake 2 — Too Many Shuffle Partitions for Small Data

```
Problem: 200 shuffle partitions (default) for 500MB data
  → 200 tasks of 2.5MB each
  → Massive task scheduling overhead
  → Job runs slower than expected

Fix:
  # Based on data size
  # Target: 128MB per partition
  # 500MB / 128MB = 4 → set to 20 (with some buffer)
  spark.conf.set("spark.sql.shuffle.partitions", "20")
  
  # Or just enable AQE — it handles this automatically
  spark.conf.set("spark.sql.adaptive.enabled", "true")
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Mistake 3 — Too Few Executors (Underparallelism)

```
Problem: 2 executors for 10GB job
  → Only 10 parallel tasks (2 executors × 5 cores)
  → Job takes 5x longer than it should

Signs: Spark UI shows low CPU utilisation, few running tasks

Fix: Increase num-executors
  10GB → needs ~14 executors (as calculated above)
```

### Mistake 4 — Not Leaving Resources for OS and NodeManager

```
Problem: Node has 64GB RAM
  Setting executor-memory = 64g per executor
  → YARN NodeManager has no RAM for itself
  → Node becomes unstable, containers get killed

Fix: Always leave at least:
  1-2 GB for OS
  1 GB for YARN NodeManager
  
  Max usable per node = 64 - 2 (OS) - 1 (NM) = 61GB
  executor-memory per executor = 61 / num_executors_per_node
```

### Mistake 5 — Forgetting memoryOverhead for PySpark

```
Problem: PySpark UDF uses lots of memory
  executor-memory = 8g → but containers are being killed at 8g usage

Why: PySpark spawns a Python subprocess for each executor
  This subprocess is NOT counted in executor-memory
  It's counted in memoryOverhead

Fix:
  spark.conf.set("spark.executor.memoryOverhead", "2g")
  # Now total container = 8g + 2g = 10g
  # Python subprocess has 2g headroom
```

### Mistake 6 — Using df.collect() on Large Data

```python
# WRONG — pulls all 10GB to driver RAM
all_rows = df.collect()   # ← driver OOM!

# RIGHT — write to storage instead
df.write.parquet("/mnt/output/")

# RIGHT — if you need sampling
sample_rows = df.limit(1000).collect()

# RIGHT — if you need aggregates
summary = df.groupBy("dept").count().collect()  # small result
```

---

## 12. Interview Q&A — Real Questions

---

**Q1. What is the difference between executor-memory and driver-memory?**

**A:** Driver memory is RAM for the driver JVM — the program that runs your code, creates the SparkContext, plans queries, schedules tasks, and collects results. It is the coordinator.

Executor memory is RAM for executor JVMs — the workers that actually run your transformation tasks and store cached data. They do the heavy processing.

Driver memory is typically small (1-4GB) unless you are calling `collect()` on large DataFrames or broadcasting many large tables. Executor memory is where most tuning happens — it must be large enough for data processing, shuffles, and caching.

Rule of thumb: increase driver memory only when you see driver OOM errors. Increase executor memory when you see executor OOM or excessive disk spill.

---

**Q2. Why do you set executor-cores to 5? Why not 10 or 1?**

**A:** 5 is the industry sweet spot for three reasons.

First, HDFS throughput. The HDFS client has throughput degradation beyond 5 concurrent read threads per executor. More than 5 cores means reading data slower, not faster.

Second, memory per task. With 5 cores sharing executor memory, each task gets a reasonable slice. With 15 cores sharing the same memory, each task gets so little RAM that spilling to disk becomes frequent.

Third, JVM overhead. With 1 core per executor you need many more executors (one per core), each with its own JVM startup cost, GC overhead, and YARN container overhead. 5 cores balances parallelism with JVM efficiency.

If the executor-memory is very large (say 40GB), you might increase to 7 cores. If memory is very small (say 2GB), reduce to 2-3 cores. But 5 is always the starting point.

---

**Q3. What is memoryOverhead and why do you need it?**

**A:** When YARN or Kubernetes allocates a container for an executor, the container must hold the executor JVM memory PLUS off-heap memory for things outside the JVM heap: JVM internal buffers, network buffers, Python subprocess memory (for PySpark), and OS overhead.

This extra memory is `memoryOverhead`. The formula is `max(executor-memory × 0.10, 384MB)`. So for an 8GB executor, YARN allocates 8GB + 819MB = about 8.8GB total.

This matters because if you set executor-memory=8g on a node where the YARN container limit is 8g, the actual allocation of 8.8g exceeds the limit and YARN kills the container with "Container killed by YARN for exceeding memory limits." The fix is either reduce executor-memory to leave room for overhead, or increase the YARN container memory limit.

In PySpark specifically, the Python subprocess that runs your code (separate from the JVM) lives in overhead. If you have heavy UDFs or large Python objects, you need to increase overhead — often to 1-2GB.

---

**Q4. If data is 10GB, how do you decide the configuration?**

**A:** I follow a systematic calculation.

10GB compressed Parquet expands to about 30-35GB in memory (3-4x compression ratio). Spark needs RAM for the input data, shuffle intermediates (can be same size as input), and output if caching. Total working set is 60-100GB.

I always set executor-cores to 5. Then I calculate executors per node: (node cores - 1 for OS) / 5. For a 16-core node: 15/5 = 3 executors per node.

Executor memory per executor: (node RAM - 2GB OS) / 3 executors. For 64GB node: 62/3 ≈ 20GB, set to 18g (leaving buffer for overhead).

Number of executors: (executors per node × nodes) - 1 for ApplicationMaster. For 5 nodes: (3×5)-1 = 14.

Driver memory: 2g is sufficient since results are written to storage, not collected.

Shuffle partitions: in-memory data (35GB) / 128MB target = 273 partitions, round to 300.

Enable AQE to handle any remaining skew automatically.

---

**Q5. What is the difference between spark.sql.shuffle.partitions and repartition()?**

**A:** `spark.sql.shuffle.partitions` is a Spark SQL configuration that controls the number of partitions produced after a shuffle operation — when you do `groupBy`, `join`, `orderBy`. The default is 200. This applies to all shuffle operations in the session unless overridden.

`repartition(n)` is a transformation that explicitly changes the number of partitions by performing a full shuffle — useful when you need to increase partition count or distribute data evenly by a specific column. `repartition(n, "column")` shuffles data so that all rows with the same column value go to the same partition.

`coalesce(n)` reduces partition count without a full shuffle — much cheaper when shrinking partitions before writing.

In practice: set `spark.sql.shuffle.partitions` based on data size (target 128MB per partition). Use `repartition()` when you need to redistribute data for better parallelism or before a join. Use `coalesce()` when reducing output file count before writing.

---

**Q6. What is Dynamic Allocation and when would you use it?**

**A:** Dynamic Allocation lets Spark automatically scale the number of executors based on workload — adding executors when tasks are queued and removing idle executors when they have been unused for a configurable timeout.

```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "5")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
```

I use dynamic allocation in shared cluster environments where multiple jobs compete for resources — a long-running streaming job should not hold 20 executors while waiting for new data. With dynamic allocation, idle executors are released to other jobs.

I use static allocation when I need predictable performance and resource isolation — for example, a high-priority production ETL job that must complete within an SLA. With static allocation, I know exactly how much compute is dedicated to my job.

In Databricks, dynamic allocation is always enabled and managed automatically by the cluster autoscaler — you just set min and max workers.

---

**Q7. What is Spark's Unified Memory Model?**

**A:** Before Spark 1.6, execution memory (for shuffles and aggregations) and storage memory (for caching) were fixed fractions of heap — if one ran out, the other couldn't lend its unused space.

The Unified Memory Model combines them into one pool. Execution and storage share a single pool and can borrow from each other. If execution needs more memory and storage has idle space, execution borrows it (and may evict cached data). If storage needs to cache data and execution has idle space, storage borrows from execution.

The total unified memory = (executor-memory - 300MB reserved) × spark.memory.fraction (default 0.6). User memory for your custom data structures gets the remaining 40%.

This model is important to understand because it means caching large DataFrames can evict shuffle data, causing recomputation. If you see unexpected cache evictions or shuffles rerunning, it often means the unified memory pool is under pressure and you need more executor-memory.

---

**Q8. A job keeps failing with "GC overhead limit exceeded". What do you do?**

**A:** "GC overhead limit exceeded" means the JVM is spending more than 98% of its time on garbage collection and recovering less than 2% of heap — the JVM is almost out of memory and spending all its time cleaning up.

Root causes: too much data per executor, too many objects accumulated in executor heap, or memory fragmentation.

My debugging approach:

Step 1 — Check Spark UI Executors tab. Look at GC time percentage per executor. If above 10%, GC is a problem.

Step 2 — Increase executor-memory. More heap means less frequent GC.

Step 3 — Reduce data per task by increasing partition count: `spark.sql.shuffle.partitions = 400` (more, smaller tasks).

Step 4 — Check for large broadcast variables or accumulators accumulating in memory.

Step 5 — If using UDFs heavily, check if they create many short-lived objects — UDFs are hard for JVM GC to optimise.

Step 6 — Enable off-heap memory for Tungsten operations:
```python
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")
```

Off-heap memory is not garbage collected, so it reduces GC pressure significantly for large shuffle operations.

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────┐
│              SPARK CONFIG QUICK REFERENCE                       │
├──────────────────────┬──────────────────────────────────────────┤
│  executor-cores      │  Always start with 5                     │
├──────────────────────┼──────────────────────────────────────────┤
│  executor-memory     │  (Node RAM - 2GB OS) / executors_per_node│
├──────────────────────┼──────────────────────────────────────────┤
│  num-executors       │  (executors_per_node × nodes) - 1        │
├──────────────────────┼──────────────────────────────────────────┤
│  driver-memory       │  1g-4g (increase for collect/broadcast)  │
├──────────────────────┼──────────────────────────────────────────┤
│  memoryOverhead      │  max(executor-memory×10%, 384MB)         │
├──────────────────────┼──────────────────────────────────────────┤
│  shuffle.partitions  │  in-memory-data-MB / 128                 │
├──────────────────────┼──────────────────────────────────────────┤
│  adaptive.enabled    │  Always true (AQE handles skew/partitions)│
└──────────────────────┴──────────────────────────────────────────┘

DATA SIZE → QUICK CONFIG LOOKUP
┌──────────┬────────────────┬──────────────┬────────────────────┐
│ Data Size│ executor-memory│ num-executors│ shuffle.partitions │
├──────────┼────────────────┼──────────────┼────────────────────┤
│  100MB   │  2g            │  1-2         │  10                │
│    1GB   │  4g            │  3-5         │  50                │
│   10GB   │  18g           │  10-15       │  200-300           │
│  100GB   │  18g           │  50-100      │  1000-2000         │
│    1TB   │  18g           │  200-500     │  5000-10000        │
└──────────┴────────────────┴──────────────┴────────────────────┘

Note: executor-memory stays ~18g (optimal for 64GB nodes)
      scale by adding more executors, not bigger executors
```

---

*End of Spark Configuration Guide*
*executor-memory · executor-cores · driver-memory · num-executors*
*For Data Sizes: 10GB · 1GB · 100MB*
