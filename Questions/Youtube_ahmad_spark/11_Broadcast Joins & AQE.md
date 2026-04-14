# 11 — Broadcast Joins & Adaptive Query Execution (AQE) in Apache Spark
> **Target Audience:** Data Engineers with 3–4 years of experience | Interview + Real-World Prep

---

## Table of Contents
1. [Three Skew Solutions — Overview](#1-three-skew-solutions--overview)
2. [What is AQE?](#2-what-is-aqe)
3. [AQE Feature 1 — Tuning Shuffle Partitions](#3-aqe-feature-1--tuning-shuffle-partitions)
4. [AQE Feature 2 — Optimizing Joins (SMJ → Broadcast)](#4-aqe-feature-2--optimizing-joins-smj--broadcast)
5. [AQE Feature 3 — Optimizing Skew Joins](#5-aqe-feature-3--optimizing-skew-joins)
6. [Sort-Merge Join — Internal Mechanics](#6-sort-merge-join--internal-mechanics)
7. [Hash Partitioning — How Spark Routes Data](#7-hash-partitioning--how-spark-routes-data)
8. [Broadcast Join — Internal Mechanics](#8-broadcast-join--internal-mechanics)
9. [Sort-Merge Join vs Broadcast Join — Full Comparison](#9-sort-merge-join-vs-broadcast-join--full-comparison)
10. [Full Code Examples](#10-full-code-examples)
11. [Spark UI — What to Look For](#11-spark-ui--what-to-look-for)
12. [Interview Questions & Answers](#12-interview-questions--answers)
13. [Real-World Project Scenarios](#13-real-world-project-scenarios)
14. [Quick Reference Cheat Sheet](#14-quick-reference-cheat-sheet)

---

## 1. Three Skew Solutions — Overview

| Technique | How it Works | Best For | Limitation |
|---|---|---|---|
| **Manual Salting** | Adds randomness to join/agg key | Any Spark version, extreme skew, aggregations | Code complexity, explode overhead |
| **AQE** | Runtime auto-detection + split of skewed partitions | Spark 3+, joins with moderate skew | Joins only, may miss extreme skew |
| **Broadcast Join** | Sends small table to every executor | Small table + large table | Small table must fit in executor memory |

---

## 2. What is AQE?

### Definition
**Adaptive Query Execution (AQE)** is a Spark runtime optimization framework introduced in Spark 3.0. Unlike the Catalyst Optimizer (which plans queries at compile time using static statistics), AQE **re-optimizes the query plan at runtime** using actual execution statistics collected after each shuffle stage.

```
Static Optimizer (pre-AQE):
  Query → Catalyst builds plan → Execute → Done
  (plan is fixed, based on estimated stats)

AQE (Spark 3+):
  Query → Initial plan → Execute Stage 1 → Collect REAL stats
                                          → Re-optimize → Execute Stage 2
                                                        → Collect REAL stats
                                                        → Re-optimize → ...
  (plan is adaptive, updated after each shuffle)
```

### Runtime Statistics AQE Uses
- Actual size of shuffle output (bytes per partition)
- Actual number of rows per partition
- Partition count and distribution

### Enable AQE
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")            # Master switch
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")   # Skew join feature
```

---

## 3. AQE Feature 1 — Tuning Shuffle Partitions

### The Problem With Static Shuffle Partitions
Default `spark.sql.shuffle.partitions = 200`. If your data only has 15 distinct keys after a `groupBy`, Spark creates 200 partitions — **185 of them empty**. Processing 185 empty task slots wastes scheduler overhead and resources.

```
Static (200 shuffle partitions, 15 distinct keys):
  Partition 0:   1200 records
  Partition 1:   900 records
  ...
  Partition 14:  750 records
  Partition 15:  0 records   ← EMPTY
  Partition 16:  0 records   ← EMPTY
  ...
  Partition 199: 0 records   ← EMPTY
  (185 empty partitions — wasted tasks)
```

### AQE Solution — Dynamic Partition Coalescing
AQE automatically coalesces small/empty partitions into appropriately sized partitions after observing actual shuffle output.

```
AQE enabled (same data):
  Combined Partition 0:  1200 + 900 + 750 = 2850 records
  Combined Partition 1:  ...
  Combined Partition 2:  ...
  (15 meaningful partitions — no empty waste)
```

### Configuration
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Target size for each coalesced partition (default: 64MB)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

# Minimum number of partitions after coalescing (prevents over-coalescing)
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
```

---

## 4. AQE Feature 2 — Optimizing Joins (SMJ → Broadcast)

### The Problem
At query planning time, Spark estimates table sizes to decide join strategy. If size estimates are wrong (stale statistics, complex subqueries), Spark might choose Sort-Merge Join for a table that's actually small — wasting shuffle bandwidth.

### AQE Solution — Dynamic Join Conversion
AQE observes the **actual size of shuffle map output** after Stage 1. If the smaller side of a Sort-Merge Join turns out to be below the broadcast threshold, AQE **converts the SMJ to a Broadcast Join on the fly** — without any manual hint from the developer.

```
Static Plan:  Sort-Merge Join (estimated 500MB — above threshold)
                   ↓
Runtime:      AQE sees actual size = 8MB (below threshold)
                   ↓
AQE Plan:     Broadcast Join ← automatic conversion!
```

### Configuration
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")

# If AQE detects the smaller side is below this threshold → convert to BJ
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")   # default: 10MB
```

### How to Confirm in Spark UI
SQL tab → Query plan → Look for `BroadcastHashJoin` in the plan that was originally planned as `SortMergeJoin`. AQE also shows `AQEShuffleRead` nodes indicating coalesced or split partitions.

---

## 5. AQE Feature 3 — Optimizing Skew Joins

### The Problem
Sort-Merge Joins on skewed data produce partitions of wildly unequal size. One task processes 100M rows while others process 100 rows — a **straggler task** that holds up the entire stage.

A **straggler task** in Spark is a task that runs significantly slower than other parallel tasks in the same stage, causing the entire job to delay because Spark waits for all tasks to complete.
Straggler = slow task that delays the whole Spark job.
### AQE Solution — Automatic Skew Partition Splitting
AQE detects skewed partitions after observing shuffle output and **automatically splits** them into smaller sub-partitions. These sub-partitions are joined independently against matching partitions (which are duplicated as needed).

```
Without AQE:
  Partition 0: ████████████████████ 1,000,000 rows (skewed)
  Partition 1: █ 100 rows
  Partition 2: █ 50 rows

With AQE skewJoin:
  Partition 0a: ████ 333,333 rows (auto-split)
  Partition 0b: ████ 333,333 rows
  Partition 0c: ████ 333,334 rows
  Partition 1:  █ 100 rows
  Partition 2:  █ 50 rows
```

### Configuration
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# A partition is considered skewed if:
# its size > skewedPartitionFactor × median_partition_size
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# AND its size > this absolute threshold
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

Both conditions must be true for AQE to split a partition.

---

## 6. Sort-Merge Join — Internal Mechanics

### Overview
Sort-Merge Join (SMJ) is Spark's default join strategy for large-large table joins. It involves three phases: **Shuffle → Sort → Merge**.

### Step 1: Shuffle (Most Expensive)
Both datasets are shuffled across the network so that rows with the **same join key** end up on the **same executor**.

```
Formula: hash(join_key) % shuffle_partitions

Executor 0 receives: all rows where hash(key) % N = 0  (from BOTH tables)
Executor 1 receives: all rows where hash(key) % N = 1  (from BOTH tables)
...
```

This involves **massive network data transfer** — all partitions of both tables are read, serialized, and sent across the network. This is the primary cost of SMJ.

### Step 2: Sort
Within each executor, rows from both tables are sorted by the join key. This enables the O(n) merge step.

### Step 3: Merge
Two sorted pointers walk through both sorted lists simultaneously. When pointers match on the join key, the rows are joined.

```
Table A (sorted):  [key=1, row_a1] [key=2, row_a2] [key=3, row_a3]
Table B (sorted):  [key=1, row_b1] [key=1, row_b2] [key=3, row_b3]

Pointer A: key=1  →  matches Pointer B: key=1  → emit (row_a1, row_b1)
                  →  matches Pointer B: key=1  → emit (row_a1, row_b2)
Pointer A: key=2  →  no match  → advance
Pointer A: key=3  →  matches Pointer B: key=3  → emit (row_a3, row_b3)
```

### Why SMJ is Skew-Sensitive
All rows with the same join key end up on the same executor (due to hash partitioning). If one key has millions of rows, one executor gets all of them → straggler task.

---

## 7. Hash Partitioning — How Spark Routes Data

Hash partitioning is the mechanism behind SMJ's shuffle step.

```
Single key:    partition = hash(key)        % num_partitions
Composite key: partition = hash(k1, k2, …)  % num_partitions
```

### Example with 3 Partitions

```
shuffle_partitions = 3

customer_id = "C001" → hash("C001") % 3 = 0 → Executor 0
customer_id = "C002" → hash("C002") % 3 = 1 → Executor 1
customer_id = "C003" → hash("C003") % 3 = 2 → Executor 2
customer_id = "C001" → hash("C001") % 3 = 0 → Executor 0  ← SAME KEY → SAME EXECUTOR

Both Table A and Table B rows with "C001" → Executor 0 → they can be joined
```

This guarantee (same key → same executor) is what makes the merge step possible, but also what causes skew.

---

## 8. Broadcast Join — Internal Mechanics

### Overview
In a Broadcast Join, the **entire small table is copied (broadcast) to every executor** that holds partitions of the large table. No shuffling of the large table is needed — each executor joins locally.

```
Small table (customers, 50 rows):
  → Driver collects it
  → Driver sends a copy to ALL executors

Large table (transactions, 3 million rows):
  → Stays in its current partitions
  → Each executor joins its local partition with its local copy of customers
```

### Step-by-Step

```
Executor 0: local partition of transactions (1M rows)  +  broadcast copy of customers (50 rows)
            → local hash join → 1M result rows

Executor 1: local partition of transactions (1M rows)  +  broadcast copy of customers (50 rows)
            → local hash join → ...

Executor 2: local partition of transactions (1M rows)  +  broadcast copy of customers (50 rows)
            → local hash join → ...

No shuffle! No network transfer of large table!
```

### Why Broadcast Join is Immune to Skew
In SMJ, data is repartitioned by join key (hash partitioning) — skewed keys pile into the same executor. In Broadcast Join, the large table **is not repartitioned at all** — it stays in whatever partition layout it already has. So uneven key distribution in the large table doesn't matter; each executor joins its local chunk independently.

### Broadcast Join Limitations
- The small table must fit in executor memory
- Default threshold: 10 MB (`spark.sql.autoBroadcastJoinThreshold`)
- Broadcasting a large table kills executors with OOM errors
- With multiple joins (4–5 tables), only one can be broadcast at a time per join

---

## 9. Sort-Merge Join vs Broadcast Join — Full Comparison

| Aspect | Sort-Merge Join | Broadcast Join |
|---|---|---|
| Shuffle required | Yes — both tables shuffled | No — only small table broadcast |
| Network transfer | High (all data moved) | Low (only small table) |
| Memory requirement | Low per executor | Small table must fit in memory |
| Skew sensitivity | High (same key → same executor) | Immune (large table not repartitioned) |
| Table size | Both can be large | One table must be small |
| Performance | Slower (shuffle + sort + merge) | Faster (no shuffle) |
| When Spark chooses it | Default for large-large joins | Auto when table < autoBroadcastJoinThreshold |
| Can force it | `hint("MERGE")` | `F.broadcast(df)` or set threshold |

---

## 10. Full Code Examples

### Setup

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

spark = SparkSession.builder.master("local[4]").appName("JoinOptimization").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

transactions_file = "../../data/data_skew/transactions.parquet"
customer_file     = "../../data/data_skew/customers.parquet"

df_transactions = spark.read.parquet(transactions_file)
df_customers    = spark.read.parquet(customer_file)
```

### Detect Skew in the Dataset

```python
# Check for skewed keys in the join column
(
    df_transactions
    .groupBy("cust_id")
    .agg(F.countDistinct("txn_id").alias("txn_count"))
    .orderBy(F.desc("txn_count"))
    .show(5, truncate=False)
)
# +----------+----------+
# |cust_id   |txn_count |
# +----------+----------+
# |C0YDPQWPBJ|17,539,732|  ← One customer with 17 million transactions!
# |CBW3FMEAU7|     7,999|
# |C3KUDEN3KO|     7,999|
# +----------+----------+
```

### Example 1 — Sort-Merge Join WITHOUT AQE (Slow, Skewed)

```python
# Disable AQE and force Sort-Merge Join
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # Disable auto broadcast

df_txn_details = df_transactions.join(df_customers, on="cust_id", how="inner")

start = time.time()
print(f"Row count: {df_txn_details.count()}")
print(f"Time (SMJ, no AQE): {time.time() - start:.2f}s")
# Observe in Spark UI: one task takes 10x-100x longer than others
```

### Example 2 — Sort-Merge Join WITH AQE (Auto Skew Fix)

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # Keep forcing SMJ for comparison

df_txn_details_aqe = df_transactions.join(df_customers, on="cust_id", how="inner")

start = time.time()
print(f"Row count: {df_txn_details_aqe.count()}")
print(f"Time (SMJ + AQE): {time.time() - start:.2f}s")
# AQE splits the skewed partition → tasks are more balanced
# Spark UI → SQL tab → see AQEShuffleRead in the query plan
```

### Example 3 — Broadcast Join (Fastest, Skew-Immune)

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # 10MB = 10 * 1024 * 1024

# Method 1: Set threshold and let Spark decide automatically
df_txn_details_bj = df_transactions.join(df_customers, on="cust_id", how="inner")

# Method 2: Explicitly hint the small table (more reliable)
df_txn_details_bj = df_transactions.join(
    F.broadcast(df_customers),   # Explicitly broadcast customers
    on="cust_id",
    how="inner"
)

start = time.time()
print(f"Row count: {df_txn_details_bj.count()}")
print(f"Time (Broadcast Join): {time.time() - start:.2f}s")
# Typical result: ~2.8 seconds vs tens of minutes for SMJ without AQE
```

### Example 4 — Full AQE Configuration (Production-Ready)

```python
spark = (
    SparkSession.builder
    .master("local[4]")
    .appName("AQEProduction")
    # AQE master switch
    .config("spark.sql.adaptive.enabled", "true")
    # Skew join handling
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
    # Dynamic partition coalescing
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
    # Auto broadcast threshold
    .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
    .getOrCreate()
)
```

### Example 5 — Checking Join Strategy in Query Plan

```python
# Check which join strategy Spark chose
df_join = df_transactions.join(F.broadcast(df_customers), "cust_id", "inner")
df_join.explain(mode="formatted")

# Look for in the output:
# BroadcastHashJoin        ← Broadcast Join was chosen
# SortMergeJoin            ← Sort-Merge Join was chosen
# BroadcastExchange        ← The small table is being broadcast
# AQEShuffleRead           ← AQE re-optimized the shuffle
```

---

## 11. Spark UI — What to Look For

### Detecting Skew (Stages Tab)
- Go to **Stages → Event Timeline**
- Look for one task bar that is dramatically longer than all others → straggler task = skew
- Go to **Stages → Task Metrics → Duration column** — sort by duration descending

### Confirming AQE is Working (SQL Tab)
- Go to **SQL → select your query**
- In the DAG visualization, look for:
  - `AQEShuffleRead` → AQE coalesced or split partitions
  - `BroadcastHashJoin` where `SortMergeJoin` was expected → AQE converted join type

### Comparing With/Without AQE (Jobs Tab)
| Metric | Without AQE | With AQE |
|---|---|---|
| Stage duration | High (straggler) | Lower (balanced tasks) |
| Task max/median duration ratio | High (100x+) | Low (2-5x) |
| Join type in DAG | SortMergeJoin | May show BroadcastHashJoin |
| Shuffle read/write size | High | Potentially reduced |

### Storage Tab for Broadcast Tables
When a broadcast join runs, the broadcast table appears in the **Accumulators** or can be seen in the broadcast exchange step size in the SQL DAG — verify it's the expected small size.

---

## 12. Interview Questions & Answers

---

### Q1. What is AQE and how is it different from the Catalyst Optimizer?

**Answer:**
The Catalyst Optimizer builds the query execution plan **before the job runs**, using table statistics (row count, data size) that may be stale or estimated. Once the plan is set, it doesn't change.

AQE (Adaptive Query Execution) re-optimizes the plan **at runtime**, between shuffle stages, using actual execution statistics. After each shuffle stage completes, AQE observes real partition sizes and row counts, then may: coalesce empty partitions, convert a Sort-Merge Join to a Broadcast Join, or split skewed partitions into smaller ones.

In short, Catalyst is a static optimizer; AQE is a dynamic optimizer.

---

### Q2. Explain the three main optimizations AQE provides.

**Answer:**
AQE provides three runtime optimizations:

**1. Dynamic shuffle partition coalescing:** If the default 200 shuffle partitions produce many empty or tiny partitions (e.g., because your data only has 15 distinct keys after a groupBy), AQE merges them down to the appropriate number, reducing scheduler overhead.

**2. Dynamic join strategy switching:** If Catalyst planned a Sort-Merge Join but AQE discovers at runtime that one side is smaller than the broadcast threshold, it converts the join to a Broadcast Join — avoiding the expensive shuffle.

**3. Dynamic skew join handling:** AQE detects partitions that are significantly larger than the median (by a configurable factor and absolute size threshold) and automatically splits them into smaller sub-partitions, distributing the skewed work across more tasks.

---

### Q3. Explain Sort-Merge Join step by step.

**Answer:**
Sort-Merge Join has three phases: Shuffle, Sort, and Merge.

In the Shuffle phase, both datasets are repartitioned using hash partitioning (`hash(join_key) % num_partitions`). This guarantees that rows with the same join key land on the same executor. This phase involves network data transfer of both entire datasets — it's the most expensive step.

In the Sort phase, each executor sorts its received data from both tables by the join key. This is required for the efficient merge step.

In the Merge phase, two sorted pointers walk through both sorted lists simultaneously. When both pointers point to matching keys, the rows are combined and emitted. The merge is O(n) once sorting is complete.

The key vulnerability: all rows with the same key go to the same executor. If one key is extremely frequent (data skew), one executor becomes a bottleneck.

---

### Q4. How does Broadcast Join work and why is it immune to data skew?

**Answer:**
In a Broadcast Join, the entire small table is serialized and sent to every executor that holds partitions of the large table. The driver collects the small table, then pushes a copy to every executor's memory. Each executor performs a local hash join between its chunk of the large table and its local copy of the small table — no shuffling of the large table occurs.

It's immune to skew because the large table is **never repartitioned**. In Sort-Merge Join, repartitioning by join key is what causes hotspots — all rows of a skewed key pile into one executor. In Broadcast Join, the large table stays in its existing partitions regardless of key distribution. Each executor joins whatever rows it already has against the complete copy of the small table.

---

### Q5. When would you choose Broadcast Join over Sort-Merge Join?

**Answer:**
Choose Broadcast Join when the smaller table fits comfortably in executor memory (typically under the `autoBroadcastJoinThreshold`, default 10 MB, though you can raise it). Specifically:

Use Broadcast Join when: the small table is a reference/lookup table (e.g., product catalog, country codes, customer segments), the join key is skewed in the large table, or shuffle overhead dominates job time.

Avoid Broadcast Join when: the small table is large (broadcasting a 10 GB table will OOM executors), you're joining multiple large tables where no side can be broadcast, or network bandwidth is the bottleneck (broadcasting across slow networks is still expensive).

A practical rule: if the smaller side of a join is under 100 MB and executor memory is sufficient, Broadcast Join is almost always the right choice.

---

### Q6. How does AQE's skew join handling differ from manual salting?

**Answer:**
Both solve data skew in joins, but differently.

AQE detects skew at **runtime** by measuring actual partition sizes after the shuffle. If a partition exceeds `skewedPartitionFactor × median_size` and the absolute `skewedPartitionThresholdInBytes`, AQE splits that partition into sub-partitions and joins each sub-partition against a duplicate of the matching data on the other side.

Manual salting works **pre-shuffle** by appending a random integer to the join key before the shuffle happens, forcing the skewed key's rows to distribute across multiple partitions from the start.

Key differences:
- AQE requires zero code changes; salting requires code modifications
- AQE only handles joins; salting also handles aggregations
- For extreme skew (1 key = 99% of data), AQE may still produce some imbalance; salting gives precise control
- AQE's split approach duplicates some of the non-skewed side's data; salting's explode increases the small table

In practice, AQE is the first line of defense; manual salting is for cases AQE can't fully handle.

---

### Q7. What is `autoBroadcastJoinThreshold` and how do you tune it?

**Answer:**
`spark.sql.autoBroadcastJoinThreshold` is the maximum size (in bytes) of a table that Spark will automatically broadcast in a join. The default is 10 MB (10485760 bytes). Tables smaller than this threshold are broadcast without any hint from the developer.

To increase it:
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)  # 100 MB
```

To disable auto-broadcast (force Sort-Merge Join for testing):
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

Tune it based on: the typical size of your dimension/lookup tables, available executor memory (broadcast table must fit in memory on every executor), and network bandwidth (larger broadcasts = more network transfer).

A common production setting is 50–100 MB if executors have sufficient memory (8 GB+).

---

### Q8. How does Broadcast Join behave with multiple joins (4–5 tables)?

**Answer:**
When joining multiple tables, each binary join is evaluated independently for broadcast eligibility. If Table A (10 MB) joins Table B (500 GB), and then the result joins Table C (8 MB), Spark can broadcast Table A in the first join and broadcast Table C in the second join.

The important constraint is that the **result of a join is generally too large to broadcast**. So in a chain of joins, only the original dimension tables (typically small) can be broadcast — not intermediate results. Spark's Catalyst Optimizer (or AQE at runtime) evaluates each join independently.

To explicitly control which tables are broadcast in a multi-join query:
```python
result = (
    df_large
    .join(F.broadcast(df_dim1), "key1")    # broadcast dim1
    .join(F.broadcast(df_dim2), "key2")    # broadcast dim2
    .join(df_large2, "key3")               # SMJ for two large tables
)
```

---

### Q9. What are the risks of setting `autoBroadcastJoinThreshold` too high?

**Answer:**
If the threshold is set too high and a large table gets broadcast:

1. **OOM on executors:** Every executor must hold the broadcast table in memory simultaneously. A 2 GB broadcast table on a 10-executor cluster = 20 GB of memory consumed just for the broadcast variable, often causing OutOfMemoryErrors.

2. **Driver bottleneck:** The driver collects the small table and serializes it for broadcast. A large table can overwhelm driver memory.

3. **Network saturation:** Broadcasting a large table to all executors simultaneously floods the network with duplicate data.

4. **Garbage collection pressure:** Large broadcast variables in JVM heap increase GC pressure, causing task pauses.

The safest approach: keep the threshold conservative (10–100 MB), and use explicit `F.broadcast()` hints only when you've verified the table's actual size is small.

---

## 13. Real-World Project Scenarios

---

### Scenario 1 — Financial Services: Transaction Enrichment at Scale

**Problem:**
A bank processes 50 million transactions daily. Each transaction must be joined with: a customer table (2 million rows, 500 MB), a merchant category table (5,000 rows, 1 MB), and a fraud risk table (200,000 rows, 50 MB). One customer (`cust_id = "CORP001"`) has 15 million transactions — 30% of the entire dataset.

**Solution — Layered Strategy:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)  # 100 MB

df_transactions = spark.read.parquet("s3://data/transactions/daily/")
df_customers    = spark.read.parquet("s3://data/dim/customers/")       # 500 MB
df_merchants    = spark.read.parquet("s3://data/dim/merchant_cat/")    # 1 MB
df_fraud_risk   = spark.read.parquet("s3://data/dim/fraud_risk/")      # 50 MB

# merchant_cat (1 MB) → auto-broadcast (below 100 MB threshold)
# fraud_risk (50 MB)  → auto-broadcast (below 100 MB threshold)
# customers (500 MB)  → SMJ + AQE handles the skew from CORP001

df_enriched = (
    df_transactions
    .join(F.broadcast(df_merchants),   "merchant_cat_id", "left")   # explicit broadcast
    .join(F.broadcast(df_fraud_risk),  "customer_id",     "left")   # explicit broadcast
    .join(df_customers,                "customer_id",     "left")   # SMJ + AQE for skew
)

df_enriched.write.mode("overwrite").partitionBy("year", "month") \
           .parquet("s3://output/enriched_transactions/")
```

**Why this works:** Small dimensions are broadcast (no shuffle, skew-immune). The large customer table uses AQE's skew join to automatically split the CORP001 partition. Layering multiple strategies is the real-world approach.

---

### Scenario 2 — When AQE is Not Enough: Combining AQE + Salting

**Problem:**
One customer has 17.5 million transactions out of 39 million total (44%). AQE's `skewedPartitionFactor = 5` means a partition must be 5x the median to be split. With 44% of data in one key, this extreme skew still produces one massive partition after AQE splitting — just fewer massive sub-partitions.

**Solution — AQE + Salting:**
```python
SALT_NUMBER = 50  # High salt for extreme skew

# Identify the top skewed keys
skewed_keys = (
    df_transactions
    .groupBy("cust_id")
    .count()
    .filter(F.col("count") > 1000000)  # Keys with > 1M rows
    .select("cust_id")
    .rdd.flatMap(lambda x: x).collect()
)

# Salt only the skewed keys (partial salting)
df_txn_salted = df_transactions.withColumn(
    "salt",
    F.when(
        F.col("cust_id").isin(skewed_keys),
        (F.rand() * SALT_NUMBER).cast("int")
    ).otherwise(F.lit(0))  # Non-skewed keys get salt=0 (no multiplication)
)

# Explode customers only for skewed keys (partial explode)
df_cust_normal  = df_customers.filter(~F.col("cust_id").isin(skewed_keys)) \
                               .withColumn("salt", F.lit(0))

df_cust_exploded = (
    df_customers.filter(F.col("cust_id").isin(skewed_keys))
    .withColumn("salt_values", F.array([F.lit(i) for i in range(SALT_NUMBER)]))
    .withColumn("salt", F.explode("salt_values"))
    .drop("salt_values")
)

df_customers_salted = df_cust_normal.union(df_cust_exploded)

# Join
df_result = (
    df_txn_salted
    .join(df_customers_salted, ["cust_id", "salt"], "inner")
    .drop("salt")
)
```

**Note:** Partial salting (only on skewed keys) avoids exploding the entire customers table — a crucial optimization when the small table is large.

---

### Scenario 3 — Choosing the Right Strategy in a Data Pipeline Review

You're reviewing a colleague's pipeline. Here's how to diagnose and recommend the right fix:

```python
def diagnose_join_strategy(df_large, df_small, join_key):
    """
    Recommend join strategy based on table sizes and skew.
    """
    # Check small table size (approximate via count * avg row size)
    small_count = df_small.count()

    # Check skew in large table
    skew_stats = (
        df_large
        .groupBy(join_key)
        .count()
        .agg(
            F.max("count").alias("max_key_count"),
            F.avg("count").alias("avg_key_count"),
            F.count("*").alias("distinct_keys")
        )
        .collect()[0]
    )

    skew_ratio = skew_stats["max_key_count"] / skew_stats["avg_key_count"]

    print(f"Small table rows:   {small_count:,}")
    print(f"Distinct join keys: {skew_stats['distinct_keys']:,}")
    print(f"Skew ratio:         {skew_ratio:.1f}x")

    if small_count < 500000:  # Rough heuristic: < 500K rows → broadcastable
        print("✅ RECOMMENDATION: Broadcast Join")
        print("   df_large.join(F.broadcast(df_small), join_key)")
    elif skew_ratio > 10:
        print("⚠️  RECOMMENDATION: AQE skewJoin + consider Salting if extreme")
    else:
        print("✅ RECOMMENDATION: Standard Sort-Merge Join with AQE")
```

---

## 14. Quick Reference Cheat Sheet

```
┌──────────────────────────────────────────────────────────────────────────┐
│               BROADCAST JOIN & AQE QUICK REFERENCE                      │
├──────────────────────────────────────────────────────────────────────────┤
│ AQE CONFIGURATION                                                        │
│  spark.sql.adaptive.enabled                        → true               │
│  spark.sql.adaptive.skewJoin.enabled               → true               │
│  spark.sql.adaptive.skewJoin.skewedPartitionFactor → 5 (default)        │
│  spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes → 256MB    │
│  spark.sql.adaptive.coalescePartitions.enabled     → true               │
│  spark.sql.adaptive.advisoryPartitionSizeInBytes   → 64MB               │
├──────────────────────────────────────────────────────────────────────────┤
│ BROADCAST JOIN                                                           │
│  Auto:     spark.sql.autoBroadcastJoinThreshold = 10485760 (10 MB)      │
│  Explicit: df_large.join(F.broadcast(df_small), "key")                  │
│  Disable:  spark.sql.autoBroadcastJoinThreshold = -1                    │
│  Check:    df.join(other,"k").explain() → look for BroadcastHashJoin    │
├──────────────────────────────────────────────────────────────────────────┤
│ SORT-MERGE JOIN INTERNALS                                                │
│  1. Shuffle: hash(key) % N → same key → same executor                   │
│  2. Sort:    both sides sorted by join key within each partition         │
│  3. Merge:   two pointers walk sorted lists → O(n) match                │
│  Weakness:  skewed key → all rows to one executor                       │
├──────────────────────────────────────────────────────────────────────────┤
│ BROADCAST JOIN INTERNALS                                                 │
│  1. Driver collects small table                                          │
│  2. Driver broadcasts copy to every executor                            │
│  3. Each executor: local hash join with its partition of large table     │
│  Strength: large table NOT repartitioned → skew-immune                  │
│  Weakness: small table must fit in executor memory                      │
├──────────────────────────────────────────────────────────────────────────┤
│ STRATEGY SELECTION GUIDE                                                 │
│  Small table fits in memory     → Broadcast Join                        │
│  Moderate skew, Spark 3+        → Enable AQE skewJoin                  │
│  Extreme skew, any version      → Manual Salting                        │
│  Known skew + Spark 3+          → AQE + Salting combined               │
│  All joins                      → Always enable AQE                    │
├──────────────────────────────────────────────────────────────────────────┤
│ SPARK UI SIGNALS                                                         │
│  Stage → Event Timeline → straggler task   → skew detected             │
│  SQL → DAG → AQEShuffleRead                → AQE coalesced partitions  │
│  SQL → DAG → BroadcastHashJoin             → broadcast join used       │
│  SQL → DAG → SortMergeJoin                 → SMJ used                  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

### Performance Benchmark Summary

| Strategy | Relative Speed | Skew Handling | Code Change | Spark Version |
|---|---|---|---|---|
| SMJ (no AQE) | Baseline (slow) | None | None | All |
| SMJ + AQE | 2–5x faster | Automatic | None | 3.0+ |
| Manual Salting | 5–20x faster | Precise | Yes | All |
| Broadcast Join | 10–50x faster | Immune | Small | All |
| AQE + Salting | Best for extreme skew | Combined | Yes | 3.0+ |

---

*Last Updated: 2026 | PySpark 3.x | For Data Engineering Interview Prep*
