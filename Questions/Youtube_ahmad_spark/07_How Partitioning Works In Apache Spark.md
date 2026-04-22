# Apache Spark – How Partitioning Works
> **Level:** 3.5 Years Data Engineering | **Stack:** Azure Databricks, PySpark, Delta Lake, Apache Iceberg

---

## Table of Contents
1. [What Is Partitioning?](#1-what-is-partitioning)
2. [Two Types of Partitioning — Don't Confuse Them](#2-two-types-of-partitioning--dont-confuse-them)
3. [Problems Partitioning Solves](#3-problems-partitioning-solves)
4. [How to Choose a Partition Column](#4-how-to-choose-a-partition-column)
5. [Single vs Multi-Level Partitioning](#5-single-vs-multi-level-partitioning)
6. [Controlling Files Per Partition — repartition vs coalesce](#6-controlling-files-per-partition--repartition-vs-coalesce)
7. [spark.sql.files.maxPartitionBytes — Read-Time Partitioning](#7-sqlfilesmaxpartitionbytes--read-time-partitioning)
8. [Partition Pruning — The Whole Point](#8-partition-pruning--the-whole-point)
9. [The Small Files Problem](#9-the-small-files-problem)
10. [Interview Q&A – Tiered](#10-interview-qa--tiered)
11. [Real-World Scenarios & Solutions](#11-real-world-scenarios--solutions)

---

## 1. What Is Partitioning?

Partitioning is dividing a large dataset into smaller, manageable chunks — either **on disk** (storage partitioning via `partitionBy`) or **in memory** (Spark's in-memory task partitions).

### The Bookshelf Analogy

```
Without Partitioning (Full Table Scan):
┌──────────────────────────────────────────────┐
│  One giant folder — all data mixed together  │
│  listening_activity.parquet (500 GB)         │
└──────────────────────────────────────────────┘
  Query: "WHERE listen_date = '2024-01-01'"
  Spark must scan ALL 500 GB to find that date → slow

With Partitioning (Partition Pruning):
┌──────────────────────────────────────────────┐
│  listening_activity/                         │
│  ├── listen_date=2024-01-01/  ← Spark reads  │
│  │       part-0.parquet          ONLY this   │
│  ├── listen_date=2024-01-02/  ← Skipped      │
│  ├── listen_date=2024-01-03/  ← Skipped      │
│  └── ...                                     │
└──────────────────────────────────────────────┘
  Spark reads 1 folder out of 365 → 365× less I/O
```

---

## 2. Two Types of Partitioning — Don't Confuse Them

This is a frequent interview trap. There are **two distinct concepts** both called "partitioning" in Spark.

| | Storage Partitioning | In-Memory Partitioning |
|---|---|---|
| **What** | Directory structure on disk (HDFS/S3/ADLS) | How Spark divides data across executor cores in RAM |
| **How** | `.write.partitionBy("col")` | `.repartition(n)` / `.coalesce(n)` / `spark.sql.shuffle.partitions` |
| **Purpose** | Partition pruning during reads, faster queries | Parallelism, even task distribution |
| **Persists?** | Yes — stays on storage | No — only during job execution |
| **Affects** | File layout, read performance | Task count, shuffle behavior, memory usage |

```
Storage Partition (on disk):              In-Memory Partition (at runtime):
listen_activity/                          Executor 1: [Partition 0] [Partition 1]
├── listen_date=2024-01-01/              Executor 2: [Partition 2] [Partition 3]
├── listen_date=2024-01-02/              Executor 3: [Partition 4] [Partition 5]
└── listen_date=2024-01-03/
```

> **Key Insight:** `partitionBy` during write creates **storage partitions**.  
> `repartition()` before write controls **how many files** go inside each storage partition.

---

## 3. Problems Partitioning Solves

### A. Query Performance (Partition Pruning / Predicate Pushdown)

Without partitioning, every query does a **full scan**. With partitioning on the right column, Spark reads only the relevant subdirectory.

```python
# Without partitioning
df.filter("listen_date = '2024-01-01'")
# Spark reads all 500 GB → scans every row → slow

# With partitionBy("listen_date")
df.filter("listen_date = '2024-01-01'")
# Spark reads ONLY listen_date=2024-01-01/ folder → fast
```

Check if pruning is happening via the query plan:
```python
df_partitioned = spark.read.parquet("path/to/partitioned/data")
df_partitioned.filter("listen_date = '2024-01-01'").explain()

# Look for: PartitionFilters: [isnotnull(listen_date#5), (listen_date#5 = 2024-01-01)]
# This confirms Spark is PRUNING — not scanning everything
```

### B. Parallelism & Resource Utilization

```
Rule: 1 Spark core processes 1 in-memory partition at a time

More partitions = more cores engaged = higher parallelism
BUT: each partition should ideally be ~128 MB
     Too small → too many tasks, scheduling overhead
     Too large → fewer tasks, cores sit idle, OOM risk
```

### C. Data Organization for Downstream Consumers

Partitioned storage makes it easy for BI tools, Iceberg, Delta Lake, and Athena to skip irrelevant data using their own metadata layers.

---

## 4. How to Choose a Partition Column

This is one of the most common interview questions. The right column must satisfy two criteria simultaneously.

### Rule 1: Cardinality Must Be in the "Goldilocks Zone"

```
Too High Cardinality (BAD):          Too Low Cardinality (BAD):
customer_id (millions of values)     is_active (only 2 values: true/false)

→ Creates millions of tiny folders   → Creates only 2 folders
→ Small files problem                → Each folder is enormous
→ Metadata overhead explodes         → No meaningful pruning

Just Right:
listen_date (~365 values/year)
country (~200 values)
region (~50 values)
year_month (~24 values for 2 years)
```

### Rule 2: It Must Match Your Most Common Filter Condition

```python
# If 90% of your queries filter by date → partition by date
df.filter("listen_date = '2024-01-01'")  # ← pruning happens ✅

# If 90% of your queries filter by country → partition by country
df.filter("country = 'IND'")             # ← pruning happens ✅

# If you partition by date but filter by song_id → NO pruning
df.filter("song_id = 12")               # ← full scan still happens ❌
```

### Cardinality Decision Framework

```
Step 1: Count distinct values of candidate column
  SELECT COUNT(DISTINCT candidate_col) FROM table

Step 2: Estimate folder size
  folder_size = total_data_size / distinct_values
  Target: each folder = 256 MB to 10 GB (sweet spot)

Step 3: Check query patterns
  Does your most common WHERE clause filter on this column?
  If yes → good partition column
  If no  → keep looking

Step 4: Watch out for temporal skew
  If you partition by date, some dates may have more data
  Consider year/month instead of full date for very large datasets
```

---

## 5. Single vs Multi-Level Partitioning

### Single-Level

```python
# Partition by date only
df_listening_actv \
    .write \
    .mode("overwrite") \
    .partitionBy("listen_date") \
    .parquet("path/listening_activity_pt")

# Folder structure:
# listening_activity_pt/
# ├── listen_date=2023-06-27/
# │       part-0.parquet
# ├── listen_date=2023-06-28/
# │       part-0.parquet
# └── ...
```

### Multi-Level (Order Matters!)

```python
# Level 1: listen_date, Level 2: listen_hour
df_listening_actv \
    .write \
    .mode("overwrite") \
    .partitionBy("listen_date", "listen_hour") \
    .parquet("path/listening_activity_pt_2")

# Folder structure:
# listening_activity_pt_2/
# ├── listen_date=2023-06-27/
# │   ├── listen_hour=8/
# │   │       part-0.parquet
# │   ├── listen_hour=10/
# │   │       part-0.parquet
# │   └── listen_hour=23/
# │           part-0.parquet
# └── ...
```

```python
# Swapped order: listen_hour first, then listen_date
df_listening_actv \
    .write \
    .mode("overwrite") \
    .partitionBy("listen_hour", "listen_date") \
    .parquet("path/listening_activity_pt_3")

# Folder structure:
# listening_activity_pt_3/
# ├── listen_hour=8/
# │   ├── listen_date=2023-06-27/
# │   └── listen_date=2023-06-28/
# ├── listen_hour=10/
# │   ├── listen_date=2023-06-27/
# │   └── ...
# └── ...
```

### Critical Rule: Put the Higher-Cardinality Column FIRST

```
If queries most commonly filter by date alone:
  → partitionBy("listen_date", "listen_hour")
  → "WHERE listen_date = X" prunes at the top level → fast

If queries most commonly filter by hour alone:
  → partitionBy("listen_hour", "listen_date")
  → "WHERE listen_hour = 10" prunes at the top level → fast

If both filters always appear together:
  → Either order works; put the more selective one first
```

---

## 6. Controlling Files Per Partition — repartition vs coalesce

### The Problem

When you write `partitionBy("listen_date")`, Spark creates **one file per in-memory partition per date folder**. If Spark had 200 in-memory partitions when writing, you get up to 200 files per date folder — the **small files problem**.

```
Default write (200 shuffle partitions, no control):
listen_activity_pt/
└── listen_date=2023-06-27/
    ├── part-000.parquet   (2 MB)
    ├── part-001.parquet   (0 KB)  ← empty!
    ├── part-002.parquet   (1 MB)
    ├── ...
    └── part-199.parquet   (3 MB)
```

### Fix: Use repartition Before Write

```python
# repartition(n): full shuffle, spreads data evenly, INCREASES or DECREASES partitions
(
    df_listening_actv
    .repartition(3)              # → exactly 3 files per date folder
    .write
    .mode("overwrite")
    .partitionBy("listen_date")
    .parquet("path/listening_activity_pt_4")
)

# Output:
# listen_date=2023-06-27/
# ├── part-0.parquet  (~equal size)
# ├── part-1.parquet  (~equal size)
# └── part-2.parquet  (~equal size)
```

### repartition vs coalesce — The Critical Difference

```python
# repartition(n): FULL SHUFFLE — can increase OR decrease partitions
#   - Evenly distributes data using hash partitioner
#   - More expensive (network shuffle)
#   - Use when: increasing partitions or when you need even distribution
df.repartition(10)

# coalesce(n): NO FULL SHUFFLE — can only DECREASE partitions
#   - Merges existing partitions into fewer ones without full redistribution
#   - Cheaper (no network shuffle)
#   - Risk: can create uneven partitions if input was already uneven
#   - Use when: reducing partitions before writing to avoid small files
df.coalesce(3)
```

```
repartition(3):                   coalesce(3):
[P1][P2][P3][P4][P5][P6]         [P1][P2][P3][P4][P5][P6]
        ↓ full shuffle                    ↓ no shuffle
  [P1'] [P2'] [P3']               [P1+P2] [P3+P4] [P5+P6]
  (even)  (even) (even)            (may be uneven if P1≠P2)
```

### repartitionByRange — for Time-Series Data

```python
# For ordered data, repartitionByRange ensures each partition holds
# a contiguous range of values — better for sorted writes (Iceberg, Delta)
df.repartitionByRange(10, "listen_date") \
  .write \
  .partitionBy("listen_date") \
  .parquet("path/output")
```

### When to Use Which

| Scenario | Use |
|---|---|
| Need to increase partitions | `repartition(n)` |
| Need to reduce partitions, data is already even | `coalesce(n)` |
| Reducing before write to avoid small files | `coalesce(n)` (cheaper) |
| Need perfectly even distribution before write | `repartition(n)` |
| Sorted/range-based partitioning (Iceberg/Delta) | `repartitionByRange(n, col)` |

---

## 7. spark.sql.files.maxPartitionBytes — Read-Time Partitioning

This config controls how Spark **splits files into in-memory partitions when reading**.

```
Default: spark.sql.files.maxPartitionBytes = 134217728 (128 MB)

Meaning: Spark will try to create partitions of at most 128 MB when reading files.

File: listening_activity.csv = 448 KB
  → 448 KB / 128 MB = 0.003 → rounds to 1 partition (whole file fits in 1 partition)
  → default_partitions = 1 ✓
```

### Experimenting With the Config

```python
# Default read
spark = SparkSession.builder.appName("PartitionTest").getOrCreate()
df_default = spark.read.csv("listening_activity.csv", header=True, inferSchema=True)
print(df_default.rdd.getNumPartitions())
# Output: 1  (448 KB fits in one 128 MB partition)

# Reduce maxPartitionBytes to 1 KB
spark.conf.set("spark.sql.files.maxPartitionBytes", "1000")  # ~1 KB
df_modified = spark.read.csv("listening_activity.csv", header=True, inferSchema=True)
print(df_modified.rdd.getNumPartitions())
# Output: 437  (448 KB / ~1 KB = ~448 chunks, minus overhead = 437)
```

### Why Doesn't 448 / 1 = 448 partitions Exactly?

Spark uses **openCostInBytes** (`spark.sql.files.openCostInBytes`, default 4 MB) to account for the cost of opening each file. This prevents creating too many tiny partitions for small files. The formula is roughly:

```
effective_size = file_size + openCostInBytes (amortized)
partitions ≈ file_size / maxPartitionBytes

But Spark also respects HDFS block boundaries and file split points,
so the exact count can differ slightly from the simple division.
448 KB / 1 KB = 448, but Spark's overhead accounting gives 437.
```

### Practical Tuning Guide

```python
# Use Case 1: Large files, want more parallelism
spark.conf.set("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))  # 64 MB
# Smaller partitions → more tasks → more parallelism

# Use Case 2: Too many small files, want fewer, larger partitions
spark.conf.set("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024))  # 256 MB
# Spark merges small files into fewer partitions → less scheduling overhead

# Use Case 3: Memory pressure — tasks OOMing on read
spark.conf.set("spark.sql.files.maxPartitionBytes", str(32 * 1024 * 1024))   # 32 MB
# Smaller chunks → each task needs less memory
```

---

## 8. Partition Pruning — The Whole Point

Partition pruning is what makes `partitionBy` worthwhile. Spark uses **predicate pushdown** to skip irrelevant partitions at the file system level.

```python
# Read partitioned data
df_pt = spark.read.parquet("path/listening_activity_pt")

# Query with filter on partition column
df_pt.filter("listen_date = '2023-06-27'").explain(True)

# Physical Plan will show:
# PartitionFilters: [isnotnull(listen_date#5), (listen_date#5 = 18074)]
# PushedFilters: []
# ReadSchema: ...

# Key line: PartitionFilters is present → PRUNING IS ACTIVE
# Spark only reads the listen_date=2023-06-27/ folder
```

### When Pruning Does NOT Happen

```python
# PRUNING WORKS: filter directly on partition column
df.filter("listen_date = '2023-06-27'")                     # ✅ pruned

# PRUNING FAILS: function applied to partition column — Spark can't push down
df.filter(F.year("listen_date") == 2023)                    # ❌ full scan
df.filter(F.col("listen_date").cast("string") == "2023...")  # ❌ full scan

# PRUNING WORKS: use partition column directly in SQL
spark.sql("SELECT * FROM table WHERE listen_date = '2023-06-27'")  # ✅ pruned
```

---

## 9. The Small Files Problem

Over-partitioning (too many small files per folder) is one of the most common production issues in Data Lakes.

### Why It Happens

```
Scenario: 365 date partitions × 200 shuffle partitions = 73,000 files

Each file: 1–5 MB
Issues:
  - HDFS NameNode metadata overhead (NameNode stores metadata for every file)
  - Spark reads each file with a separate task → 73,000 tasks for a simple query
  - Each task has scheduling overhead (~100ms) → 73,000 × 100ms = 2 hours in overhead alone
  - S3/ADLS LIST operations are slow — listing 73,000 files before reading is expensive
```

### Fixes

```python
# Fix 1: coalesce before write
df.coalesce(1) \
  .write.partitionBy("listen_date").parquet("output")
# 1 file per date folder — OK for small/medium datasets

# Fix 2: repartition by partition column before write
df.repartition("listen_date") \
  .write.partitionBy("listen_date").parquet("output")
# All records for a date go to the same in-memory partition → 1 file per folder

# Fix 3: Delta Lake OPTIMIZE (Databricks)
spark.sql("OPTIMIZE listening_activity ZORDER BY (listen_hour)")
# Compacts small files and sorts data for faster queries

# Fix 4: Iceberg table maintenance
spark.sql("""
    CALL iceberg_catalog.system.rewrite_data_files(
        table => 'db.listening_activity',
        strategy => 'sort',
        sort_order => 'listen_date ASC'
    )
""")
```

---

## 10. Interview Q&A – Tiered

---

### 🟢 Basic Level

**Q1. What is the difference between `partitionBy` and `repartition` in Spark?**

**A:** They solve different problems.
- `partitionBy` (on a DataFrameWriter) creates **directory-based storage partitions** on disk — it determines the folder layout of output files. It's for query performance and partition pruning.
- `repartition` (on a DataFrame) controls the **number of in-memory partitions** during job execution. It determines how many tasks run and how data is distributed across executors.

You often use both together: `df.repartition(5).write.partitionBy("date")` — repartition controls how many files are created inside each date folder.

---

**Q2. What is partition pruning and when does it happen?**

**A:** Partition pruning is when Spark skips reading entire directory folders during a query because the filter condition eliminates them. It happens when your filter condition directly references the partition column — for example, `WHERE listen_date = '2024-01-01'` on data partitioned by `listen_date`. You can verify it by running `.explain()` and looking for `PartitionFilters` in the physical plan. It does NOT happen when you apply functions to the partition column, because Spark can't evaluate the function at the metadata level.

---

**Q3. What factors determine a good partition column?**

**A:** Three factors:
1. **Cardinality** — not too high (millions of unique values → millions of tiny folders) and not too low (2–3 values → folders too large). The sweet spot is columns with hundreds to low thousands of distinct values, like `date`, `country`, or `region`.
2. **Query patterns** — the column must match your most common filter condition. A perfectly-sized partition is useless if your queries never filter on it.
3. **Data distribution** — avoid columns where one value contains a disproportionate share of data (skew), because that partition folder will be enormous.

---

### 🟡 Medium Level

**Q4. When would you use `coalesce` over `repartition` and vice versa?**

**A:**
- Use **`coalesce(n)`** when you want to **reduce** partitions cheaply — it avoids a full shuffle by merging existing partitions. Best used just before writing to reduce small files. Risk: if input partitions were already uneven, coalesced output will also be uneven.
- Use **`repartition(n)`** when you want to **increase** partitions or when you need **even distribution** regardless of cost. It performs a full shuffle and guarantees uniform partition sizes. Also use `repartition(n, col)` to co-locate data by a specific column — useful before a join to eliminate shuffle.

---

**Q5. What does `spark.sql.files.maxPartitionBytes` control, and when would you tune it?**

**A:** It controls the **maximum size of each in-memory partition when Spark reads files**. Default is 128 MB. Spark uses this to decide how many partitions to create when reading a file.

Tune it when:
- Files are large and you want **more parallelism** → lower the value (e.g., 64 MB)
- You have many small files and want **fewer, larger partitions** → raise the value (e.g., 256 MB)
- Tasks are hitting OOM during read → lower the value so each task processes less data

---

**Q6. You write a DataFrame with `partitionBy("listen_date")` and end up with 200 files in every date folder. Why, and how do you fix it?**

**A:** By default, `spark.sql.shuffle.partitions = 200`. When the DataFrame goes through any shuffle (or even without a shuffle, based on the number of in-memory partitions at write time), Spark creates one output file per in-memory partition per storage partition folder. With 200 in-memory partitions, you get 200 files per date folder.

Fix: Use `repartition("listen_date")` before writing — this co-locates all records for the same date into the same in-memory partition, producing **1 file per date folder**. Or use `repartition(n)` to control the exact file count.

```python
df.repartition("listen_date") \
  .write.partitionBy("listen_date").parquet("output")   # 1 file per date
```

---

### 🔴 Advanced Level

**Q7. In a multi-level `partitionBy("year", "month", "day")`, what are the trade-offs vs a single `partitionBy("date")`?**

**A:**
- **Multi-level** creates a deeper hierarchy: `year=2024/month=01/day=01/`. It allows pruning at each level independently — a query for `year=2024` skips all of 2023 and earlier without reading month/day subdirectories. This is beneficial for dashboards that often query full months or years.
- **Single date** is simpler — one level of folders, easier to manage, fewer metadata operations. Works well when queries are almost always filtered by exact date.
- **Trade-off:** Multi-level with `day` creates 365 leaf directories per year. At scale (3 years = ~1,095 folders), LIST operations on S3/ADLS become expensive. In Delta Lake and Iceberg, this is mitigated by metadata files — but in raw Parquet, it's a real cost.

Rule of thumb: use `partitionBy("year", "month")` for very large datasets queried monthly, and `partitionBy("date")` for datasets where daily queries are the norm.

---

**Q8. How does Delta Lake's `OPTIMIZE` and Z-Ordering relate to Spark partitioning?**

**A:** They work at different layers:
- `partitionBy` creates **coarse-grained** directory-level pruning — Spark skips entire folders.
- `OPTIMIZE` compacts the small files inside those folders into larger, right-sized files (targeting 1 GB each). Without OPTIMIZE, you get the small files problem described above.
- `ZORDER BY (col)` clusters rows within each Parquet file by that column's value, so a query filtering on that column reads fewer row groups within each file — **fine-grained pruning** via Parquet column statistics (min/max per row group).

Together: `partitionBy("date")` prunes at the folder level, `ZORDER BY (song_id)` prunes at the row-group level within each file. This is the recommended pattern for Delta Lake production tables.

```sql
-- Compact + sort in one operation
OPTIMIZE listening_activity ZORDER BY (song_id, listen_hour);
```

---

**Q9. You're migrating a 2 TB Hive table partitioned by `event_date` to Apache Iceberg. The date column has 730 values (2 years). Some dates are 50 GB, others are 200 MB. What partitioning strategy do you recommend for Iceberg, and why?**

**A:** Three considerations:
1. **Iceberg hidden partitioning** — Iceberg supports `PARTITION BY MONTH(event_date)` instead of raw date. This reduces partition count from 730 to 24 (2 years × 12 months), avoiding the small-date-folder problem while still enabling pruning for month-level queries.
2. **Handle skewed dates** — dates with 50 GB of data are 250× larger than 200 MB dates. Use `repartitionByRange(50, "event_date")` for skewed dates to produce right-sized files within each Iceberg partition.
3. **File sizing** — Iceberg's target file size is configurable:
```python
spark.sql("""
    ALTER TABLE iceberg_catalog.db.table
    SET TBLPROPERTIES (
        'write.target-file-size-bytes' = '536870912'  -- 512 MB per file
    )
""")
# Then run periodic compaction
spark.sql("""
    CALL iceberg_catalog.system.rewrite_data_files(
        table => 'db.table',
        strategy => 'sort',
        sort_order => 'event_date ASC, event_hour ASC'
    )
""")

Set file size
→ controls future writes
Run compaction
→ fixes already existing small files

SET TBLPROPERTIES → future file size control
rewrite_data_files → clean & optimize old data
```

---

## 11. Real-World Scenarios & Solutions

---

### 🏗️ Scenario 1 – Spotify Listening Activity: Choosing the Right Partition Column

**Context:** You have a 500 GB Spotify listening activity dataset. The data team runs two types of queries:
- **Daily dashboards:** `WHERE listen_date = '2024-01-15'` (90% of queries)
- **Hourly analysis:** `WHERE listen_date = '2024-01-15' AND listen_hour = 10` (10% of queries)

**Analysis:**
```python
# Check cardinality of candidate columns
df.select(
    F.countDistinct("listen_date").alias("date_cardinality"),   # ~730
    F.countDistinct("listen_hour").alias("hour_cardinality"),   # 24
    F.countDistinct("song_id").alias("song_cardinality")        # 500,000
).show()

# date: 730 unique values → 500GB / 730 = ~685 MB per partition → good
# hour: 24 unique values → 500GB / 24 = ~20 GB per partition → too large
# song_id: 500K values → 500GB / 500K = ~1 MB per partition → too small
```

**Decision:**
```python
# Primary partition by date (matches 90% of query patterns)
# Secondary by hour (matches the 10% hourly queries)
df_listening_actv \
    .repartition("listen_date") \       # 1 file per date (avoids small files)
    .write \
    .mode("overwrite") \
    .partitionBy("listen_date", "listen_hour") \
    .parquet("gs://bucket/listening_activity_partitioned")

# Resulting structure:
# listen_date=2024-01-15/
# ├── listen_hour=8/    part-0.parquet
# ├── listen_hour=10/   part-0.parquet
# └── listen_hour=23/   part-0.parquet
```

---

### 🏗️ Scenario 2 – Delta Lake Pipeline: Solving the Small Files Problem

**Context:** A daily ADF-triggered Databricks pipeline writes incremental data to a Delta table partitioned by `event_date`. After 6 months of daily runs, each date folder has 150–200 tiny files (2–5 MB each). Queries that used to run in 30 seconds now take 8 minutes.

**Root Cause:**
```
6 months × 30 days = 180 date folders
Each folder: 175 files × 3 MB avg = 525 MB of actual data
But 175 tasks with metadata overhead = slow LIST + open cost

Additionally: NameNode / S3 object metadata overhead for 180 × 175 = 31,500 files
```

**Solution:**
```python
# Step 1: Fix going forward — control files per write
df_incremental \
    .repartition(F.col("event_date")) \  # 1 file per date
    .write \
    .format("delta") \
    .mode("append") \
    .partitionBy("event_date") \
    .save("/delta/events")

# Step 2: Fix historical small files — OPTIMIZE
spark.sql("""
    OPTIMIZE events
    WHERE event_date >= '2024-01-01'
    ZORDER BY (user_id)
""")
# OPTIMIZE compacts 175 × 3MB files → 2 × 512 MB files per date folder

# Step 3: Schedule OPTIMIZE regularly via ADF or Databricks Jobs
# Run after each daily write to prevent accumulation
```

---

### 🏗️ Scenario 3 – Iceberg Migration: Re-Partitioning Strategy

**Context:** You're migrating a 1 TB Hive table (Parquet, partitioned by `event_date`, 365 partitions) to Apache Iceberg. Post-migration, queries are slower than on Hive.

**Diagnosis:**
```python
# Check file size distribution in Iceberg
spark.sql("""
    SELECT file_path,
           record_count,
           file_size_in_bytes / 1024 / 1024 AS file_size_mb
    FROM iceberg_catalog.db.events.files
    ORDER BY file_size_in_bytes
""").show(20)

# Output shows: 3,000+ files, avg 330 KB each → small files problem inherited from Hive
```

**Solution:**
```python
# Re-partition with proper file sizing during migration
for year_month in ["2023-01", "2023-02", ..., "2024-12"]:
    df_batch = spark.read.table("hive_metastore.db.events") \
                    .filter(F.col("event_date").startswith(year_month))
    
    # Right-size files: target 512 MB per file
    # 1 TB / 24 months = ~42 GB per month → 42GB / 512MB = ~85 files per month
    df_batch \
        .repartitionByRange(85, "event_date") \
        .writeTo("iceberg_catalog.db.events") \
        .append()

# After full migration: compact and sort
spark.sql("""
    CALL iceberg_catalog.system.rewrite_data_files(
        table => 'db.events',
        strategy => 'sort',
        sort_order => 'event_date ASC',
        options => map('target-file-size-bytes', '536870912')
    )
""")
```

---

## Quick Reference – Partitioning Cheat Sheet

```
STORAGE PARTITIONING (disk layout):
  df.write.partitionBy("col").parquet("path")
  → Creates listen_date=XXX/ directories
  → Enables partition pruning during reads

READ-TIME PARTITIONING:
  spark.sql.files.maxPartitionBytes = 128MB (default)
  → Controls how many in-memory partitions on read
  → Tune for parallelism or memory pressure

FILE COUNT CONTROL:
  df.repartition(n).write.partitionBy(...)      → n files per folder (full shuffle)
  df.coalesce(n).write.partitionBy(...)         → up to n files (no shuffle, cheaper)
  df.repartition("col").write.partitionBy("col") → 1 file per folder (best practice)

PARTITION COLUMN SELECTION:
  ✅ Cardinality: 100s–few 1000s of unique values
  ✅ Matches most common WHERE clause
  ✅ Relatively even data distribution
  ❌ NOT customer_id (too high)
  ❌ NOT boolean/flag columns (too low)

PRUNING CHECK:
  df.filter("col = 'value'").explain()
  Look for: PartitionFilters: [...] in physical plan

SMALL FILES FIX:
  Option A: df.repartition("partition_col").write.partitionBy(...)
  Option B: OPTIMIZE table ZORDER BY (other_col)       ← Delta Lake
  Option C: CALL system.rewrite_data_files(...)         ← Iceberg
```

---

*Next Topic: Spark Shuffling & Shuffle Optimization* 🚀
