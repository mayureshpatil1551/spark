# 12 — Dynamic Partition Pruning (DPP): How It Works (And When It Doesn't)
> **Target Audience:** Data Engineers with 3–4 years of experience | Interview + Real-World Prep

---

## Table of Contents
1. [What is Partition Pruning?](#1-what-is-partition-pruning)
2. [Static Partition Pruning](#2-static-partition-pruning)
3. [Dynamic Partition Pruning (DPP)](#3-dynamic-partition-pruning-dpp)
4. [How DPP Works Internally](#4-how-dpp-works-internally)
5. [Reading the Physical Query Plan](#5-reading-the-physical-query-plan)
6. [Caveats — When DPP Does NOT Work](#6-caveats--when-dpp-does-not-work)
7. [Full Code Example](#7-full-code-example)
8. [DPP vs Static Pruning vs Full Scan — Comparison](#8-dpp-vs-static-pruning-vs-full-scan--comparison)
9. [Interview Questions & Answers](#9-interview-questions--answers)
10. [Real-World Project Scenarios](#10-real-world-project-scenarios)
11. [Quick Reference Cheat Sheet](#11-quick-reference-cheat-sheet)

---

💼 Real-World Use Case

Pruning in Spark is an optimization technique where unnecessary data is skipped during query execution, so only the required data is read and processed.
In a partitioned Iceberg table, when we filter on partition column like date, Spark automatically prunes unnecessary partitions, reducing scan size and improving query performance/


## 1. What is Partition Pruning?

### The Core Idea
When a large dataset is **physically partitioned on disk** (e.g., Parquet files organized into folders by date), Spark can skip reading entire partitions that are not needed for a query. This is called **partition pruning** — Spark prunes (removes) irrelevant partitions from the scan, reducing I/O dramatically.

```
Dataset partitioned by listen_date:
  s3://data/listening_activity/
    listen_date=2023-01-01/  ← 500 MB
    listen_date=2023-01-02/  ← 500 MB
    listen_date=2023-01-03/  ← 500 MB
    ...
    listen_date=2023-12-31/  ← 500 MB
  Total: ~180 GB

Query: WHERE listen_date = '2023-06-04'
With pruning: Spark reads ONLY 500 MB instead of 180 GB → 360x less I/O
```

### Two Types of Pruning

| Type | Filter Value Known At | Example |
|---|---|---|
| **Static Pruning** | Compile time (hardcoded) | `filter(col("date") == "2023-06-04")` |
| **Dynamic Pruning (DPP)** | Runtime (from another table/subquery) | `join songs WHERE listen_date == release_date` |

---

## 2. Static Partition Pruning

### Definition
Static pruning applies when the **filter value is a literal constant** — Spark knows at query planning time exactly which partitions to skip.

```python
# listen_date is a partition column
df_listening_actv.filter(F.col("listen_date") == "2023-06-04")
```

### What Happens Internally
```
Planning phase (before execution):
  Spark sees: filter on partition column listen_date == "2023-06-04"
  Spark tells the file reader: skip all directories except listen_date=2023-06-04/
  
Execution phase:
  Reads ONLY: listen_date=2023-06-04/ folder
  Skips: all other 364 date folders
```

The key: **the filter value ("2023-06-04") is known before any data is read**. Spark can resolve it purely at planning time.

### Requirements for Static Pruning
- The filtered column must be the **partition column** (the column used in `partitionBy()` during write)
- The filter value must be a **constant literal** (not derived from another column or table)
- Table must be stored in a **partition-aware format** (Parquet, ORC, Delta, Hive)

---

## 3. Dynamic Partition Pruning (DPP)

### The Problem Static Pruning Cannot Solve
Consider this problem:

> *Analyse listening activity of users on the release date of a song, for songs released after 2020-01-01.*

```python
# Two tables:
# listening_activity  — partitioned by listen_date    (large: millions of rows)
# songs               — NOT partitioned                (small: thousands of rows)

# Join condition: listen_date == release_date  AND  song_id == song_id
```

The filter values (release dates) are **not known at compile time** — they come from the `songs` table, which is read at runtime. Static pruning cannot help here.

Without DPP, Spark does a **full scan** of the entire `listening_activity` dataset, then filters after the join — reading all 180 GB even if only 10 specific release dates are needed.

### What DPP Does
DPP allows Spark to **use runtime values from one table (songs) as a filter on the partition column of another table (listening_activity)** — before the full scan happens.

```
Step 1: Spark reads the songs table first (small table)
        → Extracts the unique release_date values: {2020-03-15, 2021-08-22, 2022-11-01, ...}

Step 2: Spark uses those release_date values as a runtime filter
        on listening_activity's partition column (listen_date)
        → Reads ONLY: listen_date=2020-03-15/, listen_date=2021-08-22/, listen_date=2022-11-01/, ...
        → Skips ALL other partitions

Step 3: Performs the join on the pruned (smaller) dataset
```

### Visual Comparison

```
WITHOUT DPP:
  songs → [filter release_date > 2020] → 50 release dates extracted
  listening_activity → FULL SCAN (180 GB) → join → filter → result
  
  Cost: 180 GB read + join + filter

WITH DPP:
  songs → [filter release_date > 2020] → 50 release dates extracted
                                        ↓
  listening_activity → [DPP: scan only 50 partitions] → join → result
  
  Cost: 50 × 500 MB = 25 GB read + join
  Savings: 155 GB (86% less I/O)
```

### DPP is Enabled by Default
```python
# DPP is ON by default in Spark 3.x
spark.conf.get("spark.sql.optimizer.dynamicPartitionPruning.enabled")
# → "true"

# To explicitly enable/disable:
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

---

## 4. How DPP Works Internally

### The Subquery Injection Mechanism
DPP works by injecting a **dynamic subquery** (called `dynamicpruningexpression`) into the partition filter of the large table's file scan. At runtime:

1. The small table (songs) is **broadcast** to all executors (this is why DPP typically requires or benefits from a broadcast join)
2. Simultaneously, the set of distinct partition key values from the small table is computed
3. That set is injected as a runtime `IN (...)` filter on the partition column of the large table's file reader
4. The file system scan uses that filter to skip irrelevant partition directories

### DPP + Broadcast Join Connection
DPP works most reliably when the **smaller table is broadcast**. Spark reuses the broadcast result for two purposes:
- The actual join operation
- The dynamic partition filter injection into the large table's scan

```
Physical Plan (simplified):
  BroadcastHashJoin [listen_date, song_id] = [release_date, song_id]
    ├── FileScan parquet listening_activity
    │     PartitionFilters: [listen_date IN dynamicpruningexpression(...)]
    │                                      ↑ DPP filter injected here
    └── BroadcastExchange (songs)
          └── Filter release_date > 2019-12-31
```

### DPP Evaluation — Reuse vs Recompute
If the small table is already being broadcast for the join, Spark **reuses** the broadcast result for the partition filter — zero extra cost. If not broadcast, Spark may need to run an extra subquery to compute the distinct filter values — slight overhead, but still much cheaper than a full scan.

---

## 5. Reading the Physical Query Plan

Understanding `explain()` output is a critical skill for diagnosing whether DPP is active.

```python
df_result.explain(True)
# Use explain("formatted") in Spark 3+ for more readable output
df_result.explain(mode="formatted")
```

### Key Indicators in the Physical Plan

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- BroadcastHashJoin [listen_date, song_id], [release_date, song_id], Inner, BuildRight
   :- FileScan parquet [activity_id, song_id, listen_time, listen_date]
   :    PartitionFilters: [(listen_date > 2019-12-31),           ← Static filter
   :                       dynamicpruningexpression(listen_date  ← DPP is ACTIVE ✅
   :                       IN subquery)]
   :    +- SubqueryAdaptiveBroadcast dynamicpruning#155, ...    ← DPP subquery
   +- BroadcastExchange ...
      +- Filter (release_date > 2019-12-31)
         +- FileScan csv [song_id, title, release_date]
```

### What Each Term Means

| Term in Plan | Meaning |
|---|---|
| `PartitionFilters` | Filters applied at the partition directory level (before reading files) |
| `DataFilters` | Filters applied to rows within files (after reading) |
| `dynamicpruningexpression(...)` | DPP is active — partition filter comes from another table at runtime ✅ |
| `SubqueryAdaptiveBroadcast` | The subquery being broadcast for DPP |
| `BroadcastHashJoin` | Join strategy — DPP works best here |
| `PushedFilters` | Filters pushed into the file reader (Parquet predicate pushdown) |
| `FileScan` with empty `PartitionFilters` | DPP NOT active — full scan occurring ❌ |

### Confirmed DPP vs Full Scan

```
DPP ACTIVE (look for):
  PartitionFilters: [dynamicpruningexpression(...)]
  
FULL SCAN (DPP not working):
  PartitionFilters: []    ← empty — no partition filter applied
  DataFilters: [...]      ← filter applied AFTER reading all data
```

---

## 6. Caveats — When DPP Does NOT Work

Understanding DPP's limitations is as important as knowing how it works.

### Caveat 1 — Large Table Must Be Partitioned
DPP requires the large table to be **physically partitioned on disk**. An in-memory DataFrame or a non-partitioned file cannot benefit from DPP — there are no partition directories to skip.

```python
# DPP works:
df_listening = spark.read.parquet("s3://data/listening_activity_partitioned_by_date/")
# ↑ Written with .write.partitionBy("listen_date")

# DPP does NOT work:
df_listening = spark.read.parquet("s3://data/listening_activity_single_file.parquet")
# ↑ Not partitioned — no directories to prune
```

### Caveat 2 — Filter Must Be on the Partition Column
The join/filter condition must reference the **partition column** of the large table. If the condition involves a non-partition column, Spark cannot skip partition directories — it must read all data.

```python
# DPP works (listen_date IS the partition column):
df_listening.join(df_songs,
    on=(df_listening.listen_date == df_songs.release_date) &
       (df_listening.song_id == df_songs.song_id))

# DPP does NOT work (song_id is NOT a partition column):
df_listening.join(df_songs, on="song_id")
# → Full scan of listening_activity; song_id is row-level data, not partition directories
```

### Caveat 3 — Small Table Must Be Small Enough to Benefit
DPP injects the distinct partition values from the small table as a runtime filter. If the small table has millions of distinct partition values (nearly as many as the large table's partitions), DPP provides little benefit — almost all partitions still need to be scanned.

DPP is most effective when the small table filters down the large table's partitions significantly (e.g., 50 out of 1,000 partitions).

### Caveat 4 — Works Best with Broadcast Join
DPP is tightly coupled with broadcast joins. If the small table is too large to broadcast, DPP may still work (via an extra subquery) but requires the optimizer to decide it's worth the overhead.

```python
# Ensure broadcast threshold covers the small table
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
```

### Caveat 5 — Join Type Matters
DPP works best with **inner joins** and **left semi-joins**. For left outer joins, DPP on the left side is not beneficial (left outer join must return all rows from the left table regardless).

### Summary of DPP Requirements

| Requirement | Why |
|---|---|
| Large table is partitioned on disk | DPP skips partition directories — no directories = nothing to skip |
| Filter is on the partition column | Partition directories are named by partition column values |
| Small table is reasonably small | Broadcast + subquery injection is the mechanism |
| Join type: inner or left semi | Outer joins on the large side can't skip partitions |
| AQE enabled (Spark 3+) | AQE improves DPP planning decisions |

---

## 7. Full Code Example

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
    .config("spark.driver.memory", "10g")
    .master("local[*]")
    .appName("DynamicPartitionPruning")
    # DPP is enabled by default; shown here for clarity
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

# ─────────────────────────────────────────────
# Step 1: Load and prepare the listening activity table
# ─────────────────────────────────────────────
df_listening_raw = spark.read.csv(
    "../data/partitioning/raw/Spotify_Listening_Activity.csv",
    header=True, inferSchema=True
)

df_listening = (
    df_listening_raw
    .withColumnRenamed("listen_date", "listen_time")
    .withColumn("listen_date", F.to_date("listen_time", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
)

# Write partitioned by listen_date — THIS is what enables DPP
df_listening.write \
    .partitionBy("listen_date") \
    .mode("overwrite") \
    .parquet("../data/partitioning/partitioned/listening_activity_pt")

# Read back the partitioned dataset
df_listening_pt = spark.read.parquet(
    "../data/partitioning/partitioned/listening_activity_pt"
)
df_listening_pt.show(5, truncate=False)
# +-----------+-------+--------------------+---------------+-----------+
# |activity_id|song_id|listen_time         |listen_duration|listen_date|
# +-----------+-------+--------------------+---------------+-----------+
# |4456       |16     |2023-07-18 10:15:...|151            |2023-07-18 |

# ─────────────────────────────────────────────
# Step 2: Load and prepare songs table
# ─────────────────────────────────────────────
df_songs_raw = spark.read.csv(
    "../data/partitioning/raw/Spotify_Songs.csv",
    header=True, inferSchema=True
)

df_songs = (
    df_songs_raw
    .withColumnRenamed("release_date", "release_datetime")
    .withColumn("release_date", F.to_date("release_datetime", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
)
df_songs.show(5, truncate=False)
# +-------+------+---------+--------------------------+------------+
# |song_id|title |artist_id|release_datetime          |release_date|
# +-------+------+---------+--------------------------+------------+
# |1      |Song_1|2        |2021-10-15 10:15:47.006571|2021-10-15  |

# ─────────────────────────────────────────────
# Step 3: Filter songs released after 2020
# ─────────────────────────────────────────────
df_selected_songs = df_songs.filter(F.col("release_date") > F.lit("2019-12-31"))

# ─────────────────────────────────────────────
# Step 4: Join — DPP activates here
# listening_date == release_date → DPP filters listening_activity partitions
# at runtime using the release_date values from df_selected_songs
# ─────────────────────────────────────────────
df_result = df_listening_pt.join(
    df_selected_songs,
    on=(df_listening_pt.listen_date == df_selected_songs.release_date) &
       (df_listening_pt.song_id     == df_selected_songs.song_id),
    how="inner"
)

# ─────────────────────────────────────────────
# Step 5: Verify DPP is active in the physical plan
# ─────────────────────────────────────────────
df_result.explain(mode="formatted")
# Look for in the output:
#   PartitionFilters: [dynamicpruningexpression(...)]  ← DPP CONFIRMED ✅
#   SubqueryAdaptiveBroadcast dynamicpruning#...       ← DPP subquery
#   BroadcastHashJoin                                  ← Join strategy

df_result.show()
# +-----------+-------+-----------+-------+-------+---------+------------+
# |activity_id|song_id|listen_date|song_id|title  |artist_id|release_date|
# +-----------+-------+-----------+-------+-------+---------+------------+
# |9760       |89     |2023-07-24 |89     |Song_89|33       |2023-07-24  |
# |7322       |64     |2023-10-25 |64     |Song_64|32       |2023-10-25  |

# ─────────────────────────────────────────────
# Step 6: Demonstrate DPP NOT working (non-partition column join)
# ─────────────────────────────────────────────
df_result_no_dpp = df_listening_pt.join(
    df_selected_songs,
    on="song_id",   # song_id is NOT the partition column → NO DPP
    how="inner"
)
df_result_no_dpp.explain(mode="formatted")
# Look for:
#   PartitionFilters: []   ← EMPTY — no DPP, full scan occurs ❌

spark.stop()
```

---

## 8. DPP vs Static Pruning vs Full Scan — Comparison

| Aspect | Full Scan | Static Pruning | Dynamic Pruning (DPP) |
|---|---|---|---|
| Filter value known at | Never — reads all | Compile time (literal) | Runtime (from another table) |
| Partitions scanned | All | 1 (exact literal match) | Only matching ones (runtime-determined) |
| Requires partitioned table | No | Yes | Yes |
| Triggered by | No filter on partition col | `col == literal` | `col == other_table.col` in join |
| Plan keyword | `FileScan` (no PartitionFilters) | `PartitionFilters: [col = lit]` | `PartitionFilters: [dynamicpruningexpression]` |
| I/O reduction | None | Maximum (exact) | High (depends on filter selectivity) |
| Default enabled | N/A | Always | Yes (Spark 3+) |

---

## 9. Interview Questions & Answers

---

### Q1. What is Dynamic Partition Pruning and how does it differ from static partition pruning?

**Answer:**
Static partition pruning occurs when the filter value is a **compile-time constant**. Spark knows at planning time which partitions to skip — for example, `filter(col("date") == "2023-06-04")` tells Spark to read only the `date=2023-06-04/` directory.

Dynamic Partition Pruning (DPP) handles cases where the filter value is **not known until runtime** — typically when it comes from joining with another table. For example, joining a large events table with a small dates table on the date column: the set of relevant dates is only known after reading the dates table. DPP injects those runtime values as a partition filter on the large table's scan, avoiding a full read.

The key difference: static pruning resolves at planning time; DPP resolves at execution time using actual data from another table.

---

### Q2. How does DPP work internally — what does "dynamicpruningexpression" mean in the query plan?

**Answer:**
When DPP is triggered, Spark inserts a `dynamicpruningexpression` node into the `PartitionFilters` of the large table's `FileScan` in the physical plan. This expression is a subquery that computes the distinct values of the join key from the small table at runtime.

The mechanism works in two steps: First, the small table is broadcast (or a subquery is run) to collect the distinct partition key values. Second, those values are passed as a runtime `IN (v1, v2, ...)` filter to the file reader. The file reader uses these values to skip partition directories that don't match — before reading any row-level data.

In the physical plan, you confirm DPP is active when you see `PartitionFilters: [dynamicpruningexpression(...)]` alongside `SubqueryAdaptiveBroadcast` in the plan.

---

### Q3. What are the requirements for DPP to work?

**Answer:**
DPP has four core requirements:

1. **The large table must be physically partitioned on disk**, written with `partitionBy()`. DPP skips partition directories — if there are no directories, there's nothing to prune.

2. **The join/filter condition must involve the partition column** of the large table. DPP prunes partition directories by their names (which correspond to the partition column values). If you join on a non-partition column, Spark can't skip directories.

3. **The small table should be small enough for broadcast or subquery execution**. DPP reuses the broadcast result for both the join and the partition filter injection. If the broadcast threshold is too low, DPP may not trigger.

4. **The join type should be inner or left semi**. Left outer joins on the large side can't skip partitions — the large table must return all its rows regardless.

---

### Q4. If the join key is the same as the partition column, does DPP always trigger?

**Answer:**
Not always. DPP triggers only when it's cost-beneficial. Spark's optimizer (and AQE) estimates whether the savings from pruning partitions outweigh the cost of running the extra subquery to collect distinct filter values.

If the small table has millions of distinct values close to the total number of partitions in the large table, DPP would scan nearly all partitions anyway — the optimizer may skip DPP. If the small table filters down to a handful of partitions out of thousands, DPP is highly beneficial and will trigger.

You can confirm DPP triggered by checking the physical plan for `dynamicpruningexpression` in `PartitionFilters`. If the `PartitionFilters` is empty, DPP did not apply.

---

### Q5. How does DPP interact with AQE?

**Answer:**
DPP and AQE are complementary and work well together in Spark 3+. AQE improves DPP's decision-making in two ways:

First, AQE uses actual runtime statistics (real partition sizes, real row counts) rather than catalog statistics, which makes the cost-benefit analysis for DPP more accurate. A table that was estimated as large may actually be small at runtime, making DPP even more valuable.

Second, with AQE enabled, the `AdaptiveSparkPlan` framework manages plan re-optimization between stages. The `SubqueryAdaptiveBroadcast` in DPP plans is specifically designed to work within AQE's adaptive planning framework.

Recommended configuration for both:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

---

### Q6. Can DPP work on Delta Lake or Hive tables?

**Answer:**
Yes. DPP works on any partition-aware storage format supported by Spark's file reader:

- **Parquet** (most common) — full DPP support
- **ORC** — full DPP support
- **Delta Lake** — full DPP support, enhanced by Delta's transaction log which tracks partition metadata efficiently
- **Hive partitioned tables** — DPP works when Spark reads Hive metastore partitions
- **CSV/JSON** — only if written with `partitionBy()` (rare in practice)

Delta Lake particularly benefits because its transaction log allows Spark to quickly identify which partitions exist without listing all files, making the DPP partition scan even faster.

---

### Q7. What is the difference between DPP and predicate pushdown?

**Answer:**
Both are scan optimization techniques, but they operate at different levels:

**Predicate pushdown** pushes row-level filters into the file reader. For columnar formats like Parquet, the file reader can skip row groups whose statistics (min/max) don't satisfy the filter — without reading the actual rows. This operates **within** a partition.

**Dynamic Partition Pruning** operates at the **partition directory level** — it decides which entire partition directories (folders on disk) to skip before any file reading begins. It operates **above** the file level.

They are complementary: DPP first reduces which partition directories are opened, then predicate pushdown further reduces which row groups within those files are read. Both appear in the physical plan:
- DPP: `PartitionFilters: [dynamicpruningexpression(...)]`
- Predicate pushdown: `PushedFilters: [IsNotNull(col), GreaterThan(col, value)]`

---

### Q8. How would you verify in a production pipeline that DPP is saving I/O?

**Answer:**
Three approaches:

1. **Physical plan check:**
```python
df.explain(mode="formatted")
# Confirm: PartitionFilters contains dynamicpruningexpression
# Without DPP: PartitionFilters is empty or has only static filters
```

2. **Spark UI — SQL tab:**
- Open the query in the SQL tab
- Look at the `FileScan` node → check `numFilesRead` and `bytesRead` metrics
- Compare with/without DPP: significantly fewer files and bytes read confirms DPP is working

3. **Spark UI — Stage metrics:**
- `Input Size` metric in the stage that reads the large table
- With DPP: input size ≈ matching partitions only
- Without DPP: input size ≈ full table size

```python
# Programmatic approach for monitoring
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
df_result.explain("formatted")  # Check for dynamicpruningexpression

# In Spark UI → SQL → your query → FileScan node:
# number of files read, bytes read — compare with expected full table size
```

---

## 10. Real-World Project Scenarios

---

### Scenario 1 — Data Warehouse: Daily Fact Table + Dimension Table Join

**Problem:**
You have a 5 TB `sales_fact` table partitioned by `sale_date` (3 years of data, ~1,460 partitions). A daily dashboard query joins it with a `promotions` dimension table (10,000 rows) to find sales during active promotions. The `promotions` table has a `start_date` and `end_date` column. Without DPP, the query scans the full 5 TB every time.

**Solution:**
```python
df_sales = spark.read.parquet("s3://dw/sales_fact/")
# Written as: df.write.partitionBy("sale_date").parquet(...)

df_promotions = spark.read.parquet("s3://dw/promotions/")

# Filter to active promotions in 2023
df_active_promo = df_promotions.filter(
    (F.col("start_date") >= "2023-01-01") &
    (F.col("end_date")   <= "2023-12-31")
)

# Join condition includes sale_date == promo date range
# DPP triggers: only 365 sale_date partitions scanned instead of 1,460
df_sales_during_promos = df_sales.join(
    df_active_promo,
    on=(df_sales.sale_date >= df_active_promo.start_date) &
       (df_sales.sale_date <= df_active_promo.end_date) &
       (df_sales.product_id == df_active_promo.product_id),
    how="inner"
)

# Verify DPP in plan
df_sales_during_promos.explain(mode="formatted")
# Expect: PartitionFilters: [dynamicpruningexpression(sale_date IN subquery)]
```

**Result:** Query scans ~25% of the table (1 year out of 4) instead of all 5 TB.

---

### Scenario 2 — Event Processing: User Activity on Specific Campaign Days

**Problem:**
A marketing team runs 20 campaigns per year. Each campaign has specific active dates. You need to find all user events (500 GB/year, partitioned by `event_date`) that occurred on campaign-active dates.

```python
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")  # 50 MB

df_events = spark.read.parquet("s3://data/user_events/")
# Partitioned by event_date

df_campaigns = spark.read.parquet("s3://data/campaigns/")
# Columns: campaign_id, campaign_name, active_date, target_segment

# Get all distinct active campaign dates
df_campaign_dates = df_campaigns.select("active_date", "campaign_id", "target_segment")

# DPP: event_date == active_date → prunes event partitions to only campaign days
df_campaign_events = df_events.join(
    F.broadcast(df_campaign_dates),    # Force broadcast → helps DPP
    on=df_events.event_date == df_campaign_dates.active_date,
    how="inner"
)

df_campaign_events \
    .groupBy("campaign_id", "target_segment") \
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("user_id").alias("unique_users")
    ) \
    .write.mode("overwrite").parquet("s3://reports/campaign_analysis/")
```

**Note:** Using `F.broadcast()` explicitly on `df_campaign_dates` ensures DPP triggers reliably. DPP is most consistent when paired with a broadcast join.

---

### Scenario 3 — Diagnosing Why DPP is NOT Working

**Problem:**
A data engineer reports that queries are still scanning the full table despite joining on a date column. You need to diagnose why.

```python
# Diagnostic checklist function
def diagnose_dpp(df_large, df_small, join_conditions, large_partition_col):
    """Diagnose whether DPP will work for a given join."""

    print("=== DPP Diagnostic ===\n")

    # Check 1: Is the large table partitioned?
    try:
        partitions = df_large.rdd.getNumPartitions()
        # Check if it was written with partitionBy
        plan_str = df_large._jdf.queryExecution().analyzed().toString()
        has_partition_filters = "PartitionFilters" in df_large.explain(True)
        print(f"1. Large table partitions:  {partitions}")
    except:
        pass

    # Check 2: Is the join condition on the partition column?
    print(f"2. Partition column:         '{large_partition_col}'")
    print(f"   Join conditions include partition col: "
          f"{'✅ YES' if large_partition_col in str(join_conditions) else '❌ NO — DPP will NOT work'}")

    # Check 3: DPP config
    dpp_enabled = spark.conf.get(
        "spark.sql.optimizer.dynamicPartitionPruning.enabled", "true"
    )
    print(f"3. DPP enabled:              {'✅ YES' if dpp_enabled == 'true' else '❌ NO'}")

    # Check 4: Broadcast threshold
    threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold", "10485760")
    print(f"4. Broadcast threshold:      {int(threshold) // 1024 // 1024} MB")
    small_count = df_small.count()
    print(f"   Small table row count:    {small_count:,}")
    print(f"   Small table broadcastable: "
          f"{'✅ Likely' if small_count < 1000000 else '⚠️ Check size'}")

    print("\n→ Run df_result.explain('formatted') and look for:")
    print("  PartitionFilters: [dynamicpruningexpression(...)]  ← DPP active")
    print("  PartitionFilters: []                               ← DPP NOT active")
```

### Common Root Causes When DPP Doesn't Trigger

```
Root Cause 1: Large table not partitioned
  Fix: Write with .write.partitionBy("date_col").parquet(path)

Root Cause 2: Join on wrong column
  Fix: Ensure join condition uses the partition column of the large table

Root Cause 3: autoBroadcastJoinThreshold too low
  Fix: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")

Root Cause 4: Outer join on large table side
  Fix: Restructure as inner join or left semi join

Root Cause 5: DPP disabled
  Fix: spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

Root Cause 6: Small table has too many distinct values
  Symptom: DPP triggers but no savings (all partitions match)
  Fix: Pre-filter the small table more aggressively before the join
```

---

### Scenario 4 — DPP with Delta Lake (Production Best Practice)

```python
from delta.tables import DeltaTable

# Write the large table as a partitioned Delta table
df_events.write \
    .format("delta") \
    .partitionBy("event_date") \
    .mode("overwrite") \
    .save("s3://data/delta/events/")

# Read it back
df_events_delta = spark.read.format("delta").load("s3://data/delta/events/")

# DPP + Delta: Delta's transaction log makes partition listing fast
# Delta also supports Z-ordering for additional data skipping within partitions
df_result = df_events_delta.join(
    F.broadcast(df_campaigns),
    on=df_events_delta.event_date == df_campaigns.active_date,
    how="inner"
)

# Delta-specific: run OPTIMIZE + ZORDER for additional pruning within partitions
# DeltaTable.forPath(spark, "s3://data/delta/events/") \
#           .optimize().executeZOrderBy("event_date", "user_id")

df_result.explain(mode="formatted")
# Same DPP indicators: dynamicpruningexpression in PartitionFilters
```

**Delta Lake advantage:** Delta's `_delta_log` transaction log allows Spark to identify which partition files exist without listing all S3 directories — significantly faster partition discovery, especially for tables with thousands of partitions.

---

## 11. Quick Reference Cheat Sheet

```
┌──────────────────────────────────────────────────────────────────────────┐
│             DYNAMIC PARTITION PRUNING QUICK REFERENCE                   │
├──────────────────────────────────────────────────────────────────────────┤
│ ENABLE DPP                                                               │
│  spark.sql.optimizer.dynamicPartitionPruning.enabled = true (default)   │
│  spark.sql.adaptive.enabled = true  (improves DPP decisions)            │
│  spark.sql.autoBroadcastJoinThreshold = 10–100MB                        │
├──────────────────────────────────────────────────────────────────────────┤
│ DPP REQUIREMENTS (all must be true)                                      │
│  ✅ Large table partitioned on disk (written with partitionBy())         │
│  ✅ Join/filter condition includes the partition column                  │
│  ✅ Small table is small enough to broadcast                             │
│  ✅ Join type is inner or left semi (not left outer on large side)       │
├──────────────────────────────────────────────────────────────────────────┤
│ CONFIRMING DPP IN EXPLAIN PLAN                                           │
│  Active:    PartitionFilters: [dynamicpruningexpression(...)]  ✅        │
│  Inactive:  PartitionFilters: []                               ❌        │
│  Also look: SubqueryAdaptiveBroadcast dynamicpruning#...                │
├──────────────────────────────────────────────────────────────────────────┤
│ STATIC vs DYNAMIC PRUNING                                                │
│  Static:  filter(col("date") == "2023-06-04")  → literal constant       │
│  Dynamic: join on (large.date == small.release_date) → runtime value    │
├──────────────────────────────────────────────────────────────────────────┤
│ DPP vs PREDICATE PUSHDOWN                                                │
│  DPP:       skips entire partition directories (folder level)           │
│  Pushdown:  skips row groups within Parquet files (file level)          │
│  They are complementary — both can apply to the same query              │
├──────────────────────────────────────────────────────────────────────────┤
│ WHEN DPP DOESN'T WORK                                                   │
│  ❌ Table not partitioned on disk                                        │
│  ❌ Filter/join on non-partition column                                  │
│  ❌ Outer join on the large table side                                   │
│  ❌ Small table too large to broadcast                                   │
│  ❌ DPP config disabled                                                  │
│  ❌ Small table has too many distinct values (near full scan anyway)     │
├──────────────────────────────────────────────────────────────────────────┤
│ FORCE DPP WITH EXPLICIT BROADCAST                                        │
│  df_large.join(F.broadcast(df_small),                                   │
│    on=df_large.partition_col == df_small.filter_col, how="inner")       │
└──────────────────────────────────────────────────────────────────────────┘
```

---

*Last Updated: 2026 | PySpark 3.x | For Data Engineering Interview Prep*
