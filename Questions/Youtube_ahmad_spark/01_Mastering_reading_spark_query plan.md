# 01 — Spark Internals: Query Planning, Transformations, Repartition, Coalesce & Physical Plan Reading
> **Target Audience:** Data Engineers with 3–4 years of experience | Interview + Real-World Prep

---

## Table of Contents
1. [Spark Catalog](#1-spark-catalog)
2. [Query Planning Phases](#2-query-planning-phases)
3. [Catalyst Optimizer — Optimizations Applied](#3-catalyst-optimizer--optimizations-applied)
4. [Narrow vs Wide Transformations](#4-narrow-vs-wide-transformations)
5. [Repartition](#5-repartition)
6. [Coalesce](#6-coalesce)
7. [Repartition vs Coalesce — Full Comparison](#7-repartition-vs-coalesce--full-comparison)
8. [Reading the Physical Plan (explain())](#8-reading-the-physical-plan-explain)
9. [Joins in the Physical Plan](#9-joins-in-the-physical-plan)
10. [GroupBy & Aggregations in the Physical Plan](#10-groupby--aggregations-in-the-physical-plan)
11. [AQE Recap in the Physical Plan](#11-aqe-recap-in-the-physical-plan)
12. [Interview Questions & Answers](#12-interview-questions--answers)
13. [Real-World Project Scenarios](#13-real-world-project-scenarios)
14. [Quick Reference Cheat Sheet](#14-quick-reference-cheat-sheet)

---

## 1. Spark Catalog

### What is the Spark Catalog?
The Spark Catalog is Spark's **metadata registry**. It stores information about databases, tables, views, and functions — everything Spark needs to plan and execute queries.

### What the Catalog Stores

| Metadata | Description |
|---|---|
| Table name | Identifier used in SQL queries |
| Database name | Namespace / schema |
| Schema (columns + types) | Column names, data types, nullability |
| Table location | Physical path on storage (S3, HDFS, local) |
| Table type | Managed vs External |
| Partition info | Which columns are partition keys, what values exist |
| Table statistics | Row count, column statistics (used by CBO) |

### Managed vs External Tables

```
Managed Table:
  CREATE TABLE db.my_table ...
  - Spark controls both metadata AND data files
  - DROP TABLE → deletes both metadata AND data files ⚠️

External Table:
  CREATE TABLE db.my_table LOCATION 's3://bucket/path/'
  - Spark controls only metadata
  - DROP TABLE → deletes only metadata, data files remain ✅
  - Preferred in production (data survives table drops)
```

### Working with the Catalog in Code

```python
# List all databases
spark.catalog.listDatabases()

# List all tables in current database
spark.catalog.listTables()

# List columns of a table
spark.catalog.listColumns("my_table")

# Check if a table exists
spark.catalog.tableExists("db_name.my_table")

# Get table metadata
spark.catalog.getTable("my_table")

# Refresh table metadata (after external changes to files)
spark.catalog.refreshTable("my_table")

# Cache a table in memory under catalog
spark.catalog.cacheTable("my_table")
spark.catalog.uncacheTable("my_table")

# Set current database
spark.catalog.setCurrentDatabase("analytics_db")
```

### Why Catalog Matters for Query Planning
When you run `df = spark.table("my_table")`, Spark:
1. Looks up the table in the catalog → finds location, schema, partition info
2. Uses partition info for **partition pruning**
3. Uses row/column statistics for **Cost-Based Optimization (CBO)**

Without catalog statistics, the optimizer must make blind guesses about table sizes → suboptimal join strategies, poor partition choices.

---

## 2. Query Planning Phases

When you run any Spark query, it passes through **four phases** before execution:

```
Your Code:
  df.filter(...).select(...).join(...).groupBy(...)

         ↓
┌─────────────────────────────┐
│  1. Unresolved Logical Plan │  ← Your code as an abstract tree
│     (Parsed)                │    Column names not yet validated
└─────────────────────────────┘
         ↓ Catalog lookup (validate column names, types)
┌─────────────────────────────┐
│  2. Resolved Logical Plan   │  ← Columns validated against schema
│     (Analyzed)              │    Types checked, functions resolved
└─────────────────────────────┘
         ↓ Catalyst Optimizer (rule-based + cost-based)
┌─────────────────────────────┐
│  3. Optimized Logical Plan  │  ← Filter pushdown, projection pushdown
│                             │    Predicate simplification, etc.
└─────────────────────────────┘
         ↓ Physical Planner (generates multiple candidates)
┌─────────────────────────────┐
│  4. Physical Plan           │  ← Concrete execution strategy
│     (Selected by CBO)       │    SortMergeJoin vs BroadcastHashJoin
└─────────────────────────────┘
         ↓ Code Generation (Tungsten)
┌─────────────────────────────┐
│  5. Execution               │  ← JVM bytecode runs on cluster
└─────────────────────────────┘
```

### Phase 1 — Unresolved (Parsed) Logical Plan
Your transformations are converted into a **tree of unresolved nodes**. Column names like `"age"` exist as strings — Spark hasn't checked if `age` actually exists in the DataFrame yet.

### Phase 2 — Resolved (Analyzed) Logical Plan
The **Catalog** is consulted to:
- Validate all column names exist
- Resolve data types
- Resolve function signatures
- Check table existence for SQL queries

If a column doesn't exist, the `AnalysisException` is thrown here (not during execution).

### Phase 3 — Optimized Logical Plan
The **Catalyst Optimizer** applies a set of rule-based and cost-based transformations. See Section 3 for details.

### Phase 4 — Physical Plan
The optimizer generates **multiple candidate physical plans** for the optimized logical plan, evaluates their estimated cost using the **Cost-Based Optimizer (CBO)**, and selects the cheapest one. This plan contains concrete operators like `SortMergeJoin`, `BroadcastHashJoin`, `HashAggregate`, `FileScan`.

---

## 3. Catalyst Optimizer — Optimizations Applied

The Catalyst Optimizer transforms the logical plan using rules. Key optimizations:

### Filter Pushdown
Moves filter conditions as close to the data source as possible.

```
Your code:          df.join(other, "id").filter(col("age") > 25)

Without pushdown:   Read ALL data → Join → Filter (age > 25)
                    ↑ Joins millions of rows that get filtered anyway

With pushdown:      Filter(age > 25) on df first → Read fewer rows → Join
                    ↑ Joins only relevant rows
```

```python
# Catalyst pushes this filter before the join automatically
df_result = (
    df_customers
    .join(df_orders, "customer_id")
    .filter(F.col("age") > 25)     # Catalyst moves this BEFORE the join
)
df_result.explain(True)
# In Optimized Plan you'll see Filter applied to df_customers before Exchange
```

### Projection Pushdown (Column Pruning)
Even if you write `SELECT *`, Spark reads only the columns actually needed downstream.

```
Your code:    df.select("*").filter(col("city") == "Boston").select("name", "age")

Without:      Read ALL columns → filter → select 2 columns
With:         Read ONLY name, age, city columns → filter → select name, age
```

This is especially powerful with Parquet (columnar format) — only the needed column chunks are read from disk.

### Other Catalyst Optimizations

| Optimization | What it Does |
|---|---|
| Constant Folding | `1 + 1` → evaluated at plan time, not runtime |
| Predicate Simplification | `true AND col > 5` → `col > 5` |
| Null Propagation | Simplifies expressions involving NULL |
| Boolean Expression Simplification | `NOT (a AND b)` → `NOT a OR NOT b` |
| Subquery Elimination | Removes redundant subqueries |
| Join Reordering (CBO) | Reorders joins to minimize intermediate data size |

---

## 4. Narrow vs Wide Transformations

This distinction determines **stage boundaries** and whether **shuffle** occurs.

### Narrow Transformations

```
Partition 0 → [filter/map] → Output Partition 0
Partition 1 → [filter/map] → Output Partition 1
Partition 2 → [filter/map] → Output Partition 2

No data crosses partition boundaries
No network transfer
Fast — runs within a single stage
```

| Operation | Type | Notes |
|---|---|---|
| `filter()` | Narrow | Row removed or kept within partition |
| `map()` / `withColumn()` | Narrow | Row transformed within partition |
| `select()` | Narrow | Column selection within partition |
| `union()` | Narrow | Partitions concatenated, no shuffle |
| `coalesce()` | Narrow | Merges partitions locally (no shuffle) |
| `sample()` | Narrow | Sampling within partition |

### Wide Transformations

```
Partition 0 ─────┐
Partition 1 ──── SHUFFLE ──── Executor 0: Partition A
Partition 2 ─────┘           Executor 1: Partition B
                             Executor 2: Partition C

Data moves across ALL executors
Network-intensive
Causes a new STAGE boundary
```

| Operation | Type | Notes |
|---|---|---|
| `groupBy()` | Wide | Rows with same key must meet on same executor |
| `join()` | Wide | (Sort-Merge Join) matching keys must co-locate |
| `repartition()` | Wide | Always shuffles |
| `distinct()` | Wide | Must compare globally |
| `orderBy()` | Wide | Global sort requires full shuffle |
| `reduceByKey()` | Wide | (RDD) Groups by key |

### Stage Boundaries
Every wide transformation creates a new stage. Spark jobs consist of:

```
Stage 1: filter → withColumn → select  (narrow — no shuffle)
              ↓ SHUFFLE (Exchange)
Stage 2: groupBy → aggregate            (wide — after shuffle)
              ↓ SHUFFLE (Exchange)
Stage 3: join → select → write          (wide — after join shuffle)
```

---

## 5. Repartition

### What Repartition Does
`repartition(n)` or `repartition(n, "column")` redistributes data across exactly `n` partitions.

```python
# Check current partition count
print(df.rdd.getNumPartitions())  # e.g., 12

# Repartition to 24 partitions
df_repart = df.repartition(24)
print(df_repart.rdd.getNumPartitions())  # 24

# Repartition by column (hash partitioning)
df_repart_col = df.repartition(24, "customer_id")
```

### Physical Plan for Repartition

```python
df.repartition(24).explain(True)
```

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Exchange RoundRobinPartitioning(24), REPARTITION_BY_NUM, [id=#12]
   +- Scan ExistingRDD[value#0]
```

Key terms in the plan:
- `Exchange` → **shuffle is happening**
- `RoundRobinPartitioning(24)` → data distributed evenly across 24 partitions in round-robin order (no column specified)
- `HashPartitioning(col, 24)` → when column is specified, hash of column determines partition

### Key Properties of Repartition

| Property | Behavior |
|---|---|
| Shuffle | ALWAYS — even if n < current partitions |
| Can increase partitions | ✅ Yes |
| Can decrease partitions | ✅ Yes |
| Default strategy | `RoundRobinPartitioning` (no column) |
| With column | `HashPartitioning` (same key → same partition) |
| Shown in physical plan | ✅ Yes — Exchange operator is explicit |

### When to Use Repartition

```python
# Use case 1: Increase parallelism before expensive operations
df_large.repartition(200).groupBy("city").agg(F.sum("revenue"))

# Use case 2: Partition by a join key before a repeated join
df.repartition(200, "customer_id")  # All customer_id rows on same partition

# Use case 3: Partition before writing (control output file count)
df.repartition(1).write.csv("output/")   # Write as single file
df.repartition(50).write.parquet("output/")  # 50 output files
```

---

## 6. Coalesce

### What Coalesce Does
`coalesce(n)` reduces the number of partitions by **merging partitions on the same executor** — without shuffling data across the network.

```python
df_coal = df.coalesce(10)
print(df_coal.rdd.getNumPartitions())  # 10
```

### Physical Plan for Coalesce

```python
df.coalesce(10).explain(True)
```

```
== Physical Plan ==
Coalesce 10
+- *(1) Scan ExistingRDD[value#0]
```

Notice: **No `Exchange` operator** — no shuffle. Spark just merges nearby partitions on the same executor.

### Why No Partitioning Scheme is Shown
Coalesce preserves the **existing data layout**. Since no new partitioning strategy is introduced (unlike repartition's RoundRobin or Hash), Spark has nothing new to display. The data simply moves fewer partitions — on the same executor — without network transfer.

```
Before coalesce(2) with 4 partitions on 2 executors:
  Executor 0: [Partition 0: 1000 rows] [Partition 1: 1000 rows]
  Executor 1: [Partition 2: 1000 rows] [Partition 3: 1000 rows]

After coalesce(2):
  Executor 0: [Partition 0: 2000 rows]  ← Partitions 0+1 merged locally
  Executor 1: [Partition 1: 2000 rows]  ← Partitions 2+3 merged locally
  
No network transfer occurred!
```

### Key Properties of Coalesce

| Property | Behavior |
|---|---|
| Shuffle | Avoids shuffle (merges locally) |
| Can increase partitions | ❌ No — only decreases |
| Can decrease partitions | ✅ Yes |
| Partitioning scheme shown | ❌ No — preserves existing layout |
| Risk | Uneven partition sizes if input was uneven |

### When to Use Coalesce

```python
# Use case 1: Reduce files after a large operation (before write)
df_result.coalesce(5).write.parquet("s3://output/")

# Use case 2: Reduce partitions after heavy filtering (many partitions become small)
df_filtered = df.filter(F.col("city") == "Boston")  # Many empty partitions
df_filtered.coalesce(10).write.parquet("output/")   # Merge into fewer

# Use case 3: Single file output (use with caution on large data)
df_small.coalesce(1).write.csv("output/report.csv")
```

---

## 7. Repartition vs Coalesce — Full Comparison

| Aspect | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Always | Avoids (local merge) |
| Direction | Increase OR decrease | Decrease only |
| Output partition sizes | Roughly equal (balanced) | May be unequal |
| Partitioning scheme | RoundRobin or Hash | Preserves existing |
| Physical plan shows | `Exchange` | `Coalesce` (no Exchange) |
| Performance cost | High (network shuffle) | Low (local merge) |
| Best for | Increasing parallelism, re-keying | Reducing file count before write |
| Data distribution | Uniform | Depends on input |

### Decision Rule

```
Do you need MORE partitions or re-key the data?
  └── YES → repartition()

Do you need FEWER partitions and don't care about perfect balance?
  └── YES → coalesce()  (cheaper)
  └── BUT: if current partitions are heavily skewed → repartition() for balance
```

---

## 8. Reading the Physical Plan (explain())

### How to Call explain()

```python
# Basic (physical plan only — most common)
df.explain()

# All plans (parsed → analyzed → optimized → physical)
df.explain(True)

# Formatted (Spark 3+ — more readable, recommended)
df.explain(mode="formatted")

# Just the cost details
df.explain(mode="cost")

# Just the codegen details
df.explain(mode="codegen")
```

### Anatomy of a Physical Plan

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false          ← AQE wrapper (Spark 3+)
+- *(3) HashAggregate(keys=[city#10], ...)   ← Final aggregation
   +- Exchange hashpartitioning(city#10, 200)← SHUFFLE (wide transformation)
      +- *(2) HashAggregate(keys=[city#10]...)← Partial aggregation (pre-shuffle)
         +- *(1) Filter (age#5 > 25)         ← Filter pushed down
            +- *(1) FileScan parquet          ← Data source scan
                 PartitionFilters: [...]      ← Partition pruning applied here
                 DataFilters: [age > 25]      ← Row-level filter
                 PushedFilters: [IsNotNull]   ← Parquet predicate pushdown
```

### Key Physical Plan Operators

| Operator | Meaning | Shuffle? |
|---|---|---|
| `FileScan` | Reading files from disk | No |
| `Filter` | Row-level filter applied | No |
| `Project` | Column selection | No |
| `Exchange` | Data shuffle across executors | **YES** |
| `Exchange hashpartitioning(col, N)` | Hash shuffle on column, N partitions | **YES** |
| `Exchange RoundRobinPartitioning(N)` | Round-robin shuffle to N partitions | **YES** |
| `BroadcastExchange` | Small table broadcast (no large shuffle) | Small table only |
| `SortMergeJoin` | Join via shuffle + sort + merge | **YES** |
| `BroadcastHashJoin` | Join via broadcast (no shuffle of large table) | No (large side) |
| `HashAggregate` | Hash-based aggregation | No (local) |
| `Partial HashAggregate` | Pre-shuffle local aggregation | No |
| `Sort` | Sorting within partition | No |
| `Coalesce` | Reduce partition count | No |
| `AdaptiveSparkPlan` | AQE wrapper — may re-optimize at runtime | Varies |
| `AQEShuffleRead` | AQE re-read of shuffle output (coalesced/split) | No |

### Stage Numbers in the Plan
Numbers like `*(1)`, `*(2)`, `*(3)` in the plan indicate **stage boundaries**:
- Same number = same stage (pipelined, no shuffle between them)
- Different number = new stage (shuffle happened between them)

```
*(1) FileScan → *(1) Filter → [Exchange] → *(2) HashAggregate
       Stage 1 ─────────────────────────────  Stage 2
```

---

## 9. Joins in the Physical Plan

### Disabling Auto-Broadcast (for Testing/Analysis)

```python
# Force Sort-Merge Join (disable auto-broadcast) to inspect SMJ plan
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df_joined = df_customers.join(df_orders, "customer_id", "inner")
df_joined.explain(True)
```

### Sort-Merge Join Plan

```
== Physical Plan ==
*(3) SortMergeJoin [customer_id#10], [customer_id#25], Inner
:- *(1) Sort [customer_id#10 ASC]
:  +- Exchange hashpartitioning(customer_id#10, 200)  ← Shuffle left table
:     +- *(1) Scan [customer_id, name, age]
+- *(2) Sort [customer_id#25 ASC]
   +- Exchange hashpartitioning(customer_id#25, 200)  ← Shuffle right table
      +- *(2) Scan [customer_id, order_id, amount]
```

Reading this:
1. Both tables are **shuffled** using `hashpartitioning(customer_id, 200)` — same key goes to same partition
2. Both sides are **sorted** by `customer_id` within each partition
3. `SortMergeJoin` merges the two sorted streams

### Broadcast Hash Join Plan

```
== Physical Plan ==
*(2) BroadcastHashJoin [customer_id#10], [customer_id#25], Inner, BuildRight
:- *(2) Scan parquet [customer_id, name, age]       ← Large table (not shuffled)
+- BroadcastExchange HashedRelationBroadcastMode     ← Small table broadcast
   +- *(1) Scan parquet [customer_id, order_id]
```

Reading this:
- `BroadcastExchange` → small table is being broadcast to all executors
- `BuildRight` → the right side is the broadcast table
- Large table is scanned without shuffle — reads its local partitions

### HashPartitioning Explained

```
Exchange hashpartitioning(cust_id#10, 200)

Meaning:
  - Data is being shuffled
  - Partitioning key: cust_id
  - Formula: hash(cust_id) % 200
  - Result: rows with same cust_id → same partition number
  - 200 output partitions created

Why needed for joins:
  - For SortMergeJoin to work, matching keys MUST be on the same executor
  - Hash partitioning guarantees this
```

---

## 10. GroupBy & Aggregations in the Physical Plan

### Two-Phase Aggregation (Partial + Final)

GroupBy with aggregation uses a **two-phase approach** to minimize shuffle data:

```python
df.groupBy("city").agg(F.sum("revenue"), F.count("*")).explain(True)
```

```
== Physical Plan ==
*(3) HashAggregate(keys=[city#10], functions=[sum(revenue#15), count(1)])
+- Exchange hashpartitioning(city#10, 200)    ← Shuffle reduced data
   +- *(2) HashAggregate(keys=[city#10],      ← PARTIAL: local aggregation
           functions=[partial_sum(revenue#15), partial_count(1)])
      +- *(1) FileScan parquet [city, revenue]
```

**What's happening:**

```
Stage 1 (no shuffle):
  Partition 0: city=Boston(rev=100), city=Boston(rev=200) → partial_sum=300, partial_count=2
  Partition 1: city=Boston(rev=150) → partial_sum=150, partial_count=1
  Partition 2: city=NYC(rev=500)    → partial_sum=500, partial_count=1

Exchange (shuffle): all city=Boston rows → same executor

Stage 2 (after shuffle):
  Executor 0 receives: (Boston, 300, 2), (Boston, 150, 1)
  Final HashAggregate: Boston → sum=450, count=3
```

**Why two phases?**
Without partial aggregation: shuffle ALL raw rows (huge data transfer).
With partial aggregation: shuffle only the aggregated summaries per partition (much smaller).

### count(distinct) Plan

```python
df.groupBy("city").agg(F.countDistinct("user_id")).explain(True)
```

```
== Physical Plan ==
*(4) HashAggregate(keys=[city#10], functions=[count(distinct user_id#12)])
+- Exchange hashpartitioning(city#10, 200)
   +- *(3) HashAggregate(keys=[city#10], functions=[partial_count(distinct user_id#12)])
      +- *(2) HashAggregate(keys=[city#10, user_id#12], functions=[])
         +- Exchange hashpartitioning(city#10, user_id#12, 200)  ← EXTRA shuffle!
            +- *(1) FileScan parquet [city, user_id]
```

**Why extra shuffle for distinct?**
- Regular `count(*)`: partial aggregate locally → one shuffle → final aggregate
- `count(distinct)`: must deduplicate globally → needs extra shuffle to gather all user_ids per city before counting

This is why `countDistinct` is expensive. For approximate results, use:
```python
F.approx_count_distinct("user_id", rsd=0.05)  # 5% relative standard deviation — much cheaper
```

---

## 11. AQE Recap in the Physical Plan

When AQE is enabled, the physical plan is wrapped in `AdaptiveSparkPlan`:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- BroadcastHashJoin [...]   ← AQE may have converted this from SortMergeJoin
   :- AQEShuffleRead         ← AQE re-read shuffle output (coalesced partitions)
   +- BroadcastExchange
```

### Key AQE Indicators in Plans

| Term | Meaning |
|---|---|
| `AdaptiveSparkPlan isFinalPlan=false` | AQE is active, plan may still change |
| `AdaptiveSparkPlan isFinalPlan=true` | AQE finalized the plan (post-execution) |
| `AQEShuffleRead` | AQE coalesced or split shuffle partitions |
| `BroadcastHashJoin` where SMJ expected | AQE converted join strategy at runtime |
| `SubqueryAdaptiveBroadcast` | DPP subquery being broadcast (see Topic 12) |

```python
# Enable AQE for all optimizations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

## 12. Interview Questions & Answers

---

### Q1. How does Spark execute a query internally? Walk me through the phases.

**Answer:**
When you run a Spark query, it passes through four phases before execution.

First, Spark analyses your code into an **Unresolved Logical Plan** — a tree of operations where column names exist as strings but haven't been validated yet.

Second, Spark consults the **Catalog** to validate column names, resolve data types, and check table existence, producing a **Resolved (Analyzed) Logical Plan**. This is where `AnalysisException` is thrown for invalid columns.

Third, the **Catalyst Optimizer** applies rule-based transformations — filter pushdown, projection pushdown, constant folding, predicate simplification — producing an **Optimized Logical Plan**.

Finally, Spark generates multiple candidate **Physical Plans**, evaluates them using the Cost-Based Optimizer (CBO) with table statistics, and selects the most efficient one. This physical plan runs on the cluster with operators like `SortMergeJoin`, `BroadcastHashJoin`, and `HashAggregate`.

During this stage, it also uses Whole-Stage Code Generation to generate Java code, convert it into bytecode, and run it faster on the cluster.

So overall, the flow is: parse, analyze, optimize, and execute.
---

### Q2. What is Filter Pushdown and why is it important?

**Answer:**
Filter pushdown is a Catalyst optimization that moves filter conditions as close to the data source as possible — ideally into the file reader itself.

Without pushdown: Spark reads all data, joins, then filters. Rows that will be discarded are still read, shuffled, and joined — wasting CPU, memory, and network bandwidth.

With pushdown: Spark applies filters before (or during) the scan. Fewer rows enter the Spark engine, reducing I/O, shuffle size, and execution time.

It operates at two levels: at the partition level (skipping entire partition directories) and at the row-group level within Parquet files (predicate pushdown into the file reader, visible as `PushedFilters` in the physical plan).

---

### Q3. What is the difference between narrow and wide transformations?

**Answer:**
Narrow transformations operate within a single partition — no data crosses partition boundaries. Examples include `filter`, `map`, `withColumn`, `select`, and `coalesce`. They are fast because there's no network transfer and they execute within a single stage.

Wide transformations require data from multiple partitions to be combined, triggering a **shuffle** — data is redistributed across executors over the network. Examples include `groupBy`, `join` (Sort-Merge), `repartition`, `distinct`, and `orderBy`. Each wide transformation creates a new stage boundary in the DAG.

The practical importance: wide transformations are the primary source of performance bottlenecks. Minimizing shuffles (via broadcast joins, caching, partition design) is a core optimization strategy.

---

### Q4. What is the difference between repartition() and coalesce()?

**Answer:**
`repartition(n)` always causes a shuffle. It can increase or decrease the number of partitions and produces roughly balanced partition sizes using `RoundRobinPartitioning` (without column) or `HashPartitioning` (with column). The shuffle is visible as an `Exchange` operator in the physical plan.

`coalesce(n)` only decreases partitions and avoids shuffle by merging partitions on the same executor locally. It's cheaper but may produce uneven partition sizes since it doesn't redistribute data globally. No `Exchange` appears in its physical plan.

**Rule of thumb:** Use `coalesce` when reducing partition count after a write or heavy filter (cheaper). Use `repartition` when you need balanced partitions, more partitions than you currently have, or partitioning by a specific column for a downstream join.

---

### Q5. Why doesn't coalesce() show a partitioning scheme in the physical plan?

**Answer:**
Because coalesce doesn't introduce a new partitioning strategy. It simply merges existing partitions on the same executor without shuffling data across the network. Since the original data layout is preserved (rows don't move between executors), there's no new partitioning scheme to display.

Repartition, by contrast, creates an entirely new data layout — `RoundRobinPartitioning` or `HashPartitioning` — which is explicitly shown in the physical plan as an `Exchange` operator. Coalesce just says "merge N partitions into fewer" — no new routing rules, so nothing new to show.

---

### Q6. How do you identify shuffle in a Spark physical plan?

**Answer:**
Look for the `Exchange` operator in the physical plan. Every `Exchange` represents a shuffle — data being redistributed across executors over the network.

Two common forms:
- `Exchange hashpartitioning(col, N)` — data is shuffled using hash of the specified column into N partitions. Appears in joins and groupBy operations.
- `Exchange RoundRobinPartitioning(N)` — data is distributed evenly in round-robin fashion. Appears when `repartition(n)` is called without a column.

Additionally, `BroadcastExchange` indicates a broadcast — the small table is sent to all executors, but the large table is NOT shuffled.

Stage boundaries in the plan (marked by `*(1)`, `*(2)`, `*(3)`) also indicate where shuffles occur: each new stage number means an `Exchange` separated the stages.

---

### Q7. What is HashPartitioning and why is it necessary for joins?

**Answer:**
HashPartitioning is Spark's mechanism for ensuring that rows with the same join key end up on the same executor. The formula is `partition = hash(join_key) % num_partitions`.

For Sort-Merge Join to work correctly, rows with `customer_id = "C001"` from both Table A and Table B must be on the same executor — otherwise, they can't be compared and joined. HashPartitioning guarantees this: both tables are shuffled using the same hash function on the join key, so matching keys co-locate.

Without hash partitioning, a join would require every executor to communicate with every other executor — an n² communication problem. Hash partitioning reduces this to O(n) by creating a deterministic assignment.

---

### Q8. What is Partial Aggregation in GroupBy and why is it a performance optimization?

**Answer:**
Partial aggregation is Spark's two-phase approach to grouped aggregations. Before shuffling data, Spark performs a **local (partial) aggregation** within each partition. Only the partial results are then shuffled, and a final aggregation merges them.

Without partial aggregation: all raw rows are shuffled to group by key. If 1 million rows have `city = Boston` across 200 partitions, all 1 million rows travel the network.

With partial aggregation: each partition computes a local partial sum/count. 200 partial results (one per partition) are shuffled instead of 1 million rows. The final aggregation merges 200 small records.

In the physical plan, partial aggregation appears as `HashAggregate(functions=[partial_sum(...)])` before the `Exchange`, and the final merge appears as `HashAggregate(functions=[sum(...)])` after.

---

### Q9. Why is count(distinct) expensive and how can you optimize it?

**Answer:**
`countDistinct` is expensive because it requires **global deduplication** — Spark cannot determine uniqueness within a single partition. To count distinct `user_id` per city, Spark must:

1. Shuffle data by both `(city, user_id)` to deduplicate globally (extra shuffle stage)
2. Shuffle again by `city` to aggregate the deduplicated data (second shuffle)

This results in two shuffle stages instead of one, with much larger intermediate data than a regular `count(*)`.

Optimization options:
- `approx_count_distinct(col, rsd=0.05)` — uses HyperLogLog algorithm, single shuffle, ~5% error
- Pre-deduplicate at the source before aggregating
- For exact counts with large cardinality, consider pre-computing in the upstream pipeline

---

### Q10. How do you analyze a slow Spark job end-to-end?

**Answer:**
I follow a structured approach:

1. **Check the physical plan** with `df.explain("formatted")` — identify all `Exchange` operators (shuffles), join strategies (`SortMergeJoin` vs `BroadcastHashJoin`), and whether partial aggregation is happening.

2. **Spark UI — Jobs/Stages tab** — find the slow stage. Check task duration distribution. If one task is much slower (straggler), it indicates data skew.

3. **Spark UI — Stage details** — check `Input Size` (I/O), `Shuffle Read/Write Size` (network), `GC Time` (memory pressure), and `Peak Execution Memory`.

4. **Identify the bottleneck type:**
   - High shuffle read/write → reduce shuffles (broadcast join, pre-aggregation)
   - Skewed tasks → salting, AQE skewJoin, broadcast
   - High GC time → memory tuning, serialization (Kryo), off-heap
   - Full table scan → add partition pruning, DPP, predicate pushdown

5. **Apply optimizations:** Enable AQE, tune `shuffle.partitions`, use broadcast hints, cache reused DataFrames, fix partition design.

---

## 13. Real-World Project Scenarios

---

### Scenario 1 — Pipeline Review: Spotting Inefficiencies from the Physical Plan

**Problem:**
A colleague's pipeline runs for 2 hours. You're asked to review and optimize it.

```python
# Colleague's code (looks innocent)
df_result = (
    spark.table("events")                          # 500 GB table, unpartitioned
    .join(spark.table("users"), "user_id")        # 10 MB users table
    .filter(F.col("event_date") >= "2024-01-01")  # Filter AFTER join
    .groupBy("country")
    .agg(F.countDistinct("user_id").alias("unique_users"))
)
df_result.explain("formatted")
```

**Analysis from the Physical Plan:**

```
Issues found in plan:
1. SortMergeJoin → users table is 10 MB — should be BroadcastHashJoin
   Fix: F.broadcast(spark.table("users")) or increase autoBroadcastJoinThreshold

2. Filter (event_date >= 2024-01-01) appears AFTER Exchange → not pushed before join
   Fix: Pre-filter events before joining

3. count(distinct user_id) → two shuffle stages
   Fix: Use approx_count_distinct if exact count not required

4. events table not partitioned → full 500 GB scan
   Fix: Repartition events table by event_date on write for future runs
```

**Optimized Code:**
```python
# Pre-filter before join (helps Catalyst even more explicitly)
df_recent_events = spark.table("events").filter(F.col("event_date") >= "2024-01-01")

df_result_optimized = (
    df_recent_events
    .join(F.broadcast(spark.table("users")), "user_id")  # Broadcast small table
    .groupBy("country")
    .agg(F.approx_count_distinct("user_id", rsd=0.02).alias("approx_unique_users"))
    # Use exact countDistinct only if business requires it
)
# Expected improvement: 2 hours → ~10 minutes
```

---

### Scenario 2 — Choosing Between repartition() and coalesce() at Write Time

**Problem:**
Your ETL pipeline produces a DataFrame with 512 shuffle partitions (default × AQE adjustments). You need to write it to S3 as Parquet. Downstream consumers process files in parallel — they prefer 20 files of ~100 MB each.

```python
df_result = (
    df_raw
    .filter(F.col("region") == "APAC")  # Heavy filter — many empty/tiny partitions
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

# Check actual partition count and sizes
print(f"Partitions before write: {df_result.rdd.getNumPartitions()}")

# Option A: coalesce (cheaper — avoids shuffle, but may be uneven)
df_result.coalesce(20).write.mode("overwrite").parquet("s3://output/apac_revenue/")

# Option B: repartition (shuffle cost, but perfectly balanced 20 files)
df_result.repartition(20).write.mode("overwrite").parquet("s3://output/apac_revenue/")

# Decision: If downstream readers are sensitive to file size uniformity → repartition
#           If cost matters and slight imbalance is OK → coalesce
```

---

### Scenario 3 — Debugging a Multi-Stage Pipeline with explain()

```python
def analyze_plan(df, label=""):
    """Print a structured analysis of the physical plan."""
    plan = df._jdf.queryExecution().explainString(True)

    print(f"\n{'='*60}")
    print(f"PLAN ANALYSIS: {label}")
    print('='*60)

    checks = {
        "Exchange (Shuffles)": plan.count("Exchange"),
        "BroadcastHashJoin":   "BroadcastHashJoin" in plan,
        "SortMergeJoin":       "SortMergeJoin" in plan,
        "Partial Aggregation": "partial_" in plan,
        "DPP Active":          "dynamicpruningexpression" in plan,
        "AQE Enabled":         "AdaptiveSparkPlan" in plan,
        "Coalesce":            "Coalesce" in plan,
    }

    for check, result in checks.items():
        if isinstance(result, bool):
            status = "✅" if result else "❌"
            print(f"  {status} {check}")
        else:
            print(f"  📊 {check}: {result}")

# Usage
df_joined = df_events.join(F.broadcast(df_users), "user_id")
df_agg = df_joined.groupBy("country").agg(F.sum("revenue"))
analyze_plan(df_agg, "Revenue by Country")
```

---

## 14. Quick Reference Cheat Sheet

```
┌──────────────────────────────────────────────────────────────────────────┐
│          SPARK QUERY PLANNING & PHYSICAL PLAN QUICK REFERENCE           │
├──────────────────────────────────────────────────────────────────────────┤
│ QUERY PLANNING PHASES                                                    │
│  1. Unresolved Logical Plan  → parsed code (columns as strings)         │
│  2. Resolved Logical Plan    → catalog lookup, types validated          │
│  3. Optimized Logical Plan   → Catalyst: filter/projection pushdown     │
│  4. Physical Plan            → CBO selects best execution strategy      │
├──────────────────────────────────────────────────────────────────────────┤
│ CATALYST OPTIMIZATIONS                                                   │
│  Filter Pushdown    → filters move closer to data source                │
│  Projection Pushdown→ only required columns read (Parquet columnar)     │
│  Constant Folding   → 1+1 computed at plan time                        │
│  Join Reordering    → CBO reorders joins to minimize intermediate data  │
├──────────────────────────────────────────────────────────────────────────┤
│ NARROW vs WIDE TRANSFORMATIONS                                           │
│  Narrow (no shuffle): filter, withColumn, select, union, coalesce       │
│  Wide   (shuffle):    groupBy, join (SMJ), repartition, distinct, orderBy│
│  Wide → new stage boundary in DAG                                       │
├──────────────────────────────────────────────────────────────────────────┤
│ REPARTITION vs COALESCE                                                  │
│  repartition(n)     → always shuffles, increase OR decrease, balanced  │
│  repartition(n,col) → hash shuffle on col, balanced, good for joins    │
│  coalesce(n)        → no shuffle, decrease only, may be uneven         │
│  Plan shows:        repartition→ Exchange | coalesce → Coalesce        │
├──────────────────────────────────────────────────────────────────────────┤
│ PHYSICAL PLAN — KEY OPERATORS                                            │
│  Exchange hashpartitioning(col, N)  → shuffle by hash of col           │
│  Exchange RoundRobinPartitioning(N) → even shuffle (repartition)       │
│  BroadcastExchange                  → small table broadcast            │
│  SortMergeJoin                      → shuffle + sort + merge           │
│  BroadcastHashJoin                  → no shuffle on large side         │
│  HashAggregate(partial_*)           → pre-shuffle local aggregation    │
│  HashAggregate(final)               → post-shuffle merge               │
│  dynamicpruningexpression           → DPP is active                   │
│  AdaptiveSparkPlan                  → AQE wrapper                      │
│  AQEShuffleRead                     → AQE coalesced/split partitions   │
├──────────────────────────────────────────────────────────────────────────┤
│ EXPLAIN MODES                                                            │
│  df.explain()                → physical plan only                      │
│  df.explain(True)            → all plans (parsed → physical)           │
│  df.explain("formatted")     → readable format (Spark 3+ recommended)  │
│  df.explain("cost")          → cost estimates                          │
├──────────────────────────────────────────────────────────────────────────┤
│ STAGE NUMBERS                                                            │
│  *(1) *(1) → same stage (no shuffle between them)                      │
│  *(1) Exchange *(2) → stage boundary — shuffle occurred               │
├──────────────────────────────────────────────────────────────────────────┤
│ AGGREGATION COST                                                         │
│  count(*)          → cheapest, single shuffle, partial agg             │
│  sum/avg/min/max   → single shuffle, partial agg                       │
│  countDistinct     → 2 shuffles, expensive → use approx_count_distinct │
└──────────────────────────────────────────────────────────────────────────┘
```

---

*Last Updated: 2026 | PySpark 3.x | For Data Engineering Interview Prep*
