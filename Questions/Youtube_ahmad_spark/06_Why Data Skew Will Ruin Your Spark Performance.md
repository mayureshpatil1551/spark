# Apache Spark – Data Skew
> **Level:** 3.5 Years Data Engineering | **Stack:** Azure Databricks, PySpark, Delta Lake

---

## Table of Contents
1. [What Is Data Skew?](#1-what-is-data-skew)
2. [How to Identify Data Skew](#2-how-to-identify-data-skew)
3. [When Does Data Skew Happen?](#3-when-does-data-skew-happen)
4. [Operations That Cause Data Skew](#4-operations-that-cause-data-skew)
5. [Why Data Skew Is Bad](#5-why-data-skew-is-bad)
6. [Fixing Data Skew – All Techniques](#6-fixing-data-skew--all-techniques)
7. [Code Examples – Simulate, Detect & Fix](#7-code-examples--simulate-detect--fix)
8. [Interview Q&A – Tiered](#8-interview-qa--tiered)
9. [Real-World Scenarios & Solutions](#9-real-world-scenarios--solutions)

---

## 1. What Is Data Skew?

Data skew happens when data is **unevenly distributed across partitions** — some partitions hold far more data than others.

```
Skewed Partitions (BAD):
┌─────────────────────────────────────────────────────────┐
│  P1: ████░░░░░░░░░░░░░░░░░░  200 MB                     │
│  P2: █░░░░░░░░░░░░░░░░░░░░░   50 MB                     │
│  P3: ████████████████████░░  800 MB  ← Skewed Partition │
│  P4: ██░░░░░░░░░░░░░░░░░░░░  100 MB                     │
│  P5: ██░░░░░░░░░░░░░░░░░░░░  100 MB                     │
└─────────────────────────────────────────────────────────┘
  P1 completes in 2s, P2 in 0.5s, P4 & P5 in 1s
  P3 takes 30s → entire stage waits for P3 → job stuck

Uniform Partitions (GOOD):
┌─────────────────────────────────────────────────────────┐
│  P1: ████████░░░░░░░░░░░░░░  250 MB                     │
│  P2: ████████░░░░░░░░░░░░░░  250 MB                     │
│  P3: ████████░░░░░░░░░░░░░░  250 MB                     │
│  P4: ████████░░░░░░░░░░░░░░  250 MB                     │
└─────────────────────────────────────────────────────────┘
  All complete in ~2.5s → stage done in 2.5s
```

> **Core Insight:** A Spark stage only completes when **all tasks in it complete**. One skewed task holds up the entire stage and all resources assigned to that stage.

---

## 2. How to Identify Data Skew

### Method 1 – Spark UI (Most Reliable)

**Step 1: Event Timeline**
- Go to: Spark UI → Jobs → Stages → Event Timeline
- Look for one task bar that is dramatically longer than all others
- Classic sign: job is stuck at **199/200 tasks** for a very long time

**Step 2: Summary Metrics Table**
- Go to: Spark UI → Stages → Summary Metrics
- Look at the **Duration** and **Input Size** columns

```
Summary Metrics (Skewed):
┌─────────────┬──────────┬──────────┬──────────┬──────────┐
│ Metric      │   Min    │  25th %  │  Median  │   Max    │
├─────────────┼──────────┼──────────┼──────────┼──────────┤
│ Duration    │  0.5 s   │  1.2 s   │  1.4 s   │  87.3 s  │ ← huge gap!
│ Input Size  │  12 MB   │  30 MB   │  35 MB   │  4.2 GB  │ ← huge gap!
└─────────────┴──────────┴──────────┴──────────┴──────────┘

Summary Metrics (Healthy):
┌─────────────┬──────────┬──────────┬──────────┬──────────┐
│ Metric      │   Min    │  25th %  │  Median  │   Max    │
├─────────────┼──────────┼──────────┼──────────┼──────────┤
│ Duration    │  1.8 s   │  2.0 s   │  2.1 s   │  2.4 s   │ ← tight range
│ Input Size  │  120 MB  │  125 MB  │  128 MB  │  135 MB  │ ← tight range
└─────────────┴──────────┴──────────┴──────────┴──────────┘
```

> **Rule of thumb:** If Max Duration > 3× Median Duration, you have skew worth investigating.

### Method 2 – Code-Level Detection

```python
from pyspark.sql import functions as F

# Step 1: Check how many records are in each partition
df.withColumn("partition_id", F.spark_partition_id()) \
  .groupBy("partition_id") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(20)

# Step 2: Check distribution of a specific skewed key
# Example: if you suspect country is skewed
df.groupBy("country") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(20)

# Step 3: After a shuffle (GroupBy / Join), check shuffle partitions
df_agg = df.groupBy("country").agg(F.sum("revenue"))
df_agg.withColumn("partition_id", F.spark_partition_id()) \
      .groupBy("partition_id") \
      .count() \
      .orderBy(F.desc("count")) \
      .show()
```

---

## 3. When Does Data Skew Happen?

Skew is a **data characteristic** that becomes a **performance problem** only during operations that **group or co-locate data** (shuffles).

```
Reading raw data:     Usually fine — Spark splits files into ~equal 128MB chunks
After a GroupBy:      Records with the same key land in the same partition → skew
After a Join:         Records with the same join key land on the same executor → skew
After repartition(n): If you repartition by a skewed column → skew follows
```

### The Shuffle Is the Trigger

```
Stage 1: Read + Filter (no shuffle)
  [P1] [P2] [P3] [P4]  ← roughly equal partitions

                ↓ GroupBy("country") — causes SHUFFLE

Stage 2: Aggregate (shuffle results)
  [IND: 5M rows] [USA: 200K rows] [CHN: 300K rows]
        ↑ 
   Skew appears HERE — all IND records routed to same partition
```

---

## 4. Operations That Cause Data Skew

### 1. GroupBy / Aggregation

```python
# If India has 80% of records, the "IND" partition is massive
df.groupBy("country").agg(F.sum("revenue"), F.count("*"))

# Partition after shuffle:
# partition_0: IND  → 8,000,000 rows  ← takes 45s
# partition_1: USA  →   500,000 rows  ← takes 2s
# partition_2: CHN  →   300,000 rows  ← takes 1.5s
# partition_3: GBR  →   200,000 rows  ← takes 1s
```

### 2. Join Operations

```python
# orders (10M rows) joined with products (500K rows) on product_id
# If product P001 appears in 70% of orders:
orders.join(products, "product_id", "inner")

# After shuffle:
# partition for P001: 7,000,000 rows  ← one executor drowns
# partition for P002:    50,000 rows
# partition for P003:    30,000 rows
```

### 3. Distinct / DropDuplicates

```python
# If duplicates are clustered around certain keys, 
# the partition holding those keys becomes large
df.dropDuplicates(["customer_id", "order_date"])
```

### 4. Window Functions

```python
from pyspark.sql.window import Window

# If one country has millions of rows, the window partition for it is massive
windowSpec = Window.partitionBy("country").orderBy("order_date")
df.withColumn("row_num", F.row_number().over(windowSpec))
```

---

## 5. Why Data Skew Is Bad

### 1. Stragglers Block the Entire Stage
In Spark, **all tasks in a stage must complete before the next stage begins**. One slow task (the straggler) makes all other executor cores sit idle, wasting compute you're paying for.

### 2. Uneven Resource Utilization
```
Time →  0s        5s       10s       30s       60s
P1:     [████]    done
P2:     [██]      done
P4:     [█████]   done
P3:     [████████████████████████████████████████] ← everyone waits
                                                   for this
```

### 3. OOM Errors
The skewed partition's executor tries to hold 4 GB of data in a memory pool sized for 128 MB. It either:
- **Spills to disk** — Spark writes shuffle data to local disk, then reads it back → very expensive I/O
- **Crashes with OOM** — if spill can't help and memory is exhausted entirely

### 4. Data Spills Are Expensive
```
Normal flow:   Shuffle data → Network → Executor RAM → Process
Spill flow:    Shuffle data → Network → Executor RAM (full) → Local Disk
               → Read back from Disk → Process
               (2-10× slower than RAM processing)
```

---

## 6. Fixing Data Skew – All Techniques

### Technique 1: Salting (Most Important — Must Know)

Add a random "salt" to the skewed key to artificially spread it across multiple partitions, then aggregate in two steps.

```python
import random
from pyspark.sql import functions as F

SALT_FACTOR = 10  # spread each key across 10 partitions

# Step 1: Add salt to the skewed DataFrame
df_salted = df.withColumn(
    "salted_country",
    F.concat(F.col("country"), F.lit("_"), (F.rand() * SALT_FACTOR).cast("int"))
)
# Now "IND" becomes "IND_0", "IND_1", ..., "IND_9"

# Step 2: First-level aggregation (on salted key)
df_partial = df_salted.groupBy("salted_country") \
    .agg(F.sum("revenue").alias("partial_revenue"))

# Step 3: Strip the salt and do final aggregation
df_final = df_partial \
    .withColumn("country", F.split(F.col("salted_country"), "_")[0]) \
    .groupBy("country") \
    .agg(F.sum("partial_revenue").alias("total_revenue"))
```

**Why it works:**
```
Before salting:  IND → 8M rows → 1 partition
After salting:   IND_0 → 800K, IND_1 → 800K, ..., IND_9 → 800K
                 → 10 partitions, each manageable
```

### Technique 2: Broadcast Join (for Join Skew)

If the smaller table in a join is small enough (<200 MB), broadcast it to all executors. No shuffle = no skew.

```python
from pyspark.sql.functions import broadcast

# products is small (500K rows, ~50 MB)
result = orders.join(broadcast(products), "product_id", "inner")

# Config: raise threshold if your "small" table is between 10MB-200MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 209715200)  # 200 MB
```

### Technique 3: Skew Hint (Spark 3.0+ / Databricks)

Spark 3.0 introduced **Adaptive Query Execution (AQE)** with automatic skew join handling.

```python
# Enable AQE (on by default in Databricks)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# AQE detects skewed partitions and splits them automatically
# Skew threshold: partition size > skewFactor × median partition size
spark.conf.set("spark.sql.adaptive.skewJoin.skewFactor", "5")       # default
spark.conf.set("spark.sql.adaptive.skewJoin.minPartitionSize", "256mb")

# Or: use explicit SKEW HINT in SQL
spark.sql("""
    SELECT /*+ SKEW('orders', 'product_id') */ *
    FROM orders
    JOIN products ON orders.product_id = products.product_id
""")
```

### Technique 4: Increase Shuffle Partitions

If skew is mild, simply increasing `spark.sql.shuffle.partitions` can dilute it.

```python
# Default is 200 — for large datasets or skewed GroupBy, increase it
spark.conf.set("spark.sql.shuffle.partitions", "800")
# More partitions = each partition holds less data = skew is spread thinner
```

### Technique 5: Repartition Before Join

Manually repartition the skewed DataFrame to redistribute data more evenly before the join.

```python
# Repartition by a composite key to spread the load
df_orders_repart = orders.repartition(400, "product_id", "order_date")
result = df_orders_repart.join(products, "product_id")
```

### Technique 6: Filter + Union (Isolate Skewed Keys)

Handle skewed keys separately, then union results.

```python
# Identify the skewed key
SKEWED_KEY = "IND"

# Split data
df_skewed  = df.filter(F.col("country") == SKEWED_KEY)
df_normal  = df.filter(F.col("country") != SKEWED_KEY)

# Process skewed portion with more partitions
df_skewed_agg = df_skewed.repartition(50).groupBy("country").agg(F.sum("revenue"))

# Process normal portion normally
df_normal_agg = df_normal.groupBy("country").agg(F.sum("revenue"))

# Combine
df_final = df_skewed_agg.union(df_normal_agg)
```

### Comparison of Techniques

| Technique | Best For | Complexity | Works in Streaming? |
|---|---|---|---|
| Salting | GroupBy skew | Medium | No |
| Broadcast Join | Join with small table | Low | Yes |
| AQE Skew Join | Join skew (Spark 3+) | Low (auto) | No |
| Increase shuffle partitions | Mild skew | Very Low | No |
| Repartition before join | Join skew | Low | Yes |
| Filter + Union | Known skewed keys | Medium | No |

---

## 7. Code Examples – Simulate, Detect & Fix

### Full End-to-End Example

```python
from pyspark.sql import SparkSession, functions as F
import random

spark = SparkSession.builder.appName("SkewDemo").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "10")

# ── 1. Create a Skewed Dataset ─────────────────────────────────────────
countries = (
    ["IND"] * 800000 +   # 80% of data
    ["USA"] * 100000 +
    ["CHN"] *  70000 +
    ["GBR"] *  30000
)
revenues = [random.uniform(100, 10000) for _ in range(len(countries))]

df_skewed = spark.createDataFrame(
    zip(countries, revenues),
    schema=["country", "revenue"]
)

# ── 2. Create a Uniform Dataset ────────────────────────────────────────
uniform_countries = ["IND", "USA", "CHN", "GBR"] * 250000
df_uniform = spark.createDataFrame(
    zip(uniform_countries, revenues),
    schema=["country", "revenue"]
)

# ── 3. Detect Skew — Check Partition Sizes ─────────────────────────────
print("=== SKEWED PARTITION DISTRIBUTION ===")
df_skewed \
    .withColumn("partition_id", F.spark_partition_id()) \
    .groupBy("partition_id") \
    .count() \
    .orderBy(F.desc("count")) \
    .show()

print("=== UNIFORM PARTITION DISTRIBUTION ===")
df_uniform \
    .withColumn("partition_id", F.spark_partition_id()) \
    .groupBy("partition_id") \
    .count() \
    .orderBy(F.desc("count")) \
    .show()

# ── 4. Fix Using Salting ───────────────────────────────────────────────
SALT_FACTOR = 10

df_salted = df_skewed.withColumn(
    "salted_country",
    F.concat(F.col("country"), F.lit("_"), (F.rand() * SALT_FACTOR).cast("int"))
)

df_partial = df_salted \
    .groupBy("salted_country") \
    .agg(F.sum("revenue").alias("partial_revenue"))

df_result = df_partial \
    .withColumn("country", F.split(F.col("salted_country"), "_")[0]) \
    .groupBy("country") \
    .agg(F.sum("partial_revenue").alias("total_revenue")) \
    .orderBy(F.desc("total_revenue"))

df_result.show()

# ── 5. After Salting — Check Partition Sizes ───────────────────────────
print("=== PARTITION DISTRIBUTION AFTER SALTING ===")
df_salted \
    .withColumn("partition_id", F.spark_partition_id()) \
    .groupBy("partition_id") \
    .count() \
    .orderBy(F.desc("count")) \
    .show()

# ── 6. Broadcast Join Fix ──────────────────────────────────────────────
products = spark.createDataFrame(
    [("P001", "Laptop"), ("P002", "Phone"), ("P003", "Tablet")],
    schema=["product_id", "product_name"]
)

# Without broadcast: sort-merge join → skew if P001 dominates
# With broadcast: no shuffle → no skew
orders = spark.createDataFrame(
    [("P001",) * 7000 + ("P002",) * 2000 + ("P003",) * 1000][0],
    schema=["product_id"]
)

result_broadcast = orders.join(F.broadcast(products), "product_id")
result_broadcast.groupBy("product_name").count().show()
```

---

## 8. Interview Q&A – Tiered

---

### 🟢 Basic Level

**Q1. What is data skew in Spark?**

**A:** Data skew is when data is unevenly distributed across partitions — some partitions hold significantly more data than others. This causes certain tasks to take much longer than others (stragglers), which holds up the entire stage since Spark waits for all tasks to finish before moving to the next stage.

---

**Q2. How do you identify data skew using the Spark UI?**

**A:** Two places:
1. **Event Timeline** — one task bar is dramatically longer than all others. Classic symptom: job stuck at 199/200 tasks.
2. **Summary Metrics** — a huge gap between Median and Max for Duration and Input Size columns. If Max Duration is 3× or more than Median, skew is likely present.

---

**Q3. Which Spark operations typically cause data skew?**

**A:** Operations that trigger a shuffle and group records by key:
- **GroupBy / Aggregations** — records with the same key go to the same partition
- **Joins** — records with the same join key are co-located on the same executor
- **Window Functions** — `partitionBy` in window specs concentrates records by key
- **Distinct / dropDuplicates** — deduplication shuffles records by the specified columns

---

### 🟡 Medium Level

**Q4. What is salting and how does it fix GroupBy skew?**

**A:** Salting appends a random number (the "salt") to the skewed key to artificially break one large group into multiple smaller groups. For example, "IND" becomes "IND_0", "IND_1", ..., "IND_9" with a salt factor of 10. This spreads 8M "IND" rows across 10 partitions instead of 1. You then do a partial aggregation on the salted key, strip the salt, and do a final aggregation. The cost is **two aggregation passes** instead of one — acceptable tradeoff for eliminating skew.

---

**Q5. When would you use a broadcast join vs. salting to fix skew?**

**A:**
- **Broadcast join** when the skew is in a **join** and the **smaller table is under ~200 MB** — this eliminates the shuffle entirely, so there's no skew to worry about. It's simpler and should be tried first.
- **Salting** when the skew is in a **GroupBy/aggregation**, or when both tables in a join are large (broadcast isn't possible). Salting requires more code but works regardless of table sizes.

---

**Q6. What is AQE's skew join optimization? How is it different from manual salting?**

**A:** Adaptive Query Execution (Spark 3.0+) detects skewed partitions **at runtime** by comparing each partition's size to the median. When a partition exceeds `skewFactor × median` (default 5×), Spark automatically splits that partition and processes it in smaller pieces — essentially doing salting automatically without any code changes. The difference from manual salting: AQE is dynamic (no code needed, adapts to actual data), while salting is static (you hardcode the salt factor and key). AQE is enabled by default on Databricks.

---

### 🔴 Advanced Level

**Q7. You're running a sort-merge join between a 500 GB fact table and a 200 MB lookup table. The job is skewed. What do you try first, and why?**

**A:** First try a **broadcast join**. 200 MB is above the default 10 MB threshold but within practical limits — increase `spark.sql.autoBroadcastJoinThreshold` to 209715200 (200 MB) and add a broadcast hint. This eliminates the shuffle entirely, which is better than any skew fix that still involves shuffling. If the lookup table is genuinely 200 MB compressed but expands significantly in-memory, check driver memory (`spark.driver.memory`) since the driver collects the broadcast table. If broadcast isn't feasible, fall back to AQE skew join optimization since we're on Spark 3+ / Databricks.

---

**Q8. In your Iceberg migration project, you're migrating a 500 GB Hive table partitioned by `event_date`. Some dates have 10× more records than others. How do you handle this?**

**A:** Three-part approach:
1. **Identify skewed dates** upfront — query the source table: `SELECT event_date, COUNT(*) FROM source GROUP BY event_date ORDER BY COUNT(*) DESC LIMIT 20`
2. **Process skewed dates separately** with higher parallelism:
```python
SKEWED_DATES = ["2024-01-01", "2024-03-31", "2024-12-31"]  # month-end dates
for date in SKEWED_DATES:
    df = spark.read.table("hive.source").filter(f"event_date = '{date}'")
    df.repartition(50).writeTo("iceberg.target").append()   # extra partitions

NON_SKEWED_DATES = ...
for date in NON_SKEWED_DATES:
    df = spark.read.table("hive.source").filter(f"event_date = '{date}'")
    df.repartition(10).writeTo("iceberg.target").append()   # normal partitions
```
3. After migration, run `OPTIMIZE` on the Iceberg table to compact small files created by over-repartitioning.

---

**Q9. Salting introduces two aggregation passes. In what scenario would this actually be slower than just dealing with the skew?**

**A:** Salting can be counterproductive when:
1. **The dataset is small** — two aggregation passes on a few million rows costs more than the skew itself
2. **The skew is mild** — if Max/Median ratio is only 2-3×, increasing `spark.sql.shuffle.partitions` is cheaper
3. **Network is the bottleneck** — salting creates more shuffle partitions, increasing network traffic. If your cluster has slow inter-node networking, this overhead can exceed the skew cost
4. **Salt factor is too high** — if you use salt=100 on a 1GB dataset, you create 100× more shuffle tasks with almost no data each — task scheduling overhead dominates

Rule of thumb: apply salting when skew ratio > 5× AND dataset size is large enough that the two-pass cost is negligible.

---

## 9. Real-World Scenarios & Solutions

---

### 🏗️ Scenario 1 – GroupBy Skew in a Golden Record MDM Pipeline

**Context:** In your MDM pipeline, you're deduplicating customer records using `groupBy("country", "source_system")` followed by complex aggregations to compute a golden record. India (IND) accounts for 70% of records. The job consistently gets stuck at 199/200 tasks for 40+ minutes.

**Diagnosis:**
```python
# Check key distribution
df_customers.groupBy("country") \
    .count() \
    .orderBy(F.desc("count")) \
    .show()
# Output: IND → 7,200,000  |  USA → 400,000  |  others → <200,000 each
```

**Solution – Salting + AQE:**
```python
# Enable AQE as a safety net
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Salt the skewed column
SALT = 20  # spread IND across 20 partitions → ~360K rows each

df_salted = df_customers.withColumn(
    "salted_key",
    F.concat(
        F.col("country"), F.lit("_"), F.col("source_system"),
        F.lit("_"), (F.rand() * SALT).cast("int")
    )
)

# First pass: partial golden record computation per salted key
df_partial = df_salted.groupBy("salted_key").agg(
    F.max("updated_at").alias("latest_update"),
    F.first("email").alias("email"),
    F.sum("order_count").alias("partial_order_count")
)

# Second pass: final merge after stripping salt
df_golden = df_partial \
    .withColumn("country_source",
        F.regexp_extract(F.col("salted_key"), r"^(.+)_\d+$", 1)
    ) \
    .groupBy("country_source") \
    .agg(
        F.max("latest_update").alias("latest_update"),
        F.first("email").alias("email"),
        F.sum("partial_order_count").alias("total_orders")
    )
```

**Result:** Job time dropped from 55 minutes to 8 minutes.

---

### 🏗️ Scenario 2 – Join Skew in a Delta Lake Sales Pipeline

**Context:** A Delta Lake pipeline joins `orders` (500 GB, 800M rows) with `products` (50 MB, 200K rows) on `product_id`. One product ("FLAGSHIP_PHONE") appears in 60% of orders. The join stage keeps hitting OOM on one executor.

**Solution – Broadcast Join:**
```python
from pyspark.sql.functions import broadcast

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))  # 100 MB

# products is 50 MB — well within broadcast limit
orders = spark.read.format("delta").load("/delta/orders")
products = spark.read.format("delta").load("/delta/products")

result = orders.join(broadcast(products), "product_id", "left")

result.write.format("delta").mode("overwrite").save("/delta/order_details")
```

**If products were too large to broadcast:**
```python
# Alternative: AQE skew join + repartition
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewFactor", "3")  # more aggressive detection

result = orders.repartition(400, "product_id") \
               .join(products.repartition(400, "product_id"), "product_id")
```

---

### 🏗️ Scenario 3 – Window Function Skew in a Time-Series Pipeline

**Context:** You're computing rolling 7-day revenue per country using a window function. The job takes 2 hours for India but 3 minutes for other countries.

```python
from pyspark.sql.window import Window

# PROBLEMATIC: all India rows go to one executor
windowSpec = Window.partitionBy("country").orderBy("event_date") \
                   .rowsBetween(-6, 0)

df.withColumn("rolling_revenue", F.sum("revenue").over(windowSpec))
```

**Solution – Process Skewed Country Separately:**
```python
# Separate India (70% of data) from others
df_india  = df.filter(F.col("country") == "IND").repartition(50, "event_date")
df_others = df.filter(F.col("country") != "IND")

# Apply window function to each separately
windowSpec = Window.partitionBy("country").orderBy("event_date").rowsBetween(-6, 0)

df_india_windowed  = df_india.withColumn("rolling_revenue", F.sum("revenue").over(windowSpec))
df_others_windowed = df_others.withColumn("rolling_revenue", F.sum("revenue").over(windowSpec))

# Combine
df_final = df_india_windowed.union(df_others_windowed)
df_final.write.format("delta").mode("overwrite").save("/delta/rolling_revenue")
```

---

## Quick Reference – Data Skew Cheat Sheet

```
DETECT:
  Spark UI → Stage Summary Metrics → Max Duration >> Median Duration
  Code: df.withColumn("pid", spark_partition_id()).groupBy("pid").count()

CAUSE → FIX MAPPING:
  GroupBy skew                  → Salting (2-pass aggregation)
  Join skew + small table       → Broadcast join
  Join skew + both tables large → AQE skewJoin (Spark 3+) or Salting
  Mild skew                     → Increase spark.sql.shuffle.partitions
  Known skewed keys             → Filter + Union
  Window function skew          → Separate + Union approach

SALTING FORMULA:
  salted_key = original_key + "_" + random(0, SALT_FACTOR)
  SALT_FACTOR = ceil(skewed_partition_size / target_partition_size)
  e.g., 8M rows skewed, target 500K per partition → SALT_FACTOR = 16 → use 20

AQE CONFIG (Databricks default: ON):
  spark.sql.adaptive.enabled                    = true
  spark.sql.adaptive.skewJoin.enabled           = true
  spark.sql.adaptive.skewJoin.skewFactor        = 5      ← lower to detect sooner
  spark.sql.adaptive.skewJoin.minPartitionSize  = 256mb
```

---

*Next Topic: Spark Shuffle & Partitioning Strategy* 🚀
