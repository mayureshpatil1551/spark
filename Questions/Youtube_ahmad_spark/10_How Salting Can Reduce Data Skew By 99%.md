# 10 — How Salting Can Reduce Data Skew by 99% in Apache Spark
> **Target Audience:** Data Engineers with 3–4 years of experience | Interview + Real-World Prep

---

## Table of Contents
1. [What is Data Skew?](#1-what-is-data-skew)
2. [What is Salting?](#2-what-is-salting)
3. [Salting in Joins — Step-by-Step](#3-salting-in-joins--step-by-step)
4. [Salting in Aggregations — Step-by-Step](#4-salting-in-aggregations--step-by-step)
5. [Full Code Examples](#5-full-code-examples)
6. [Spark 3 — Adaptive Query Execution (AQE)](#6-spark-3--adaptive-query-execution-aqe)
7. [When to Use vs Avoid Salting](#7-when-to-use-vs-avoid-salting)
8. [Interview Questions & Answers](#8-interview-questions--answers)
9. [Real-World Project Scenarios](#9-real-world-project-scenarios)
10. [Quick Reference Cheat Sheet](#10-quick-reference-cheat-sheet)

---

## 1. What is Data Skew?

### Definition

Data skew occurs when **one or a few keys contain significantly more records** than other keys — causing those records to land in the same partition, creating a "fat" partition that takes much longer to process than all others.

### How Spark Distributes Data

Spark uses this formula to decide which partition a row goes to during a shuffle:

```
partition = hash(key) % shuffle_partitions
```

Since `hash(key)` is **deterministic**, all rows with the same key always go to the **same partition**.

### Skew Example

```
Dataset with shuffle_partitions = 3:

Key=1 → 1,000,000 rows → all go to partition 1  ← SKEWED (1 executor works 200,000x more)
Key=2 →           5 rows → partition 0
Key=3 →           6 rows → partition 2

Result: Partition 1 takes 10 minutes. Partitions 0 & 2 take 1 second.
        Job duration = 10 minutes (worst partition wins).
```

### How to Spot Skew in Spark UI

- **Stage page → Task metrics:** One or a few tasks have dramatically longer durations
- **Task timeline:** A few long bars vs many short bars
- **Shuffle read size:** One task reads far more data than others
- Rule of thumb: If the **max task duration > 3x the median**, you likely have skew

```python
# Detect skew programmatically
df.withColumn("partition", F.spark_partition_id()) \
  .groupBy("partition") \
  .count() \
  .orderBy(F.col("count").desc()) \
  .show()
```

---

## 2. What is Salting?

### Definition

**Salting = Adding a random number (salt) to the join/aggregation key** so that rows that previously all hashed to the same partition now hash to different partitions.

### The Math Behind It

```
Without salting:
  hash(key=1) % 3 = partition 1  ← ALL 1M rows go here

With salting (salt_number = 3, salt ∈ {0, 1, 2}):
  hash(key=1, salt=0) % 3 = partition 0  ← ~333K rows
  hash(key=1, salt=1) % 3 = partition 1  ← ~333K rows
  hash(key=1, salt=2) % 3 = partition 2  ← ~333K rows
```

The skewed 1M rows are now **evenly split** across 3 partitions.

### Salt Number

The **salt number** controls how many buckets you split the skewed data into.

```python
# Best practice: align salt number with shuffle partitions
SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))
```

Setting `SALT_NUMBER = 10` means:
- Skewed dataset: each row gets a random salt from `{0, 1, 2, ..., 9}`
- Non-skewed dataset: each row is **exploded** into 10 rows (one per salt value)
- Join is now on `(original_key, salt)` instead of just `original_key`

---

## 3. Salting in Joins — Step-by-Step

### The Problem

```
Dataset A (skewed):          Dataset B (uniform):
key=1 → 1,000,000 rows       key=1 → 1 row
key=2 → 5 rows               key=2 → 1 row
key=3 → 3 rows               key=3 → 1 row

Join on key → ALL 1M rows of key=1 go to the SAME partition → straggler task
```

### The 3-Step Salting Solution

```
Step 1: Add random salt to the SKEWED (larger) dataset
        key=1, salt=0
        key=1, salt=1
        key=1, salt=2  ← same key, now 3 different (key, salt) combinations

Step 2: EXPLODE the UNIFORM (smaller) dataset with ALL salt values
        key=1, salt=0   ← replicated
        key=1, salt=1   ← replicated
        key=1, salt=2   ← replicated

Step 3: Join on (key, salt) instead of just (key)
        hash(key=1, salt=0) % 3 → partition 0
        hash(key=1, salt=1) % 3 → partition 1
        hash(key=1, salt=2) % 3 → partition 2
```

### ⚠️ Critical Rule: Always Explode the SMALLER Dataset

Explode multiplies rows. If you explode the wrong (larger) dataset:
- `SALT_NUMBER = 10` and skewed dataset has 1M rows → 10M rows after explode
- This defeats the purpose and makes things worse

**Always explode the smaller/non-skewed dataset.**

### Visual Before/After

```
BEFORE SALTING:
Partition 0: [key=1 × 1,000,000]  ← straggler
Partition 1: [key=2 × 5]
Partition 2: [key=3 × 3]

AFTER SALTING (salt_number=3):
Partition 0: [key=1 × ~333K, key=2 × 1, key=3 × 1]  ← balanced
Partition 1: [key=1 × ~333K, key=2 × 1, key=3 × 1]  ← balanced
Partition 2: [key=1 × ~333K, key=2 × 1, key=3 × 1]  ← balanced
```

---

## 4. Salting in Aggregations — Step-by-Step

### The Problem

```python
df_skew.groupBy("value").count()
# All 1M rows with value=0 → same partition → single slow task
```

### The 2-Phase Salting Solution

Salting in aggregations uses a **map-side partial aggregation** pattern:

```
Phase 1 — Partial aggregation (distributed):
  Add salt → groupBy(value, salt) → partial count()
  
  value=0, salt=0 → count = 333,333
  value=0, salt=1 → count = 333,333
  value=0, salt=2 → count = 333,334

Phase 2 — Final merge (lightweight):
  groupBy(value) → sum(partial_count)
  
  value=0 → total count = 1,000,000
```

The first groupBy is distributed across partitions (salt distributes key=0).
The second groupBy has only a **tiny dataset** (one row per value per salt bucket) — no skew possible.

```python
(
    df_skew
    .withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))
    .groupBy("value", "salt")          # Phase 1: distributed partial aggregation
    .agg(F.count("value").alias("partial_count"))
    .groupBy("value")                  # Phase 2: final merge (tiny data, no skew)
    .agg(F.sum("partial_count").alias("count"))
    .show()
)
```

### Supported Aggregations with Salting

| Aggregation | Salting Approach | Notes |
|---|---|---|
| `count(*)` | Phase 1: count → Phase 2: sum | Straightforward |
| `sum(col)` | Phase 1: sum → Phase 2: sum | Straightforward |
| `max(col)` | Phase 1: max → Phase 2: max | Associative — works perfectly |
| `min(col)` | Phase 1: min → Phase 2: min | Associative — works perfectly |
| `avg(col)` | Phase 1: sum + count → Phase 2: sum/count | Cannot use avg directly in Phase 1 |
| `count_distinct` | Phase 1: collect_set → Phase 2: flatten + count | Complex; consider HyperLogLog |

```python
# avg with salting — correct approach
(
    df_skew
    .withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))
    .groupBy("value", "salt")
    .agg(
        F.sum("amount").alias("partial_sum"),
        F.count("amount").alias("partial_count")
    )
    .groupBy("value")
    .agg(
        (F.sum("partial_sum") / F.sum("partial_count")).alias("avg_amount")
    )
    .show()
)
```

---

## 5. Full Code Examples

### Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("SaltingDemo").getOrCreate()

# Disable AQE for demo (to observe skew behavior manually)
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "3")

SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))  # = 3
```

### Creating Skewed and Uniform Datasets

```python
# Uniform dataset (evenly distributed)
df_uniform = spark.createDataFrame([i for i in range(1_000_000)], IntegerType())

# Skewed dataset (value=0 has 999,990 rows — extreme skew)
df0 = spark.createDataFrame([0] * 999_990, IntegerType()).repartition(1)
df1 = spark.createDataFrame([1] * 15,       IntegerType()).repartition(1)
df2 = spark.createDataFrame([2] * 10,       IntegerType()).repartition(1)
df3 = spark.createDataFrame([3] * 5,        IntegerType()).repartition(1)
df_skew = df0.union(df1).union(df2).union(df3)

# Verify skew — partition 0 has 999,990 rows
df_skew.withColumn("partition", F.spark_partition_id()) \
       .groupBy("partition").count().orderBy("partition").show()
# +---------+------+
# |partition| count|
# +---------+------+
# |        0|999990|  ← SKEWED
# |        1|    15|
# |        2|    10|
# |        3|     5|
# +---------+------+
```

### Join WITHOUT Salting (Demonstrates Skew)

```python
df_joined_skewed = df_skew.join(df_uniform, "value", "inner")

df_joined_skewed.withColumn("partition", F.spark_partition_id()) \
                .groupBy("partition").count().show()
# +---------+-------+
# |partition|count  |
# +---------+-------+
# |0        |1000005|  ← ONE partition has 1M rows — straggler!
# |1        |    15 |
# +---------+-------+
```

### Join WITH Salting (Balanced)

```python
# Step 1: Add random salt to the SKEWED dataset
df_skew_salted = df_skew.withColumn(
    "salt",
    (F.rand() * SALT_NUMBER).cast("int")
)

# Step 2: Explode the UNIFORM (smaller) dataset with all salt values
df_uniform_exploded = (
    df_uniform
    .withColumn("salt_values", F.array([F.lit(i) for i in range(SALT_NUMBER)]))
    .withColumn("salt", F.explode(F.col("salt_values")))
    .drop("salt_values")
)
# Each row in df_uniform now appears SALT_NUMBER times (one per salt value)
# This is the trade-off: small dataset grows by SALT_NUMBER factor

# Step 3: Join on (value, salt) instead of just (value)
df_joined_salted = df_skew_salted.join(df_uniform_exploded, ["value", "salt"], "inner")

df_joined_salted.withColumn("partition", F.spark_partition_id()) \
                .groupBy("value", "partition").count() \
                .orderBy("value", "partition").show()
# +-----+---------+------+
# |value|partition| count|
# +-----+---------+------+
# |    0|        0|332774|  ← value=0 now split across 3 partitions
# |    0|        1|333601|
# |    0|        2|333615|
# |    1|        0|     6|
# |    1|        1|     9|
# |    2|        0|     2|
# |    2|        1|     2|
# |    2|        2|     6|
# |    3|        0|     3|
# |    3|        1|     2|
# +-----+---------+------+
```

### Aggregation WITH Salting (2-Phase)

```python
# WITHOUT salting — skewed aggregation
df_skew.groupBy("value").count().show()
# +-----+------+
# |value| count|
# +-----+------+
# |    0|999990|  ← All 1M rows aggregated in one task
# |    2|    10|
# |    3|     5|
# |    1|    15|
# +-----+------+

# WITH salting — 2-phase distributed aggregation
(
    df_skew
    .withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))
    # Phase 1: partial aggregation — distributed across partitions via salt
    .groupBy("value", "salt")
    .agg(F.count("value").alias("partial_count"))
    # Phase 2: final merge — tiny dataset, no skew possible
    .groupBy("value")
    .agg(F.sum("partial_count").alias("count"))
    .show()
)
# +-----+------+
# |value| count|
# +-----+------+
# |    0|999990|  ← Same result, but computed across 3 partitions
# |    1|    15|
# |    2|    10|
# |    3|     5|
# +-----+------+
```

### Helper Function — Reusable Salting Utility

```python
def apply_salting_join(df_skewed, df_small, join_keys: list, salt_number: int):
    """
    Apply salting to resolve skewed joins.
    
    Args:
        df_skewed:   The large, skewed DataFrame
        df_small:    The smaller, non-skewed DataFrame
        join_keys:   List of join key column names
        salt_number: Number of salt buckets (typically = shuffle_partitions)
    
    Returns:
        Joined DataFrame with skew resolved
    """
    # Add random salt to skewed dataset
    df_skewed_salted = df_skewed.withColumn(
        "_salt", (F.rand() * salt_number).cast("int")
    )
    
    # Explode small dataset with all salt values
    df_small_exploded = (
        df_small
        .withColumn("_salt_arr", F.array([F.lit(i) for i in range(salt_number)]))
        .withColumn("_salt", F.explode(F.col("_salt_arr")))
        .drop("_salt_arr")
    )
    
    # Join on original keys + salt
    salted_keys = join_keys + ["_salt"]
    df_joined = df_skewed_salted.join(df_small_exploded, salted_keys, "inner")
    
    # Drop the salt column from result
    return df_joined.drop("_salt")


# Usage
result = apply_salting_join(
    df_skewed=df_skew,
    df_small=df_uniform,
    join_keys=["value"],
    salt_number=SALT_NUMBER
)
```

---

## 6. Spark 3 — Adaptive Query Execution (AQE)

### What is AQE?

Spark 3 introduced **Adaptive Query Execution (AQE)**, which automatically handles skew during runtime — without manual salting.

```python
# Enable AQE (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### How AQE Handles Skew

1. Spark runs the query normally at first
2. At shuffle boundaries, AQE **inspects actual partition sizes**
3. If a partition is detected as skewed (size > threshold), AQE **automatically splits it** into smaller sub-partitions
4. Joins are executed on the split partitions — effectively doing what salting does, automatically

### AQE Skew Detection Parameters

```python
# A partition is skewed if:
# size > skewedPartitionFactor × median partition size
# AND size > skewedPartitionThresholdInBytes
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")        # default
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")  # default
```

### AQE vs Manual Salting — When to Use Each

| Scenario | AQE | Manual Salting |
|---|---|---|
| Moderate skew (predictable) | ✅ Sufficient | Overkill |
| Extreme skew (99% in one key) | ⚠️ May still struggle | ✅ Better control |
| Ad-hoc / exploratory queries | ✅ Automatic | Too much setup |
| Repeated production pipelines | ⚠️ Runtime overhead | ✅ Predictable performance |
| Skew in aggregations | ❌ AQE doesn't handle | ✅ Salting required |
| Streaming workloads | ❌ AQE is batch-only | ✅ Manual salting |

> **Interview insight:** AQE handles **join skew** but does **NOT** handle **aggregation skew**. For aggregation skew, manual salting is still the best approach.

---

## 7. When to Use vs Avoid Salting

### Use Salting When:
- Join key has extreme skew (one key dominates the dataset)
- Aggregation key is skewed (groupBy produces one huge partition)
- Spark UI shows straggler tasks (one task 10x+ longer than others)
- AQE is disabled or insufficient for the level of skew
- You're working with streaming micro-batches (AQE unavailable)

### Avoid Salting When:
- Data is already balanced — salting adds unnecessary complexity
- A **broadcast join** is possible (small table fits in memory, no shuffle needed)
  ```python
  df_large.join(F.broadcast(df_small), "key")  # No shuffle, no skew possible
  ```
- Salt number is too high — the exploded small dataset becomes too large
  - `SALT_NUMBER = 100`, small table = 10M rows → 1B rows after explode
- AQE is enabled and sufficient for the level of skew
- You can **pre-aggregate** to reduce skew before the join

### Cost-Benefit of Salting

```
Benefit:
  Reduces straggler task duration by SALT_NUMBER factor
  (1M rows in 1 task → ~333K rows in 3 tasks with SALT_NUMBER=3)

Cost:
  Small dataset is multiplied by SALT_NUMBER (explode overhead)
  Slightly more complex code to maintain
  Extra shuffle stage for the explode operation
```

---

## 8. Interview Questions & Answers

---

### Q1. What is data skew in Spark and why is it a problem?

**Answer:**
Data skew occurs when one or a few partition keys contain significantly more data than others. Because Spark uses `hash(key) % shuffle_partitions` to route rows during shuffles, all rows with the same key always land in the same partition and are processed by the same executor task.

The problem is that **a Spark stage only completes when ALL tasks finish**. If one task processes 1 million rows while others process 5 rows each, the entire stage waits for the slow task — making the job as slow as its worst partition. This is called a **straggler task**.

---

### Q2. Explain the salting technique for joins. What are the exact steps?

**Answer:**
Salting resolves join skew by adding a random number (salt) to the join key, breaking one large partition into multiple smaller ones.

**Steps:**
1. Choose a `SALT_NUMBER` (typically equal to `shuffle_partitions`)
2. Add a random integer from `{0, ..., SALT_NUMBER-1
