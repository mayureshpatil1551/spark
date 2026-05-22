# 🎯 Salting & Data Skew — 30 Interview Questions & Answers
> **Topic: How Salting Reduces Data Skew by 99%**
> **Tailored for: Mayuresh Patil | 3+ Years PySpark | AbbVie/Cognizant**

---

## 🟢 LEVEL 1 — Basic (Fresher / 1–2 Years)

---

**Q1. What is data skew in Spark?**

Data skew means one or few partition keys have way more records than others. This makes one executor work very hard while others sit idle.

```
Partition 0 → 1,000,000 records  ← bottleneck
Partition 1 → 15 records
Partition 2 → 10 records
```
Job waits for Partition 0 to finish. That's skew.

---

**Q2. Why is data skew a problem?**

Spark's job finishes only when the **slowest task** finishes. If one partition has millions of rows and others have 10, 99% of your executors are idle — wasting time and money.

---

**Q3. What is salting?**

Salting = **adding a random number to the join/group key** so that records with the same key spread across multiple partitions instead of going to one.

```python
# Before salting: all key=1 rows → same partition
# After salting: key=1 with salt=0 → partition 0
#                key=1 with salt=1 → partition 1
#                key=1 with salt=2 → partition 2
```

---

**Q4. What is a salt number?**

Salt number = how many buckets you want to split the skewed key into.

```python
SALT_NUMBER = 3   # splits skewed key into 3 buckets
# Random salt values assigned: 0, 1, or 2
```

More salt = better distribution, but also more data explosion on the small table.

---

**Q5. What is the formula Spark uses to assign data to partitions?**

```
partition = hash(key) % shuffle_partitions

# Without salting:
hash(1) % 3 = always same partition  ← skew

# With salting:
hash(1, 0) % 3 = partition 0
hash(1, 1) % 3 = partition 1
hash(1, 2) % 3 = partition 2
```

---

**Q6. Is salting a built-in Spark feature?**

No. Salting is a **manual coding technique**. You write the logic yourself — add random column to large table, explode the small table, then join on both columns.

*(AQE in Spark 3 handles some skew automatically, but manual salting gives more control.)*

---

**Q7. What are the 3 steps to apply salting in a join?**

```
Step 1: Add random salt column to the LARGE (skewed) dataset
        df_large.withColumn("salt", (rand() * SALT_NUMBER).cast("int"))

Step 2: Explode the SMALL dataset with all salt values
        df_small.withColumn("salt_values", array([lit(0), lit(1), lit(2)]))
                .withColumn("salt", explode("salt_values"))

Step 3: Join on (original_key + salt)
        df_large.join(df_small, ["value", "salt"], "inner")
```

---

**Q8. Why do we explode the small dataset, not the large one?**

`explode()` multiplies rows by SALT_NUMBER.

- Exploding **large** table (1M rows × 3) = 3M rows → worse problem
- Exploding **small** table (1000 rows × 3) = 3000 rows → totally fine

**Always explode the smaller dataset.**

---

**Q9. What does spark_partition_id() do?**

It returns the **partition number** each row belongs to. Used to check if data is evenly distributed.

```python
df.withColumn("partition", F.spark_partition_id()) \
  .groupBy("partition").count().show()
```

---

**Q10. How do you detect data skew before fixing it?**

```python
# Check partition distribution
df.withColumn("partition", F.spark_partition_id()) \
  .groupBy("partition").count() \
  .orderBy("partition").show()

# Or check key distribution
df.groupBy("join_key").count().orderBy(F.desc("count")).show(10)
```

If one partition/key has 99% of rows → skew detected.

---

## 🟡 LEVEL 2 — Intermediate (2–4 Years)

---

**Q11. How do you choose the SALT_NUMBER?**

Best practice: set it equal to `spark.sql.shuffle.partitions`.

```python
SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))
```

This aligns salt buckets with output partitions for maximum distribution.

**Constraint:** `small_table_rows × SALT_NUMBER` must comfortably fit in memory. If small table is large, reduce SALT_NUMBER.

---

**Q12. How does salting work in aggregations (groupBy)?**

Use **two-phase aggregation**:

```python
# Phase 1: Partial aggregation per (key, salt) bucket
df.withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))
  .groupBy("value", "salt")
  .agg(F.count("value").alias("partial_count"))

# Phase 2: Final merge
  .groupBy("value")
  .agg(F.sum("partial_count").alias("total_count"))
```

Phase 1 distributes the work. Phase 2 merges small partial results.

---

**Q13. Which aggregation functions are safe to use with salting?**

| Function | Safe? | Phase 2 |
|---|---|---|
| count | ✅ Yes | sum(partial_count) |
| sum | ✅ Yes | sum(partial_sum) |
| min / max | ✅ Yes | min/max of partials |
| avg | ⚠️ Careful | sum/count separately, then divide |
| countDistinct | ❌ Not directly | Use approx_count_distinct |

---

**Q14. When should you NOT use salting?**

- Small table can be **broadcast** → use broadcast join (simpler, faster)
- Data is already evenly distributed → no need
- SALT_NUMBER too high → exploded small table becomes too large
- Spark 3 with AQE enabled and skew is moderate → let AQE handle it

---

**Q15. What is the difference between AQE skew handling and manual salting?**

| Aspect | AQE | Manual Salting |
|---|---|---|
| Code changes | None | Required |
| Works on aggregations | ❌ No | ✅ Yes |
| Extreme skew (99% one key) | May be insufficient | Very reliable |
| Control | Automatic | Full control |
| Spark version | 3.0+ only | All versions |

**Best practice:** Enable AQE always. Use manual salting for extreme or known skew patterns.

---

**Q16. What AQE configs handle skew joins automatically?**

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Threshold — partition is skewed if size > 5x median AND > 256MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

---

**Q17. Why should you disable AQE when testing salting manually?**

AQE may interfere by auto-splitting partitions, making it hard to measure the true effect of your salting logic.

```python
spark.conf.set("spark.sql.adaptive.enabled", "false")  # Disable for clean testing
```

Enable AQE back in production after validating salting works.

---

**Q18. How do you verify salting worked correctly?**

```python
# After salting, check partition distribution
df_joined \
    .withColumn("partition", F.spark_partition_id()) \
    .groupBy("value", "partition") \
    .count() \
    .orderBy("value", "partition") \
    .show()

# Expect: skewed key is now spread ~evenly across partitions
# Before: |0| 1,000,000  |1| 15
# After:  |0| 333,330  |1| 333,601  |2| 333,615
```

Also check Spark UI → Stage → Event Timeline for balanced task durations.

---

**Q19. What happens if you forget to drop the salt column after joining?**

The output DataFrame will have an extra `salt` column — garbage data from the user's perspective. Always clean up:

```python
df_joined = df_skew.join(df_uniform, ["value", "salt"], "inner")
                   .drop("salt")           # ← remove salt from output
                   .drop("salt_values")    # ← remove array column if present
```

---

**Q20. Can salting produce wrong results?**

Yes, if done incorrectly:

- If you add random salt to the large table but **forget to explode the small table** → salt values won't match → zero join results
- If you use `countDistinct` in salted aggregation without two-phase logic → wrong count
- If you use the same salt column name in both tables before joining → column ambiguity error

Always validate: `df_result.count()` should match expected row count.

---

## 🔴 LEVEL 3 — Advanced (3–5 Years | Your Level)

---

**Q21. You have a 500 GB transactions table. One customer_id has 40% of all records. How do you fix the join skew?**

**Answer:**

```python
SALT_NUMBER = 200  # High salt for extreme skew

# Step 1: Salt the large transactions table
df_txn_salted = df_transactions.withColumn(
    "salt", (F.rand() * SALT_NUMBER).cast("int")
)

# Step 2: Explode the small customers table
df_cust_exploded = (
    df_customers
    .withColumn("salt_values", F.array([F.lit(i) for i in range(SALT_NUMBER)]))
    .withColumn("salt", F.explode("salt_values"))
    .drop("salt_values")
)

# Step 3: Join on (customer_id + salt)
df_result = df_txn_salted \
    .join(df_cust_exploded, ["customer_id", "salt"], "inner") \
    .drop("salt")

df_result.write.parquet("s3://output/enriched_transactions/")
```

Result: 40% skew distributed across 200 partitions → ~0.2% per partition.

---

**Q22. In your AbbVie API pipelines, when would data skew occur and how would you detect it?**

**Answer (from your experience):**

"In AbbVie's Veeva/Ravex API ingestion, skew could occur when one `facility_id` or `hcp_id` has significantly more records than others — common in enterprise healthcare data where one large hospital system generates bulk records.

I'd detect it by:
```python
df.groupBy("facility_id").count().orderBy(F.desc("count")).show(10)
```

If one facility has 10x more records than others and it's a join key, I'd apply salting on that join."

---

**Q23. What is partial salting and when do you use it?**

Partial salting = apply salt **only to the skewed keys**, not all keys.

```python
SKEWED_KEYS = ["CORP001", "ENTERPRISE_A"]  # Known skewed customer IDs
SALT_NUMBER = 50

# Salt only skewed rows; non-skewed get salt=0
df_large_salted = df_large.withColumn(
    "salt",
    F.when(F.col("key").isin(SKEWED_KEYS), (F.rand() * SALT_NUMBER).cast("int"))
     .otherwise(F.lit(0))
)

# Explode only skewed keys in small table; non-skewed get salt=0
df_small_skewed  = df_small.filter(F.col("key").isin(SKEWED_KEYS)) \
    .withColumn("salt_values", F.array([F.lit(i) for i in range(SALT_NUMBER)])) \
    .withColumn("salt", F.explode("salt_values")).drop("salt_values")

df_small_normal  = df_small.filter(~F.col("key").isin(SKEWED_KEYS)) \
    .withColumn("salt", F.lit(0))

df_small_salted  = df_small_skewed.union(df_small_normal)

df_result = df_large_salted.join(df_small_salted, ["key", "salt"], "inner").drop("salt")
```

**Why use partial salting?** Avoids multiplying the entire small table — only multiplies the rows that actually need it.

---

**Q24. How does salting interact with Spark's physical plan?**

Without salting:
```
Exchange hashpartitioning(key, 200)
→ All rows with same key go to same partition → skew
```

With salting:
```
Exchange hashpartitioning(key, salt, 200)
→ Same key but different salt → different partitions → balanced
```

You can verify in `explain("formatted")`:
```python
df_joined.explain("formatted")
# Look for: hashpartitioning(value#5, salt#10, 3)  ← salt is now part of hash key
```

---

**Q25. How would you handle skew in a groupBy on a high-volume advertiser_id?**

```python
SALT_NUMBER = 50

result = (
    df_impressions
    # Phase 1: salt + partial aggregation
    .withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))
    .groupBy("advertiser_id", "salt")
    .agg(
        F.count("*").alias("partial_impressions"),
        F.sum("cost").alias("partial_cost")
    )
    # Phase 2: final merge
    .groupBy("advertiser_id")
    .agg(
        F.sum("partial_impressions").alias("total_impressions"),
        F.sum("partial_cost").alias("total_cost")
    )
)
```

Note: For `avg`, don't average the partial averages — compute `sum/count` separately.

---

**Q26. How is salting different from repartition() to fix skew?**

| Aspect | Salting | repartition(n, col) |
|---|---|---|
| How it works | Changes the hash key itself | Changes number of partitions |
| Fixes key-level skew | ✅ Yes | ❌ No — same key still same partition |
| Works for joins | ✅ Yes | ❌ Not directly |
| Works for aggregations | ✅ Yes | ❌ No |

`repartition("skewed_col")` still maps all same-key rows to the same partition — skew remains. Salting breaks that mapping by changing what's being hashed.

---

**Q27. How do you measure skew ratio programmatically in production?**

```python
def check_skew(df, label=""):
    stats = (
        df.withColumn("pid", F.spark_partition_id())
          .groupBy("pid").count()
          .agg(
              F.max("count").alias("max_rows"),
              F.avg("count").alias("avg_rows")
          ).collect()[0]
    )
    ratio = stats["max_rows"] / stats["avg_rows"]
    status = "⚠️ SKEWED" if ratio > 5 else "✅ BALANCED"
    print(f"{label} | Skew Ratio: {ratio:.1f}x | {status}")

# Usage
check_skew(df_before, "Before Salting")
check_skew(df_after,  "After Salting")

# Before: Skew Ratio: 66666.0x  ⚠️ SKEWED
# After:  Skew Ratio: 1.0x       ✅ BALANCED
```

---

**Q28. What are the risks of setting SALT_NUMBER too high?**

- **Small table explodes too large** → if customers table is 5 GB and SALT_NUMBER=200 → 1 TB exploded → OOM on executors
- **Too many small tasks** → scheduler overhead increases
- **Diminishing returns** → beyond a point, distribution doesn't improve further

**Rule:** `small_table_size × SALT_NUMBER < executor_memory × 0.5`

---

**Q29. In Spark UI, how do you confirm skew was fixed by salting?**

**Before salting:**
- Stages tab → Event Timeline → one very long bar (straggler task)
- Task metrics → max duration = 100x median duration

**After salting:**
- Event Timeline → all task bars roughly same length
- Max/median duration ratio → close to 1x
- Shuffle read size → distributed evenly across tasks

Also check: `SQL → DAG → Exchange` node — the hash key should now include the `salt` column.

---

**Q30. Interviewer asks: "You reduced batch processing time by 50% at AbbVie — did salting play a role? Walk me through it."**

**Script answer:**

*"Yes, salting was one of the levers. In our API ingestion from Veeva, we joined the raw activity records against a reference table on `hcp_id`. One HCP (a large hospital network) had roughly 8–10x more records than others — classic data skew.*

*I first confirmed the skew using `groupBy('hcp_id').count()` and the Spark UI's Event Timeline which showed one straggler task running 40× longer than others.*

*I applied salting: added a random salt column to the large activity DataFrame, exploded the small reference table with matching salt values [0 to N-1], and joined on `(hcp_id, salt)`. After that, the straggler task disappeared — all tasks completed within 2× of each other.*

*Combined with broadcast joins for other small reference tables, caching the base DataFrame that fed into 3 downstream operations, and fixing shuffle partition count — the overall job went from around 40 minutes to under 20 minutes."*

---

## 📋 Quick Revision Summary

```
┌────────────────────────────────────────────────────────────────┐
│              SALTING — 30 QUESTIONS SUMMARY                    │
├────────────────────────────────────────────────────────────────┤
│ WHAT     │ Add random salt to join/agg key → break hotspot    │
│ WHEN     │ One key has >> records than others                 │
│ HOW JOINS│ Salt large table + explode small table + join both │
│ HOW AGG  │ Phase 1: groupBy(key,salt) → Phase 2: groupBy(key) │
│ SALT_NUM │ = shuffle_partitions (default rule)                │
│ ALWAYS   │ Explode the SMALLER dataset                        │
│ VERIFY   │ spark_partition_id() + Spark UI Event Timeline     │
│ CLEANUP  │ .drop("salt") from final output                    │
├────────────────────────────────────────────────────────────────┤
│ SAFE FUNCTIONS    │ count, sum, min, max                       │
│ CAREFUL           │ avg → (sum/count separately)              │
│ AVOID             │ countDistinct → use approx_count_distinct  │
├────────────────────────────────────────────────────────────────┤
│ ALTERNATIVES      │ Broadcast Join > Salting (if table small) │
│                   │ AQE skewJoin (Spark 3, auto, joins only)  │
│ AQE CONFIGS       │ adaptive.enabled=true                     │
│                   │ adaptive.skewJoin.enabled=true            │
└────────────────────────────────────────────────────────────────┘
```

---

*Topic: 10 — Salting & Data Skew | Mayuresh Patil Interview Prep | 2026*
