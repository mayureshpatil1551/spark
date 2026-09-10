# Complete Interview Q&A Script
### PySpark | Databricks | Azure | SQL | Data Engineering

> **Format:** Short Answer (say in 30 seconds) → Detailed Answer (say in 2 minutes) → Code where needed

---

## SECTION 1 — DATA PIPELINE DESIGN & ARCHITECTURE

---

### Q1. How would you design a data pipeline to process and analyze customer review data at scale?

**SHORT:**
"I'd use a medallion architecture — Bronze for raw reviews, Silver for cleaned/parsed text, Gold for aggregated sentiment scores — orchestrated by ADF with Databricks for processing."

**DETAILED:**
"In production, customer reviews come from multiple sources — APIs, mobile apps, web forms. I'd land raw JSON into ADLS Bronze via ADF. Databricks processes it in Silver — parse text, remove duplicates, standardise encoding. Gold layer runs sentiment aggregations per product, per region, per time window. For scale, I'd partition by `review_date` and use Spark's distributed NLP libraries. For real-time reviews, Event Hubs feeds Databricks Structured Streaming instead of batch."

```
Source APIs → Event Hubs → Databricks Streaming → Bronze Delta
                                                  ↓
                                    Silver (clean + parse text)
                                                  ↓
                                    Gold (sentiment + aggregation)
                                                  ↓
                                    Snowflake → Dashboard
```

---

### Q2. Write a SQL query to find customers who placed orders on consecutive days.

**SHORT:**
"Use LAG window function to get previous order date per customer, then filter where difference between current and previous date equals 1 day."

**DETAILED:**
"I partition by customer_id, order by order_date, use LAG to get previous order date, then check if the difference is exactly 1 day. This is more efficient than a self-join on large datasets."

```sql
WITH ordered AS (
    SELECT
        customer_id,
        order_date,
        LAG(order_date) OVER (
            PARTITION BY customer_id
            ORDER BY order_date
        ) AS prev_order_date
    FROM orders
)
SELECT DISTINCT customer_id
FROM ordered
WHERE DATEDIFF(order_date, prev_order_date) = 1;
```

---

### Q3. Explain ROW_NUMBER(), RANK(), DENSE_RANK()

**SHORT:**
"ROW_NUMBER always gives unique sequential numbers. RANK gives same rank to ties but skips numbers. DENSE_RANK gives same rank to ties without skipping."

**DETAILED:**
"All three are window functions but behave differently on ties."

```sql
-- Data: scores = 100, 100, 90, 80

SELECT score,
    ROW_NUMBER() OVER (ORDER BY score DESC),  -- 1, 2, 3, 4
    RANK()       OVER (ORDER BY score DESC),  -- 1, 1, 3, 4
    DENSE_RANK() OVER (ORDER BY score DESC)   -- 1, 1, 2, 3
FROM scores;
```

"ROW_NUMBER → deduplication (pick row 1 per group).
RANK → leaderboard where ties skip positions.
DENSE_RANK → leaderboard where ties don't skip.
I use ROW_NUMBER most in my pipelines for SCD2 and deduplication."

---

### Q4. Daily sales reports show incorrect numbers after a deployment. How do you investigate?

**SHORT:**
"I compare Gold output row counts vs Silver row counts, check if transformation logic changed in the deployment, and use Delta time travel to compare today's data vs yesterday's."

**DETAILED:**
"Step 1 — Isolate the layer: compare Gold totals vs Silver totals vs Bronze counts. Step 2 — Use Delta time travel to see what changed."

```python
# Compare today vs yesterday using Delta time travel
df_today     = spark.read.format("delta").load(gold_path)
df_yesterday = spark.read.format("delta") \
    .option("versionAsOf", "previous_version") \
    .load(gold_path)

df_today.agg(sum("amount")).show()
df_yesterday.agg(sum("amount")).show()
```

"Step 3 — Check git diff of the deployment — which notebook or SQL changed. Step 4 — Run the old transformation on the same input data and compare output. Step 5 — Add zero-row validation so future deployments fail loudly instead of silently producing wrong numbers."

---

### Q5. How do you handle schema evolution in a production data pipeline?

**SHORT:**
"Bronze uses mergeSchema to accept new columns. Silver validates schema against expected — alerts on new columns, fails on missing ones. Gold schema is strictly controlled — changes need PR approval."

**DETAILED:**
```python
# Bronze — accept everything
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append").save(bronze_path)

# Silver — detect and alert
expected = set(spark.read.format("delta").load(silver_path).schema.fieldNames())
incoming = set(df.schema.fieldNames())

new_cols     = incoming - expected  # New columns from source
missing_cols = expected - incoming  # Source dropped columns

if missing_cols:
    raise Exception(f"CRITICAL: Source dropped columns: {missing_cols}")
if new_cols:
    send_alert(f"New columns detected: {new_cols}")
```

"Gold schema changes require code review, Snowflake ALTER TABLE, and BI dashboard validation before deployment."

---

### Q6. Migrate a large on-premise data warehouse to cloud with minimal downtime.

**SHORT:**
"Five phases: assess, schema migrate, historical load, CDC-based incremental sync for parallel run, then cutover. Zero-downtime achieved by running old and new in parallel until validated."

**DETAILED:**
```
Phase 1 — Assess:
  Inventory all tables, row counts, data types, dependencies

Phase 2 — Schema Migration:
  Convert Oracle/Teradata DDL → Azure SQL / Delta Lake DDL
  Key mappings: NUMBER → DECIMAL, VARCHAR2 → VARCHAR, DATE → DATETIME2

Phase 3 — Historical Load:
  ADF parallel JDBC read → ADLS Bronze → Delta Silver
  Run during off-hours, business continues on old system

Phase 4 — CDC Parallel Run (2-4 weeks):
  Enable CDC on source → apply changes to Azure in real-time
  Daily reconciliation: source totals == Azure totals

Phase 5 — Cutover:
  Stop CDC → apply final delta → flip connection string
  Keep source in read-only for 30-day rollback window
```

---

### Q7. Difference between repartition and coalesce in Spark.

**SHORT:**
"Repartition does a full shuffle — can increase or decrease partitions, gives even distribution. Coalesce avoids shuffle — only decreases partitions by merging existing ones, faster but can cause uneven sizes."

**DETAILED:**
```python
df.repartition(50)          # Full shuffle — even distribution
                            # Use BEFORE wide transformations or joins

df.coalesce(10)             # No shuffle — merges existing partitions
                            # Use BEFORE writing to reduce small files

# Real example from ECDP:
# Before writing to Silver Delta — reduce 200 shuffle partitions to 10 files
df_transformed.coalesce(10).write.format("delta").save(silver_path)

# Before a skewed join — redistribute evenly
df_large.repartition(100, col("cost_center")).join(df_small, "cost_center")
```

"Rule: coalesce before write to fix small files. Repartition before joins or heavy aggregations for even data distribution."

---

### Q8. How do you optimize a SQL query running on billions of records?

**SHORT:**
"Partition pruning first, then push filters early, use columnar format, avoid functions on indexed columns, check execution plan for full scans."

**DETAILED:**
```sql
-- BAD: Function on column prevents partition pruning
WHERE YEAR(transaction_date) = 2024

-- GOOD: Range filter enables partition pruning
WHERE transaction_date >= '2024-01-01'
  AND transaction_date <  '2025-01-01'

-- BAD: SELECT * reads all columns (columnar format penalty)
SELECT * FROM gl_transactions WHERE fiscal_year = 2024

-- GOOD: Select only needed columns
SELECT doc_number, amount, cost_center
FROM gl_transactions
WHERE fiscal_year = 2024

-- For Delta Lake: ZORDER on filter columns
OPTIMIZE gl_transactions ZORDER BY (fiscal_year, cost_center);
```

"In Snowflake — check clustering depth, use result cache, separate warehouse for ETL vs reporting. In Spark — check Spark UI for full table scans, enable AQE."

---

### Q9. Multiple upstream systems send late-arriving data. How do you ensure accurate downstream reporting?

**SHORT:**
"Partition Silver by business_date not ingestion_date. Use Delta MERGE for late records so they land in correct partition. Reprocess affected Gold partitions using replaceWhere."

**DETAILED:**
```python
# Late record for 2024-01-10 arrives on 2024-01-13
# MERGE puts it in correct business_date partition

DeltaTable.forPath(spark, silver_path) \
    .alias("t").merge(
        df_late.alias("s"),
        "t.doc_number = s.doc_number AND t.line_num = s.line_num"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Recompute Gold for affected date partition only
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "business_date = '2024-01-10'") \
    .save(gold_path)
```

"Set a late arrival SLA — e.g., data older than 7 days goes to a reconciliation queue, not the main pipeline."

---

### Q10. How do you implement data validation and reconciliation between source and target?

**SHORT:**
"Compare row counts, checksums, and sum of key numeric columns between source and target after every load. Write results to a DQ table. Fail pipeline if variance exceeds threshold."

**DETAILED:**
```python
# After each load — reconciliation checks
source_count = spark.read.jdbc(url, "source_table", properties).count()
target_count = spark.read.format("delta").load(silver_path).count()

source_sum = spark.read.jdbc(url, "source_table", properties) \
    .agg(sum("amount")).collect()[0][0]
target_sum = spark.read.format("delta").load(silver_path) \
    .agg(sum("amount")).collect()[0][0]

variance_pct = abs(source_sum - target_sum) / source_sum * 100

# Write DQ results
dq_result = {
    "batch_id": batch_id,
    "source_count": source_count,
    "target_count": target_count,
    "count_match": source_count == target_count,
    "sum_variance_pct": variance_pct,
    "status": "PASS" if variance_pct < 0.01 else "FAIL"
}

if variance_pct >= 0.01:
    raise ValueError(f"Reconciliation failed: {variance_pct:.4f}% variance")
```

---

### Q11. Given a string, find the first non-repeating character.

**SHORT:**
"Use Python's OrderedDict or Counter to count frequencies, then return the first character with count = 1."

```python
from collections import Counter

def first_non_repeating(s):
    count = Counter(s)
    for char in s:
        if count[char] == 1:
            return char
    return None

print(first_non_repeating("aabbcde"))  # c
print(first_non_repeating("aabb"))     # None
```

---

### Q12. Given an array, find all pairs whose sum equals a target value.

**SHORT:**
"Use a hash set — store seen numbers, check if (target - current number) exists in the set. O(n) time complexity."

```python
def find_pairs(arr, target):
    seen = set()
    pairs = []
    for num in arr:
        complement = target - num
        if complement in seen:
            pairs.append((complement, num))
        seen.add(num)
    return pairs

print(find_pairs([1, 2, 3, 4, 5], 6))  # [(1,5), (2,4)]
```

---

## SECTION 2 — BANKING / FINANCIAL DOMAIN

---

### Q13. Design pipeline for millions of financial transactions with high accuracy and low latency.

**SHORT:**
"Event Hubs for real-time ingestion, Databricks Structured Streaming for processing with exactly-once semantics, Delta Lake MERGE for idempotent writes, reconciliation checks after every micro-batch."

**DETAILED:**
```
Transaction Source
      ↓
Event Hubs (partition by account_id for ordering)
      ↓
Databricks Structured Streaming
  - Exactly-once with checkpointing
  - Dedup on transaction_id within watermark window
  - Validate: amount > 0, account exists, currency valid
      ↓
Delta Lake Silver (MERGE on transaction_id)
      ↓
Gold (real-time balance, fraud signals, regulatory reports)
      ↓
Downstream risk systems (< 5 min latency SLA)
```

"For accuracy: idempotent writes using MERGE, reconciliation every batch comparing source event count vs Delta row count."

---

### Q14. Difference between OLTP and OLAP in banking.

**SHORT:**
"OLTP handles real-time transactions — ATM, payments, trades. OLAP handles analytical queries — risk reports, P&L, regulatory submissions. They have opposite optimization goals."

| | OLTP | OLAP |
|--|------|------|
| Purpose | Transaction processing | Analytical reporting |
| Query type | INSERT/UPDATE single rows | SELECT aggregations on millions |
| Schema | Normalised (3NF) | Denormalised (Star/Snowflake) |
| Example | Core banking system | Risk reporting warehouse |
| In banking | Trade booking, payments | Basel III reports, P&L |

---

### Q15. SQL: Accounts with transactions exceeding threshold within 24 hours.

```sql
SELECT
    account_id,
    transaction_date,
    SUM(amount) AS total_24hr
FROM transactions
WHERE transaction_date >= DATEADD(hour, -24, GETDATE())
GROUP BY account_id, transaction_date
HAVING SUM(amount) > 100000   -- threshold
ORDER BY total_24hr DESC;
```

"For fraud detection pattern — find accounts where ANY 24-hour rolling window exceeds threshold:"
```sql
SELECT DISTINCT t1.account_id
FROM transactions t1
JOIN transactions t2
    ON t1.account_id = t2.account_id
    AND t2.transaction_date BETWEEN t1.transaction_date
                                AND DATEADD(hour, 24, t1.transaction_date)
GROUP BY t1.account_id, t1.transaction_id
HAVING SUM(t2.amount) > 100000;
```

---

### Q16. Trade pipeline producing duplicate records. How do you identify and eliminate?

**SHORT:**
"Identify duplicates using GROUP BY on business key + HAVING COUNT > 1. Eliminate using ROW_NUMBER dedup in Silver and Delta MERGE on primary key to prevent re-insertion."

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Identify duplicates
df_dups = df.groupBy("trade_id") \
    .count() \
    .filter(col("count") > 1)

print(f"Duplicate trade_ids: {df_dups.count()}")

# Eliminate — keep latest by timestamp
window = Window.partitionBy("trade_id").orderBy(col("updated_at").desc())

df_dedup = df \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# Prevent future duplicates — MERGE on trade_id
DeltaTable.forPath(spark, silver_path) \
    .alias("t").merge(df_dedup.alias("s"), "t.trade_id = s.trade_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

---

### Q17. Source system changes column datatype causing pipeline failures. Prevent and handle.

**SHORT:**
"Schema validation at Bronze entry point detects type changes before they break Silver. Use mergeSchema for additive changes. Alert and halt for breaking changes like type changes."

```python
# Schema validation before processing
expected_schema = spark.read.format("delta").load(silver_path).schema

for field in df_incoming.schema.fields:
    expected_field = expected_schema[field.name] if field.name in \
        [f.name for f in expected_schema.fields] else None

    if expected_field and expected_field.dataType != field.dataType:
        raise Exception(
            f"BREAKING CHANGE: Column '{field.name}' "
            f"changed from {expected_field.dataType} "
            f"to {field.dataType}. Pipeline halted."
        )
```

"Long term: set up schema registry (or Delta constraints) so upstream teams can't change types without a formal change request process."

---

### Q18. Explain data lineage and why it matters in financial institutions.

**SHORT:**
"Data lineage tracks where data came from, how it was transformed, and where it went. In finance it's essential for regulatory compliance — auditors need to trace a P&L number back to the original trade booking system."

**DETAILED:**
"In ECDP, I used Databricks Unity Catalog for automated column-level lineage and a custom audit table for cross-system lineage."

```
Oracle (trade booking)
    ↓  [ADF Copy]
Bronze Delta (raw trades)
    ↓  [Databricks notebook: clean + validate]
Silver Delta (conformed trades)
    ↓  [Databricks notebook: aggregate]
Gold Delta (P&L summary)
    ↓  [Snowflake Connector]
Snowflake (risk reporting)
    ↓  [Power BI]
Risk Dashboard
```

"Regulators like Basel III require you to prove every number in a capital adequacy report traces back to a verified source. Without lineage, that's impossible."

---

### Q19. Downstream risk application receiving stale data. How do you investigate?

**SHORT:**
"Check pipeline audit table for last successful run time. Check if ADF trigger fired. Check Databricks job run history. Check Snowflake load history. Find exactly which layer is stale."

```python
# Investigation steps:

# Step 1 — When did Gold last update?
spark.sql("DESCRIBE HISTORY gold.risk_summary").show(3)

# Step 2 — Compare Gold timestamp vs expected freshness SLA
last_update = spark.sql("""
    SELECT MAX(loaded_at) as last_load
    FROM gold.risk_summary
""").collect()[0][0]

print(f"Last Gold update: {last_update}")
print(f"Staleness: {datetime.now() - last_update}")

# Step 3 — Check Silver → same query
# Step 4 — Check ADF Monitor for failed/skipped triggers
# Step 5 — Check if source system had delays
```

---

### Q20. Design historical data archive strategy maintaining query performance.

**SHORT:**
"Hot data (0-2 years) in Delta Lake with full indexing. Warm data (2-7 years) in Delta with ZORDER, no active partitions. Cold data (7+ years) in ADLS Archive tier. Single query interface via Delta views."

```python
# Tiering strategy
# Hot — current year, fully optimized
OPTIMIZE gold.transactions ZORDER BY (account_id, transaction_date)

# Move warm data to separate table
spark.sql("""
    INSERT INTO gold.transactions_archive
    SELECT * FROM gold.transactions
    WHERE transaction_date < '2022-01-01'
""")

spark.sql("""
    DELETE FROM gold.transactions
    WHERE transaction_date < '2022-01-01'
""")

# Unified view for queries — transparent to consumers
spark.sql("""
    CREATE OR REPLACE VIEW gold.transactions_all AS
    SELECT * FROM gold.transactions
    UNION ALL
    SELECT * FROM gold.transactions_archive
""")
```

---

### Q21. Multiple apps need same dataset but different refresh frequencies.

**SHORT:**
"Single Silver source of truth, multiple Gold marts with different refresh schedules — real-time mart via streaming, daily mart via batch, weekly mart via scheduled job."

```
Silver (single source)
    ├── Gold_Realtime  → Streaming (Event Hubs → Structured Streaming)
    │                    Consumer: Fraud detection (seconds latency)
    │
    ├── Gold_Daily     → ADF scheduled trigger (6 AM daily)
    │                    Consumer: Risk reports
    │
    └── Gold_Weekly    → ADF weekly trigger (Sunday 2 AM)
                         Consumer: Management dashboards
```

"Each Gold mart has its own SLA and pipeline. Silver is never modified by Gold processes — read-only source."

---

### Q22. Critical data load fails just before market opening. Steps to recover.

**SHORT:**
"Identify which stage failed, use pipeline state table to skip completed stages, fix the issue and rerun from failure point. If data is corrupt, use Delta time travel to restore previous good state."

```python
# Step 1 — Identify failure point
spark.sql("DESCRIBE HISTORY silver.trades").show(5)

# Step 2 — If Silver is corrupt, restore from previous version
DeltaTable.forPath(spark, silver_path).restoreToVersion(previous_good_version)

# Step 3 — Skip completed stages using state table
# (Bronze SUCCESS → skip Bronze, rerun only Silver + Gold)

# Step 4 — Validate after recovery
source_count = get_source_count()
target_count = spark.read.format("delta").load(silver_path).count()
assert source_count == target_count, "Count mismatch after recovery"

# Step 5 — Notify downstream systems of delay + ETA
send_alert("Recovery complete. Data available as of 08:45 AM")
```

---

### Q23. SQL: Daily average transaction amount per customer.

```sql
SELECT
    customer_id,
    transaction_date,
    AVG(amount)   AS daily_avg_amount,
    SUM(amount)   AS daily_total,
    COUNT(*)      AS transaction_count
FROM transactions
GROUP BY customer_id, transaction_date
ORDER BY customer_id, transaction_date;
```

"With window function for running context:"
```sql
SELECT
    customer_id,
    transaction_date,
    amount,
    AVG(amount) OVER (
        PARTITION BY customer_id, transaction_date
    ) AS daily_avg,
    AVG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg
FROM transactions;
```

---

## SECTION 3 — PYSPARK & DATABRICKS

---

### Q24. How do you handle data skewness in PySpark? Explain Salting.

**SHORT:**
"Enable AQE for automatic skew handling. For manual fix — use salting: add a random number to the join key on the large table, explode the small table to match, join on salted key, then aggregate."

**DETAILED:**
```python
# Step 1 — Enable AQE (handles skew automatically in Spark 3.x)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Step 2 — Manual Salting (when AQE is disabled or insufficient)
from pyspark.sql.functions import col, lit, rand, floor, explode, array

SALT_FACTOR = 10

# Salt the large skewed table
df_large_salted = df_large.withColumn(
    "salt_key",
    concat(col("join_key"), lit("_"), floor(rand() * SALT_FACTOR))
)

# Explode the small table to match all salt values
salt_values = [str(i) for i in range(SALT_FACTOR)]
df_small_exploded = df_small.withColumn(
    "salt_key",
    explode(array([concat(col("join_key"), lit(f"_{s}")) for s in salt_values]))
)

# Join on salted key
df_joined = df_large_salted.join(df_small_exploded, "salt_key", "inner")

# Aggregate back to original granularity
df_result = df_joined.groupBy("join_key").agg(sum("amount").alias("total"))
```

"In ECDP, one country_code had 70% of records. AQE alone dropped job time from 3 hours to 90 min. Salting further reduced it to 35 minutes."

---

### Q25. cache() vs persist() — storage levels and memory management.

**SHORT:**
"cache() is shorthand for persist(MEMORY_AND_DISK). persist() lets you specify the storage level explicitly — memory only, disk only, serialized, replicated, etc."

```python
from pyspark import StorageLevel

df.cache()
# Equivalent to:
df.persist(StorageLevel.MEMORY_AND_DISK)

# Other storage levels:
df.persist(StorageLevel.MEMORY_ONLY)          # Fast but OOM risk
df.persist(StorageLevel.DISK_ONLY)            # Slow but safe for huge DFs
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Serialized — less memory, more CPU
df.persist(StorageLevel.MEMORY_ONLY_2)        # Replicated — fault tolerant

# Always unpersist when done
df.unpersist()
```

"I use MEMORY_AND_DISK for DataFrames reused 2+ times in a pipeline. I always call unpersist() explicitly — relying on Spark's LRU eviction can cause unexpected OOM in long-running notebooks."

---

### Q26. Row-Level Deletions in Delta Lake without rewriting entire partition.

**SHORT:**
"Delta Lake's DELETE statement uses the transaction log to mark rows as deleted in specific files only — not the whole partition. Only affected data files are rewritten."

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/silver/gl_transactions")

# Delete specific rows — only rewrites files containing those rows
target.delete("doc_number = '12345' AND fiscal_year = 2024")

# For GDPR right-to-erasure — delete all records for a customer
target.delete(col("customer_id") == "CUST_9876")

# Check what was deleted using history
spark.sql("DESCRIBE HISTORY delta.`/silver/gl_transactions`").show(3)

# Verify
spark.read.format("delta").load("/silver/gl_transactions") \
    .filter("doc_number = '12345'").count()  # Should be 0
```

"Delta's file-level statistics let it skip files that can't contain the deleted rows — so a targeted delete on a small subset only touches a few Parquet files, not the whole table."

---

### Q27. Small File Problem. How does Auto-Compaction solve it?

**SHORT:**
"Small files happen when many small writes create thousands of tiny Parquet files. Auto-compaction in Databricks automatically merges them after writes without manual OPTIMIZE commands."

**DETAILED:**
```python
# Problem: 200 shuffle partitions × daily incremental = 200 tiny files per day
# After 90 days: 18,000 small files → query reads 18,000 file handles → slow

# Solution 1 — Enable Auto-compaction on the table
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Or set on table level permanently
spark.sql("""
    ALTER TABLE silver.gl_transactions
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# Solution 2 — Coalesce before writing
df.coalesce(10).write.format("delta").mode("append").save(path)

# Solution 3 — Manual OPTIMIZE (weekly maintenance)
spark.sql("OPTIMIZE silver.gl_transactions ZORDER BY (fiscal_year, cost_center)")
```

"In ECDP, after 3 months a table had 48,000 small files. Query went from 2 min to 22 min. After OPTIMIZE + enabling auto-compaction: back to 2.5 min and stayed stable."

---

### Q28. Spark UDFs vs Vectorized UDFs — performance difference.

**SHORT:**
"Regular UDFs process row-by-row, breaking Catalyst optimization and serializing data between JVM and Python. Vectorized (Pandas) UDFs use Apache Arrow to process entire batches — 10-100x faster."

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# BAD — Regular Python UDF (row-by-row, slow)
@udf(returnType=DoubleType())
def convert_amount_udf(amount, rate):
    return float(amount) * float(rate)  # Processed one row at a time

df.withColumn("amount_usd", convert_amount_udf(col("amount"), col("fx_rate")))

# GOOD — Pandas/Vectorized UDF (batch processing via Arrow)
@pandas_udf(DoubleType())
def convert_amount_vectorized(amount: pd.Series, rate: pd.Series) -> pd.Series:
    return amount * rate  # Processes entire column at once

df.withColumn("amount_usd", convert_amount_vectorized(col("amount"), col("fx_rate")))
```

"Rule: always prefer built-in Spark functions first, then Pandas UDFs, then regular UDFs as last resort. Regular UDFs disable Catalyst optimizer — they're a black box to Spark's query planner."

---

## SECTION 4 — AZURE ARCHITECTURE

---

### Q29. How to design pipeline to handle Schema Drift using ADF and Databricks.

**SHORT:**
"ADF has built-in schema drift support in Mapping Data Flows. For notebook-based pipelines, detect drift in Databricks at Bronze entry, use mergeSchema for additive changes, halt and alert for breaking changes."

```python
# In Databricks — Schema drift detection
def handle_schema_drift(df_incoming, silver_path):
    current_schema = spark.read.format("delta").load(silver_path).schema
    incoming_schema = df_incoming.schema

    current_fields = {f.name: f.dataType for f in current_schema.fields}
    incoming_fields = {f.name: f.dataType for f in incoming_schema.fields}

    new_cols     = set(incoming_fields) - set(current_fields)
    dropped_cols = set(current_fields)  - set(incoming_fields)
    type_changes = {
        col: (current_fields[col], incoming_fields[col])
        for col in set(current_fields) & set(incoming_fields)
        if current_fields[col] != incoming_fields[col]
    }

    if dropped_cols or type_changes:
        raise Exception(
            f"BREAKING SCHEMA CHANGE:\n"
            f"Dropped: {dropped_cols}\n"
            f"Type changes: {type_changes}"
        )

    if new_cols:
        log_and_alert(f"New columns: {new_cols} — adding to Bronze only")
        return df_incoming.write.format("delta") \
            .option("mergeSchema", "true").mode("append").save(silver_path)

    return df_incoming.write.format("delta").mode("append").save(silver_path)
```

---

### Q30. Checkpointing in Structured Streaming. What if checkpoint dir is deleted?

**SHORT:**
"Checkpoint stores stream progress — offsets processed, state store. If deleted, the stream loses all progress and restarts from the beginning (or earliest offset), potentially reprocessing all historical data."

```python
df_stream = spark.readStream \
    .format("eventhubs") \
    .options(**eventhub_conf) \
    .load()

df_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/silver_stream/") \
    .trigger(processingTime="1 minute") \
    .start("/silver/transactions/")

# Checkpoint directory contains:
# /checkpoints/silver_stream/
#   ├── commits/       ← which micro-batches completed
#   ├── offsets/       ← which Event Hub offsets were processed
#   └── state/         ← aggregation state (for stateful ops)
```

"If checkpoint is deleted:
- Stateless stream → reprocesses from beginning (duplicates in target)
- Stateful stream → loses all aggregation state (wrong running totals)
- Fix: always use MERGE as sink — idempotent writes handle reprocessing safely"

---

### Q31. SCD Type 2 in a streaming environment.

**SHORT:**
"Use foreachBatch to apply SCD2 MERGE logic on each micro-batch — expire old records, insert new versions. Can't do SCD2 in a single streaming write because it requires read-modify-write pattern."

```python
def apply_scd2(micro_batch_df, batch_id):
    target = DeltaTable.forPath(spark, dim_path)

    # Step 1: Expire changed records
    target.alias("t").merge(
        micro_batch_df.alias("s"),
        "t.business_key = s.business_key AND t.is_current = true"
    ).whenMatchedUpdate(
        condition="t.attribute != s.attribute",
        set={
            "is_current": "false",
            "end_date":   "current_date()"
        }
    ).execute()

    # Step 2: Insert new versions
    new_versions = micro_batch_df \
        .withColumn("is_current",     lit(True)) \
        .withColumn("effective_date", current_date()) \
        .withColumn("end_date",       lit(None).cast(DateType()))

    new_versions.write.format("delta") \
        .mode("append").save(dim_path)

# Apply to stream
df_stream.writeStream \
    .foreachBatch(apply_scd2) \
    .option("checkpointLocation", checkpoint_path) \
    .start()
```

---

### Q32. Copy Activity vs Data Flows in ADF — cost and use cases.

**SHORT:**
"Copy Activity is cheap and fast for simple source-to-sink moves with no transformation. Data Flows use Spark compute — more expensive but handle complex transformations visually."

| | Copy Activity | Mapping Data Flow |
|--|--------------|-------------------|
| Cost | Low (DIU-based) | High (Spark cluster) |
| Transformation | Minimal (column mapping only) | Complex (joins, aggregations, pivots) |
| Best for | Raw ingestion to Bronze | Moderate transformations without Databricks |
| Speed | Very fast startup | 2-3 min cluster startup overhead |
| Debugging | Limited | Full visual data preview |

"In ECDP: I use Copy Activity for Oracle → Bronze (pure move, no transform). I use Databricks notebooks for Bronze → Silver (complex PySpark logic). Data Flows only for simple reference table lookups."

---

### Q33. Infrastructure as Code for data pipelines — Azure Bicep / Terraform.

**SHORT:**
"Store ADF ARM templates, Databricks cluster configs, and ADLS policies in Git. Deploy via Azure DevOps YAML pipelines. Terraform for multi-resource provisioning, Bicep for Azure-native cleaner syntax."

```hcl
# Terraform — provision ADF + ADLS + Key Vault
resource "azurerm_data_factory" "ecdp_adf" {
  name                = "ecdp-adf-${var.env}"
  resource_group_name = var.resource_group
  location            = var.location

  identity { type = "SystemAssigned" }
}

resource "azurerm_storage_account" "ecdp_adls" {
  name                     = "ecdpstorage${var.env}"
  resource_group_name      = var.resource_group
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true   # Hierarchical namespace = ADLS Gen2
}
```

```yaml
# Azure DevOps — deploy ADF ARM template
- task: AzureResourceManagerTemplateDeployment@3
  inputs:
    deploymentScope: 'Resource Group'
    csmFile: 'arm/adf_template.json'
    csmParametersFile: 'arm/parameters.$(env).json'
    overrideParameters: '-factoryName ecdp-adf-$(env)'
```

---

## SECTION 5 — DATA MODELING & SQL

---

### Q34. Dynamic Data Masking in a Lakehouse to protect PII.

**SHORT:**
"In Databricks Unity Catalog — create column masking functions tied to user groups. Finance team sees real values, analysts see masked values. Zero application code change needed."

```sql
-- Databricks Unity Catalog
CREATE OR REPLACE FUNCTION mask_account_number(account_no STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('finance_team') THEN account_no
    ELSE CONCAT('****-****-', RIGHT(account_no, 4))
END;

ALTER TABLE silver.customers
ALTER COLUMN account_number
SET MASK mask_account_number;

-- Finance team sees: 1234-5678-9012
-- Analyst sees:      ****-****-9012

-- Snowflake equivalent
CREATE MASKING POLICY mask_pii AS (val STRING)
RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() = 'PII_ADMIN' THEN val
    ELSE '***MASKED***' END;
```

---

### Q35. Data Skipping in Delta Lake vs Traditional SQL Indexing.

**SHORT:**
"Delta collects min/max stats per column per file during write. At query time, files where max < filter value are skipped entirely — no index B-tree lookup needed, just file-level metadata comparison."

```
Traditional SQL Index:
  B-tree structure → lookup row location → fetch row
  Overhead: index maintenance on every write

Delta Lake Data Skipping:
  File stats: {min_amount: 100, max_amount: 5000}
  Query: WHERE amount > 10000
  → This file's max is 5000 < 10000 → SKIP entire file
  → Zero I/O on skipped files

-- Enable stats collection (default for first 32 columns)
ALTER TABLE silver.transactions
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '10');

-- ZORDER improves skipping by co-locating similar values
OPTIMIZE silver.transactions ZORDER BY (fiscal_year, account_id);
```

"Delta skipping works at file level — 1000 files, skip 950 = read only 50 files. Traditional indexes work at row level — still must open the file."

---

### Q36. Many-to-Many relationships in Star Schema without cartesian product.

**SHORT:**
"Introduce a bridge table between the two dimensions. Fact table joins to bridge table, bridge table joins to both dimension tables. Aggregate after joining to avoid row multiplication."

```sql
-- Bridge table handles many-to-many
-- Example: One transaction → many cost centers (split billing)

CREATE TABLE bridge_transaction_costcenter (
    transaction_id  VARCHAR(20),
    cost_center_id  VARCHAR(10),
    allocation_pct  DECIMAL(5,2)   -- 60% + 40% = 100%
);

-- Query: total spend per cost center (correct — no cartesian)
SELECT
    cc.cost_center_name,
    SUM(f.amount * b.allocation_pct / 100) AS allocated_spend
FROM fact_transactions f
JOIN bridge_transaction_costcenter b ON f.transaction_id = b.transaction_id
JOIN dim_cost_center cc              ON b.cost_center_id = cc.cost_center_id
GROUP BY cc.cost_center_name;
```

---

### Q37. Broadcast Hints — impact on worker memory, when to avoid.

**SHORT:**
"Broadcast sends a full copy of the small table to every executor. If the table is larger than executor memory, it causes OOM. Avoid broadcasting tables > 200MB or when executor memory is constrained."

```python
from pyspark.sql.functions import broadcast

# GOOD — small lookup table (< 10MB)
df_result = large_df.join(broadcast(small_lookup), "key")

# BAD — table grew over time, now 500MB
# This will cause OOM on executors
df_result = large_df.join(broadcast(medium_df), "key")

# Check before broadcasting
table_size_mb = small_df.count() * len(small_df.columns) * 8 / (1024*1024)
print(f"Table size estimate: {table_size_mb:.1f} MB")

if table_size_mb < 100:
    df_result = large_df.join(broadcast(small_df), "key")
else:
    # Let Spark decide or use sort-merge join
    df_result = large_df.join(small_df, "key")

# Control threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50MB
```

---

### Q38. Monitor pipeline failures and alerting with Azure Monitor + Log Analytics.

**SHORT:**
"ADF sends diagnostic logs to Log Analytics workspace. Write KQL queries to detect failures. Set up alert rules that trigger Action Groups — email, Teams, PagerDuty — when failures occur."

```
Setup:
ADF → Diagnostic Settings → Log Analytics Workspace
                          → Activity Runs Logs
                          → Pipeline Runs Logs
                          → Trigger Runs Logs
```

```kql
-- KQL Query in Log Analytics — find pipeline failures in last 24 hours
ADFPipelineRun
| where TimeGenerated > ago(24h)
| where Status == "Failed"
| project TimeGenerated, PipelineName, RunId, FailureType, Message
| order by TimeGenerated desc
```

```kql
-- Alert: pipeline failed more than 2 times in 1 hour
ADFPipelineRun
| where Status == "Failed"
| summarize FailureCount = count() by PipelineName, bin(TimeGenerated, 1h)
| where FailureCount > 2
```

```python
# In Databricks — send custom metric to Log Analytics
import requests, json

def send_log_analytics_alert(workspace_id, shared_key, pipeline_name, error):
    body = json.dumps([{
        "PipelineName": pipeline_name,
        "ErrorMessage": error,
        "Severity": "Critical",
        "TimeGenerated": datetime.utcnow().isoformat()
    }])
    # POST to Log Analytics HTTP Data Collector API
    requests.post(
        f"https://{workspace_id}.ods.opinsights.azure.com/api/logs",
        data=body,
        headers={"Log-Type": "PipelineAlerts"}
    )
```

"Action Group then routes the alert to email, Teams webhook, or PagerDuty depending on severity."

---

### Q39. Azure IR vs Self-Hosted IR.

**SHORT:**
"Azure IR runs in Microsoft's cloud — for cloud-to-cloud connections. Self-Hosted IR runs on your own VM — for on-premises sources behind a firewall that Azure can't reach directly."

| | Azure IR | Self-Hosted IR |
|--|----------|---------------|
| Location | Microsoft managed cloud | Your VM / on-prem server |
| Use for | ADLS, Azure SQL, REST APIs | Oracle on-prem, SQL Server on-prem, SAP |
| Setup | Zero setup | Install agent on VM, register with ADF |
| HA | Built-in | Install on 2+ VMs for HA |
| Cost | Per DIU-hour | VM cost you manage |

"In ECDP — Azure IR for all ADLS and Azure SQL activities. Self-Hosted IR installed on a dedicated VM for Oracle on-prem JDBC connections."

---

### Q40. Pipeline stuck in Queued state — top 3 things to check.

**SHORT:**
"1. Integration Runtime capacity — all nodes busy. 2. Concurrent run limit hit — pipeline set to max 1 concurrent run. 3. Trigger overlap — previous run still active when next trigger fired."

```
Check 1 — IR Capacity:
  ADF Monitor → Integration Runtimes → check active sessions vs max
  Fix: increase max parallel jobs on IR or scale up SHIR nodes

Check 2 — Concurrency Setting:
  ADF Pipeline Settings → Concurrency = 1 (only 1 run allowed at a time)
  Fix: increase concurrency or check why previous run is still active

Check 3 — Trigger Overlap:
  If pipeline takes 3 hours but trigger fires every 2 hours
  → Queue builds up indefinitely
  Fix: increase trigger interval or enable "Depend on last" trigger option
  in ADF to skip if previous run is still active
```

---

### Q41. Star Schema vs Snowflake Schema.

**SHORT:**
"Star Schema — dimensions are denormalised, single join to fact, fast queries. Snowflake Schema — dimensions are normalised into sub-dimensions, more joins needed, saves storage. Use Star for reporting speed, Snowflake for data integrity."

```
Star Schema:
  Fact_Sales → Dim_Product    (single table)
             → Dim_Customer   (single table)
             → Dim_Date       (single table)
  Joins: 3 joins maximum

Snowflake Schema:
  Fact_Sales → Dim_Product → Dim_ProductCategory → Dim_ProductBrand
  Joins: 5+ joins
```

"In ECDP and FinOps — I used Star Schema for the Snowflake reporting layer. Executives run simple SQL. More joins = slower queries = unhappy stakeholders."

---

### Q42. Second most recent login for every user using window functions.

```sql
SELECT user_id, login_time AS second_recent_login
FROM (
    SELECT
        user_id,
        login_time,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY login_time DESC
        ) AS rn
    FROM user_logins
) ranked
WHERE rn = 2;
```

"Use ROW_NUMBER not RANK here — if a user logged in twice at the exact same time, RANK would give both rank 1, skipping rank 2. ROW_NUMBER always gives distinct sequential numbers."

---

### Q43. MERGE statement vs UPSERT.

**SHORT:**
"MERGE is a SQL standard statement that handles INSERT, UPDATE, and DELETE in one atomic operation. UPSERT is a concept — insert if not exists, update if exists. MERGE implements UPSERT plus DELETE in one statement."

```sql
-- MERGE — one statement, handles all three operations
MERGE INTO target_table AS t
USING source_table AS s
ON t.id = s.id

WHEN MATCHED AND t.status != s.status
    THEN UPDATE SET t.status = s.status, t.updated_at = GETDATE()

WHEN NOT MATCHED BY TARGET
    THEN INSERT (id, name, status) VALUES (s.id, s.name, s.status)

WHEN NOT MATCHED BY SOURCE   -- Row in target but not in source
    THEN DELETE;             -- This is what UPSERT alone can't do

-- Delta Lake MERGE (same concept)
DeltaTable.forPath(spark, path) \
    .alias("t").merge(source_df.alias("s"), "t.id = s.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

---

### Q44. How do you explain a technical bottleneck (data lag) to a non-technical client?

**SHORT:**
"Use an analogy — never use technical terms. Translate to business impact. State the fix and timeline. End with what you're doing to prevent recurrence."

**SAY THIS:**

*"Think of our data pipeline like a highway connecting your source system to your reports. Right now, there's a traffic jam at one section — the overnight data load is taking longer than expected because the volume of transactions grew 3x this month compared to last month, and we hadn't scaled the road width accordingly.*

*The impact is: your morning reports are showing data that's about 2 hours old instead of 30 minutes old.*

*We've already widened the road — added more processing capacity — and the lag will be back to normal by tomorrow morning. Going forward, we've set up automatic road-widening that kicks in when traffic exceeds a threshold, so this won't happen again."*

---

## QUICK CHEAT SHEET

| Topic | Key Line to Remember |
|-------|---------------------|
| Skew | AQE first, salting for manual fix, 5 cores/executor rule |
| cache vs persist | cache = MEMORY_AND_DISK, persist = you choose the level |
| Delta DELETE | File-level rewrite only, not full partition |
| Small files | Auto-compaction + coalesce before write + weekly OPTIMIZE |
| UDF vs Pandas UDF | 10-100x faster, Arrow batch processing, still slower than built-ins |
| Schema drift | mergeSchema for additive, halt+alert for breaking |
| Checkpoint deleted | Stream restarts from beginning, use MERGE sink for safety |
| SCD2 streaming | foreachBatch → expire old → insert new version |
| Copy vs Data Flow | Copy = cheap + fast + no transform, Data Flow = expensive + Spark |
| Broadcast hint | Avoid if table > 100-200MB, causes executor OOM |

---

*Interview script — All questions covered — PySpark | Databricks | Azure | SQL*
