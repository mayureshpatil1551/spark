# 🎯 Spark Interview Scripts — Short + Detailed Answers
> **Mayuresh Patil | Azure DE | PySpark · Databricks · Delta Lake · Iceberg**
> Format: Quick Answer (2–3 lines) + Detailed Script (5–7 lines) + Code

---

# 🔷 SECTION 1 — PySpark & Databricks Optimization

---

## Q1. Design a data pipeline to process millions of financial transactions with high accuracy and low latency.

### ⚡ Quick Answer (Speak in 30 sec)
> *"I'd use a Medallion Architecture — Raw → Trusted → Curated. Ingest in micro-batches via ADF into ADLS Gen2, transform with PySpark on Databricks using partitioning on transaction_date and Z-ordering on account_id. Write to Delta Lake with MERGE for accuracy."*

### 📋 Detailed Script
> *"For financial transactions at scale, I follow a layered design.*
>
> *Raw Layer: ingest transactions from source via ADF into ADLS Gen2 as Parquet or Delta — no transformation, full history preserved.*
>
> *Trusted Layer: PySpark jobs on Databricks apply DQ checks — null checks, duplicate detection using ROW_NUMBER(), range validation for amounts. Bad records go to a quarantine table, not dropped.*
>
> *Curated Layer: business aggregations — daily totals, account summaries — written to Delta Lake, partitioned by transaction_date, Z-ordered on account_id for fast lookups.*
>
> *For low latency: enable AQE, use broadcast joins for small reference tables, and use incremental MERGE instead of full reloads. On my FinOps project, this pattern reduced pipeline execution time by 30% on 26TB of financial data."*

```python
# Partition + Z-order for financial transactions
df.write.format("delta") \
    .partitionBy("transaction_date") \
    .mode("overwrite") \
    .save(target_path)

spark.sql("OPTIMIZE schema.transactions ZORDER BY (account_id, txn_type)")
```

**One-Liner:** *"Medallion + partitioning + Z-ordering + MERGE = accurate, low-latency financial pipeline."*

---

## Q2. SQL — Customers who placed orders on consecutive days.

### ⚡ Quick Answer
> *"Use LAG() window function to get the previous order date per customer, then filter where the difference is exactly 1 day."*

### 📋 Detailed Script
> *"Consecutive days = current order_date minus previous order_date equals 1. I use LAG() to get the previous order date within each customer's partition."*

```sql
WITH order_gaps AS (
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
FROM order_gaps
WHERE DATEDIFF(order_date, prev_order_date) = 1;
```

**One-Liner:** *"LAG() to get previous order date → DATEDIFF = 1 → consecutive days found."*

---

## Q3. ROW_NUMBER() vs RANK() vs DENSE_RANK()

### ⚡ Quick Answer
> *"ROW_NUMBER gives unique numbers always. RANK skips numbers after ties. DENSE_RANK doesn't skip after ties. I use ROW_NUMBER for deduplication and DENSE_RANK for business rankings."*

### 📋 Detailed Script

| Function | Scores: 100,100,90,80 | Notes |
|---|---|---|
| ROW_NUMBER() | 1, 2, 3, 4 | Always unique — arbitrary tiebreak |
| RANK() | 1, 1, 3, 4 | Gaps after ties |
| DENSE_RANK() | 1, 1, 2, 3 | No gaps after ties |

```sql
SELECT
    customer_id, revenue,
    ROW_NUMBER()  OVER (PARTITION BY region ORDER BY revenue DESC) AS row_num,
    RANK()        OVER (PARTITION BY region ORDER BY revenue DESC) AS rnk,
    DENSE_RANK()  OVER (PARTITION BY region ORDER BY revenue DESC) AS dense_rnk
FROM sales;
```

> *"In my pipelines, I use ROW_NUMBER() to deduplicate CDC records — keep the latest record per primary key. I use DENSE_RANK() for business leaderboards where ties should get the same rank with no number skipped."*

**One-Liner:** *"ROW_NUMBER for dedup; DENSE_RANK for rankings where ties matter."*

---

## Q4. Daily sales reports incorrect after deployment. How do you investigate?

### ⚡ Quick Answer
> *"Check what changed in the deployment — schema, business logic, or filter conditions. Use Delta time travel to compare current vs yesterday's data. Trace from Gold back to Bronze to find where numbers diverge."*

### 📋 Detailed Script
> *"Structured diagnosis:*
>
> *Step 1: What changed in the deployment? Pipeline logic, SQL joins, partition columns, DQ filters?*
>
> *Step 2: Use Delta time travel to compare the Gold table before and after the deployment:*

```sql
-- Compare today vs before deployment
SELECT 'current', SUM(revenue) FROM gold.daily_sales
UNION ALL
SELECT 'pre-deploy', SUM(revenue) FROM gold.daily_sales
TIMESTAMP AS OF '2024-01-14 18:00:00';
```

> *Step 3: Trace backwards — Gold → Silver → Bronze. Where does the number diverge? That's the layer with the bug.*
>
> *Step 4: Check audit logs — how many rows were written at each layer? If Bronze = 1M but Silver = 800K, the Silver DQ filter is dropping too many records.*
>
> *Step 5: Fix, reprocess affected partitions using replaceWhere, validate row counts, redeploy."*

**One-Liner:** *"Time travel to compare versions + layer-by-layer row count audit = find root cause fast."*

---

## Q5. How would you handle schema evolution in a production pipeline?

### ⚡ Quick Answer
> *"Three layers of defense: mergeSchema=true for new columns, drift detection to alert on changes, and Bronze raw layer as a reprocessing safety net."*

### 📋 Detailed Script

```python
# Layer 1: mergeSchema handles new columns automatically
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append").save(target_path)

# Layer 2: Drift detection — alert before writing
expected = {"patient_id", "amount", "txn_date", "account_id"}
actual   = set(df.columns)
new_cols     = actual - expected
missing_cols = expected - actual

if new_cols:     log_alert(f"New columns: {new_cols}")
if missing_cols: df = df.withColumn(next(iter(missing_cols)), F.lit(None))

# Layer 3: Bronze raw = always saved as-is for reprocessing
df_raw.write.mode("append").parquet(bronze_path)  # No schema enforcement here
```

> *"On my eCDP pharma project, Salesforce API objects occasionally added new fields. With mergeSchema=true, these were captured without pipeline failure. I also added an alert so the team knows when drift happens — not just silently swallowed."*

**One-Liner:** *"mergeSchema + drift alert + Bronze raw = resilient schema evolution without silent failures."*

---

## Q6. Migrate large on-premise data warehouse to cloud with minimal downtime.

### ⚡ Quick Answer
> *"Mirror in parallel, validate with reconciliation checks, cut over table by table, keep legacy as fallback for 30 days."*

### 📋 Detailed Script
> *"On my FinOps project, I migrated 26TB of SAP financial data to Azure. The approach:*
>
> ***Phase 1 — Mirror:*** Run new cloud pipeline parallel to on-prem. Both receive writes. Never touch production.*
>
> ***Phase 2 — Validate:*** Automated reconciliation — compare row counts, checksums, and sample spot-checks between on-prem and cloud at every Medallion layer.*

```python
# Reconciliation check
src_count  = spark.read.jdbc(oracle_url, table).count()
tgt_count  = spark.read.format("delta").load(target_path).count()
match      = src_count == tgt_count
log_audit(table, src_count, tgt_count, "MATCH" if match else "MISMATCH")
```

> ***Phase 3 — Cutover:*** Table by table — not all at once. Monitor each for 24–48 hours.*
>
> ***Phase 4 — Decommission:*** Legacy stays live 30 days as fallback, then retired.*
>
> *Result: zero data loss, zero downtime — business users never noticed the switch."*

**One-Liner:** *"Mirror → Validate → Cutover table-by-table → 30-day fallback = zero-downtime migration."*

---

## Q7. ⭐ repartition() vs coalesce()

### ⚡ Quick Answer
> *"repartition always shuffles — can increase or decrease partitions, evenly balanced. coalesce only decreases — avoids shuffle by merging locally, faster but may be uneven."*

### 📋 Detailed Script

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Always | Avoids (local merge) |
| Can increase partitions | ✅ Yes | ❌ No |
| Output balance | Even | May be uneven |
| Plan shows | `Exchange` | `Coalesce` (no Exchange) |
| Use case | More parallelism, re-key by column | Reduce files before write |

```python
# repartition — always shuffles, balanced output
df.repartition(200, "account_id")  # HashPartitioning on account_id

# coalesce — no shuffle, merges locally
df.coalesce(10).write.parquet("s3://output/")  # Fewer output files
```

> *"In my FinOps pipeline on 26TB financial data, I repartition by transaction_date before heavy joins so matching keys co-locate. After aggregations, I coalesce before writing to reduce small output files."*

**One-Liner:** *"repartition for parallelism and rebalancing; coalesce for cheap partition reduction before write."*

---

## Q8. ⭐ Optimize a SQL query running on billions of records.

### ⚡ Quick Answer
> *"Enable partition pruning on date columns, replace correlated subqueries with JOINs, broadcast small tables, and use Delta Z-ordering on high-cardinality filter columns."*

### 📋 Detailed Script
> *"Five-step optimization approach:*
>
> *1. EXPLAIN first — find full scans, missing partition pruning, Sort-Merge Joins on small tables.*
> *2. Filter early — push WHERE conditions before JOINs, not after.*
> *3. Replace correlated subqueries with pre-aggregated JOINs — O(n) not O(n²).*
> *4. Broadcast small tables — no shuffle on the large side.*
> *5. Z-ordering — co-locate related data within Parquet files for better data skipping.*

```sql
-- Before: correlated subquery (slow)
SELECT * FROM transactions t
WHERE amount > (SELECT AVG(amount) FROM transactions WHERE region = t.region);

-- After: pre-aggregated JOIN (fast)
SELECT t.*
FROM transactions t
JOIN (SELECT region, AVG(amount) avg_amt FROM transactions GROUP BY region) r
ON t.region = r.region
WHERE t.amount > r.avg_amt;
```

> *"On the FinOps project, Z-ordering on account_id and transaction_date cut query latency by 40% across 300+ business reports on 26TB of Delta Lake data."*

**One-Liner:** *"EXPLAIN → partition prune → broadcast small tables → Z-order → replace subqueries with JOINs."*

---

## Q9. Late-arriving data — ensure accurate downstream reporting.

### ⚡ Quick Answer
> *"Use watermarking to define how late data is accepted. MERGE handles late records correctly in the target. Reprocess affected partitions on a schedule if needed."*

### 📋 Detailed Script

```python
# Structured Streaming — watermark accepts data up to 2 hours late
df_stream = df_raw_stream \
    .withWatermark("event_time", "2 hours") \
    .groupBy(
        F.window("event_time", "1 hour"),
        "account_id"
    ).agg(F.sum("amount").alias("hourly_total"))
```

> *"For batch pipelines, I store a reprocessing window — typically 3 days. Any late record with an event_date in the last 3 days triggers a partition-level reprocess using replaceWhere.*
>
> *For downstream reports, I add a `report_as_of_timestamp` column — the report clearly states when it was generated, so stakeholders know late arrivals after that time are in the next run.*
>
> *On the eCDP pharma project with 10M+ daily records, some API sources sent data 2–6 hours late. My watermark + replaceWhere pattern handled this without needing full reloads."*

**One-Liner:** *"Watermark for streaming; replaceWhere for batch reprocessing; report_as_of_timestamp for stakeholder transparency."*

---

## Q10. Data validation and reconciliation between source and target.

### ⚡ Quick Answer
> *"Compare row counts, checksums, and key business metrics between source and target after every pipeline run. Log results to an audit table. Alert if mismatch exceeds threshold."*

### 📋 Detailed Script

```python
def reconcile(source_df, target_df, key_col, amount_col, table_name):
    src_count  = source_df.count()
    tgt_count  = target_df.count()
    src_sum    = source_df.agg(F.sum(amount_col)).collect()[0][0]
    tgt_sum    = target_df.agg(F.sum(amount_col)).collect()[0][0]

    count_match  = src_count == tgt_count
    sum_match    = abs(src_sum - tgt_sum) < 0.01  # Float tolerance

    # Find records in source but missing in target
    missing = source_df.join(target_df, key_col, "left_anti").count()

    result = {
        "table": table_name,
        "src_count": src_count, "tgt_count": tgt_count,
        "count_match": count_match,
        "src_sum": src_sum, "tgt_sum": tgt_sum,
        "sum_match": sum_match,
        "missing_records": missing,
        "status": "PASS" if (count_match and sum_match and missing == 0) else "FAIL"
    }
    log_audit(result)
    if result["status"] == "FAIL":
        send_alert(f"Reconciliation FAILED for {table_name}")
    return result
```

> *"I run reconciliation after every pipeline run in the FinOps project — comparing Oracle source counts vs Delta Lake target counts. Any mismatch triggers a PagerDuty alert. This is how I maintain 99.9% data accuracy on financial reports."*

**One-Liner:** *"Count + sum + left_anti join = three-layer reconciliation that catches every class of data loss."*

---

## Q11. Python — First non-repeating character in a string.

### ⚡ Quick Answer
> *"Use an OrderedDict to count character frequencies while preserving order. Return the first character with count = 1."*

```python
from collections import OrderedDict

def first_non_repeating(s: str) -> str:
    counts = OrderedDict()
    for ch in s:
        counts[ch] = counts.get(ch, 0) + 1
    for ch, count in counts.items():
        if count == 1:
            return ch
    return ""  # All characters repeat

# Test
print(first_non_repeating("aabccd"))  # → 'b'
print(first_non_repeating("aabb"))    # → ''
```

**One-Liner:** *"OrderedDict preserves insertion order — first key with count=1 is the answer. O(n) time, O(1) space (26 chars max)."*

---

## Q12. Python — Find all pairs whose sum equals a target.

```python
def find_pairs(arr: list, target: int) -> list:
    seen   = set()
    result = set()
    for num in arr:
        complement = target - num
        if complement in seen:
            result.add((min(num, complement), max(num, complement)))
        seen.add(num)
    return list(result)

# Test
print(find_pairs([1, 5, 3, 7, 2, 8], 10))  # → [(3,7), (2,8)]
print(find_pairs([1, 1, 2, 3], 4))          # → [(1,3)]
```

**One-Liner:** *"Hash set for O(n) lookup — for each number, check if target-number exists in seen set. Use set for result to avoid duplicate pairs."*

---

---

# 🔷 SECTION 2 — FINANCIAL / BANKING QUESTIONS

---

## Q13. Identify accounts with transactions exceeding threshold in 24 hours.

```sql
SELECT
    account_id,
    txn_date,
    SUM(amount) AS total_24h,
    COUNT(*)    AS txn_count
FROM transactions
WHERE txn_date >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
GROUP BY account_id, txn_date
HAVING SUM(amount) > 50000  -- threshold
ORDER BY total_24h DESC;
```

**One-Liner:** *"GROUP BY account + 24-hour window + HAVING threshold = straightforward fraud detection query."*

---

## Q14. Trade pipeline producing duplicate records — identify and eliminate.

### ⚡ Quick Answer
> *"Use ROW_NUMBER() to find duplicates. MERGE INTO the target ensures idempotency. For existing duplicates, use replaceWhere to overwrite affected partitions with deduplicated data."*

```python
from pyspark.sql.window import Window

# Step 1: Deduplicate incoming CDC batch
window = Window.partitionBy("trade_id").orderBy(F.desc("updated_at"))
df_deduped = df_cdc \
    .withColumn("rn", F.row_number().over(window)) \
    .filter(F.col("rn") == 1).drop("rn")

# Step 2: MERGE ensures idempotency — won't create new duplicates
delta_table.alias("t").merge(
    df_deduped.alias("s"),
    "t.trade_id = s.trade_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

## Q15. Source column datatype changes — prevent and handle.

### ⚡ Quick Answer
> *"Add schema validation before writing. Catch TypeMismatch errors and send alerts. Use a schema registry or store expected schema in config."*

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Expected schema stored in config
expected_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("amount",     DoubleType(), False),
    StructField("txn_date",   StringType(), True)
])

def validate_schema(df, expected):
    actual_types = {f.name: f.dataType for f in df.schema.fields}
    for field in expected:
        actual_type = actual_types.get(field.name)
        if actual_type != field.dataType:
            raise TypeError(
                f"Schema mismatch on '{field.name}': "
                f"expected {field.dataType}, got {actual_type}"
            )
    return True

try:
    validate_schema(df_source, expected_schema)
except TypeError as e:
    send_alert(str(e))
    df_source = df_source.withColumn("amount", F.col("amount").cast("double"))
```

**One-Liner:** *"Schema contract validation before write + auto-cast with alert = graceful handling without pipeline crash."*

---

## Q16. Data lineage — why important in financial institutions?

### ⚡ Quick Answer
> *"Data lineage tracks where data came from, how it was transformed, and where it went. In finance, it's required for regulatory audits, debugging wrong numbers, and impact analysis when source systems change."*

### 📋 Detailed Script
> *"In financial institutions, regulators (like RBI, SEC, FINRA) require proof that reported numbers are accurate and traceable. Lineage answers: where did this trade record come from? What transformations were applied? Who approved the data?*
>
> *In my FinOps project, I implemented lineage through:*
> *1. Audit columns — every table has `source_system`, `ingestion_timestamp`, `pipeline_run_id`*
> *2. Modak Nabu audit logging — every pipeline run records rows_read, rows_written, source, target*
> *3. Unity Catalog — tracks which notebook wrote which Delta table, with full query history*
>
> *When a CFO says 'these Q3 numbers look wrong', lineage lets me trace the number back to its source record in 10 minutes instead of 10 days."*

**One-Liner:** *"Lineage = full paper trail from source to report — mandatory for financial audits and debugging wrong numbers."*

---

## Q17. Calculate daily average transaction amount per customer.

```sql
SELECT
    customer_id,
    CAST(txn_datetime AS DATE)   AS txn_date,
    COUNT(*)                     AS txn_count,
    SUM(amount)                  AS daily_total,
    AVG(amount)                  AS daily_avg,
    MAX(amount)                  AS max_txn,
    MIN(amount)                  AS min_txn
FROM transactions
GROUP BY customer_id, CAST(txn_datetime AS DATE)
ORDER BY customer_id, txn_date;
```

---

---

# 🔷 SECTION 3 — PYSPARK, CLOUD, SQL, SOFT SKILLS

---

## Q18. ⭐ Data Skewness in PySpark — Salting technique.

### ⚡ Quick Answer
> *"Skew = one key has millions of rows, one executor stalls. Fix with: broadcast join for small tables, AQE skewJoin for automatic splitting, or manual salting to distribute skewed keys across partitions."*

### 📋 Detailed Script

```python
# Manual Salting — distribute skewed keys
SALT_NUMBER = 50

# Step 1: Add random salt to large (skewed) table
df_large = df_large.withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))

# Step 2: Explode small table with all salt values
df_small = df_small \
    .withColumn("salt_values", F.array([F.lit(i) for i in range(SALT_NUMBER)])) \
    .withColumn("salt", F.explode("salt_values")) \
    .drop("salt_values")

# Step 3: Join on (key + salt) — now distributes evenly
df_result = df_large.join(df_small, ["join_key", "salt"], "inner").drop("salt")

# AQE alternative (Spark 3.x) — automatic
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

> *"On the eCDP project, one facility_id had 40% of all records. Broadcast join on the reference table eliminated the skew completely. For aggregations, I used two-phase salted groupBy."*

**One-Liner:** *"Broadcast for small tables; AQE for moderate skew; manual salting for extreme skew — know all three."*

---

## Q19. transform() vs udf() — which is more efficient?

### ⚡ Quick Answer
> *"transform() applies a custom function to DataFrame columns using native Spark operations — still optimized by Catalyst. UDFs are black boxes — Catalyst can't optimize them and Python UDFs require expensive JVM-to-Python serialization per row."*

### 📋 Detailed Script

```python
# UDF — slow: row-by-row, JVM↔Python serialization, no Catalyst optimization
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def classify_amount(amount):
    if amount > 10000: return "high"
    elif amount > 1000: return "medium"
    else: return "low"

df.withColumn("category", classify_amount(F.col("amount")))  # Slow

# Native Spark — fast: Tungsten optimized, Catalyst optimized
df.withColumn("category",
    F.when(F.col("amount") > 10000, "high")
     .when(F.col("amount") > 1000,  "medium")
     .otherwise("low")
)  # Always prefer this

# Pandas UDF (Vectorized) — if UDF is unavoidable
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def classify_vectorized(amounts: pd.Series) -> pd.Series:
    return amounts.apply(lambda x: "high" if x > 10000 else "medium" if x > 1000 else "low")
```

**One-Liner:** *"Native Spark functions first. If UDF needed, use Pandas UDF — vectorized batch processing instead of row-by-row."*

---

## Q20. Azure Integration Runtime vs Self-Hosted IR.

### ⚡ Quick Answer
> *"Azure IR runs in Azure's managed cloud — for cloud-to-cloud connections. Self-Hosted IR runs on your on-premises machine — for connecting to on-prem databases behind a firewall."*

| | Azure IR | Self-Hosted IR |
|---|---|---|
| Location | Azure managed cloud | Your on-prem / private VNet |
| Use case | Cloud → Cloud (S3, ADLS, Blob) | On-prem Oracle, SQL Server |
| Maintenance | Zero — Microsoft managed | You manage it |
| Firewall | Not behind your firewall | Inside your network |

> *"On the FinOps project, I used Azure IR for ADLS Gen2 and Azure SQL connections. For Oracle on-premises ingestion in the eCDP project, we had a Self-Hosted IR installed on a VM inside the network perimeter."*

**One-Liner:** *"Azure IR for cloud; Self-Hosted IR for on-prem or private network sources — key for hybrid architectures."*

---

## Q21. Pipeline stuck in Queued state — top 3 checks.

### ⚡ Quick Answer
> *"1. Integration Runtime capacity — all IRs busy. 2. Concurrent pipeline limits hit. 3. Upstream trigger or dependency not resolved."*

### 📋 Detailed Script
> *"Three things I check in order:*
>
> *1. **Integration Runtime concurrency** — Azure IR has a max concurrent runs limit. If 10 pipelines are running on the same IR, the 11th queues. Fix: increase IR max concurrent jobs or add another IR.*
>
> *2. **ADF pipeline concurrency setting** — each pipeline has a `Concurrency` property. If set to 1, a second trigger waits for the first to finish. Fix: increase concurrency or change trigger to not overlap.*
>
> *3. **Trigger dependency or tumbling window** — if a tumbling window trigger has a dependency on another trigger, it waits for that upstream window to close. Check trigger monitor for blocking dependency."*

**One-Liner:** *"IR capacity → pipeline concurrency setting → trigger dependency — check in this order."*

---

## Q22. Secrets management in production pipeline.

### ⚡ Quick Answer
> *"Azure Key Vault stores secrets. Databricks accesses them via a secret scope backed by Key Vault using dbutils.secrets.get(). ADF reads them via Linked Service with Key Vault reference. Zero secrets in code or YAML."*

```python
# Databricks — never hardcode passwords
jdbc_pass = dbutils.secrets.get(scope="prod-keyvault", key="oracle-password")
api_token = dbutils.secrets.get(scope="prod-keyvault", key="sf-api-token")

# Use in connection
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("password", jdbc_pass) \  # From Key Vault
    .load()
```

**One-Liner:** *"Key Vault + Databricks secret scope = credentials never touch code, logs, or Git history."*

---

## Q23. Star Schema vs Snowflake Schema — when to choose?

### ⚡ Quick Answer
> *"Star schema has denormalized dimensions — fewer joins, faster queries, ideal for OLAP. Snowflake schema normalizes dimensions into sub-tables — less storage, more joins, better for complex hierarchies."*

| | Star Schema | Snowflake Schema |
|---|---|---|
| Dimensions | Denormalized (flat) | Normalized (split) |
| Joins | Fewer (fact + dims) | More (dim + sub-dims) |
| Query speed | Faster | Slower |
| Storage | More | Less |
| Best for | OLAP dashboards, BI | Complex hierarchies |

> *"In the FinOps project, I used Star Schema for financial reporting — fact table with transaction metrics, dimension tables for date, account, and cost center. Fewer joins = faster query for the 300+ business reports."*

**One-Liner:** *"Star for read-heavy BI queries; Snowflake when storage matters or dimensions have deep hierarchies."*

---

## Q24. Second most recent login per user — window function.

```sql
SELECT user_id, login_time AS second_most_recent_login
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

**One-Liner:** *"ROW_NUMBER() DESC per user → filter rn=2 → second most recent. rn=1 is most recent."*

---

## Q25. MERGE statement vs UPSERT.

### ⚡ Quick Answer
> *"MERGE is the SQL standard — handles INSERT, UPDATE, and DELETE in one statement based on conditions. UPSERT is typically just INSERT-or-UPDATE (no DELETE). MERGE is more powerful."*

```sql
-- MERGE: handles insert + update + optional delete
MERGE INTO target_table AS t
USING source_data AS s
ON t.account_id = s.account_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
    UPDATE SET t.balance = s.balance, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (account_id, balance, updated_at)
    VALUES (s.account_id, s.balance, s.updated_at)
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;  -- This is what UPSERT can't do
```

> *"I use Delta Lake MERGE heavily — it's idempotent, handles CDC correctly, and supports soft deletes. This is the core of every incremental load in my FinOps and eCDP pipelines."*

**One-Liner:** *"MERGE = UPSERT + DELETE in one atomic statement — the production-grade pattern for CDC and incremental loads."*

---

## Q26. Explain a technical bottleneck to a non-technical client.

### ⚡ Quick Answer
> *"Translate bottleneck into business impact, state the cause in one simple sentence, and give a timeline — never use jargon."*

### 📋 Script
> *"I'd say something like: 'Your daily report is currently arriving 2 hours late because one of the upstream data sources is sending records slower than expected — like a traffic jam on a highway. We've identified the exact point of congestion and are implementing a fix that will be live by tomorrow morning. Going forward, we'll add an early-warning alert so you know 30 minutes in advance if this happens again.'*
>
> *Key principles:*
> *— Business impact first (2-hour delay)*
> *— Simple analogy for the cause (traffic jam)*
> *— Concrete fix timeline (tomorrow morning)*
> *— Prevention measure (30-min alert)*
> *Never say: 'We have a Spark shuffle skew on the join stage causing executor stall.'"*

**One-Liner:** *"Business impact → simple analogy → fix timeline → prevention. No jargon. Ever."*

---

---

# 🔷 SECTION 4 — ADVANCED SPARK & DATABRICKS

---

## Q27. ⭐ Skew Join optimization in Spark 3.x — manual if AQE disabled.

### ⚡ Quick Answer
> *"Spark 3 AQE automatically splits skewed partitions. If AQE is disabled, apply manual salting — add random salt to join keys so skewed rows spread across partitions."*

```python
# AQE approach (Spark 3.x)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# Manual salting (AQE disabled)
N = 50
df_skewed  = df_large.withColumn("salt", (F.rand() * N).cast("int"))
df_small   = df_small \
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)])))

df_result = df_skewed.join(df_small, ["key", "salt"]).drop("salt")
```

**One-Liner:** *"AQE handles skew automatically in Spark 3. Without AQE — add random salt to join key, explode small table, join on composite key."*

---

## Q28. cache() vs persist() — storage level defaults.

### ⚡ Quick Answer
> *"cache() is shorthand for persist(MEMORY_AND_DISK_DESER) — no configuration. persist() lets you choose storage level: MEMORY_ONLY, DISK_ONLY, OFF_HEAP, etc."*

| | `cache()` | `persist(level)` |
|---|---|---|
| Default level | MEMORY_AND_DISK_DESER | Configurable |
| Serialized | No (JVM objects) | Depends on level |
| Flexibility | None | Full control |
| Use when | Quick reuse | Fine-grained memory control |

```python
from pyspark import StorageLevel

df.cache()                                       # MEMORY_AND_DISK_DESER
df.persist(StorageLevel.MEMORY_ONLY)             # Fastest, may drop on OOM
df.persist(StorageLevel.MEMORY_AND_DISK_SER)     # Serialized — less memory
df.persist(StorageLevel.DISK_ONLY)               # Slowest — low memory usage
df.unpersist()  # Always free when done
```

**One-Liner:** *"cache() for quick wins; persist() when you need to control memory vs speed trade-off."*

---

## Q29. Row-Level Deletions in Delta Lake without rewriting entire partition.

### ⚡ Quick Answer
> *"Enable Deletion Vectors — Delta marks rows as deleted in a separate vector file without rewriting the Parquet file. Physical removal happens during the next OPTIMIZE run."*

```python
# Enable Deletion Vectors (Databricks Runtime 12.1+)
spark.conf.set("spark.databricks.delta.enableDeletionVectors", "true")

# Row-level delete — only marks rows, doesn't rewrite file
spark.sql("""
    DELETE FROM schema.patient_records
    WHERE patient_id = 'PAT-001'
    AND record_date = '2024-01-15'
""")

# Physical removal happens during OPTIMIZE
spark.sql("OPTIMIZE schema.patient_records")
```

> *"Without deletion vectors, every DELETE triggers a Copy-on-Write — the entire Parquet file containing that row is rewritten. Deletion Vectors make row-level deletes near-instant and defer the physical work to OPTIMIZE."*

**One-Liner:** *"Deletion Vectors = mark-then-delete — instant logical delete, physical cleanup deferred to OPTIMIZE."*

---

## Q30. Small File Problem — Auto-Compaction in Databricks.

### ⚡ Quick Answer
> *"Many small Parquet files = slow scans (Spark opens each file individually). Auto-Compaction in Databricks merges small files after each write automatically — no manual OPTIMIZE needed."*

```python
# Enable Auto-Compaction (Databricks Delta)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Or at table level
spark.sql("""
    ALTER TABLE schema.transactions
    SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')
""")

# Optimized Write — reduces file count at write time
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```

> *"On the FinOps project, our daily incremental loads were creating ~200 small files per partition. Auto-Compaction merged them into target-size files automatically, improving read performance without manual OPTIMIZE scheduling."*

**One-Liner:** *"Auto-Compaction = automatic OPTIMIZE after writes — fixes small file problem without scheduling maintenance jobs."*

---

## Q31. ⭐ Vectorized UDFs vs Regular Python UDFs.

### ⚡ Quick Answer
> *"Regular Python UDF: row-by-row, JVM↔Python serialization per row, Catalyst black box. Pandas UDF (Vectorized): processes entire batch as pandas Series, ~10x faster, still a black box to Catalyst."*

```python
# Regular Python UDF — slow (row by row)
@udf(StringType())
def classify_slow(x):
    return "high" if x > 10000 else "low"

# Pandas UDF — vectorized batch processing (~10x faster)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def classify_fast(amounts: pd.Series) -> pd.Series:
    return amounts.map(lambda x: "high" if x > 10000 else "low")

# Always prefer native Spark (fastest)
df.withColumn("cat", F.when(F.col("amt") > 10000, "high").otherwise("low"))
```

**One-Liner:** *"Native Spark > Pandas UDF > Python UDF — each 10x slower than the previous."*

---

## Q32. Schema Drift handling with ADF + Databricks.

### ⚡ Quick Answer
> *"ADF detects and passes new schema to Databricks. Databricks uses mergeSchema=true to absorb new columns. Alert sent on drift so downstream teams are aware."*

```python
# ADF: use "Schema drift" option in Data Flow or pass raw JSON to Databricks

# Databricks: absorb new columns with mergeSchema
df_source.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append").save(bronze_path)

# Drift detection in Databricks
existing_cols = set(spark.read.format("delta").load(bronze_path).columns)
incoming_cols = set(df_source.columns)
new_cols = incoming_cols - existing_cols
if new_cols:
    log_alert(f"Schema drift — new columns: {new_cols}")
    notify_downstream_teams(new_cols)
```

**One-Liner:** *"ADF passes raw data, Databricks absorbs schema change with mergeSchema=true, alert sent — pipeline never breaks."*

---

## Q33. Checkpointing in Structured Streaming — what if deleted?

### ⚡ Quick Answer
> *"Checkpoint stores Kafka offsets, state, and progress. If deleted, Spark loses track of where it was — it restarts from the beginning (or latest, depending on config), risking duplicate or missed records."*

```python
# Checkpoint enables fault tolerance + exactly-once semantics
df_stream.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "s3://checkpoints/prod/transactions/") \
    .trigger(processingTime="60 seconds") \
    .start()

# If checkpoint is deleted:
# → startingOffsets defaults to "latest" → all unprocessed records between
#   deletion and restart are LOST
# → Or if set to "earliest" → ALL historical records reprocessed = DUPLICATES

# Best practice: never delete checkpoint in production
# For intentional reset: set startingOffsets = "earliest" + use MERGE target
```

**One-Liner:** *"Checkpoint deletion = lose offset tracking → data loss or duplicates on restart. Treat checkpoint directory as sacred in production."*

---

## Q34. Dynamic Data Masking in a Lakehouse for PII data.

### ⚡ Quick Answer
> *"In Databricks Unity Catalog, create column masking policies — PHI columns return masked values for unauthorized users, real values for clinical team. No data is duplicated."*

```sql
-- Step 1: Create masking policy in Unity Catalog
CREATE OR REPLACE FUNCTION mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
    WHEN is_member('clinical_team') THEN ssn          -- Real value
    ELSE CONCAT('***-**-', RIGHT(ssn, 4))             -- Masked
END;

-- Step 2: Apply to column
ALTER TABLE schema.patient_records
ALTER COLUMN ssn SET MASK mask_ssn;

-- clinical_team users see: 123-45-6789
-- All others see:         ***-**-6789
```

> *"In my eCDP pharma project, patient SSN, DOB, and email were masked using Unity Catalog policies. This ensured HIPAA compliance without creating separate masked copies of the data."*

**One-Liner:** *"Unity Catalog column masking = same table, role-based visibility — no duplicate datasets for PII compliance."*

---

## Q35. Data Skipping in Delta Lake vs Traditional SQL Indexing.

### ⚡ Quick Answer
> *"Delta uses min/max statistics stored in the transaction log to skip Parquet files that can't contain matching rows. No index structure to maintain — it's automatic. SQL indexes have overhead on write but enable exact row lookups."*

| | Delta Data Skipping | SQL Index |
|---|---|---|
| Mechanism | Min/max stats per file | B-tree / hash structure |
| Granularity | File level | Row level |
| Write overhead | Low (stats auto-collected) | High (index rebuild) |
| Query benefit | Skip irrelevant files | Exact row lookup |
| Best for | Analytical range queries | OLTP point lookups |

```sql
-- Delta skipping: query filters on account_id
-- Delta checks: does any file have min(account_id) <= 'ACC-001' <= max(account_id)?
-- Files outside that range are skipped entirely

-- Z-ordering improves data skipping by co-locating similar values
OPTIMIZE schema.transactions ZORDER BY (account_id, txn_date);
```

**One-Liner:** *"Delta skipping is automatic file-level pruning using min/max stats — no index maintenance, works best with Z-ordering."*

---

## Q36. Monitor pipeline failures — Azure Monitor + Log Analytics.

### ⚡ Quick Answer
> *"ADF sends diagnostic logs to Log Analytics workspace. Write KQL queries to detect failures. Azure Monitor alert triggers on failure count — notifies via email, Teams, or PagerDuty."*

```kql
-- Log Analytics KQL — Alert when pipeline fails
AzureDiagnostics
| where ResourceType == "FACTORIES/PIPELINES"
| where status_s == "Failed"
| where TimeGenerated >= ago(1h)
| project TimeGenerated, pipelineName_s, status_s, message_s
| order by TimeGenerated desc
```

```python
# Databricks — custom metric logging for monitoring
import logging

def run_pipeline_with_monitoring(pipeline_name, func):
    try:
        start = time.time()
        result = func()
        duration = time.time() - start
        logger.info(f"PIPELINE_SUCCESS | {pipeline_name} | {duration:.1f}s")
        return result
    except Exception as e:
        logger.error(f"PIPELINE_FAILED | {pipeline_name} | {str(e)}")
        send_teams_alert(pipeline_name, str(e))  # Webhook to Teams
        raise
```

**One-Liner:** *"ADF → Log Analytics → KQL query → Azure Monitor alert → Teams/PagerDuty. Full observability in 30 minutes of setup."*

---

---

## 📋 MASTER QUICK-SPEAK REFERENCE

```
┌────────────────────────────────────────────────────────────────────────────┐
│  TOPIC              ONE-LINER TO OPEN YOUR ANSWER                         │
├────────────────────────────────────────────────────────────────────────────┤
│  Financial pipeline │ Medallion + partitioning + Z-order + MERGE          │
│  Consecutive days   │ LAG() → DATEDIFF = 1 → consecutive found            │
│  ROW_NUMBER etc     │ ROW_NUMBER=dedup; DENSE_RANK=no-gap ranking          │
│  Sales incorrect    │ Time travel to compare + layer-by-layer audit        │
│  Schema evolution   │ mergeSchema + drift alert + Bronze fallback          │
│  Cloud migration    │ Mirror → validate → cutover table-by-table           │
│  repartition/coal   │ repartition shuffles and balances; coalesce merges  │
│  Slow SQL           │ EXPLAIN → prune → broadcast → Z-order → fix subq   │
│  Late data          │ Watermark streaming; replaceWhere batch             │
│  Reconciliation     │ Count + sum + left_anti = 3-layer check             │
│  Data skew          │ Broadcast → AQE → manual salting                    │
│  UDF vs transform   │ Native > Pandas UDF > Python UDF (10x each)         │
│  Azure IR           │ Azure IR = cloud; Self-Hosted = on-prem             │
│  Queued pipeline    │ IR capacity → concurrency setting → trigger dep      │
│  Secrets            │ Key Vault + secret scope = zero secrets in code      │
│  Star vs Snowflake  │ Star=fast queries; Snowflake=complex hierarchies    │
│  MERGE vs UPSERT    │ MERGE = UPSERT + DELETE in one atomic statement      │
│  Deletion Vectors   │ Mark-then-delete, physical cleanup at OPTIMIZE       │
│  Small files        │ Auto-Compaction = automatic OPTIMIZE after writes    │
│  Checkpoint deleted │ Offset lost → data loss or duplicates on restart    │
│  Data masking       │ Unity Catalog column mask = role-based visibility    │
│  Data skipping      │ Min/max file stats + Z-ordering = automatic pruning │
│  Monitoring         │ ADF → Log Analytics → KQL → Alert → Teams          │
└────────────────────────────────────────────────────────────────────────────┘
```

---

*Interview Answer Scripts | All Sections | Mayuresh Patil | Aug 2026*
