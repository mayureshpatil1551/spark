# Databricks Interview Answers (Q1-100)

**Format:** L1 = short answer (10-15 sec). L2 = detailed answer (30-45 sec). Pseudo code where useful.

---

## Section 1: Lakehouse, Compute, Governance

### Q1. Explain the Lakehouse architecture. Where do compute, storage and metadata live?
**L1:** Storage is cloud object storage (ADLS/S3) in open Delta format. Compute is separate (Databricks clusters or SQL warehouses). Metadata lives in the Delta transaction log and Unity Catalog.

**L2:** Data sits in ADLS Gen2 as Parquet files plus a `_delta_log` folder, which gives ACID transactions. Compute is Spark/Photon on clusters, and it scales independently of storage. Unity Catalog is the central metadata and governance layer: tables, permissions, lineage. So one copy of data serves BI, ETL and ML.

```
storage  = ADLS (parquet + _delta_log)
compute  = cluster / SQL warehouse (Spark + Photon)
metadata = Unity Catalog (catalog.schema.table) + Delta log
```

### Q2. What problems does the Lakehouse solve vs a pure warehouse or pure lake?
**L1:** Warehouse is costly and weak for unstructured data and ML. Lake has no ACID or quality control. Lakehouse gives both in one place.

**L2:** A warehouse locks data in a proprietary format, costs more, and doesn't suit ML or raw files. A pure lake is cheap but becomes a "swamp": no transactions, no schema enforcement, no updates. Lakehouse adds ACID, schema enforcement, time travel and governance on cheap storage, so I avoid copying data between a lake and a warehouse.

### Q3. Difference between DBR, standard Spark and Photon?
**L1:** Spark is the open-source engine. DBR is Databricks' optimized Spark plus Delta and libraries. Photon is a native C++ engine that makes SQL/DataFrame queries faster.

**L2:** Standard Spark is the open-source JVM engine. Databricks Runtime bundles an optimized Spark, Delta Lake, connectors and ML libraries. Photon is a vectorized engine written in C++ that runs under the same API. I get faster scans, joins and aggregations without changing code, and it helps most on SQL-heavy and Delta workloads.

### Q4. When do you pick Serverless SQL vs All-Purpose vs Job clusters?
**L1:** Serverless SQL for BI and ad-hoc queries. All-Purpose for interactive development. Job clusters for scheduled production runs.

**L2:** Serverless SQL warehouses start in seconds and suit dashboards and analysts. All-Purpose clusters are shared and stay up, so I use them for notebooks and development, but they cost more. Job clusters spin up for a run and terminate, so they are cheaper and isolated. All my production ETL runs on job clusters.

### Q5. How does Unity Catalog change security, governance and discovery vs Hive metastore?
**L1:** Hive metastore is per workspace. Unity Catalog is one central governance layer across workspaces, with fine-grained access, lineage and audit.

**L2:** Hive metastore had per-workspace metadata and weak, table-level control. Unity Catalog gives a three-level namespace (catalog.schema.table), centralized grants to groups, row filters and column masks, automatic lineage and audit logs, plus search and tags for discovery. I manage access once and it applies in every workspace.

```sql
GRANT SELECT ON TABLE main.sales.orders TO `analysts`;
```

### Q6. How do you handle late-arriving data in Delta without duplicates?
**L1:** Use MERGE on the business key, so late records update or insert instead of appending duplicates.

**L2:** I never blindly append. I dedupe the incoming batch on the key (latest timestamp wins), then MERGE into the target. I add a condition so an older record cannot overwrite a newer one. For partitioned data I also limit the MERGE to the affected date range.

```python
src = src.dropDuplicates(["id"])   # or keep latest by event_ts
MERGE INTO tgt t USING src s ON t.id = s.id
WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### Q7. Best practices for MERGE INTO performance on large tables?
**L1:** Dedupe the source, add partition/cluster predicates in the ON clause, keep files well sized, and avoid full-table scans.

**L2:** First, make sure the source has one row per key, or MERGE fails or multiplies work. Second, add a filter on the partition or clustering column in the ON condition so Delta skips files. Third, keep file sizes healthy with OPTIMIZE or auto compaction. Fourth, use Photon, enable deletion vectors and optimized writes, and merge only the changed data, not the whole day's load.

### Q8. How do you reduce the search space in a MERGE?
**L1:** Add a predicate on the partition or clustered column in the ON condition so Delta only scans relevant files.

**L2:** MERGE has to find matching rows in the target. If the ON clause only has the key, it may scan every file. I add something like `t.load_date >= '2026-10-01'`, or the min/max date from the source, so partition pruning and data skipping cut the scan. Z-order or liquid clustering on the key helps too.

```sql
ON t.id = s.id AND t.load_date IN (SELECT DISTINCT load_date FROM s)
```

---

## Section 2: Delta Operations

### Q9. Impact of partitioning on MERGE performance?
**L1:** Good partitioning lets MERGE skip partitions. Bad or too many partitions causes small files and slow merges.

**L2:** If the partition column is in the MERGE condition, Delta only touches those partitions, so it's much faster. If it isn't, MERGE scans everything. Over-partitioning (for example by user_id) creates tiny files and heavy metadata. I partition by a coarse column like date and use clustering for the rest.

### Q10. Scenario: MERGE is slow despite indexing-like techniques. What next?
**L1:** Check duplicates in the source, skew, how many files are rewritten, and small files, using the Spark UI and table history.

**L2:** I open DESCRIBE HISTORY to see files added and removed. If a small change rewrites many files, my predicate is not pruning. Then in the Spark UI I look for skewed tasks and large shuffle. I also check the source for duplicates, cluster size, and small files. Fixes: tighter predicate, dedupe, compaction, deletion vectors, a bigger cluster.

### Q11. How do you implement idempotent loads in Delta?
**L1:** Use MERGE on a key, or replaceWhere for a partition, so re-running gives the same result.

**L2:** I design each run to be repeatable: MERGE on business key for upserts, or `replaceWhere` to overwrite exactly the partition I'm loading. For streaming, checkpoints handle exactly-once. I can also record a batch ID, so a retry skips work that already completed.

```python
df.write.format("delta").mode("overwrite") \
  .option("replaceWhere", "load_date = '2026-10-29'").saveAsTable("t")
```

### Q12. Pitfalls of INSERT OVERWRITE on partitioned Delta tables?
**L1:** With static mode it can overwrite the whole table. Use dynamic partition overwrite or replaceWhere.

**L2:** By default, INSERT OVERWRITE replaces the entire table, not just the partitions in my data, so I can lose data. I set `partitionOverwriteMode=dynamic`, or better, use `replaceWhere` with an explicit condition. It also rewrites whole partitions, so it's expensive for small changes.

### Q13. How do you safely recreate a table in the same location?
**L1:** Use CREATE OR REPLACE TABLE. Don't delete the folder manually.

**L2:** Deleting the folder destroys the Delta log and history. For an external table, I can drop the table definition (the data stays) and recreate it pointing to the same path, but CREATE OR REPLACE TABLE is safer because it is atomic and keeps history.

### Q14. What is CREATE OR REPLACE TABLE and why prefer it?
**L1:** It replaces a table's definition and data in one atomic step and keeps the history.

**L2:** Unlike DROP then CREATE, readers never see a missing table, and the operation is a single transaction. Time travel history and permissions are preserved. It's what I use for full refreshes and schema changes.

```sql
CREATE OR REPLACE TABLE gold.sales AS SELECT ... FROM silver.sales;
```

### Q15. How does Delta handle deletes and updates physically?
**L1:** Files are immutable. Delta rewrites the affected files, or with deletion vectors, marks rows as deleted.

**L2:** Parquet files can't be edited, so an UPDATE or DELETE finds the files with matching rows, writes new files with the changes, and records the old ones as removed in the log. With deletion vectors, Delta only marks rows as deleted in a small side file, and physically rewrites later during OPTIMIZE. Old files stay until VACUUM.

### Q16. Impact of frequent UPDATE/DELETE on file layout and compaction?
**L1:** It creates many rewritten and small files, so I need regular OPTIMIZE and VACUUM.

**L2:** Every change rewrites files, so storage grows and files get fragmented. Reads slow down because of many small files. I enable deletion vectors, auto compaction, and schedule OPTIMIZE and VACUUM, or use predictive optimization.

---

## Section 3: File Sizes and Table Health

### Q17. How do you tune file sizes and what targets?
**L1:** Aim for roughly 128 MB to 1 GB per file, depending on table size.

**L2:** Too small means too many tasks and metadata work. Too large hurts parallelism and MERGE. For most tables I target 128-256 MB, and larger tables go toward 1 GB. I use OPTIMIZE, and can set `delta.targetFileSize`. Databricks also auto-tunes it by table size.

### Q18. What is auto compaction and how do you enable it?
**L1:** It merges small files automatically after a write. Enable it with a table property.

**L2:** After a write, if there are many small files in a partition, Delta runs a small compaction job. I turn it on per table or per session.

```sql
ALTER TABLE t SET TBLPROPERTIES (delta.autoOptimize.autoCompact = true,
                                 delta.autoOptimize.optimizeWrite = true);
```

### Q19. Scenario: Streaming creates thousands of small files. How to fix?
**L1:** Enable optimized writes and auto compaction, increase trigger interval, and schedule OPTIMIZE.

**L2:** Streaming writes a little data every micro-batch, so files stay small. I increase the trigger interval so each batch holds more data, enable optimizeWrite and autoCompact, or use `availableNow` batches. As a backup I schedule OPTIMIZE during low traffic.

### Q20. optimizeWrite: how to use it and its side effects?
**L1:** It shuffles data before writing to produce right-sized files. Side effect: extra shuffle and slightly slower writes.

**L2:** I set `spark.databricks.delta.optimizeWrite.enabled=true`. Instead of each task writing its own tiny file, data is repartitioned so files come out near target size. The cost is an extra shuffle, so write latency rises a bit. It's worth it for partitioned tables and MERGE.

### Q21. Interaction between auto-optimize, Z-Order and Liquid Clustering?
**L1:** Auto-optimize fixes file sizes at write time. Z-Order and Liquid Clustering organize data for skipping. Liquid Clustering replaces partitioning and Z-Order.

**L2:** Auto-optimize (optimized write + auto compaction) controls file size. Z-Order co-locates data by chosen columns, but only when I run OPTIMIZE and it rewrites data. Liquid Clustering does the same job incrementally, lets me change keys later, and replaces both partitioning and Z-Order. I don't use Z-Order on a liquid-clustered table.

```sql
CREATE TABLE t (...) CLUSTER BY (customer_id, event_date);
OPTIMIZE t;
```

### Q22. How do you monitor Delta table health?
**L1:** Use DESCRIBE DETAIL for file count and size, and DESCRIBE HISTORY for operations.

**L2:** DESCRIBE DETAIL gives numFiles and sizeInBytes, so I calculate average file size. DESCRIBE HISTORY shows each write, MERGE, OPTIMIZE and their metrics (files added or removed, rows changed). If average file size is tiny or file count keeps growing, I compact.

```sql
DESCRIBE DETAIL t;   DESCRIBE HISTORY t;
```

### Q23. Which system tables help track Delta operations and performance?
**L1:** Unity Catalog system tables like query history, audit, lineage and billing usage.

**L2:** `system.query.history` shows query runtimes, `system.access.audit` shows who accessed what, `system.access.table_lineage` shows data flow, and `system.billing.usage` shows cost. Together with DESCRIBE HISTORY I can find slow queries and expensive tables.

### Q24. Scenario: MERGE fails with a concurrency conflict. How to retry safely?
**L1:** Retry with backoff, and reduce conflicts by narrowing the MERGE predicate.

**L2:** Conflicts like ConcurrentAppendException happen when two writers touch the same files. I make MERGE idempotent so a retry is safe, wrap it in a retry loop with backoff, and add partition predicates so concurrent jobs touch different partitions. Deletion vectors and Liquid Clustering also reduce conflicts.

```python
for attempt in range(3):
    try: run_merge(); break
    except ConcurrentModificationException: sleep(2 ** attempt)
```

### Q25. How do you implement row-level deletes efficiently at scale?
**L1:** Delete with a partition predicate, use deletion vectors, then compact.

**L2:** I put the deletes in a staging table and run a single MERGE with `WHEN MATCHED THEN DELETE`, which scans once instead of many small DELETEs. Deletion vectors avoid rewriting whole files immediately. Later, OPTIMIZE or REORG physically removes the rows.

### Q26. Anti-patterns for high-frequency upserts in Delta?
**L1:** Tiny frequent MERGEs, no predicates, duplicate source rows, over-partitioning and no compaction.

**L2:** The common mistakes are merging every few seconds, which creates tiny files and conflicts; merging with no partition filter, which scans everything; not deduping the source; and skipping OPTIMIZE. Better is micro-batching bigger intervals and using clustering on the key.

---

## Section 4: Spark Internals

### Q27. Explain lazy evaluation and why it matters for debugging.
**L1:** Spark only builds a plan for transformations and runs it when an action is called.

**L2:** Transformations like filter and join only add to the DAG. Work starts at actions like count or write. This lets Catalyst optimize the whole plan. For debugging, it means an error shows up at the action, not at the faulty line, so I use `explain()` or run small actions step by step to find the issue.

### Q28. Narrow vs wide transformations?
**L1:** Narrow needs no shuffle (map, filter). Wide needs a shuffle (groupBy, join).

**L2:** In narrow transformations each output partition depends on one input partition, so they run in one stage. Wide ones such as groupBy, join, distinct and orderBy move data across the network, so they create a new stage and cost more. I try to reduce wide operations.

### Q29. How does Catalyst turn a logical plan into a physical plan?
**L1:** It parses the query, optimizes the logical plan with rules, then picks a physical plan.

**L2:** Steps: parse SQL, resolve columns into an analyzed logical plan, apply rule-based optimizations (pushdown, pruning), generate several physical plans, choose the cheapest, and then generate code. `explain(True)` shows each stage.

### Q30. What optimizations does Catalyst perform?
**L1:** Predicate pushdown, column pruning, constant folding, join reordering and broadcast selection.

**L2:** Filters are pushed to the data source so fewer files are read. Only needed columns are read. Constant expressions are computed once. Joins can be reordered and Spark picks broadcast for small tables. All of this happens automatically.

### Q31. How does Tungsten improve CPU and memory efficiency?
**L1:** It stores data in a compact binary format and generates efficient code, reducing JVM overhead.

**L2:** Tungsten manages memory itself in a binary format, so there are fewer Java objects and less garbage collection. It's cache-friendly and uses whole-stage code generation, merging operators into one tight loop. The result is lower CPU use and memory footprint.

### Q32. What is off-heap memory in Spark and when does it help?
**L1:** Memory outside the JVM heap. It reduces garbage collection pauses.

**L2:** On-heap data is managed by the JVM garbage collector, which can pause large jobs. Off-heap memory sits outside the heap, so GC doesn't touch it. I enable it with `spark.memory.offHeap.enabled` and set a size. It helps GC-heavy jobs with big shuffles or caching.

---

## Section 5: Jobs, Stages, Shuffle

### Q33. Life cycle of a Spark job: job to stage to task?
**L1:** An action creates a job. The job splits into stages at shuffles. Each stage runs tasks, one per partition.

**L2:** When I call an action, Spark builds a DAG and submits a job. The DAG is cut into stages wherever a shuffle is needed. Each stage has one task per partition, and tasks run in parallel on executors. A stage finishes when all its tasks finish.

### Q34. What determines stage boundaries?
**L1:** Wide transformations (shuffles).

**L2:** Operations that need data from many partitions, like groupBy, join and repartition, force a shuffle, so Spark ends one stage and starts another. Narrow transformations are pipelined within a stage.

### Q35. How does Spark decide the number of shuffle partitions?
**L1:** From `spark.sql.shuffle.partitions` (default 200), and AQE can adjust it at runtime.

**L2:** After a shuffle, the number of partitions equals that setting unless I override it. With Adaptive Query Execution on, Spark looks at actual shuffle sizes and coalesces small partitions automatically, so I rely on AQE rather than guessing.

### Q36. Impact of `spark.sql.shuffle.partitions` on performance and memory?
**L1:** Too low means huge partitions and spill. Too high means tiny tasks and overhead.

**L2:** With too few partitions, each task handles a lot of data, so memory spills to disk or OOM happens. With too many, scheduling overhead dominates. I aim for roughly 100-200 MB per partition and let AQE tune it.

### Q37. Scenario: after groupBy, 200 shuffle partitions but only 15 have data. Why and fix?
**L1:** The group key has only about 15 distinct values, so the rest are empty. Reduce partitions or use AQE.

**L2:** Hash partitioning sends each key to one partition, so 15 distinct keys fill at most 15 partitions. The other 185 stay empty. I enable AQE so it coalesces them, or lower shuffle partitions. If a few keys are very big, I use salting.

### Q38. How do you safely run VACUUM without breaking Time Travel users?
**L1:** Keep retention longer than the oldest time travel need, and do a dry run first.

**L2:** VACUUM deletes old files, so queries on older versions will fail. The default retention is 7 days. I confirm no one needs older versions, set `delta.deletedFileRetentionDuration` properly, and run `DRY RUN` to see what will be deleted. I never go below the safe retention.

```sql
VACUUM t DRY RUN;   VACUUM t RETAIN 168 HOURS;
```

### Q39. Scenario: MERGE INTO is very slow. Step-by-step plan?
**L1:** Dedupe source, add predicates, cluster on the key, compact files, then tune the cluster.

**L2:** (1) Make sure the source is deduplicated. (2) Add a partition or date predicate in ON. (3) Cluster or Z-order on the merge key. (4) Compact small files. (5) Enable deletion vectors and optimized writes. (6) Check for skew in the Spark UI. (7) Use Photon and a right-sized cluster.

### Q40. How do you use partition pruning and Z-Order/Liquid Clustering to speed up MERGE?
**L1:** Pruning skips whole partitions. Clustering on the key reduces files that need to be read and rewritten.

**L2:** I include the partition column in the MERGE condition so only relevant partitions are scanned. Within them, Z-Order or Liquid Clustering on the merge key keeps matching rows in few files, so data skipping works and fewer files are rewritten.

---

## Section 6: Streaming and Inconsistent Performance

### Q41. Scenario: streaming checkpoint directory grows unbounded. What to check?
**L1:** Check for state without a watermark, and the state store settings.

**L2:** Large checkpoints usually mean unbounded state, for example a streaming aggregation or dedup with no watermark. I add a watermark, switch to the RocksDB state store, and review how many state versions are retained. I also check source file tracking if using Auto Loader.

### Q42. How do you handle late data with watermarks and avoid state blow-up?
**L1:** Set a watermark that matches how late data really arrives, so old state is dropped.

**L2:** The watermark tells Spark how long to keep state for late events. I measure real lateness in the data and set the watermark just above it. Events later than that are dropped from the stream, so I send them to a late table and fix with a batch job.

```python
df.withWatermark("event_time", "10 minutes").groupBy(window("event_time","5 minutes"), "id").count()
```

### Q43. Scenario: watermark too short loses events, too long explodes state. How to tune?
**L1:** Analyze the delay distribution and pick a middle value, then handle very late events separately.

**L2:** I check lateness, for example the 99th percentile, and set the watermark to cover that. Anything later goes to a side table and a batch job corrects it. I also use RocksDB for large state and keep windows small.

### Q44. How do you debug a job that sometimes takes minutes and sometimes hours?
**L1:** Compare the fast and slow runs in the Spark UI: data size, skew, spill and cluster.

**L2:** I compare the same stages across runs. Typical causes: more input data, skewed keys, shuffle spill, a different join strategy, spot node loss, small files, or competing jobs on a shared cluster. Finding which stage changed points to the cause.

### Q45. What three things do you check in the Spark UI for inconsistent performance?
**L1:** Task time distribution (skew), shuffle and spill, and executor/GC health.

**L2:** First, the stage task summary: if max is far above median, there's skew. Second, shuffle read size and disk spill. Third, executors tab for GC time, lost executors and idle cores. I also check input size.

### Q46. Scenario: shuffle spill varies between runs. What factors?
**L1:** Changing data volume or skew, different node types, and other jobs sharing the cluster.

**L2:** Spill depends on data per task and memory per task. If input grew, keys got skewed, spot nodes were replaced with smaller ones, or other jobs shared the cluster memory, spill changes. AQE changing partition counts also changes it.

### Q47. How do you detect if AQE changed the join strategy?
**L1:** Compare the final plan in the SQL tab, or use `explain`.

**L2:** With AQE, the initial plan can change at runtime. In the Spark UI SQL tab I look at the final plan (isFinalPlan = true) and see if a sort-merge join became a broadcast join. I compare plans across runs.

### Q48. Scenario: plan shows broadcast join in fast runs, sort-merge in slow runs. Why?
**L1:** The table size crossed the broadcast threshold, or statistics were off.

**L2:** Spark broadcasts only if the table is under `autoBroadcastJoinThreshold` after filters. On some days the data or filter selectivity makes it bigger, so Spark falls back to sort-merge. Stale stats can also mislead it. Fix: broadcast hint if safe, update stats, raise the threshold carefully.

```python
df.join(broadcast(small), "id")
```

### Q49. How do you stabilize plans when performance must be predictable?
**L1:** Use join hints, fresh statistics and fixed key settings.

**L2:** I refresh stats with ANALYZE TABLE, use explicit hints for known joins, pin important settings like shuffle partitions and broadcast thresholds, and keep data layout consistent. For critical jobs I also test on production-sized data.

### Q50. Scenario: a stage has 10k tasks but most are empty. What misconfiguration?
**L1:** Shuffle partitions set far too high for the data, or AQE off.

**L2:** If `spark.sql.shuffle.partitions` is, say, 10,000 and the data is small, most partitions are empty. It can also come from over-partitioned input. I lower the value or turn on AQE so it coalesces.

### Q51. How do you detect over-partitioning from Spark UI and metrics?
**L1:** Very small tasks, scheduler delay larger than compute time, and many tiny files.

**L2:** In the stage summary, median input per task is only KBs, task time is a few milliseconds, and total time is mostly scheduling. The table also shows thousands of small files in DESCRIBE DETAIL.

### Q52. Scenario: after adding a new filter, the query gets slower. How?
**L1:** The filter can block pushdown or change the plan, for example by using a UDF or a function on a column.

**L2:** If the filter wraps the column in a function, cast or UDF, Spark can't push it down or use partition pruning, so it reads more data. Filters can also change size estimates, which changes join strategy, for example losing a broadcast.

### Q53. How can a filter prevent optimizations like join reordering or broadcast?
**L1:** UDF or non-deterministic filters can't be pushed down, and bad estimates change the join.

**L2:** Filters with UDFs are black boxes for Catalyst. A filter placed on the wrong side of an outer join can't be pushed. And if Spark misestimates the filtered size, it may choose a poor join order or skip the broadcast.

### Q54. Scenario: adding a column to a select makes the query 5x slower. Why?
**L1:** The column may be very wide, or require an extra join, UDF or JSON parsing.

**L2:** Parquet is columnar, so one extra large column (long string, nested JSON) means more bytes read. It could also be derived with a UDF, explode or an additional join, which adds shuffle. Catalyst no longer prunes that column either.

### Q55. How do you identify expensive projections in the plan?
**L1:** Run `explain` and look for UDFs, JSON parsing and heavy expressions in Project nodes.

**L2:** I read the physical plan for `BatchEvalPython`, `from_json`, regex or `explode` in Project operators. In the SQL tab, I check time spent in those nodes. I replace UDFs with built-in functions and parse JSON once.

### Q56. Scenario: a join that was fast is now slow after data growth. What changed?
**L1:** It likely fell from broadcast to sort-merge, or skew and shuffle grew.

**L2:** The small table may have grown past the broadcast limit. Keys may have become skewed. Shuffle partitions may now be too few, and stats may be stale. I check the plan, task skew and spill, then fix with hints, more partitions, AQE skew join or salting.

---

## Section 7: Modeling and Query Optimization

### Q57. When to denormalize vs keep normalized?
**L1:** Denormalize for read-heavy analytics. Keep normalized for frequently updated or shared data.

**L2:** Joins are expensive at scale, so in Gold I denormalize into wide tables for BI. In Silver I keep a cleaner normalized model so updates happen in one place. The choice depends on query patterns and update frequency.

### Q58. Scenario: analysts join many small dimensions to a huge fact. Performance is poor.
**L1:** Broadcast the dimensions, or pre-join them into a Gold table.

**L2:** Small dimensions should be broadcast joins. If analysts always use the same combination, I build a pre-joined wide table or a materialized view. I also cluster the fact on the common filters and select only needed columns.

### Q59. How do pre-joined dimension or aggregate tables help?
**L1:** They remove repeated joins and heavy aggregations at query time.

**L2:** I compute the join or aggregation once in the pipeline and store it in Gold. Dashboards then read a small, ready table, so queries are fast and costs drop. Incremental refresh keeps it current.

### Q60. Scenario: `count(distinct user_id)` is extremely slow. How to optimize?
**L1:** Use `approx_count_distinct`, or pre-aggregate before counting.

**L2:** Exact distinct count shuffles every unique value. Options: use `approx_count_distinct` if a small error is OK, pre-aggregate per day and then combine, or dedupe with groupBy first. More shuffle partitions also help.

```sql
SELECT approx_count_distinct(user_id) FROM events;
```

### Q61. When is `approx_count_distinct` acceptable and how do you explain error?
**L1:** For dashboards and trends where a small error is fine, not for finance or audit.

**L2:** It uses HyperLogLog++ and returns within about 5% relative error by default, which I can tighten with the rsd parameter at higher cost. I use it for traffic and engagement metrics, but never for billing or compliance numbers.

### Q62. Scenario: window with partition by customer_id order by event_time is slow.
**L1:** It shuffles and sorts by customer, and big or skewed customers make tasks slow.

**L2:** A window needs all rows of a customer in one task, sorted. If some customers have millions of rows, those tasks are slow. I filter and select fewer columns first, avoid unbounded frames, and pre-aggregate where possible.

### Q63. How do you handle very skewed customers in window calculations?
**L1:** Process heavy customers separately, or break their data into time buckets.

**L2:** AQE doesn't fix window skew. I identify the top heavy keys, handle them with a different approach, or split by time bucket and combine boundary values. Sometimes pre-aggregating to a smaller grain removes the need for the big window.

---

## Section 8: Failures and Infrastructure

### Q64. Scenario: job fails intermittently with "task killed" or "executor lost". What to check?
**L1:** Memory (OOM), spot preemption, and node failures, using executor logs and cluster events.

**L2:** I check the cluster event log for spot termination, executor stderr for OutOfMemory or exit code 137, and the Spark UI for GC time and skew. Large shuffles can also kill executors. Fixes: more memory, fewer cores per executor, on-demand nodes, or less skew.

### Q65. How do you tell spot preemption from application OOM?
**L1:** Preemption shows in cluster events. OOM shows in executor logs.

**L2:** For preemption, the event log says the instance was terminated by the cloud, and the executor disappears without Java errors. For OOM, the executor log has `OutOfMemoryError` or a container kill (137), and usually memory pressure and GC appear first.

### Q66. Scenario: jobs fail more at certain hours. What multi-tenant or quota issues?
**L1:** Peak concurrency, cloud quota limits, spot shortage, or storage throttling.

**L2:** At peak hours many jobs compete for VM quota, pools or the same cluster. ADLS can throttle under heavy load, and spot capacity may run out. I look at failure timestamps, quota usage, and move jobs to different times or use on-demand nodes.

### Q67. How do you use cluster and driver logs for root cause?
**L1:** Read driver logs, executor logs and the event log, starting from the first error.

**L2:** The driver log shows the stack trace and job flow. Executor stderr shows task-level failures. The cluster event log shows scaling and termination. I find the first failure, not the last, since later errors are usually a result.

### Q68. Scenario: driver logs show frequent GC pauses. What tuning?
**L1:** Avoid collecting big data to the driver, and increase driver memory.

**L2:** Common causes are `collect()` of large data, big broadcasts, too many partitions or tasks, and listing millions of small files. I remove collects, reduce partitions, compact files, and use a bigger driver node.

---

## Section 9: Table Design

### Q69. Indexing-like techniques for SCD queries?
**L1:** Cluster on the business key and current flag or effective date.

**L2:** SCD2 queries filter by key and is_current or date range. I use Liquid Clustering (or Z-Order) on business key and effective date, so lookups skip most files. I avoid partitioning on high-cardinality keys.

### Q70. How do you design fact tables for high-volume event data?
**L1:** Append-only Delta table, clustered by event date and ID, with narrow columns.

**L2:** Events are mostly inserts, so I keep the table append-only, cluster by date and the most filtered ID, avoid over-partitioning, keep files at a good size, and apply a retention policy for old data.

### Q71. Best partitioning/clustering for time-series event tables?
**L1:** Partition or cluster by date, and cluster by ID.

**L2:** I use coarse time partitions (day or month) so each partition has at least about 1 GB, or Liquid Clustering on event date. Within that, I cluster on the common lookup column.

### Q72. Scenario: queries filter by date range and a high-cardinality ID. Layout?
**L1:** Cluster by date and ID with Liquid Clustering.

**L2:** I don't partition by ID. I use Liquid Clustering on `(event_date, id)`, so date pruning narrows files and ID data skipping narrows further.

```sql
CREATE TABLE events (...) CLUSTER BY (event_date, id);
```

### Q73. How do you balance date partitioning with ID clustering?
**L1:** Partition coarsely by date, only if partitions are big, and cluster by ID.

**L2:** Partitions should be large (about 1 GB+). I use date for pruning, and Z-Order or clustering on ID inside it. Where data is smaller, I skip partitioning and use only Liquid Clustering on both.

### Q74. Anti-patterns in medallion design?
**L1:** Too many layers, business logic in Bronze, and duplicate copies of data.

**L2:** Each layer should have a clear purpose: Bronze raw, Silver clean, Gold business-ready. Anti-patterns: extra layers with no value, transforming heavily in Bronze, creating many near-identical tables, and no ownership, so nobody knows which table is trusted.

### Q75. Scenario: pipelines are hard to maintain due to many intermediate tables. How to simplify?
**L1:** Merge unnecessary layers and use views, and standardize with a framework.

**L2:** I remove tables that exist only to pass data through, replace some with views or temp steps, use a config-driven framework (like I did to cut source onboarding time by 50%), and document lineage so dependencies are clear.

### Q76. How do you document lineage and dependencies in Databricks?
**L1:** Unity Catalog lineage, plus comments and tags.

**L2:** Unity Catalog captures table and column lineage automatically, and I can query it in `system.access.table_lineage`. I add table comments, tags and owners, and keep pipeline dependencies in job definitions.

---

## Section 10: Data Quality and Governance

### Q77. Role of DLT expectations?
**L1:** They are declarative data quality rules with actions: warn, drop or fail.

**L2:** In Delta Live Tables, I attach rules to a table. Failed rows can be logged only, dropped, or fail the pipeline. The metrics go to the event log, so I can track quality over time.

```python
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
```

### Q78. How do you define and enforce DQ rules at bronze/silver/gold?
**L1:** Bronze: minimal checks. Silver: cleansing rules. Gold: business rules and reconciliation.

**L2:** Bronze keeps raw data and only records issues. Silver enforces schema, null checks, types, dedupe. Gold checks business rules and totals against source. I keep rules in config, so they're reusable.

### Q79. Scenario: bad data slips into gold. What checks were missing?
**L1:** Likely missing validation between Silver and Gold, such as null, duplicate, range and reconciliation checks.

**L2:** Possible gaps: no uniqueness check, no referential check against dimensions, no row-count or amount reconciliation, no freshness check and no anomaly detection. I add a validation gate that must pass before publishing to Gold.

### Q80. How do you implement quarantine tables?
**L1:** Split valid and invalid rows, and write invalid ones to a quarantine table with a reason.

**L2:** I flag rows with a rule result, send valid rows forward, and write failed rows to a quarantine table with error reason and timestamp. The pipeline keeps running, and later I fix and reprocess the quarantined data.

```python
valid   = df.filter(rule_ok)
invalid = df.filter(~rule_ok).withColumn("reason", ...)
invalid.write.mode("append").saveAsTable("quarantine.orders")
```

### Q81. What metrics do you track to measure data quality over time?
**L1:** Completeness, uniqueness, validity, freshness and reconciliation.

**L2:** I track null percentage, duplicate count, rule failure rate, row-count drift against source, freshness (data delay), and quarantine volume. I trend them daily and alert on sudden changes.

### Q82. How do you handle PII across layers?
**L1:** Classify it, restrict raw access, and mask or tokenize from Silver onward.

**L2:** I tag PII columns, keep Bronze access limited to engineers, and in Silver hash, tokenize or mask. In Unity Catalog I use column masks and row filters, so users see only what they are allowed to.

### Q83. Scenario: masking needed in Silver but not Bronze. How to implement?
**L1:** Restrict Bronze access, and mask in Silver using transformation or Unity Catalog column masks.

**L2:** Bronze stays raw with tight permissions. For Silver, I either hash or mask columns during the transformation, or attach a column mask function so different groups see different values.

```sql
ALTER TABLE silver.cust ALTER COLUMN phone SET MASK mask_phone;
```

### Q84. How do you use Unity Catalog tags for governance?
**L1:** Tag sensitive tables and columns, such as `pii`, to find and control them.

**L2:** I add tags for sensitivity, domain and owner. Tags help search for all PII columns, drive masking and access decisions, and support audits.

### Q85. Best practices for naming and organization?
**L1:** Consistent snake_case names, catalog per environment, schema per layer or domain.

**L2:** Example: `prod.silver.sales_orders`. Catalog for environment, schema for layer or domain, table names descriptive, and no spaces or random abbreviations. Consistency helps governance and discovery.

### Q86. How do you design pipelines for both BI and ML?
**L1:** Share a clean Silver layer, then build Gold aggregates for BI and feature tables for ML.

**L2:** Ingest once, clean in Silver, and branch from there. BI gets Gold aggregates and star schemas. ML gets feature tables at detailed grain. A single source keeps numbers consistent.

### Q87. Scenario: ML needs raw features, BI needs aggregates. How to serve both?
**L1:** Keep detailed Silver for ML and derive Gold aggregates from it.

**L2:** I don't ingest twice. Silver holds clean detail. ML reads Silver or feature tables. BI reads Gold aggregates built from the same Silver, so both agree.

### Q88. How do you create feature tables in Databricks?
**L1:** A Delta table with a primary key (and timestamp), registered in Unity Catalog.

**L2:** I compute features per entity, keep a primary key, add a timestamp key for point-in-time correctness, and register it with the Feature Engineering client. Models then reuse and look up the same features.

```python
fe.create_table(name="main.ml.cust_features",
                primary_keys=["customer_id"], df=features_df)
```

### Q89. Considerations for freshness, latency, consistency across BI and ML?
**L1:** Match refresh speed to the need, and use the same source so both are consistent.

**L2:** BI can often live with hourly or daily refresh. ML serving may need fresher features. I decide batch vs streaming per use case, keep a single source of truth, and ensure features use point-in-time data to avoid leakage.

---

## Section 11: Reliability, Backfills, Versioning

### Q90. How do you do incremental loads that update both aggregates and detail?
**L1:** MERGE changes into detail first, then recompute aggregates only for affected keys or dates.

**L2:** Process the new batch once, MERGE into the detail table, and find the affected dates or keys. Then rebuild just those slices of the aggregate from detail. Both updates run in the same job, in order.

### Q91. Scenario: aggregates and detail get out of sync after a failed job. Prevent or recover?
**L1:** Always derive aggregates from detail, make jobs idempotent, and rerun the affected slice.

**L2:** To prevent this, order the steps and track batch status. To recover, rerun the job: because it's idempotent, the aggregate for affected dates is recomputed from detail. I also add reconciliation checks comparing totals.

### Q92. How do you design idempotent jobs for safe retries?
**L1:** Use MERGE or replaceWhere and parameterized inputs, so reruns give the same result.

**L2:** The job takes a fixed input range, writes with overwrite-by-partition or upsert, and doesn't append blindly. For streaming I rely on checkpoints. Retry then causes no duplicates.

### Q93. Patterns for backfills and reprocessing?
**L1:** Run a separate parameterized backfill job in date chunks.

**L2:** I reuse the same transformation code with a date range parameter, process in chunks (day or month), write with replaceWhere, validate counts after each chunk, and run on its own cluster.

```python
for d in date_ranges: run_pipeline(d.start, d.end)  # replaceWhere per chunk
```

### Q94. Scenario: reprocess 6 months due to a logic bug. How to plan?
**L1:** Fix and test on a small range, reprocess in chunks, validate, then inform downstream.

**L2:** (1) Fix the logic and test on one day. (2) Take a backup point (version or clone). (3) Reprocess month by month with replaceWhere. (4) Validate counts and totals. (5) Notify downstream consumers. If something goes wrong, I RESTORE.

### Q95. How do you avoid hurting current SLAs during big backfills?
**L1:** Use a separate cluster, off-peak hours, and small chunks.

**L2:** A backfill on its own job cluster doesn't compete with daily runs. I schedule it at low-traffic times, process in chunks, and avoid writing the same partitions the daily job writes at that moment.

### Q96. Strategies for blue/green or shadow pipelines?
**L1:** Run the new pipeline in parallel to a new table, compare, then switch.

**L2:** The new pipeline writes to a shadow table. I compare it with production (counts, totals, sample rows). When it matches, I switch consumers using a view or rename, and keep the old table for rollback.

### Q97. How do you version data models and transformations?
**L1:** Code in Git, deployments through CI/CD, and Delta versions for data.

**L2:** Notebooks and pipeline code go in Git with pull requests and CI/CD deployment. Data versioning comes from Delta table versions (time travel). Schema changes are tracked with migration scripts or notes.

### Q98. Scenario: a model change breaks dashboards. How to roll back safely?
**L1:** Restore the table to the previous version and revert the code.

**L2:** I use `RESTORE TABLE ... TO VERSION AS OF` to bring back the last good data, and redeploy the previous code version. To avoid this in future, dashboards read from stable views and I test changes first.

```sql
RESTORE TABLE gold.sales TO VERSION AS OF 42;
```

### Q99. How do you implement backward-compatible schema changes in Delta?
**L1:** Make additive changes only, and avoid dropping or renaming without a plan.

**L2:** Adding columns is safe. For renames or drops I enable column mapping, keep the old column during a transition period, and expose a stable view to consumers. I announce changes and deprecate gradually.

```sql
ALTER TABLE t ADD COLUMN new_col STRING;
```

### Q100. Risks of using `mergeSchema` in production?
**L1:** It can silently accept unwanted columns and break downstream consumers.

**L2:** With mergeSchema, any new column in the source is added automatically, so typos or unexpected fields pollute the table. It can also cause type conflicts and surprise downstream jobs. In production I prefer a controlled schema check and explicit ALTER TABLE, and use mergeSchema only for approved sources.

---

*End of document.*
