# Spark & Databricks Concepts — Interview Answers

Terminology note: "lazy evolution" in the question list means **lazy evaluation**, and "repetition" means **repartition**. Use the correct terms in the interview.

Contents
1. What is Apache Spark
2. Why do we use Apache Spark
3. What is lazy evaluation
4. How lazy evaluation works internally
5. What is lineage
6. How do you define compute memory and driver
7. 1 driver, 2 executors, 10 records: how many partitions
8. repartition vs coalesce
9. partitionBy vs bucketing
10. "99.9% latency": what does it mean

---

## 1. What is Apache Spark

**Short answer (say this first):**
Apache Spark is an open-source, distributed data processing engine. It splits data into partitions, processes them in parallel across a cluster, and keeps intermediate data in memory where possible, which makes it much faster than disk-based MapReduce.

**Key points to add:**
- It is a **compute engine**, not a storage system. Data lives in ADLS, S3, HDFS, Delta, Iceberg, databases, etc. Spark reads it, processes it, and writes it back.
- Core abstractions: RDD (low level), DataFrame / Dataset (structured, optimized by Catalyst), Spark SQL.
- Libraries on top: Spark SQL, Structured Streaming, MLlib, GraphX.
- Languages: Python (PySpark), Scala, Java, R, SQL.
- Architecture: one **driver** (plans and coordinates) plus many **executors** (run tasks on partitions), managed by a **cluster manager** (YARN, Kubernetes, standalone; on Databricks the platform manages this).

---

## 2. Why do we use Apache Spark

| Reason | Explanation |
|---|---|
| Scale | Processes data larger than one machine's memory by distributing partitions across nodes. |
| Speed | In-memory processing, pipelined narrow transformations, Catalyst optimizer, Tungsten code generation. |
| Unified engine | Batch, SQL, streaming and ML in one framework. |
| Fault tolerance | Lost partitions are recomputed from lineage, so no full restart. |
| Connectivity | Reads and writes many formats and sources: Parquet, Delta, Iceberg, JDBC (Oracle), REST-fed files, CSV, JSON. |
| Ecosystem fit | First-class engine on Databricks, Synapse, EMR, and works with Delta Lake and Iceberg table formats. |

**When NOT to use Spark:** small data that fits on one machine (pandas or plain SQL is simpler and faster), or very low-latency single-record lookups (Spark has startup and scheduling overhead).

---

## 3. What is lazy evaluation

**Definition:** Spark does not execute transformations immediately. It records them as a plan. Real work starts only when an **action** is called.

**Transformations (lazy):** `select`, `filter`, `withColumn`, `join`, `groupBy`, `union`, `distinct`, `repartition`
**Actions (trigger execution):** `count`, `collect`, `show`, `take`, `write` / `save`, `foreach`

```python
df = spark.read.parquet("/data/sales")        # lazy: nothing read yet
df2 = df.filter("amount > 100")               # lazy
df3 = df2.groupBy("region").sum("amount")     # lazy
df3.show()                                    # ACTION: the job runs now
```

**Why it exists (benefits):**
1. **Optimization across the whole chain.** Spark sees the full plan, so it can push filters down, prune columns, and combine steps.
2. **Avoids wasted work.** Intermediate results are never computed if they are not needed.
3. **Pipelining.** Narrow transformations run together in one pass per partition without writing intermediates.

**Gotcha to mention:** because of laziness, errors in a transformation (such as a bad column name in some cases, or a bad cast on data) may only surface when the action runs, which can look like the error is on the action line. Also, without `cache()`, the same plan is recomputed for each action.

---

## 4. How lazy evaluation works internally

Step by step, from your code to execution:

1. **You write transformations.** Each call adds a node to an **unresolved logical plan**. Nothing runs.
2. **An action is called.** This triggers the pipeline below.
3. **Analysis.** Spark resolves table names, column names and data types against the catalog. Result: **analyzed logical plan**.
4. **Logical optimization (Catalyst).** Rule-based rewrites such as:
   - predicate pushdown (filter as early as possible, even into the file scan)
   - column pruning (read only needed columns)
   - constant folding, combining projections
   Result: **optimized logical plan**.
5. **Physical planning.** Spark generates candidate physical plans (for example sort-merge join vs broadcast hash join) and picks one using cost and statistics. Result: **physical plan**.
6. **Code generation (Tungsten).** Whole-stage code generation compiles pipelined operators into efficient JVM bytecode.
7. **DAG creation.** The plan becomes a DAG of RDD dependencies.
8. **DAG scheduler splits the DAG into stages.** A new stage starts at every **shuffle boundary** (wide dependency such as `groupBy`, `join`, `repartition`).
9. **Task scheduler creates tasks.** One task per partition per stage, sent to executors.
10. **Executors run tasks** and return results to the driver (or write them to storage).
11. **Adaptive Query Execution (AQE, Spark 3+).** At runtime, after shuffle stages finish, Spark can re-optimize: coalesce small shuffle partitions, switch join strategy, handle skewed joins.

**Inspect it yourself:**
```python
df3.explain(True)   # shows parsed, analyzed, optimized logical and physical plans
```
Also check the **Spark UI → SQL tab / Stages tab** to see the stages and tasks.

**One-line summary for interview:**
"Transformations only build a logical plan. On an action, Catalyst optimizes it into a physical plan, the DAG scheduler cuts it into stages at shuffle boundaries, and tasks run per partition on executors."

---

## 5. What is lineage

**Definition:** Lineage is the record of how a dataset (RDD / DataFrame) was derived from its parent datasets through transformations. It is a graph (DAG) of dependencies.

**Why it matters:**
- **Fault tolerance.** If an executor dies and a partition is lost, Spark recomputes only that partition by replaying its transformations from the source, using lineage. No data replication is needed.
- **Planning.** The DAG scheduler uses it to build stages.

**Narrow vs wide dependency (important follow-up):**
- **Narrow:** each child partition depends on one parent partition (`map`, `filter`). Cheap to recompute.
- **Wide:** a child partition depends on many parent partitions (`groupBy`, `join`). Needs a shuffle. Recovery is costlier.

**See it:**
```python
rdd = df.rdd
print(rdd.toDebugString().decode())
```

**Truncating lineage:** very long lineage (iterative jobs) slows planning and recovery. `checkpoint()` saves data to reliable storage and cuts the lineage.

**Do not confuse with data lineage in Unity Catalog:** that is table/column-level lineage across your data platform for governance. Spark lineage is the internal recompute graph. If the interviewer says "lineage", clarify which one, or answer Spark first and mention Unity Catalog lineage as the governance version.

---

## 6. How do you define compute memory and driver

### Roles
- **Driver:** runs your main program and the SparkSession. Builds the plan, schedules tasks, tracks executors, collects results of actions like `collect()` / `toPandas()`, and hosts broadcast variables before shipping them.
- **Executor:** a JVM process on a worker node. Runs tasks, holds cached data, performs shuffles.
- **Cores per executor:** number of tasks an executor can run in parallel.

### Executor memory layout (unified memory model)
For an executor with heap size H:
1. **Reserved memory:** about 300 MB, fixed for Spark internals.
2. **Usable memory** = H − 300 MB.
3. **Spark memory** = `spark.memory.fraction` (default **0.6**) of usable memory. Shared by:
   - **Execution** memory (shuffles, joins, sorts, aggregations)
   - **Storage** memory (cache, broadcast). `spark.memory.storageFraction` (default **0.5**) is the part protected from eviction by execution.
4. **User memory:** the remaining ~40% for your own data structures and UDF objects.
5. **Memory overhead** (outside heap): `spark.executor.memoryOverhead`, default max(384 MB, 10% of executor memory). Used for Python worker processes (PySpark), native buffers, and so on.

Execution and storage can borrow from each other. Execution can evict cached blocks, but not the other way around beyond the protected storage region.

### Illustrative sizing example (for explanation only; use your own numbers)
Worker node: 16 cores, 64 GB RAM.
- Leave 1 core and about 1 GB for OS and daemons: 15 cores, about 63 GB.
- Use about 5 cores per executor, giving 3 executors per node.
- Memory per executor: 63 / 3 = 21 GB, minus about 10% overhead gives roughly 19 GB executor memory.

(The "about 5 cores per executor" guideline comes from keeping I/O throughput healthy. On Databricks you do not tune this by hand.)

### On Databricks specifically
- You choose a **driver node type** and **worker node type** (and min/max workers for autoscaling). Databricks runs **one executor per worker node** using most of that node's memory and cores, so node type is effectively how you size executors.
- You usually do not set `spark.executor.memory` manually. You pick the VM size (memory-optimized for joins/shuffles/caching, compute-optimized for CPU-heavy work, storage-optimized for heavy local disk/caching).
- The driver is sized separately. Make it larger if you collect big results, broadcast big tables, or run many concurrent notebooks on a shared cluster.

### Driver memory problems
- `collect()` or `toPandas()` on large data causes **driver OOM**. Fix: write to storage, aggregate first, or use `limit`.
- Many small files or huge partition counts load the driver with task metadata.
- Fix options: `spark.driver.memory` (non-Databricks), bigger driver node, avoid collecting.

### Executor memory problems
- **Executor OOM** is usually from data skew, too few or too large partitions, big shuffles, or heavy UDFs. Fix: more partitions, handle skew (salting or AQE skew join), bigger nodes, avoid wide rows.
- **Spill to disk** shows in the Spark UI Stages tab. Some spill is normal, a lot means memory pressure.

⚠️ **Accuracy tip:** only quote specific cluster sizes (for example "Standard_DS4_v2, 8 workers") if they are your real configuration. If you do not remember, say: "I chose node types based on workload, memory-optimized for join-heavy jobs, and let autoscaling handle the load."

---

## 7. 1 driver, 2 executors, 10 records: how many partitions

**Key point first:** the driver does not hold partitions of data. Partitions are processed by **executor cores**. So what matters is **total cores across the executors**, not the driver count.

**Parallelism = number of executors × cores per executor.**

| Cores per executor | Total task slots | Reasonable partitions for 10 records |
|---|---|---|
| 1 | 2 | 2 |
| 2 | 4 | 4 |
| 4 | 8 | up to 8 |

**Answer to give:**
"With only 10 records the data is tiny, so I would not over-partition. A sensible choice is one partition per available core, so the executors are all busy without creating empty partitions. If each of the 2 executors has 2 cores, that is 4 partitions. Anything above 10 would just create empty partitions, and for 10 records even 1 partition is fine. Partitions mainly matter for large data, where the usual guidance is about 2 to 4 tasks per core and partition sizes around 100 to 200 MB."

**Related facts:**
- File reads: partition count depends on file sizes and `spark.sql.files.maxPartitionBytes` (default 128 MB), so 10 records in one small file gives 1 partition.
- `spark.sql.shuffle.partitions` defaults to **200**, which is far too many for 10 records. With AQE enabled, Spark coalesces small shuffle partitions automatically.
- `spark.default.parallelism` applies to RDD operations.

**Check in code:**
```python
df.rdd.getNumPartitions()
df = df.repartition(4)
```

---

## 8. repartition vs coalesce

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | **Full shuffle** | **No full shuffle** (merges existing partitions on the same executor where possible) |
| Direction | Increase or decrease | **Decrease only** |
| Data distribution | Even, balanced partitions | Can be uneven (skewed partition sizes) |
| Cost | Higher (network and disk) | Lower |
| Can partition by column | Yes: `repartition(n, "col")` | No |
| Typical use | Fix skew, increase parallelism, partition by a join/write key | Reduce the number of output files cheaply before a write |

```python
df.repartition(200)                 # even redistribution, full shuffle
df.repartition(50, "customer_id")   # hash-partition by key
df.coalesce(10)                     # shrink to 10 partitions, no full shuffle
```

**Rules of thumb:**
- Reducing partitions before writing to avoid small files: `coalesce`.
- Increasing parallelism or fixing skew: `repartition`.
- Caution: `coalesce` to a very low number can collapse upstream parallelism in the same stage, making the whole stage run with few tasks. If that hurts, use `repartition` instead.
- With AQE and Delta, `optimize` / auto compaction and optimized writes on Databricks reduce the need for manual coalesce.

**Where did you use it?** This is project-specific. See `project_interview_answers.md`, section on repartition/coalesce, for a safe way to answer.

---

## 9. partitionBy vs bucketing

| | `partitionBy` | Bucketing (`bucketBy`) |
|---|---|---|
| Layout | One **directory per distinct value** (`date=2026-01-01/`) | Fixed number of **bucket files**, rows assigned by `hash(column) % numBuckets` |
| Best column type | **Low cardinality** (date, country, region) | **High cardinality** (customer_id, order_id) |
| Main benefit | **Partition pruning**: queries filtering on that column skip other directories | **Avoids shuffle** in joins/aggregations on the bucket key if both sides are bucketed the same way, and helps sampling |
| Risk | Too many partitions give a small-files problem and metadata overhead | Needs the same bucket count and key on both sides; fixed at write time |
| Write API | `df.write.partitionBy("col")` | `df.write.bucketBy(n, "col").sortBy(...).saveAsTable("t")` (must use a table, not a plain path) |

```python
# partitionBy
df.write.partitionBy("load_date").format("parquet").save("/path")

# bucketing
df.write.bucketBy(16, "customer_id").sortBy("customer_id").saveAsTable("sales_bucketed")
```

**Choosing:**
- Filters mostly on date or region: `partitionBy`.
- Frequent large joins on the same high-cardinality key: bucketing.
- Both can be combined: partition by date, bucket by id.

**Important for your stack (Delta and Iceberg):**
- **Delta Lake does not support Spark's `bucketBy`.** On Databricks you would use partitioning for low-cardinality columns and **Z-ORDER** or **liquid clustering** for high-cardinality filter/join columns.
- **Apache Iceberg** supports a **bucket transform** in the partition spec (hidden partitioning), for example `bucket(16, id)`, which gives bucket-like behavior natively.

**Rule for partition columns:** avoid partitioning small tables, and avoid columns that create thousands of tiny partitions. A common guideline is to keep each partition at least around 1 GB of data on Delta.

---

## 10. "99.9% latency": what does it mean

This phrase has two common meanings. Clarify which one the interviewer means.

### Meaning A: percentile latency (p99.9)
"99.9th percentile latency" means **99.9% of requests/records complete within X time**, and only 0.1% take longer. Example: p99.9 = 2 seconds means 999 out of 1,000 requests finish in 2 seconds or less.
- Averages hide slow outliers. Percentiles (p50, p95, p99, p99.9) show tail latency, which is what users actually feel.
- Typically measured for **streaming or API systems**: end-to-end event latency from source to sink.

### Meaning B: availability / SLA of 99.9% ("three nines")
99.9% uptime allows about **43.8 minutes of downtime per month** (about 8.76 hours per year). This is about reliability, not speed.

### Honest answer if this is not from your work
Only claim a latency or availability number if you actually measured it. A safe script:

"My pipelines are mostly batch, so I think in terms of SLA completion windows and data freshness rather than request latency. I did not work with a measured 99.9% latency target. If I had to, I would define it as p99.9 end-to-end latency, measure it with timestamps at ingestion and at write, and track it through the Spark Structured Streaming progress metrics or logged timestamps."

⚠️ If you did mention "99.9%" on your resume, tell me the exact bullet and I will write an answer that matches what you can honestly defend.
