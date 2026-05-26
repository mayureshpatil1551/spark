# 🎯 Interview Answer Scripts — Mayuresh Patil
## Part 1: Apache Spark + Apache Iceberg
> Short · Simple · Speakable in 30–90 seconds · Project-grounded

---

# 🔷 SECTION 1 — APACHE SPARK

---

### S1. ⭐ Explain Spark architecture — Driver, Executors, Cluster Manager

**One-liner:** Spark has a Master-Worker model — the Driver plans the work, Executors do the work, and the Cluster Manager allocates resources.

**Answer Script:**
> "Spark has three components. The **Driver** is the brain — it runs your PySpark code, creates the DAG, and schedules tasks. The **Executors** are workers — they run the actual tasks and store cached data. The **Cluster Manager** — like YARN or Databricks — allocates resources to executors.
>
> In my AbbVie project, our Databricks cluster had the driver coordinating ingestion jobs, and multiple executors processing partitions of Iceberg data from S3 in parallel. When I needed to process 10M+ records daily, I tuned executor memory and cores to maximize parallelism."

**Strong Interview One-Liner:**
> *"Driver plans, Executors execute, Cluster Manager allocates — Spark is a distributed compute engine built on this three-tier model."*

**Common Mistake to Avoid:**
> Don't say "Driver executes the code on workers." The Driver runs your application logic and schedules tasks — it does NOT run transformations itself.

---

### S2. ⭐ RDD vs DataFrame vs Dataset — when would you use RDD?

**One-liner:** RDD is low-level with no optimization; DataFrame is high-level with Catalyst optimization; Dataset adds type safety (Java/Scala only).

**Answer Script:**
> "RDD is the oldest API — it gives you full control but zero optimization. DataFrame is the modern API — it goes through the Catalyst optimizer and Tungsten engine, making it 10–100x faster. Dataset adds compile-time type safety but is only available in Scala/Java.
>
> In my PySpark work at AbbVie, I always use DataFrames. RDD would only make sense for very custom low-level operations — like a custom partitioner — which I haven't needed."

**Strong Interview One-Liner:**
> *"Always use DataFrames in production PySpark — they give you Catalyst optimization and Tungsten memory management for free."*

**Common Mistake to Avoid:**
> Don't say Dataset is available in Python. In PySpark, you only have RDD and DataFrame. Dataset is a Scala/Java concept.

---

### S3. ⭐ What is lazy evaluation? Why is it important?

**One-liner:** Spark doesn't execute transformations immediately — it builds a plan and executes only when an action is triggered.

**Answer Script:**
> "Lazy evaluation means transformations like `filter()` or `join()` don't run immediately — Spark just records them in a DAG. Only when you call an **action** like `count()`, `show()`, or `write()` does execution start.
>
> This is powerful because Spark can **see your entire pipeline** before running it and apply optimizations — like pushing filters before joins, or skipping columns you never use. In my ingestion pipelines, this meant Spark could push partition filters all the way to the Iceberg scan, avoiding full table reads."

**Strong Interview One-Liner:**
> *"Lazy evaluation lets Catalyst see the full pipeline before running it — enabling optimizations that would be impossible with eager execution."*

**Common Mistake to Avoid:**
> Don't confuse lazy with slow. Lazy evaluation makes Spark faster, not slower — it enables global optimization.

---

### S4. ⭐ Narrow vs Wide transformations

**One-liner:** Narrow = no shuffle, fast; Wide = shuffle required, expensive, creates a stage boundary.

**Answer Script:**
> "Narrow transformations like `filter()`, `withColumn()`, and `select()` work within each partition — no data moves across executors. Wide transformations like `groupBy()`, `join()` (Sort-Merge), and `repartition()` require a shuffle — data moves across the network.
>
> Every wide transformation creates a **stage boundary** in the DAG. In my AbbVie pipelines, I minimized wide transformations by using broadcast joins for small reference tables — this eliminated the shuffle entirely for those joins and was one of the reasons I got a 30% reduction in execution time."

**Strong Interview One-Liner:**
> *"Wide transformations = shuffles = expensive. The goal is to minimize shuffles through broadcast joins, early filters, and smart partitioning."*

**Common Mistake to Avoid:**
> Don't say `coalesce()` is a wide transformation. It avoids shuffle by merging partitions locally — it's narrow.

---

### S5. ⭐ What is a shuffle and why is it expensive?

**One-liner:** Shuffle = redistributing data across all executors over the network — involves disk I/O, network transfer, and serialization.

**Answer Script:**
> "A shuffle happens during wide transformations like `groupBy` or Sort-Merge Join. Spark needs to send rows with the same key to the same executor — so data is written to disk on the mapper side, transferred over the network, and read on the reducer side.
>
> It's expensive for three reasons: **disk I/O** (shuffle files written to disk), **network transfer** (all-to-all communication), and **serialization** (data must be converted to bytes for transfer).
>
> In my pipelines, I always check the Spark UI's Shuffle Read/Write metrics. When I saw high shuffle on reference table joins, I switched to broadcast joins — which dropped shuffle to near zero."

**Strong Interview One-Liner:**
> *"Shuffle is the #1 performance killer in Spark — disk I/O + network + serialization all at once."*

**Common Mistake to Avoid:**
> Don't say shuffle always means data is sorted. Shuffle just redistributes data — sorting is a separate step in Sort-Merge Join.

---

### S6. ⭐ Jobs, Stages, and Tasks

**One-liner:** Job = one action; Stage = group of tasks with no shuffle; Task = one partition processed by one executor core.

**Answer Script:**
> "Every time you call an action like `count()` or `write()`, Spark creates a **Job**. Each job is split into **Stages** — a stage is a group of transformations that can run without a shuffle between them. Each stage is split into **Tasks** — one task processes one partition.
>
> So if you have 200 shuffle partitions, a stage has 200 tasks running in parallel. In my AbbVie Spark UI, I'd look at the stage view — if one task took 10x longer than others, that was a data skew signal."

**Strong Interview One-Liner:**
> *"Job → Stages → Tasks: the hierarchy maps to Action → Shuffle boundaries → Partitions."*

**Common Mistake to Avoid:**
> Don't say one job has one stage. A single `groupBy().count()` already creates two stages — one before the shuffle, one after.

---

### S7. ⭐ `repartition()` vs `coalesce()` — when to use each?

**One-liner:** `repartition()` always shuffles and can increase or decrease partitions; `coalesce()` only decreases and avoids shuffle by merging locally.

**Answer Script:**
> "`repartition(n)` always causes a shuffle — it redistributes data evenly using round-robin or hash. Use it when you need more partitions or need to partition by a column for a downstream join.
>
> `coalesce(n)` only reduces partitions, merges them on the same executor, and avoids network transfer — much cheaper.
>
> In my pipelines, I use `coalesce()` before writing Iceberg files to reduce the number of output files. I use `repartition()` when I need to increase parallelism before a heavy groupBy or when data is skewed."

**Strong Interview One-Liner:**
> *"Use coalesce to reduce files before write; use repartition when you need balanced parallelism or more partitions."*

**Common Mistake to Avoid:**
> Don't use `coalesce(1)` on a large DataFrame before writing — it creates a single-executor bottleneck. Use it only for small outputs.

---

### S8. ⭐ `cache()` vs `persist()` — storage levels

**One-liner:** `cache()` is shorthand for `persist(MEMORY_AND_DISK_DESER)`; `persist()` lets you choose the storage level.

**Answer Script:**
> "Both store a DataFrame so Spark doesn't recompute it. `cache()` uses the default storage level — memory, and spills to disk if needed. `persist()` lets you choose: `MEMORY_ONLY`, `DISK_ONLY`, `MEMORY_AND_DISK`, etc.
>
> Caching is lazy — it materializes on the first action. Always call `unpersist()` when done.
>
> In my AbbVie pipelines, I cached the base enriched DataFrame that was fed into 3 downstream aggregations — this avoided re-reading 500MB from S3 three times."

**Strong Interview One-Liner:**
> *"Cache when a DataFrame is used 2+ times downstream — it pays off when the computation is expensive."*

**Common Mistake to Avoid:**
> Don't cache DataFrames used only once — caching has overhead. It only helps when reuse justifies the storage cost.

---

### S9. ⭐ AQE — Adaptive Query Execution

**One-liner:** AQE re-optimizes your query plan at runtime using actual data statistics, not estimates.

**Answer Script:**
> "AQE is a Spark 3 feature that re-optimizes the plan after each shuffle stage using real runtime statistics. It does three things: **coalesces empty shuffle partitions** (so you don't run 185 empty tasks), **converts Sort-Merge Joins to Broadcast Joins** if it discovers one side is small, and **splits skewed partitions** automatically.
>
> I always enable AQE in my Databricks pipelines. It was especially useful in my AbbVie project where some joins had moderate skew — AQE handled it without any code changes."

**Strong Interview One-Liner:**
> *"AQE turns Spark from a static planner to a dynamic optimizer — it adjusts the plan based on what the data actually looks like at runtime."*

**Common Mistake to Avoid:**
> Don't say AQE fixes all skew. For extreme skew (one key = 90% of data), AQE may not be enough — manual salting is still needed.

---

### S10. ⭐ Broadcast Join — when to use it?

**One-liner:** Broadcast join sends the small table to every executor — no shuffling of the large table, immune to data skew.

**Answer Script:**
> "In a broadcast join, Spark copies the entire small table to every executor. Each executor then joins its local partition of the large table against that local copy — no network shuffle of the large table happens.
>
> Use it when one table is small enough to fit in executor memory — typically under 100MB, though I tune the threshold to 50–100MB in production.
>
> In AbbVie, my reference tables — XREF mappings, facility codes, drug lookup tables — were all under 20MB. I broadcast those in every join, which eliminated huge shuffle stages and contributed to my 30% performance gain."

**Strong Interview One-Liner:**
> *"Broadcast join is the single best optimization for large-small table joins — zero shuffle on the large side and completely immune to data skew."*

**Common Mistake to Avoid:**
> Don't broadcast large tables. Broadcasting a 500MB table will OOM every executor. Always verify the actual table size before broadcasting.

---

### S11. Dynamic Partition Pruning (DPP)

**One-liner:** DPP uses runtime values from a small table to skip irrelevant partitions of a large partitioned table before reading.

**Answer Script:**
> "DPP allows Spark to skip reading partitions of a large table based on values from another table — at runtime, not at planning time.
>
> For example, if I join `listening_activity` (partitioned by `listen_date`) with a `songs` table on `release_date = listen_date`, DPP extracts the distinct release dates from the songs table and uses them to skip all other date partitions in the listening table.
>
> In my Iceberg pipelines, I designed partitioning to align with join keys specifically to enable DPP — so date-based joins only scan relevant partitions."

**Strong Interview One-Liner:**
> *"DPP = runtime partition pruning using another table's values — critical for date-partitioned joins in data lakes."*

**Common Mistake to Avoid:**
> DPP only works when the large table is physically partitioned on disk AND the join/filter column matches the partition column.

---

### S12. ⭐ Data Skew — how to handle it

**One-liner:** Data skew = one partition has far more data than others, causing one slow straggler task.

**Answer Script:**
> "Skew happens when one key — like a top customer ID — has millions of rows while others have hundreds. That partition takes 100x longer than others and holds up the entire stage.
>
> Three solutions: First, **broadcast join** — if the other table is small, broadcast it and skip shuffle entirely. Second, **AQE skew join** — Spark 3 auto-splits skewed partitions at runtime. Third, **manual salting** — add a random salt column to the join key to distribute skewed rows across multiple partitions.
>
> At AbbVie, one facility ID had 40% of all records. I used broadcast join for the reference table, which eliminated the skew completely."

**Strong Interview One-Liner:**
> *"Skew is the most common Spark performance problem — detect it in Spark UI Event Timeline, fix it with broadcast, AQE, or salting."*

**Common Mistake to Avoid:**
> Don't just `repartition()` to fix skew. If the same key dominates, repartitioning on that key maps all rows back to the same partition — skew remains.

---

### S13. Spark Fault Tolerance

**One-liner:** Spark uses lineage — if a partition is lost, it recomputes it from the original source using the recorded transformation history.

**Answer Script:**
> "Spark tracks every transformation in a DAG called the **lineage**. If an executor fails and loses a partition, Spark re-runs just that partition's computation from the last stable point — either the source or a checkpoint.
>
> Task failures retry up to 4 times by default. Executor failures cause tasks on that executor to be rescheduled on surviving executors. Driver failure kills the job — that's why streaming jobs use checkpointing.
>
> In my Databricks pipelines, I set checkpoints for long streaming jobs and use idempotent MERGE operations so re-runs are safe."

**Strong Interview One-Liner:**
> *"Lineage is Spark's fault tolerance mechanism — lost data is recomputed, not restored from a replica."*

**Common Mistake to Avoid:**
> Don't confuse Spark fault tolerance with HDFS replication. Spark doesn't replicate data by default — it recomputes from lineage.

---

### S14. Catalyst Optimizer

**One-liner:** Catalyst is Spark's query optimizer that transforms your logical plan into the most efficient physical execution plan.

**Answer Script:**
> "Catalyst works in 4 phases. First, it **parses** your code into a logical plan. Second, it **analyzes** it by validating columns against the catalog. Third, it **optimizes** the logical plan — applying filter pushdown, projection pushdown, constant folding. Fourth, it generates multiple **physical plans** and picks the cheapest using cost-based optimization.
>
> The most impactful rule for my work is **filter pushdown** — Catalyst pushes filters as close to the source as possible, so Iceberg only reads the relevant partitions and Parquet row groups."

**Strong Interview One-Liner:**
> *"Catalyst is why DataFrames are faster than RDDs — it optimizes globally across your entire pipeline before a single task runs."*

**Common Mistake to Avoid:**
> Don't say Catalyst only does filter pushdown. It also handles projection pushdown, join reordering, and constant folding — all critical optimizations.

---

### S15. Tungsten

**One-liner:** Tungsten is Spark's execution engine that manages memory off-heap, uses CPU cache-aware algorithms, and generates optimized JVM bytecode.

**Answer Script:**
> "Tungsten works below the DataFrame API level. It stores data in compact binary format off the JVM heap — avoiding garbage collection pauses. It uses CPU cache-aware data structures for sorting and hashing. And it generates optimized JVM bytecode for entire pipeline stages — called whole-stage code generation.
>
> You see Tungsten in action in the physical plan — operators prefixed with `*` like `*(1) Filter` mean Tungsten code generation is active. This is why DataFrames are 10–100x faster than RDDs for the same computation."

**Strong Interview One-Liner:**
> *"Tungsten removed the JVM overhead from Spark — off-heap memory + code generation makes it near-native speed."*

**Common Mistake to Avoid:**
> Don't confuse Catalyst with Tungsten. Catalyst optimizes the query plan (what to do); Tungsten optimizes execution (how to do it fast).

---

### S16. Checkpointing vs Caching vs Persistence

**One-liner:** Cache = memory storage, lineage preserved; Checkpoint = disk storage, lineage broken; Persist = configurable storage level.

**Answer Script:**
> "Cache stores data in memory (spills to disk). Persist is the same but you choose the storage level — memory-only, disk-only, or both. Both preserve the lineage — if data is lost, Spark recomputes from source.
>
> Checkpoint is different — it writes to stable storage like S3 or HDFS and **cuts the lineage**. If data is lost, it restores from the checkpoint, not from the source. Use it for long lineage chains or streaming jobs.
>
> In my streaming pipelines, I always set a checkpoint location on S3 so jobs can resume from the last offset after a failure."

**Strong Interview One-Liner:**
> *"Cache for reuse; checkpoint for fault tolerance in long-running or streaming jobs."*

**Common Mistake to Avoid:**
> Don't use checkpointing for regular batch DataFrame reuse — caching is cheaper and faster.

---

### S17. ⭐ Slow Spark job — how do you diagnose?

**One-liner:** Open Spark UI → find the slow stage → check for skew, shuffle size, or I/O bottleneck → fix the root cause.

**Answer Script:**
> "My step-by-step approach: First, open Spark UI and find the slowest **stage**. Then look at the **Event Timeline** — if one task bar is much longer than others, it's data skew. Check **Shuffle Read/Write size** — high shuffle means too many wide transformations.
>
> Then I run `explain('formatted')` to check the physical plan — look for unnecessary `Exchange` operators (shuffles), Sort-Merge Joins on small tables (should be broadcast), and whether DPP is active.
>
> At AbbVie, I diagnosed a 40-minute job that was doing an SMJ on a 5MB reference table. Switching to broadcast join dropped it to 8 minutes."

**Strong Interview One-Liner:**
> *"Spark UI + explain() is my debugging toolkit — Spark tells you exactly where the time is going if you know where to look."*

**Common Mistake to Avoid:**
> Don't just throw more resources at a slow job. Always diagnose first — adding executors won't fix data skew or an unnecessary shuffle.

---

### S18. ⭐ Spark Memory Management

**One-liner:** Spark splits executor memory into Execution memory (shuffles, sorts, joins) and Storage memory (cache, broadcasts) — both share a unified pool in Spark 2+.

**Answer Script:**
> "Spark executor memory has two main regions. **Execution memory** handles active operations — shuffle buffers, sort buffers, join hash tables. **Storage memory** holds cached DataFrames and broadcast variables.
>
> In Spark 2+, they share a unified pool — execution can borrow from storage and vice versa. If execution memory is full, it spills to disk — you'll see this as high 'spill' in Spark UI.
>
> When I saw memory spills in my AbbVie jobs, I increased `spark.executor.memory` and reduced `spark.sql.shuffle.partitions` so each partition was smaller — less memory pressure per task."

**Strong Interview One-Liner:**
> *"Spill to disk means OOM is approaching — tune executor memory and partition count before it becomes a job failure."*

**Common Mistake to Avoid:**
> Don't set `spark.executor.memory` too high. Leave 10–15% for OS overhead, otherwise the executor gets killed by the OS.

---

### S19. Cluster Mode vs Client Mode

**One-liner:** Client mode = driver runs on your machine; Cluster mode = driver runs inside the cluster.

**Answer Script:**
> "In **client mode**, the driver runs on the machine submitting the job. If you disconnect, the job fails. This is good for interactive notebooks in Databricks.
>
> In **cluster mode**, the driver runs on a worker node inside the cluster. The job continues even if you disconnect — ideal for production batch jobs.
>
> In Databricks, interactive notebooks use client mode by default. My ADF-triggered production jobs run in cluster mode so they're not affected by my machine's connectivity."

**Strong Interview One-Liner:**
> *"Client mode for development; cluster mode for production — always use cluster mode for scheduled jobs."*

**Common Mistake to Avoid:**
> Don't run long production jobs in client mode — if the connection drops, the entire job fails.

---

### S20. ⭐ UDFs — why expensive and alternatives

**One-liner:** Python UDFs break Tungsten optimization, require JVM-to-Python serialization for every row, and run row by row.

**Answer Script:**
> "Python UDFs are expensive for three reasons: First, Spark must **serialize each row** from JVM to Python process using Pickle — two round trips per row. Second, UDFs are black boxes to Catalyst — it can't optimize across them. Third, they run **row by row** instead of vectorized batch processing.
>
> Alternatives: First, use **native Spark functions** whenever possible — they run in Tungsten with no serialization. Second, use **Pandas UDFs** — they process entire batches, not rows, so serialization is much cheaper.
>
> In my AbbVie work, I replaced several Python UDFs with `F.when().otherwise()` chains — same logic, but 5–10x faster."

**Strong Interview One-Liner:**
> *"If you can express it with native Spark functions, never write a UDF — Catalyst can't optimize black boxes."*

**Common Mistake to Avoid:**
> Don't use regular Python UDFs for anything performance-critical. Always check if a native function exists first.

---

---

# 🔷 SECTION 2 — APACHE ICEBERG

---

### I1. ⭐ What is Apache Iceberg and why choose it?

**One-liner:** Iceberg is an open table format for big data that adds ACID transactions, time travel, and schema/partition evolution on top of Parquet files.

**Answer Script:**
> "Apache Iceberg is a table format — it sits on top of Parquet files on S3 and adds features that plain Parquet doesn't have: ACID transactions, time travel queries, schema evolution without rewriting data, and hidden partitioning.
>
> We chose Iceberg at AbbVie over Delta Lake because Iceberg is **engine-agnostic** — our team uses Spark for ingestion but Trino for ad-hoc queries. Delta Lake has limited Trino support. Also, Iceberg's hidden partitioning meant our queries didn't need to know the physical partition layout."

**Strong Interview One-Liner:**
> *"Iceberg brings data warehouse reliability — ACID, time travel, schema evolution — to an open data lake on S3."*

**Common Mistake to Avoid:**
> Don't say Iceberg replaces Parquet. Iceberg uses Parquet (or ORC) as its file format — it adds a metadata layer on top.

---

### I2. ⭐ Iceberg table architecture — metadata, manifest list, manifest file, data files

**One-liner:** Iceberg has a 4-layer metadata hierarchy: metadata file → manifest list → manifest files → actual Parquet data files.

**Answer Script:**
> "Iceberg has a layered metadata structure. At the top is the **metadata file** — it points to the current snapshot. The snapshot points to a **manifest list** — a list of all manifest files. Each **manifest file** tracks a set of Parquet data files and stores column-level statistics (min/max). The actual **data files** are at the bottom.
>
> This structure means Iceberg can skip files without scanning them — using column stats from manifest files. No directory listing needed — O(1) metadata lookups instead of slow S3 LIST operations."

**Strong Interview One-Liner:**
> *"Iceberg's metadata hierarchy enables file-level skipping — it knows exactly which files contain your data without scanning everything."*

**Common Mistake to Avoid:**
> Don't confuse manifest list with manifest file. Manifest list is the index of manifests; each manifest file tracks actual data files.

---

### I3. ⭐ Hidden Partitioning in Iceberg

**One-liner:** Hidden partitioning lets Iceberg derive partition values from column transforms — no extra column needed in the data, no change needed in queries.

**Answer Script:**
> "In Hive-style partitioning, you add a physical column like `event_date` and queries must filter on that exact column name. In Iceberg, hidden partitioning uses **transforms** like `days(event_ts)` — Iceberg derives the partition value from the timestamp automatically.
>
> So when a query filters `WHERE event_ts BETWEEN '2024-01-01' AND '2024-01-31'`, Iceberg automatically prunes to only January partitions — without the query knowing about the partition scheme.
>
> At AbbVie, this was critical. Our 20TB dataset was partitioned by `days(record_date)` — queries just filtered on `record_date` and Iceberg handled pruning automatically."

**Strong Interview One-Liner:**
> *"Hidden partitioning = partition logic lives in the table metadata, not in queries — cleaner code, automatic pruning."*

**Common Mistake to Avoid:**
> Don't add a separate partition column when using Iceberg hidden partitioning. That defeats the purpose — just use the transform.

---

### I4. Partition Evolution in Iceberg

**One-liner:** Iceberg lets you change the partition strategy without rewriting existing data — old and new partitioning coexist transparently.

**Answer Script:**
> "In Hive, changing partitioning means rewriting all existing data. In Iceberg, partition evolution is metadata-only — you just update the partition spec. Old files keep the old partitioning; new files use the new spec. Queries work transparently across both.
>
> At AbbVie, when our data volume grew, we changed from monthly to daily partitioning on a key table. With Iceberg, this was a single `ALTER TABLE` command — no 20TB rewrite needed."

```sql
-- Change from monthly to daily partitioning
ALTER TABLE iceberg_catalog.pharma.events
REPLACE PARTITION FIELD months(event_date) WITH days(event_date);
```

**Strong Interview One-Liner:**
> *"Partition evolution = change strategy without migration — this alone made Iceberg worth it for our 20TB dataset."*

**Common Mistake to Avoid:**
> Don't try this with Delta Lake — Delta doesn't support partition evolution. You'd need to rewrite the table.

---

### I5. ⭐ Time Travel in Iceberg — pharma audit use case

**One-liner:** Iceberg keeps a history of snapshots — you can query any previous version of the table using a timestamp or snapshot ID.

**Answer Script:**
> "Every write in Iceberg creates a new snapshot. Time travel lets you query any past snapshot using a timestamp or version number.
>
> In our pharmaceutical project, this was critical for regulatory audits. Auditors need to see exactly what the data looked like at a specific point in time — we couldn't just look at current state. With Iceberg, any historical query was just:

```sql
SELECT * FROM pharma.patient_records
TIMESTAMP AS OF '2023-06-01';
```

> No separate archive tables needed — time travel covered 18+ years of snapshots."

**Strong Interview One-Liner:**
> *"Time travel in Iceberg = built-in audit trail — perfect for regulated industries like pharma where historical accuracy is a compliance requirement."*

**Common Mistake to Avoid:**
> Time travel only works if you haven't run `expire_snapshots`. Always keep a reasonable snapshot retention window for compliance needs.

---

### I6. Compaction in Iceberg

**One-liner:** Compaction merges many small Parquet files into fewer larger files — improving scan performance and reducing metadata overhead.

**Answer Script:**
> "Every write to Iceberg creates new Parquet files. Over time — especially with frequent small appends — you get thousands of tiny files. Reading them is slow because Spark has to open, read, and close each one.
>
> Iceberg's `rewrite_data_files` compaction merges small files into the target size — typically 128MB to 1GB.

```python
from pyiceberg.catalog import load_catalog
# Or via Spark SQL:
spark.sql("""
  CALL iceberg_catalog.system.rewrite_data_files(
    table => 'pharma.patient_records',
    strategy => 'binpack'
  )
""")
```

> I run compaction weekly on high-write tables in our AbbVie pipeline."

**Strong Interview One-Liner:**
> *"Compaction is like OPTIMIZE in Delta — fix the small files problem to keep scan performance healthy."*

**Common Mistake to Avoid:**
> Don't run compaction during peak ingestion hours — it competes for S3 I/O with your pipeline.

---

### I7. ⭐ Copy-on-Write (CoW) vs Merge-on-Read (MoR)

**One-liner:** CoW rewrites entire files on every update — fast reads, slow writes; MoR writes delta files and merges on read — fast writes, slower reads.

**Answer Script:**
> "In **Copy-on-Write**, when you UPDATE or DELETE a record, Iceberg rewrites the entire Parquet file containing that record. Reads are always fast because files are clean. But writes are expensive for high-frequency updates.
>
> In **Merge-on-Read**, updates write a small delta/delete file. Reads must merge base files with delta files — slightly slower reads but much faster writes.
>
> At AbbVie, our ingestion tables use CoW — we write in large batches once daily, so rewrite cost is fine and reads are always fast for analytics."

**Strong Interview One-Liner:**
> *"CoW for read-heavy analytical tables; MoR for write-heavy CDC or streaming tables."*

**Common Mistake to Avoid:**
> Don't use MoR if your team runs many ad-hoc analytics queries — the merge-on-read overhead compounds across many queries.

---

### I8. ⭐ Schema Evolution in Iceberg

**One-liner:** Iceberg supports safe schema evolution — add/rename/drop/reorder columns without rewriting data, with full backward compatibility.

**Answer Script:**
> "Iceberg schema evolution is metadata-only — no data rewrite needed. You can:
> - **Add** a column — new files have it, old files return null
> - **Rename** a column — tracked by column ID, not name
> - **Drop** a column — it's hidden from queries but still in old files
> - **Reorder** columns — safe because Iceberg tracks by ID, not position
>
> At AbbVie, Salesforce API responses occasionally added new fields. With Iceberg, I used `mergeSchema=true` and the table schema auto-updated — no pipeline failure."

**Strong Interview One-Liner:**
> *"Iceberg tracks columns by ID, not name — that's why renaming and reordering are safe without breaking existing data."*

**Common Mistake to Avoid:**
> Don't confuse schema evolution (backward-compatible changes) with `overwriteSchema` (destructive full schema replacement).

---

### I9. ⭐ Delta Lake vs Iceberg vs Hudi

**One-liner:** Delta = best for Databricks; Iceberg = best for multi-engine open ecosystems; Hudi = best for high-frequency CDC/streaming updates.

**Answer Script:**
> "All three add ACID transactions and time travel to data lakes, but they differ in strengths.
>
> **Delta Lake** is tightly integrated with Databricks — best tooling for Spark, DLT, Unity Catalog. But has limited support outside the Databricks ecosystem.
>
> **Iceberg** is truly engine-agnostic — Spark, Trino, Flink, Presto all work natively. Best for multi-engine architectures and has the most advanced partitioning — hidden partitioning and partition evolution.
>
> **Hudi** is optimized for record-level upserts and CDC — fastest write performance for high-frequency streaming updates. Popular at Uber for exactly this use case.
>
> We chose Iceberg at AbbVie because our analytics team uses Trino alongside Spark."

**Strong Interview One-Liner:**
> *"If you're all-in on Databricks, use Delta. If you need multi-engine flexibility, use Iceberg. If you need fast record-level upserts, use Hudi."*

**Common Mistake to Avoid:**
> Don't say one is strictly better than the others. The right choice depends on your engine ecosystem and write patterns.

---

### I10. ⭐ 20TB Migration to Iceberg — zero downtime

**One-liner:** Parallel migration with validation gates, incremental sync, then cutover — never touching the live system until verified.

**Answer Script:**
> "Migrating 20TB with zero downtime required a phased approach.
>
> **Phase 1 — Mirror:** We ran the new Iceberg pipeline in parallel with the legacy system for 2 weeks. Both received writes simultaneously.
>
> **Phase 2 — Validate:** We compared row counts, checksums, and sample records between legacy and Iceberg at each layer.
>
> **Phase 3 — Cutover:** Once validation passed, downstream consumers were redirected to Iceberg — one table at a time.
>
> **Phase 4 — Decommission:** Kept legacy live for 30 days as a fallback, then retired it.
>
> The biggest technical challenge was partition design — initial use of `patient_id` as partition key created millions of tiny files. We switched to `year(event_date)` with Z-ordering on `patient_id`."

**Strong Interview One-Liner:**
> *"Zero-downtime migration = mirror, validate, cutover, decommission — never a big bang switch."*

**Common Mistake to Avoid:**
> Don't migrate all tables at once. Migrate one table, validate fully, then move to the next — parallel validation across all tables is unmanageable.

---

*End of Part 1: Spark + Iceberg*
*Continue reading Part 2: Delta Lake + Architecture + Governance + Azure*


# 🎯 Interview Answer Scripts — Mayuresh Patil
## Part 2: Delta Lake · Architecture · Governance · Azure · SQL · Kafka
> Short · Simple · Speakable in 30–90 seconds · Project-grounded

---

# 🔷 SECTION 3 — DELTA LAKE

---

### D1. ⭐ What is Delta Lake? How does it differ from plain Parquet?

**One-liner:** Delta Lake adds ACID transactions, time travel, and schema enforcement on top of Parquet files using a transaction log called `_delta_log`.

**Answer Script:**
> "Plain Parquet is just a file format — no transactions, no updates, no history. Delta Lake adds a **`_delta_log` folder** alongside the Parquet files. Every write, update, or delete creates a new JSON entry in that log. The log is the source of truth — reading a Delta table means reading the log to find which files are current.
>
> This gives you: ACID transactions (all-or-nothing writes), time travel (query past versions), schema enforcement (reject bad schemas), and safe concurrent writes.
>
> At AbbVie, where we have 10M+ records daily with strict auditability requirements, ACID is non-negotiable."

**Strong Interview One-Liner:**
> *"Delta Lake = Parquet + `_delta_log` = ACID + time travel + schema enforcement."*

**Common Mistake to Avoid:**
> Don't say Delta replaces Parquet. Delta Lake uses Parquet files internally — it adds the transaction layer on top.

---

### D2. ⭐ ACID in Delta — how does `_delta_log` enable it?

**One-liner:** Every operation writes a commit entry to `_delta_log` — atomically. If the commit file is there, the transaction succeeded. If not, it never happened.

**Answer Script:**
> "Delta achieves ACID through its transaction log. Every write creates a new JSON commit file in `_delta_log`. This is atomic — either the file is written completely or not at all.
>
> **Atomicity:** Commit file exists = success; if the job crashes mid-write, the commit file is never created — the partial data is invisible.
>
> **Isolation:** Readers always see the latest committed snapshot — they never see in-progress writes.
>
> **Consistency:** Schema enforcement rejects writes that violate the schema before any data lands.
>
> **Durability:** Once the commit file is on S3/ADLS, it's permanent — even if the cluster crashes afterward."

**Strong Interview One-Liner:**
> *"The `_delta_log` is the backbone of Delta ACID — it's an append-only ledger where every transaction is either fully recorded or doesn't exist."*

**Common Mistake to Avoid:**
> Don't say Delta uses database-style locks. Delta uses **optimistic concurrency** — writers proceed independently and conflict resolution happens at commit time.

---

### D3. Schema Enforcement vs Schema Evolution in Delta

**One-liner:** Schema enforcement rejects writes that don't match the schema; schema evolution allows adding new columns with `mergeSchema=true`.

**Answer Script:**
> "**Schema enforcement** is the default — Delta rejects any write whose schema doesn't match the table's schema. This prevents accidental schema drift from breaking downstream consumers.
>
> **Schema evolution** is opt-in — when a source adds new columns, you use `mergeSchema=true` to let Delta automatically add those columns to the table schema.

```python
# Schema evolution — auto-add new columns
df.write.format("delta").option("mergeSchema", "true").mode("append").save(path)
```

> At AbbVie, Salesforce API responses sometimes added new fields. I used `mergeSchema=true` in those pipelines so new columns are captured without pipeline failures."

**Strong Interview One-Liner:**
> *"Enforcement protects data integrity; evolution handles source system changes — use both strategically."*

**Common Mistake to Avoid:**
> Don't confuse `mergeSchema` with `overwriteSchema`. `mergeSchema` adds columns; `overwriteSchema` completely replaces the schema — dangerous in production.

---

### D4. Small Files Problem — OPTIMIZE and VACUUM

**One-liner:** Many writes create many small Parquet files. OPTIMIZE compacts them; VACUUM removes old unreferenced files.

**Answer Script:**
> "Every Spark task writes one file per partition. With 200 partitions writing daily, you accumulate thousands of tiny files quickly — causing slow scans because Spark opens each file individually.
>
> **OPTIMIZE** compacts small files into larger ones (targeting ~1GB). Optionally with `ZORDER BY` to cluster related data together.
>
> **VACUUM** removes Parquet files no longer referenced by the transaction log — it cleans up old files from updates and deletes. Default retention is 7 days to support time travel.

```sql
OPTIMIZE schema.table ZORDER BY (customer_id, event_date);
VACUUM schema.table RETAIN 168 HOURS;
```

> I schedule OPTIMIZE weekly and VACUUM after it in our AbbVie pipelines."

**Strong Interview One-Liner:**
> *"OPTIMIZE for performance; VACUUM for storage cost — run them together as weekly maintenance."*

**Common Mistake to Avoid:**
> Never run `VACUUM RETAIN 0 HOURS` in production — it destroys time travel history permanently.

---

### D5. Z-Ordering vs Liquid Clustering

**One-liner:** Z-Ordering sorts data within files by multiple columns to improve data skipping; Liquid Clustering is the newer, incremental version that doesn't require full rewrite.

**Answer Script:**
> "**Z-Ordering** co-locates related data within Parquet files using a Z-curve algorithm. When you query by `customer_id`, Parquet's min/max statistics can skip most files because similar IDs are grouped together.
>
> **Liquid Clustering** (Databricks Runtime 13+) is the evolution — it achieves the same goal but **incrementally**, without rewriting the whole table. You can also change clustering columns without a full rewrite.
>
> For our current setup at AbbVie on Databricks, I use Z-Ordering on high-cardinality filter columns like `patient_id` and `facility_id` that can't be partition columns due to cardinality."

**Strong Interview One-Liner:**
> *"Z-ordering for multi-column data skipping; Liquid Clustering if you need incremental, maintainable clustering without full rewrites."*

**Common Mistake to Avoid:**
> Don't Z-order on your partition column — it's already clustered at the folder level. Z-order on high-cardinality columns that aren't partitioned.

---

### D6. ⭐ Recovering from accidental deletion — 50,000 records

**One-liner:** Use Delta time travel — restore the table to the version before the deletion.

**Answer Script:**
> "This is exactly what Delta's time travel is designed for. Find the version just before the bad operation:

```sql
-- Find the version before the deletion
DESCRIBE HISTORY schema.my_table;

-- Restore to that version
RESTORE TABLE schema.my_table TO VERSION AS OF 42;
-- OR
RESTORE TABLE schema.my_table TO TIMESTAMP AS OF '2024-01-15 08:00:00';
```

> This is instant — it just updates the `_delta_log` pointer. No data is rewritten.
>
> Important: this only works if VACUUM hasn't removed the old files. That's why I keep the default 7-day retention — for exactly this kind of production recovery."

**Strong Interview One-Liner:**
> *"Delta time travel = instant rollback — RESTORE takes seconds because it's just a log pointer update."*

**Common Mistake to Avoid:**
> Don't VACUUM immediately after discovering an issue — you might delete the files you need for recovery.

---

---

# 🔷 SECTION 4 — DATA ARCHITECTURE & MODELING

---

### A1. ⭐ Data Lakehouse vs Data Lake vs Data Warehouse

**One-liner:** Data Lake = cheap storage, no structure; Data Warehouse = expensive, structured; Lakehouse = cheap storage + warehouse features.

**Answer Script:**
> "A **Data Lake** stores raw data in any format cheaply on S3 or ADLS — but no ACID, no transactions, no guaranteed quality.
>
> A **Data Warehouse** like Snowflake or Redshift has ACID, fast queries, strict schema — but is expensive and doesn't handle unstructured data well.
>
> A **Lakehouse** combines both — open formats (Parquet + Iceberg/Delta) on cheap object storage, with warehouse-level features: ACID, schema enforcement, time travel, SQL support.
>
> At AbbVie, we built a Lakehouse using Iceberg on S3 — we get S3 storage costs with ACID and time travel for regulatory compliance."

**Strong Interview One-Liner:**
> *"Lakehouse = Data Lake + Data Warehouse features — the best of both worlds, on open storage."*

**Common Mistake to Avoid:**
> Don't say Lakehouse is just a marketing term. It's a real architectural pattern with specific technical capabilities enabled by Iceberg, Delta, or Hudi.

---

### A2. ⭐ Medallion Architecture

**One-liner:** Raw → Trusted → Curated — each layer adds quality, structure, and business value.

**Answer Script:**
> "Medallion is a three-layer data architecture.
>
> **Raw (Bronze):** Ingest data as-is from sources — no transformation, full history preserved. Source of truth if anything goes wrong downstream.
>
> **Trusted (Silver):** Clean, validate, deduplicate, enforce types. Apply DQ checks here — reject or quarantine bad records.
>
> **Curated (Gold):** Business-level aggregations, joins, and enrichments optimized for consumption by analytics and ML teams.
>
> At AbbVie, I refactored legacy SQL scripts into this Medallion pattern — it gave us clear data lineage, easier debugging, and regulatory auditability at each layer."

**Strong Interview One-Liner:**
> *"Medallion = Raw data in, quality data out — each layer adds trust, structure, and business value."*

**Common Mistake to Avoid:**
> Don't skip the Raw layer to save storage. Raw is your insurance policy — you can always re-derive Silver and Gold from it.

---

### A5. ⭐ Partitioning — how to choose the column?

**One-liner:** Partition on low-to-medium cardinality columns that are always in your WHERE clause and have even data distribution.

**Answer Script:**
> "A good partition column has these properties: **low-to-medium cardinality** (10–10,000 values, not millions), **frequently used in filters** (so pruning triggers), and **even distribution** (avoid one date having 90% of data).
>
> **Date columns** like `event_date` or `load_date` are ideal for time-series data.
>
> Avoid high-cardinality columns like `user_id` — they create millions of tiny directories, killing metadata performance.
>
> At AbbVie, I partition by `year(event_date)` at the coarse level, combined with Z-ordering on `patient_id` and `facility_id` for fine-grained data skipping within partitions."

**Strong Interview One-Liner:**
> *"Partition for pruning, Z-order for skipping — they work at different granularities and complement each other."*

**Common Mistake to Avoid:**
> Don't partition on high-cardinality columns. A `user_id` partition on 50M users = 50M directories on S3 — catastrophic for metadata.

---

### A8. ⭐ Designing a pipeline for 500 Oracle tables → Iceberg

**One-liner:** Build a metadata-driven framework — config-driven ingestion, not 500 separate scripts.

**Answer Script:**
> "You never write 500 individual scripts. You build a **metadata-driven framework** where a config table or YAML file defines each table's ingestion rules — source table name, load type (full/incremental), partition column, primary key, target path.
>
> The framework reads this config and runs one generic PySpark class for all tables:

```python
config = load_config("oracle_tables.yaml")  # 500 table definitions
for table in config:
    ingestion_engine.run(table)  # One framework, all tables
```

> This is exactly what I built at AbbVie. It handled 10M+ records daily across all source tables with a single deployable codebase — no duplication."

**Strong Interview One-Liner:**
> *"500 tables = one framework, not 500 scripts — metadata-driven design is the only scalable approach."*

**Common Mistake to Avoid:**
> Don't hardcode table names or schema in PySpark code. Any hardcoded logic becomes a maintenance nightmare when tables change.

---

---

# 🔷 SECTION 5 — MDM, GOVERNANCE & DATA QUALITY

---

### G1. ⭐ What is MDM and what problem does it solve?

**One-liner:** MDM creates one trusted, unified record for each business entity by merging data from multiple source systems.

**Answer Script:**
> "Master Data Management solves the problem of the same entity — a patient, customer, or product — existing with different IDs across different systems.
>
> At AbbVie, a patient might be `ORC-001` in Oracle, `SF-A123` in Salesforce, and `SNF-XY99` in Snowflake. Without MDM, analytics teams see three different patients. With MDM, we unify them into one `MASTER-001` golden record — the single source of truth for downstream analytics and reporting."

**Strong Interview One-Liner:**
> *"MDM answers: 'How many unique customers do we actually have?' — impossible without unifying IDs across systems."*

**Common Mistake to Avoid:**
> Don't say MDM is just deduplication. It's deeper — it involves survivorship rules, XREF mappings, and maintaining the unification as source systems change.

---

### G2. ⭐ Golden Record / XREF system

**One-liner:** XREF is a cross-reference mapping table that links source system IDs to a master ID — enabling the Golden Record.

**Answer Script:**
> "The XREF table is the core of our identity resolution. It maps every source system ID to a single master ID:

```
Source    | Source ID  | Master ID
Oracle    | ORC-001    | MDM-PAT-001
Salesforce| SF-A123    | MDM-PAT-001
Snowflake | SNF-XY99   | MDM-PAT-001
```

> When I join any source table to the XREF, I get the master ID. Then I apply survivorship rules to merge all source records into one Golden Record — choosing the most trusted or most recent value per field.
>
> This Golden Record then feeds all downstream MDM workflows and analytics."

**Strong Interview One-Liner:**
> *"XREF is the bridge between fragmented source IDs and a unified master entity — without it, Golden Record is impossible."*

**Common Mistake to Avoid:**
> Don't assume XREF is static. Source systems add new records constantly — XREFs need incremental updates, not just a one-time load.

---

### G3. Survivorship Rules in PySpark

**One-liner:** Survivorship rules decide which source system "wins" for each field when multiple sources conflict.

**Answer Script:**
> "Survivorship rules define: when Oracle says the patient's email is X and Salesforce says it's Y — which do we trust?
>
> Common rules:
> - **Most recent wins** — trust the source with the latest `updated_at`
> - **Source priority wins** — Oracle is tier-1, Salesforce is tier-2
> - **Most complete wins** — prefer the non-null value

```python
from pyspark.sql.window import Window

# Most recent wins
window = Window.partitionBy("master_id").orderBy(F.desc("updated_at"))
df_golden = df_merged \
    .withColumn("rn", F.row_number().over(window)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")
```

**Strong Interview One-Liner:**
> *"Survivorship rules are your business logic for conflict resolution — the most important and most debated part of any MDM project."*

**Common Mistake to Avoid:**
> Don't apply one rule to all fields. Email might use "most recent wins" while date-of-birth uses "source priority wins." Rules vary by field.

---

### G4. ⭐ Data Quality checks — Bronze → Silver

**One-liner:** Apply null checks, duplicate checks, range checks, and referential integrity checks before promoting data to Silver.

**Answer Script:**
> "In my Medallion pipeline, DQ gates sit between Raw and Trusted. My standard checks:
>
> - **Null checks** on mandatory fields (patient_id, facility_id, record_date)
> - **Duplicate check** — deduplicate on primary key
> - **Range validation** — no negative amounts, no future dates
> - **Referential integrity** — every facility_id must exist in the facilities table
>
> Records that fail DQ go to a **quarantine table**, not rejected outright — so the business can review and fix them. Valid records flow to Silver.
>
> At AbbVie, this quarantine pattern helped us reduce bad data reaching downstream analytics by catching issues at the boundary."

**Strong Interview One-Liner:**
> *"DQ checks at Bronze→Silver are your quality firewall — quarantine bad records, don't silently drop them."*

**Common Mistake to Avoid:**
> Don't silently drop bad records. Always write them to a quarantine table with the failure reason — the business needs to know what failed and why.

---

### G5. CDC Duplicate Records — how to fix

**One-liner:** Deduplicate using window functions, then MERGE into the target to upsert.

**Answer Script:**
> "CDC streams can deliver the same event multiple times due to network retries or at-least-once delivery. My fix is two steps:
>
> Step 1 — Deduplicate the incoming batch — keep the latest record per key:

```python
window = Window.partitionBy("record_id").orderBy(F.desc("updated_at"))
df_deduped = df_cdc.withColumn("rn", F.row_number().over(window)) \
                   .filter(F.col("rn") == 1).drop("rn")
```

> Step 2 — Use MERGE INTO the target — so even if a record arrives twice across batches, the second MERGE is a no-op for unchanged records.
>
> This two-step pattern makes the pipeline **idempotent** — safe to re-run without creating duplicates."

**Strong Interview One-Liner:**
> *"Dedup incoming + MERGE into target = idempotent pipeline that handles CDC duplicates safely."*

**Common Mistake to Avoid:**
> Don't just append CDC records to the target. Appending creates duplicates — always MERGE to handle updates and late arrivals correctly.

---

### G7. Modak Nabu — audit and governance

**One-liner:** Nabu is a data pipeline observability tool — we used it to track every pipeline run with metadata like rows read, rows written, status, and errors.

**Answer Script:**
> "Nabu acts as our audit logging layer. After every pipeline run, we write an audit record:

```python
audit = {
    "pipeline": "oracle_to_iceberg_patients",
    "run_id": run_id,
    "rows_read": source_count,
    "rows_written": target_count,
    "status": "SUCCESS",
    "timestamp": datetime.now()
}
nabu_client.log(audit)
```

> This gave us three things: **traceability** — every record can be traced to its source pipeline, **incident response** — when bad data appeared, we could identify exactly which run introduced it, and **compliance** — auditors could see full data lineage for regulatory reviews."

**Strong Interview One-Liner:**
> *"Nabu is our pipeline's black box flight recorder — every run is logged so we always know what happened to every record."*

**Common Mistake to Avoid:**
> Don't log only errors. Log every run — successful ones too. You need to trace a successful run that produced wrong results, not just failed ones.

---

### G8. ⭐ GDPR and PII handling

**One-liner:** GDPR requires identifying PII, masking or encrypting it at rest, and supporting the right to be forgotten (deletion).

**Answer Script:**
> "For GDPR compliance in our pharma data platform, we do three things:
>
> **1. PII identification** — tag columns like patient_name, email, DOB as sensitive in the data catalog.
>
> **2. Data masking** — replace real PII with masked values in non-production environments:

```python
df.withColumn("email", F.sha2(F.col("email"), 256))  # One-way hash
df.withColumn("name", F.lit("MASKED"))
```

> **3. Right to erasure** — when a deletion request comes, we use Iceberg's row-level delete to remove that patient's records and expire the old snapshots.
>
> In Databricks, Unity Catalog's column masking policies enforce this automatically."

**Strong Interview One-Liner:**
> *"GDPR in a data lake = tag PII, mask in non-prod, and ensure you can delete a specific record — all three are non-negotiable."*

**Common Mistake to Avoid:**
> Don't think GDPR only applies to EU data. AbbVie is a global pharma company — treat all patient data with GDPR-level care regardless of region.

---

---

# 🔷 SECTION 6 — AZURE & ORCHESTRATION

---

### C1. ⭐ ADF vs Databricks Workflows

**One-liner:** ADF orchestrates cross-system ETL (copy, trigger, schedule); Databricks Workflows orchestrates Spark-native compute jobs.

**Answer Script:**
> "**ADF** is Azure's orchestration service. It's great for: copying data between systems, triggering Databricks notebooks, scheduling pipelines, integrating with non-Spark sources. It has a GUI for non-engineers.
>
> **Databricks Workflows** is code-first job orchestration — ideal when all your compute is Spark-based, you want notebook chaining, or you need complex dependency management within Databricks.
>
> At AbbVie, our pattern is: **ADF handles the trigger** (file arrives on S3, schedule fires at 2 AM), then **calls a Databricks notebook** for all PySpark transformation logic."

**Strong Interview One-Liner:**
> *"ADF for orchestration and triggers; Databricks for compute — use them together, not as alternatives."*

**Common Mistake to Avoid:**
> Don't try to do heavy data transformations inside ADF Data Flow. For complex PySpark logic, always trigger a Databricks notebook from ADF.

---

### C3. ⭐ Unity Catalog

**One-liner:** Unity Catalog is Databricks' centralized governance layer — one place for access control, data lineage, and auditing across all workspaces.

**Answer Script:**
> "Unity Catalog sits above the Databricks workspace level — it manages data permissions, lineage, and discovery across all workspaces in one place.
>
> Key features: **fine-grained access control** — row and column level security; **data lineage** — tracks which notebook wrote which table; **centralized audit logs** — who accessed what and when.
>
> At AbbVie, I used Unity Catalog to restrict access to PHI columns — only authorized users in the clinical team could query patient PII. Everyone else got masked values through column masking policies."

**Strong Interview One-Liner:**
> *"Unity Catalog = one governance layer for all Databricks workspaces — RBAC, lineage, and audit in one place."*

**Common Mistake to Avoid:**
> Don't confuse Unity Catalog with the Hive metastore. Unity Catalog is multi-workspace, multi-cloud — it's the successor to workspace-level Hive metastore.

---

### C5. Azure Key Vault — secrets management

**One-liner:** Key Vault stores secrets, passwords, and tokens — Databricks uses a secret scope backed by Key Vault to access them securely.

**Answer Script:**
> "You never store passwords in code or notebooks. In Databricks, you create a **secret scope** backed by Azure Key Vault, then access secrets using `dbutils.secrets.get()`:

```python
# In Databricks notebook — no hardcoded passwords
jdbc_password = dbutils.secrets.get(scope="abbvie-kv", key="oracle-password")
sf_token      = dbutils.secrets.get(scope="abbvie-kv", key="salesforce-token")
```

> The secret value is never printed or logged — Databricks redacts it automatically. Key Vault also supports rotation — when a password changes, you update it in Key Vault and all pipelines pick it up without redeployment."

**Strong Interview One-Liner:**
> *"Key Vault + Databricks secret scope = zero secrets in code — security requirement, not optional."*

**Common Mistake to Avoid:**
> Never hardcode credentials in notebooks, even temporarily. Notebooks get committed to Git — credentials exposed in history forever.

---
# 🎯 Azure Interview Answer Scripts — C6, C7, C8, C9
> **Mayuresh Patil | Azure Cloud & CI/CD Section**
> Short · Simple · Speakable · Project-Grounded

---

## C6. What is ADLS Gen2 and how does it differ from regular blob storage?

**One-liner:** ADLS Gen2 is Azure's enterprise data lake storage — it adds a hierarchical file system, fine-grained security, and big data performance on top of standard Azure Blob Storage.

### Answer Script

> "Azure Blob Storage is general-purpose object storage — good for storing files, backups, and static assets. ADLS Gen2 is built on top of Blob Storage but adds features specifically for big data workloads.
>
> The three key differences:
>
> **1. Hierarchical Namespace (HNS):**
> Regular Blob Storage is flat — everything is just an object with a key. ADLS Gen2 has real directories and folders like a file system. This matters because operations like renaming a folder are atomic and O(1) — not O(n) like in flat Blob Storage.
>
> **2. Fine-grained Access Control:**
> ADLS Gen2 supports POSIX-compliant ACLs — you can set permissions at the file level, not just the container level. Regular Blob only supports RBAC at the container level.
>
> **3. Performance for Analytics:**
> ADLS Gen2 is optimized for parallel reads — used by Spark, ADF, Databricks, HDInsight natively. It supports the `abfss://` protocol for secure access.

```python
# ADLS Gen2 path in Databricks (uses abfss:// protocol)
adls_path = "abfss://container@storageaccount.dfs.core.windows.net/data/patients/"

# Regular Blob Storage path (uses wasbs:// protocol)
blob_path = "wasbs://container@storageaccount.blob.core.windows.net/data/patients/"

# Reading from ADLS Gen2 in PySpark
df = spark.read.format("delta").load(adls_path)
```

> In my AbbVie project, all Databricks Delta Lake tables are stored on ADLS Gen2 — the hierarchical namespace makes directory operations fast when compacting or reorganizing partitions."

---

### Comparison Table

| Feature | Azure Blob Storage | ADLS Gen2 |
|---|---|---|
| Namespace | Flat (key-value objects) | Hierarchical (real folders) |
| Folder rename | O(n) — copies all objects | O(1) — atomic, instant |
| Access control | Container-level RBAC | File/folder-level ACLs + RBAC |
| Protocol | `wasbs://` | `abfss://` (secure) |
| Big data optimized | Limited | ✅ Native Spark, ADF, Databricks |
| Cost | Same pricing | Same pricing (it's built on Blob) |
| Use case | File storage, backups | Data Lake, Lakehouse, Analytics |

---

### Strong Interview One-Liner
> *"ADLS Gen2 is Blob Storage with a file system brain — hierarchical namespace, POSIX ACLs, and analytics-grade performance."*

### Common Mistake to Avoid
> Don't say ADLS Gen2 is a completely separate storage service. It's Azure Blob Storage with the hierarchical namespace feature enabled — same underlying infrastructure, upgraded capabilities.

---

---

## C7. When would you use Azure Synapse Analytics over Databricks?

**One-liner:** Use Synapse when you need a unified workspace combining SQL data warehouse, Spark, and Power BI in one place; use Databricks when you need advanced Spark optimization, MLflow, or best-in-class data engineering.

### Answer Script

> "Both are Azure big data platforms, but they serve slightly different primary use cases.
>
> **Choose Azure Synapse when:**
> - Your team is SQL-first — analysts who prefer T-SQL over PySpark
> - You need a dedicated SQL pool (MPP data warehouse) for BI workloads
> - You want one unified workspace for SQL, Spark, pipelines, and Power BI integration
> - The workload is primarily structured data reporting — not complex ML or data engineering
>
> **Choose Databricks when:**
> - You need best-in-class PySpark performance with advanced optimizations
> - You're building ML pipelines — MLflow is native, not bolted on
> - You need Unity Catalog for enterprise governance
> - Your team is data engineering or ML-heavy
> - You need Delta Live Tables or DLT for declarative pipeline development

```
                Synapse                          Databricks
         ┌──────────────────┐            ┌──────────────────────┐
         │ SQL Pool (MPP)   │            │ Advanced Spark        │
         │ Serverless SQL   │            │ Delta Live Tables     │
         │ Spark (built-in) │            │ MLflow / ML           │
         │ Power BI link    │            │ Unity Catalog         │
         │ ADF integration  │            │ Delta Lake optimized  │
         └──────────────────┘            └──────────────────────┘
         Best: SQL teams, BI reporting   Best: DE + ML teams
```

> At AbbVie, we use Databricks — our workloads are data engineering heavy (Iceberg, PySpark, Medallion pipelines, 10M+ records daily). The advanced Spark tuning and Unity Catalog were more important for us than Synapse's SQL pool."

---

### Decision Guide

| Scenario | Use |
|---|---|
| SQL analysts running T-SQL reports | Synapse |
| Data engineers building PySpark pipelines | Databricks |
| Building ML models at scale | Databricks |
| BI dashboard connected to data warehouse | Synapse |
| Complex ETL with Delta Live Tables | Databricks |
| Mixed team — SQL + BI + some Spark | Synapse (unified workspace) |
| Enterprise governance + Unity Catalog | Databricks |

---

### Strong Interview One-Liner
> *"Synapse if you're SQL-first and BI-focused; Databricks if you're PySpark-first and data engineering or ML-focused."*

### Common Mistake to Avoid
> Don't say one is strictly better. Both are valid choices. The answer always depends on the team's skill set and primary workload type.

---

---

## C8. ⭐ How would you implement a CI/CD pipeline for Databricks notebooks using Azure DevOps?

**One-liner:** Git branching strategy + automated pytest on PR + Azure DevOps YAML pipeline deploys notebooks and job configs to Databricks on merge to main.

### Answer Script

> "My CI/CD setup for Databricks on Azure DevOps has four components: version control, automated testing, deployment pipeline, and secrets management.

**Step 1 — Git Branching Strategy:**
```
main        → Production (protected, requires PR + review)
  └── dev   → Integration (features merge here first)
        └── feature/oracle-incremental-load
        └── feature/salesforce-pagination
        └── hotfix/schema-drift-fix
```

**Step 2 — Unit Tests (run on every PR):**
```python
# tests/test_dq_checks.py
import pytest
from pyspark.sql import SparkSession
from src.dq_checks import apply_null_check

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]") \
           .appName("test").getOrCreate()

def test_null_patient_id_removed(spark):
    data = [(1, "Alice"), (None, "Bob"), (3, "Charlie")]
    df   = spark.createDataFrame(data, ["patient_id", "name"])
    result = apply_null_check(df, "patient_id")
    assert result.count() == 2
    assert result.filter("patient_id IS NULL").count() == 0
```

**Step 3 — Azure DevOps YAML Pipeline:**
```yaml
# azure-pipelines.yml
trigger:
  branches:
    include:
      - main
      - dev

variables:
  - group: databricks-secrets  # Linked to Azure Key Vault

stages:

# ─── CI Stage: Run Tests on PR ───
- stage: Test
  jobs:
  - job: RunUnitTests
    pool:
      vmImage: ubuntu-latest
    steps:
    - task: UsePythonVersion@0
      inputs:
        versionSpec: '3.9'

    - script: |
        pip install pytest pyspark delta-spark
      displayName: 'Install dependencies'

    - script: |
        pytest tests/ -v --junitxml=test-results.xml
      displayName: 'Run unit tests'

    - task: PublishTestResults@2
      inputs:
        testResultsFiles: 'test-results.xml'
        testRunTitle: 'PySpark Unit Tests'

# ─── CD Stage: Deploy to Databricks (only on main merge) ───
- stage: Deploy
  condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
  jobs:
  - job: DeployToDatabricks
    pool:
      vmImage: ubuntu-latest
    steps:
    - script: |
        pip install databricks-cli
      displayName: 'Install Databricks CLI'

    - script: |
        # Configure Databricks CLI using Key Vault secrets
        databricks configure --token <<EOF
        $(DATABRICKS_HOST)
        $(DATABRICKS_TOKEN)
        EOF
      displayName: 'Configure Databricks CLI'
      env:
        DATABRICKS_HOST: $(DATABRICKS_HOST)
        DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)

    - script: |
        # Deploy notebooks to Databricks workspace
        databricks workspace import_dir \
          ./notebooks \
          /Shared/production \
          --overwrite \
          --language PYTHON
      displayName: 'Deploy Notebooks'

    - script: |
        # Update Databricks Job configuration
        databricks jobs reset \
          --job-id $(JOB_ID_ORACLE_INGESTION) \
          --json-file ./job_configs/oracle_ingestion_job.json

        databricks jobs reset \
          --job-id $(JOB_ID_SALESFORCE_INGESTION) \
          --json-file ./job_configs/salesforce_ingestion_job.json
      displayName: 'Update Job Configs'

    - script: |
        # Optional: Trigger a smoke test job run
        run_id=$(databricks runs submit \
          --json-file ./job_configs/smoke_test_job.json \
          | python -c "import sys,json; print(json.load(sys.stdin)['run_id'])")
        echo "Smoke test run_id: $run_id"
      displayName: 'Run Smoke Test'
```

**Step 4 — Secrets Management:**
```
Azure Key Vault
  ├── DATABRICKS_HOST   → https://adb-xxx.azuredatabricks.net
  ├── DATABRICKS_TOKEN  → dapi_xxx
  ├── JOB_ID_ORACLE     → 12345
  └── JOB_ID_SALESFORCE → 12346

Azure DevOps Variable Group → linked to Key Vault
→ Pipeline reads secrets at runtime, never stored in YAML
```

> This setup means: every PR triggers automated tests, every merge to main auto-deploys to production Databricks. No manual notebook uploads. No environment drift."

---

### Environment Promotion Flow

```
Developer pushes feature branch
         ↓
  PR opened → CI runs pytest
         ↓
  PR approved + merged to dev
         ↓
  Deploy to DEV Databricks workspace (auto)
         ↓
  Integration test passes
         ↓
  PR from dev → main
         ↓
  Deploy to PRODUCTION Databricks workspace (auto)
         ↓
  Smoke test job runs to confirm deployment
```

---

### Folder Structure

```
project/
├── notebooks/
│   ├── bronze/
│   │   └── oracle_ingestion.py
│   ├── silver/
│   │   └── patient_cleaning.py
│   └── gold/
│       └── revenue_aggregation.py
├── src/
│   ├── dq_checks.py
│   ├── ingestion_utils.py
│   └── config_loader.py
├── tests/
│   ├── test_dq_checks.py
│   └── test_ingestion_utils.py
├── job_configs/
│   ├── oracle_ingestion_job.json
│   └── salesforce_ingestion_job.json
├── azure-pipelines.yml
└── requirements.txt
```

---

### Strong Interview One-Liner
> *"CI/CD for Databricks = automated pytest on every PR + automated notebook deployment on every merge to main — zero manual steps in production."*

### Common Mistake to Avoid
> Never store `DATABRICKS_TOKEN` or secrets in the YAML file or in notebook code. Always use Azure DevOps Variable Groups linked to Key Vault. Secrets in YAML get committed to Git history — exposed permanently.

---

---

## C9. How did you automate infrastructure deployment using Python SQL generation scripts?

**One-liner:** Built a Jinja2-template-based Python script that dynamically generated 300+ SQL configuration files from a single YAML config — eliminating 15+ hours/week of manual setup.

### Answer Script

> "Before my automation, the team maintained 300+ SQL scripts manually — table DDLs, views, stored procedures, configuration tables — one set per environment (Dev, Validation, Production). Every change meant manually editing scripts across 3 environments. Error-prone, slow, 15+ hours per week.
>
> I built a metadata-driven SQL generation framework in Python."

---

### The Problem

```
Before automation:
  Dev environment:     300 SQL scripts (manual)
  Validation env:      300 SQL scripts (manually copied + edited)
  Production env:      300 SQL scripts (manually copied + edited)
  
  Total: 900 scripts to maintain
  Risk: One missed edit = environment mismatch = production bug
  Time: 15+ hours/week just for config management
```

---

### The Solution

**Step 1 — YAML Config (single source of truth):**
```yaml
# table_configs.yaml
environments:
  dev:
    schema: "pharma_dev"
    s3_path: "s3://abbvie-dev/iceberg/"
  validation:
    schema: "pharma_val"
    s3_path: "s3://abbvie-val/iceberg/"
  production:
    schema: "pharma_prod"
    s3_path: "s3://abbvie-prod/iceberg/"

tables:
  - name: PATIENT_RECORDS
    primary_key: patient_id
    partition_col: record_date
    columns:
      - { name: patient_id,   type: STRING,    nullable: false }
      - { name: facility_id,  type: STRING,    nullable: false }
      - { name: record_date,  type: DATE,      nullable: false }
      - { name: diagnosis,    type: STRING,    nullable: true  }
      - { name: updated_at,   type: TIMESTAMP, nullable: true  }

  - name: FACILITY_MASTER
    primary_key: facility_id
    partition_col: load_date
    columns:
      - { name: facility_id,   type: STRING, nullable: false }
      - { name: facility_name, type: STRING, nullable: true  }
      - { name: region,        type: STRING, nullable: true  }
```

**Step 2 — Jinja2 SQL Templates:**
```sql
-- templates/create_table.sql.j2
CREATE TABLE IF NOT EXISTS {{ schema }}.{{ table.name }} (
    {% for col in table.columns %}
    {{ col.name }} {{ col.type }}{% if not col.nullable %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}

    {% endfor %}
)
USING iceberg
PARTITIONED BY ({{ table.partition_col }})
LOCATION '{{ s3_path }}{{ table.name | lower }}/'
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);
```

```sql
-- templates/create_audit_view.sql.j2
CREATE OR REPLACE VIEW {{ schema }}.{{ table.name }}_AUDIT_VW AS
SELECT
    {{ table.primary_key }},
    {% for col in table.columns %}
    {{ col.name }}{% if not loop.last %},{% endif %}

    {% endfor %}
    current_timestamp() AS view_generated_at
FROM {{ schema }}.{{ table.name }}
WHERE updated_at >= CURRENT_DATE - INTERVAL 30 DAYS;
```

**Step 3 — Python Generation Engine:**
```python
import yaml
from jinja2 import Environment, FileSystemLoader
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_sql_scripts(config_path: str, target_env: str, output_dir: str):
    """
    Generate all SQL scripts for a given environment from YAML config.
    
    Args:
        config_path: Path to YAML config file
        target_env:  Environment name (dev / validation / production)
        output_dir:  Where to write generated SQL files
    """
    # Load config
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    env_config = config["environments"][target_env]
    schema     = env_config["schema"]
    s3_path    = env_config["s3_path"]

    # Setup Jinja2 template engine
    jinja_env = Environment(
        loader=FileSystemLoader("templates/"),
        trim_blocks=True,
        lstrip_blocks=True
    )

    # Generate scripts for each table
    generated_files = []
    for table in config["tables"]:
        table_name = table["name"]
        logger.info(f"Generating scripts for {table_name} in {target_env}...")

        # 1. CREATE TABLE script
        create_template = jinja_env.get_template("create_table.sql.j2")
        create_sql = create_template.render(
            schema=schema,
            s3_path=s3_path,
            table=table
        )
        create_file = os.path.join(output_dir, target_env, f"CREATE_{table_name}.sql")
        _write_file(create_file, create_sql)
        generated_files.append(create_file)

        # 2. AUDIT VIEW script
        view_template = jinja_env.get_template("create_audit_view.sql.j2")
        view_sql = view_template.render(
            schema=schema,
            table=table
        )
        view_file = os.path.join(output_dir, target_env, f"VIEW_{table_name}_AUDIT.sql")
        _write_file(view_file, view_sql)
        generated_files.append(view_file)

    logger.info(f"✅ Generated {len(generated_files)} SQL files for {target_env}")
    return generated_files


def _write_file(path: str, content: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)


def execute_scripts(output_dir: str, env: str, spark):
    """Execute all generated SQL scripts for an environment."""
    sql_dir = os.path.join(output_dir, env)
    sql_files = sorted(os.listdir(sql_dir))  # Sorted ensures CREATE before VIEW

    for sql_file in sql_files:
        file_path = os.path.join(sql_dir, sql_file)
        with open(file_path, "r") as f:
            sql = f.read()
        try:
            spark.sql(sql)
            logger.info(f"✅ Executed: {sql_file}")
        except Exception as e:
            logger.error(f"❌ Failed: {sql_file} | Error: {e}")
            raise


# ─── Main Entry Point ───
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate SQL scripts for environment setup")
    parser.add_argument("--env",    required=True, help="dev | validation | production")
    parser.add_argument("--config", default="config/table_configs.yaml")
    parser.add_argument("--output", default="generated_sql/")
    parser.add_argument("--execute", action="store_true", help="Execute scripts after generation")
    args = parser.parse_args()

    files = generate_sql_scripts(args.config, args.env, args.output)

    if args.execute:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("InfraSetup").getOrCreate()
        execute_scripts(args.output, args.env, spark)
```

**Step 4 — Run it:**
```bash
# Generate scripts for production
python generate_sql.py --env production --config config/table_configs.yaml --output generated_sql/

# Generate AND execute for dev
python generate_sql.py --env dev --execute

# Output structure:
# generated_sql/
#   dev/
#     CREATE_PATIENT_RECORDS.sql
#     VIEW_PATIENT_RECORDS_AUDIT.sql
#     CREATE_FACILITY_MASTER.sql
#     ...
#   production/
#     (same files, different schema + S3 paths)
```

**Step 5 — Integrate into CI/CD:**
```yaml
# azure-pipelines.yml — infrastructure setup stage
- stage: SetupInfrastructure
  jobs:
  - job: GenerateAndRunSQL
    steps:
    - script: |
        pip install pyyaml jinja2 pyspark
        python generate_sql.py \
          --env $(TARGET_ENV) \
          --config config/table_configs.yaml \
          --execute
      displayName: 'Generate and execute SQL configs'
      env:
        TARGET_ENV: $(Build.SourceBranch)  # dev, validation, or production
```

---

### Before vs After

```
BEFORE:                            AFTER:
──────────────────────────────     ──────────────────────────────────
300 scripts × 3 environments       1 YAML config + templates
= 900 files to maintain            = Auto-generate 900 files

Manual copy-paste per change        1 command:
15+ hours/week                      python generate_sql.py --env prod

Frequent typos, missed updates      Zero copy-paste errors
Environment drift common            Environments always in sync

No version control on SQL           YAML + templates in Git
Hard to audit changes               Diff shows exactly what changed
```

---

### Strong Interview One-Liner
> *"Config-driven + template-based SQL generation — one YAML file, one command, consistent environments everywhere — eliminated 15+ hours/week of manual work and human error."*

### Common Mistake to Avoid
> Don't hardcode environment-specific values (schema names, S3 paths) in templates. All environment differences should live in the YAML config — templates stay generic and reusable.

---

---

## 📋 Quick Revision Summary — C6 to C9

```
┌──────────────────────────────────────────────────────────────────────────┐
│  C6  ADLS Gen2                                                           │
│  = Blob Storage + Hierarchical Namespace + POSIX ACLs + Analytics perf  │
│  Protocol: abfss://   |   Key benefit: atomic folder ops, ACL at file   │
├──────────────────────────────────────────────────────────────────────────┤
│  C7  Synapse vs Databricks                                               │
│  Synapse  → SQL-first, BI teams, MPP data warehouse, Power BI           │
│  Databricks → PySpark-first, DE + ML, Unity Catalog, DLT, Delta Lake    │
│  Answer: "Depends on team skills and primary workload type"              │
├──────────────────────────────────────────────────────────────────────────┤
│  C8  CI/CD for Databricks                                                │
│  Git branching → pytest on PR → YAML pipeline → deploy notebooks        │
│  Secrets: Azure DevOps Variable Group linked to Key Vault               │
│  NEVER store tokens in YAML or code                                     │
├──────────────────────────────────────────────────────────────────────────┤
│  C9  Python SQL Automation                                               │
│  YAML config → Jinja2 templates → Python generates 900 SQL files        │
│  1 source of truth → zero copy-paste → 15+ hrs/week saved              │
│  Integrated into Azure DevOps CI/CD pipeline                            │
└──────────────────────────────────────────────────────────────────────────┘
```

---

*Azure Interview Scripts — C6/C7/C8/C9 | Mayuresh Patil | May 2026*

### C8. ⭐ CI/CD for Databricks — Azure DevOps

**One-liner:** Git-branching strategy + automated testing + Azure DevOps pipeline deploys notebooks and job configs across Dev → Staging → Production.

**Answer Script:**
> "My CI/CD flow: Developers work on feature branches, PR into `dev` branch. Azure DevOps pipeline runs unit tests using `pytest`. On merge to `main`, a release pipeline deploys notebooks to Databricks workspace and updates job configs.

```yaml
# azure-pipelines.yml (simplified)
- script: pip install pytest pyspark
- script: pytest tests/
- script: |
    databricks workspace import_dir ./notebooks /Shared/prod --overwrite
    databricks jobs reset --job-id $(JOB_ID) --json-file job_config.json
```

> Secrets like `DATABRICKS_TOKEN` live in Azure DevOps Variable Groups linked to Key Vault — never in the pipeline YAML."

**Strong Interview One-Liner:**
> *"CI/CD for Databricks = automated tests on PR + automated deployment on merge — no manual notebook uploads in production."*

**Common Mistake to Avoid:**
> Don't deploy notebooks manually to production. Any manual step creates drift between environments and is not reproducible.

---

---

# 🔷 SECTION 7 — INGESTION PATTERNS

---

### P2. ⭐ Full Load vs Incremental Load — watermarking

**One-liner:** Full load replaces everything; incremental load uses a watermark timestamp to fetch only new or changed records since the last run.

**Answer Script:**
> "**Full load** reads all records from source and overwrites the target. Simple but expensive for large tables.
>
> **Incremental load** uses a watermark — the max `updated_at` from the last successful run. The next run fetches only records changed after that watermark:

```python
# Read last watermark from audit table
last_run = spark.sql("SELECT MAX(watermark) FROM audit.pipeline_runs").collect()[0][0]

# Fetch only new/changed records
df_incremental = spark.read.jdbc(
    url, table,
    predicates=[f"updated_at > '{last_run}'"]
)
# After successful write, update watermark
```

> At AbbVie, I implemented incremental loads for all 10M+ daily record pipelines — full loads were only used on first run or explicit backfill."

**Strong Interview One-Liner:**
> *"Watermark-based incremental = process only what changed — essential for high-volume daily pipelines."*

**Common Mistake to Avoid:**
> Don't use `LIMIT` or date filters in SQL for incremental logic. Use a persistent watermark stored in an audit table — reliable and restartable.

---

# 🎯 Interview Scripts — P3 to P9 (Ingestion Patterns)
> **Short · Speakable · Exact Words · Mayuresh Patil**

---

## P3. ⭐ How did you integrate with Salesforce and Smartsheet APIs?

**Speak This:**

> *"I used the Salesforce REST API and Smartsheet SDK in Python. For Salesforce, I authenticated using OAuth2 client credentials and queried objects using SOQL. For Smartsheet, I used their official Python SDK with an API token stored in Azure Key Vault.*
>
> *The main challenges were — first, **pagination**. Both APIs return data in pages of 200–500 records. I built a loop that fetches pages until no next-page token is returned.*
>
> *Second, **rate limiting**. Salesforce throttles heavy API consumers. I added exponential backoff retry logic — if I get a 429, wait 2 seconds, retry, double wait on next failure.*
>
> *Third, **schema drift**. Salesforce objects occasionally had new fields added. I handled this with `mergeSchema=true` when writing to Iceberg so new fields are captured without pipeline failure."*

```python
import requests
import time

# -----------------------------
# Step 1: Salesforce Authentication
# -----------------------------

auth_response = requests.post(
    auth_url,
    data={
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password
    }
)

auth_response.raise_for_status()

auth_data = auth_response.json()

access_token = auth_data["access_token"]
instance_url = auth_data["instance_url"]

# -----------------------------
# Step 2: Prepare Headers
# -----------------------------

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# -----------------------------
# Step 3: Salesforce Paginated API with Retry Logic
# -----------------------------

def fetch_salesforce_records(
    soql_query,
    headers,
    instance_url,
    api_version="v58.0",
    max_retries=3
):
    records = []

    # Initial query endpoint
    next_url = (
        f"{instance_url}/services/data/"
        f"{api_version}/query?q={soql_query}"
    )

    while next_url:

        for attempt in range(max_retries):

            try:
                response = requests.get(next_url, headers=headers)

                # Retry for rate limit
                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    print(f"Rate limited. Retrying in {wait_time} sec...")
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                break

            except requests.exceptions.RequestException as e:

                if attempt == max_retries - 1:
                    raise Exception(f"API request failed: {e}")

                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time} sec...")
                time.sleep(wait_time)

        data = response.json()

        # Append records
        records.extend(data.get("records", []))

        # Pagination handling
        next_records_url = data.get("nextRecordsUrl")

        if next_records_url:
            next_url = f"{instance_url}{next_records_url}"
        else:
            next_url = None

    return records

# -----------------------------
# Step 4: Execute SOQL Query
# -----------------------------

query = """
SELECT Id, Name, LastModifiedDate
FROM Account
"""

records = fetch_salesforce_records(
    soql_query=query,
    headers=headers,
    instance_url=instance_url
)

print(f"Total records fetched: {len(records)}")

```

**One-Liner:** *"Salesforce + Smartsheet = OAuth2 auth, paginated fetch, exponential retry, schema-resilient write to Iceberg."*

**Mistake to Avoid:** Don't hardcode API tokens. Always pull from Key Vault at runtime.

---

## P4. What is idempotency in pipelines?

**Speak This:**

> *"Idempotency means — run the pipeline once or ten times, you always get the same result. No duplicates, no data loss.*
>
> *I ensure idempotency in three ways.*
>
> *First, I use **MERGE INTO** instead of INSERT. If a record already exists, MERGE updates it. If not, it inserts. Running it twice is safe — second run is a no-op for unchanged records.*
>
> *Second, for partition writes, I use **replaceWhere** — overwrite only the specific date partition being reprocessed, not the entire table.*
>
> *Third, I store a **watermark** in an audit table. If the pipeline re-runs, it picks up the same watermark and fetches the same records — then MERGE handles the rest safely.*
>
> *In AbbVie, ADF automatically retries failed pipelines. Without idempotency, every retry would duplicate data."*

```python
# MERGE = idempotent upsert
delta_table.alias("t").merge(
    df_source.alias("s"),
    "t.record_id = s.record_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# replaceWhere = overwrite only one partition
df.write.format("delta") \
    .option("replaceWhere", "load_date = '2024-01-15'") \
    .mode("overwrite").save(path)
```

**One-Liner:** *"Idempotency = MERGE not INSERT — the pipeline can crash and restart safely without corrupting data."*

**Mistake to Avoid:** Don't assume `mode("overwrite")` is idempotent. It replaces everything — if the source data changed between runs, you get different results.

---

## P5. ⭐ How did you achieve 99.9% data uptime for 10M+ daily records?

**Speak This:**

> *"99.9% uptime means less than 9 hours of downtime per year. I achieved this through four practices.*
>
> *First, **idempotent pipelines** — every run uses MERGE so retries are safe. ADF auto-retries failed jobs up to 3 times.*
>
> *Second, **audit logging with Modak Nabu** — after every run, I write rows_read, rows_written, status, and timestamp to an audit table. If something goes wrong, I can pinpoint exactly which run and which records were affected.*
>
> *Third, **error handling and quarantine** — bad records don't fail the pipeline. They go to a quarantine table with the failure reason. The good records continue processing.*
>
> *Fourth, **event-based triggers** — instead of fixed-time schedules, I use ADLS event triggers for file-based sources. Pipeline fires only when the file actually arrives — no wasted runs on missing files.*
>
> *Together these meant: failures are caught fast, retries are safe, and bad data never blocks good data."*

**One-Liner:** *"99.9% uptime = idempotent pipelines + audit logging + quarantine pattern + event-based triggers."*

**Mistake to Avoid:** Don't rely on schedule triggers for file-based sources. If the file is late, the pipeline runs on nothing. Use event-based triggers instead.

---

## P6. ⭐ How would you design a pipeline for exactly-once semantics?

**Speak This:**

> *"Exactly-once means every record is processed and written exactly one time — not zero times, not twice.*
>
> *For **batch pipelines**, I achieve this with two things: a persistent watermark so we fetch the same records on retry, and MERGE INTO the target so duplicate records are deduplicated at write time. Even if the pipeline runs twice for the same window, the result is identical.*
>
> *For **streaming pipelines** like Kafka to Delta, exactly-once needs three things together: first, Kafka consumer offsets committed only after successful write. Second, a checkpoint location on S3 so Spark knows the last processed offset. Third, MERGE in `foreachBatch` so re-processed batches don't create duplicates.*"

```python
# Streaming exactly-once — Kafka → Delta
def upsert_batch(batch_df, batch_id):
    DeltaTable.forPath(spark, target_path).alias("t") \
        .merge(batch_df.alias("s"), "t.id = s.id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

df_stream.writeStream \
    .foreachBatch(upsert_batch) \
    .option("checkpointLocation", "s3://checkpoints/stream/") \
    .trigger(processingTime="60 seconds") \
    .start()
```

> *"Checkpoint tracks offsets — if the job crashes, it restarts from the last committed offset. MERGE handles any re-processed records safely. That's exactly-once end to end."*

**One-Liner:** *"Exactly-once = watermark + MERGE for batch; checkpoint + MERGE in foreachBatch for streaming."*

**Mistake to Avoid:** Checkpoint alone doesn't give exactly-once if you use `mode("append")`. You need MERGE to handle duplicates from replayed batches.

---

## P7. ⭐ 20TB live dataset migration — zero downtime strategy

**Speak This:**

> *"Zero downtime on a 20TB migration means you can never just switch off the old system and switch on the new one. You need a phased approach.*
>
> *I followed four phases.*
>
> ***Phase 1 — Mirror.** Run the new Iceberg pipeline in parallel with the legacy system for 2 weeks. Both receive writes. Old system stays live.*
>
> ***Phase 2 — Validate.** Compare row counts, checksums, and sample records between legacy and Iceberg at every layer — Raw, Trusted, Curated. Automate these reconciliation checks.*
>
> ***Phase 3 — Cutover.** Once validation passes, redirect downstream consumers to Iceberg one table at a time — not all at once. Monitor each cutover for 24–48 hours.*
>
> ***Phase 4 — Decommission.** Keep the legacy system running for 30 days as a fallback. Then retire it.*
>
> *The biggest technical challenge was partition design. We initially partitioned by patient_id — 10 million tiny S3 directories, metadata listing took longer than actual processing. Fixed by switching to year(event_date) partitioning with Z-ordering on patient_id within partitions."*

**One-Liner:** *"20TB zero-downtime = mirror in parallel, validate with reconciliation, cutover table by table, decommission after 30-day fallback."*

**Mistake to Avoid:** Never migrate all tables in one cutover weekend. If something breaks, rollback of 20TB is catastrophic. Always migrate table by table with validation between each.

---

## P8. Source file arrives with unexpected extra or missing columns

**Speak This:**

> *"This happens regularly in production — it's called schema drift.*
>
> *For **extra columns** (source added new fields): I use `mergeSchema=true` when writing to Delta or Iceberg. New columns are automatically added to the table schema. I also send an alert so the team knows a new field arrived.*
>
> *For **missing columns** (source removed a field): The write will have nulls for that column in the target. I detect this by comparing expected vs actual columns before writing:*"

```python
expected = {"patient_id", "name", "dob", "facility_id", "updated_at"}
actual   = set(df.columns)

extra_cols   = actual - expected
missing_cols = expected - actual

if missing_cols:
    log_alert(f"Missing columns: {missing_cols} — filling with NULL")
    for col in missing_cols:
        df = df.withColumn(col, F.lit(None))

if extra_cols:
    log_alert(f"New columns detected: {extra_cols}")

# Write with mergeSchema — handles both cases
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append").save(target_path)
```

> *"Raw Bronze layer saves the original file as-is — so even if my transformation mishandled a column, I can always reprocess from raw."*

**One-Liner:** *"Extra columns → mergeSchema=true + alert. Missing columns → fill with NULL + alert. Bronze raw layer = safety net for both."*

**Mistake to Avoid:** Don't fail the entire pipeline on schema drift. Log and alert, then handle gracefully — dropping a pipeline for a new column is not acceptable in production.

---

## P9. 📌 Metadata-driven ingestion framework — 500+ tables

**Speak This:**

> *"A metadata-driven framework means — instead of writing one PySpark script per table, you write one generic engine and drive it with configuration.*
>
> *We had 500+ Oracle tables to ingest into Iceberg. Writing 500 scripts is unmaintainable — any change to logging or DQ logic means updating 500 files.*
>
> *I built a framework where a YAML config defines each table's rules. One Python class handles all tables.*"

```yaml
# pipeline_config.yaml
tables:
  - name: PATIENT_RECORDS
    source: oracle
    primary_key: patient_id
    load_type: incremental
    watermark_col: updated_at
    partition_col: record_date
    target: iceberg.bronze.patient_records

  - name: FACILITY_MASTER
    source: oracle
    primary_key: facility_id
    load_type: full
    partition_col: load_date
    target: iceberg.bronze.facility_master
```

```python
# One generic engine drives all 500 tables
class IngestionEngine:
    def run(self, table_config):
        # 1. Read source based on config
        df = self.read_source(table_config)

        # 2. Apply DQ checks
        df_clean, df_bad = self.apply_dq(df, table_config["primary_key"])

        # 3. Write bad records to quarantine
        self.write_quarantine(df_bad, table_config["name"])

        # 4. MERGE clean records to Iceberg
        self.merge_to_iceberg(df_clean, table_config)

        # 5. Log audit record
        self.log_audit(table_config["name"], df_clean.count())

# Run all tables from config
config = yaml.load(open("pipeline_config.yaml"))
engine = IngestionEngine(spark)

for table in config["tables"]:
    engine.run(table)
```

> *"To add a new table, I add one YAML block — no code change needed. To fix a DQ bug, I fix it in one place — it applies to all 500 tables instantly. This is how we processed 10M+ records daily without a 500-file maintenance nightmare."*

**One-Liner:** *"Metadata-driven = one engine + config file — adding a new table is one YAML block, not a new script."*

**Mistake to Avoid:** Don't hardcode any table-specific logic inside the engine class. The moment you add a special `if table_name == 'X'` block, you've broken the pattern. All table-specific rules belong in the config.

---

## 📋 Quick Speak Reference

```
┌─────────────────────────────────────────────────────────────────────┐
│  P3  Salesforce/Smartsheet  → OAuth2 + pagination + retry + merge  │
│  P4  Idempotency            → MERGE not INSERT + watermark          │
│  P5  99.9% uptime           → idempotent + audit + quarantine       │
│  P6  Exactly-once           → checkpoint + MERGE in foreachBatch    │
│  P7  20TB migration         → mirror → validate → cutover → retire  │
│  P8  Schema drift           → mergeSchema + alert + Bronze fallback │
│  P9  Metadata framework     → YAML config + one generic engine      │
└─────────────────────────────────────────────────────────────────────┘
```

---
*Ingestion Patterns P3–P9 | Mayuresh Patil | May 2026*


### P4. Idempotency in pipelines

**One-liner:** An idempotent pipeline produces the same result whether run once or multiple times — safe to re-run after failures.

**Answer Script:**
> "Idempotency means re-running the pipeline doesn't create duplicate records or wrong results.
>
> Three patterns I use:
> - **MERGE INTO instead of INSERT** — handles re-runs gracefully by updating existing records rather than duplicating
> - **Overwrite partition** — overwrite only the specific date partition being reprocessed
> - **Deduplication before write** — deduplicate incoming data on the primary key before every write
>
> At AbbVie, all my Iceberg ingestion jobs use MERGE — so if an ADF trigger fires the job twice due to a retry, the second run is a no-op for unchanged records."

**Strong Interview One-Liner:**
> *"Idempotency = MERGE not INSERT — the difference between a pipeline you can safely retry and one that corrupts data on re-run."*

**Common Mistake to Avoid:**
> Don't assume a pipeline is idempotent just because it uses `mode("overwrite")`. Overwrite replaces everything — if the source changed between runs, you might not get the same result.

---

### P7. ⭐ 20TB Zero-Downtime Migration Strategy

*(See Iceberg section I10 for the detailed answer — same question, same answer)*

---

---

# 🔷 SECTION 8 — SQL

---

### Q1. ⭐ Window Functions vs Aggregate Functions

**One-liner:** Aggregate functions collapse rows into one; window functions compute over a group but keep every row.

**Answer Script:**
> "Aggregate functions like `SUM()` or `COUNT()` with `GROUP BY` collapse all rows in a group into one result row.
>
> Window functions compute the same aggregation but **keep every row** — each row gets its aggregated value alongside its original data:

```sql
-- Aggregate: 1 row per city
SELECT city, COUNT(*) FROM customers GROUP BY city;

-- Window: all rows preserved, each with their city count
SELECT city, customer_id,
       COUNT(*) OVER (PARTITION BY city) AS city_count,
       RANK() OVER (PARTITION BY city ORDER BY revenue DESC) AS rank
FROM customers;
```

> I use window functions heavily in my AbbVie pipelines for deduplication (ROW_NUMBER), running totals, and identifying the latest record per entity."

**Strong Interview One-Liner:**
> *"Window functions = aggregation without losing rows — critical for rankings, deduplication, and running totals."*

**Common Mistake to Avoid:**
> Don't confuse `PARTITION BY` in window functions with table partitioning. They're completely different concepts — one is SQL syntax, the other is physical storage.

---

### Q2. ⭐ ROW_NUMBER vs RANK vs DENSE_RANK

**One-liner:** ROW_NUMBER = unique always; RANK = gaps after ties; DENSE_RANK = no gaps after ties.

**Answer Script:**
> "Given scores: 100, 100, 90, 80:
>
> - `ROW_NUMBER()` → 1, 2, 3, 4 (always unique — arbitrary tiebreak)
> - `RANK()` → 1, 1, 3, 4 (tied = same rank, then skips)
> - `DENSE_RANK()` → 1, 1, 2, 3 (tied = same rank, no skip)
>
> I use `ROW_NUMBER()` for deduplication — picking exactly one row per key. `RANK()` and `DENSE_RANK()` for business ranking reports where ties matter."

**Strong Interview One-Liner:**
> *"ROW_NUMBER for deduplication; DENSE_RANK when business needs 'no gaps' in rankings like 1st, 2nd, 3rd place."*

**Common Mistake to Avoid:**
> Don't use `ROW_NUMBER()` for business ranking reports — it arbitrarily breaks ties, which may not be correct. Use `RANK` or `DENSE_RANK` when tie handling matters.

---

### Q5. ⭐ Optimize a slow SQL query with large table joins

**One-liner:** Check execution plan, push filters early, use indexes/partitions, avoid SELECT *, rewrite correlated subqueries as joins.

**Answer Script:**
> "My approach to slow SQL:
>
> 1. **Run EXPLAIN** — find the most expensive operation (full scan? nested loop join?)
> 2. **Push filters early** — filter before joining, not after
> 3. **Avoid SELECT \*** — read only needed columns
> 4. **Replace correlated subqueries** with joins — correlated subqueries run once per row
> 5. **Check join order** — join smaller filtered tables first
> 6. **Partition pruning** — ensure filters on partition columns are used
>
> At AbbVie, I found a query doing a correlated subquery inside a loop — replaced it with a pre-aggregated subquery join, dropped runtime from 45 minutes to 4 minutes."

**Strong Interview One-Liner:**
> *"EXPLAIN first, optimize second — never guess where the bottleneck is in a complex SQL query."*

**Common Mistake to Avoid:**
> Don't add indexes blindly. In OLAP (analytical) systems like Iceberg or Delta, indexes rarely exist — use partitioning and data skipping instead.

---

### Q4. Convert correlated subquery to a JOIN

**Answer Script:**
> "Correlated subqueries run once per row — O(n²) performance. Replace with a pre-aggregated subquery join:

```sql
-- Slow: correlated subquery (runs once per customer row)
SELECT customer_id, total_spend
FROM customers c
WHERE total_spend > (
    SELECT AVG(total_spend) FROM customers WHERE region = c.region
);

-- Fast: pre-aggregated join (runs once total)
SELECT c.customer_id, c.total_spend
FROM customers c
JOIN (
    SELECT region, AVG(total_spend) AS avg_spend
    FROM customers GROUP BY region
) avg_by_region ON c.region = avg_by_region.region
WHERE c.total_spend > avg_by_region.avg_spend;
```

**Strong Interview One-Liner:**
> *"Correlated subquery = O(n²); join with pre-aggregated subquery = O(n) — always flatten correlated queries."*

---

---

# 🔷 SECTION 9 — STREAMING & KAFKA

---

### K1. ⭐ How Kafka works internally

**One-liner:** Kafka is a distributed log — producers write to topics, which are split into partitions, and consumers read from offsets within partitions.

**Answer Script:**
> "Kafka stores messages in **topics**. Each topic is split into **partitions** — ordered, immutable logs. Each message has an **offset** — a unique ID within its partition.
>
> **Producers** write messages to partitions (by key or round-robin). **Consumers** in a **consumer group** each read from assigned partitions — each partition goes to exactly one consumer in the group, enabling load balancing.
>
> Kafka retains messages for a configurable period — consumers can re-read from any offset. This is why Spark Structured Streaming can replay from a checkpoint offset after a failure."

**Strong Interview One-Liner:**
> *"Kafka is a distributed, replicated, ordered log — its offset model is what enables reliable exactly-once streaming semantics."*

**Common Mistake to Avoid:**
> Don't say Kafka is a message queue. It's a distributed log — messages aren't deleted after consumption. Multiple consumer groups can independently read the same data.

---

### K3. ⭐ Kafka → Delta micro-batch streaming job

**Answer Script:**
> "Here's my standard pattern for Kafka → Delta with exactly-once semantics:

```python
# Read from Kafka
df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "pharma-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payload
df_parsed = df_stream.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data"),
    F.col("timestamp")
).select("data.*", "timestamp")

# Write with MERGE for idempotency
def upsert_to_delta(batch_df, batch_id):
    DeltaTable.forPath(spark, target_path).alias("t") \
        .merge(batch_df.alias("s"), "t.id = s.id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

df_parsed.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "s3://checkpoints/pharma_events/") \
    .trigger(processingTime="60 seconds") \
    .start().awaitTermination()
```

> Checkpoint + MERGE gives exactly-once semantics — even if the job restarts."

**Strong Interview One-Liner:**
> *"Checkpoint location + foreachBatch with MERGE = exactly-once Kafka → Delta micro-batch — the production-safe pattern."*

**Common Mistake to Avoid:**
> Don't use `mode("append")` in streaming writes without deduplication. Use `foreachBatch` with MERGE to handle late arrivals and job restarts safely.

---

---

# 🔷 SECTION 10 — PROJECT SCENARIOS (Behavioral)

---

### B1. ⭐ Tell me about yourself

**Answer Script:**
> *"I'm Mayuresh Patil, a Data Engineer at Cognizant, working on the AbbVie pharmaceutical data platform since July 2023. My core work is building and optimizing PySpark pipelines on Apache Iceberg on AWS S3. I've led a 20TB legacy data migration, built identity resolution systems using XREF mappings for MDM, and architect high-availability ingestion frameworks processing 10M+ records daily from Oracle, Snowflake, and Salesforce APIs. I optimized Spark partitioning and query plans to achieve a 30% reduction in pipeline execution time. I hold Databricks Certified Data Engineer Associate, Azure, and AWS certifications."*

---

### B4. ⭐ Biggest technical challenge

**Answer Script:**
> *"The 20TB migration was the biggest. The legacy system was live — zero downtime was a hard requirement.*
>
> *The technical challenge: our initial partition design used `patient_id` as the partition column — creating 10 million tiny directories on S3. Metadata listing was taking longer than the actual data processing.*
>
> *I redesigned the partition strategy: switched to `year(event_date)` as the partition column and used Iceberg's Z-ordering on `patient_id` and `facility_id` for data skipping within partitions. This reduced file count by 95% and cut scan times significantly.*
>
> *We then ran the migration in parallel with validation gates at each layer before cutting over."*

---

### B5. ⭐ How did you achieve 30% reduction in pipeline execution time?

**Answer Script:**
> *"It was a combination of targeted optimizations — not one single fix.*
>
> *First: I replaced Sort-Merge Joins with broadcast joins for all small reference tables — XREF mappings, facility codes, drug lookups. All under 20MB. This eliminated multiple shuffle stages.*
>
> *Second: I tuned `spark.sql.shuffle.partitions` from the default 200 to a number appropriate for each job's data volume. Some jobs only had 15 distinct keys — 185 empty partitions were wasted tasks.*
>
> *Third: I fixed the partition design on Iceberg tables so DPP (Dynamic Partition Pruning) triggered correctly — queries that were scanning the full 20TB now scanned only the relevant date range.*
>
> *Fourth: I enabled AQE to auto-coalesce remaining empty partitions and handle residual skew.*
>
> *Together, these dropped the end-to-end daily pipeline from ~4 hours to under 3 hours — a 30% reduction."*

---

### B6. 15+ hours/week automation — Python SQL generation

**Answer Script:**
> *"We had 300+ SQL configuration scripts — table DDLs, views, stored procedures — that had to be manually maintained across Dev, Validation, and Production environments.*
>
> *Every environment change meant manually editing and running hundreds of scripts. 15+ hours per week, error-prone.*
>
> *I built a Python automation using Jinja2 templating — a YAML config file defined all 300+ tables with their properties. The script generated the correct SQL for each environment dynamically:*

```python
config = yaml.load("table_configs.yaml")
for table in config['tables']:
    sql = template.render(schema=env_schema, **table)
    execute(sql)
```

> *This eliminated manual work completely, standardized all environments, and reduced configuration errors to zero."*

---

### B7. ⭐ Pipeline arrives 3 hours late — how to diagnose?

**Answer Script:**
> *"My structured approach:*
>
> *Step 1: Check ADF Monitor — did the trigger fire on time? Or was there a delay in the source file arriving?*
>
> *Step 2: Check Databricks job logs — which stage took longest?*
>
> *Step 3: Open Spark UI for the slow stage — look at Event Timeline for straggler tasks (data skew), check Shuffle Read/Write size (too much shuffle), and check Input Size (full scan instead of partition pruned scan).*
>
> *Step 4: Run `explain('formatted')` — is DPP active? Is there an unnecessary Sort-Merge Join?*
>
> *Root causes I've seen: data skew on one key (fix: broadcast or salt), DPP not triggering (fix: align filter columns with partition columns), upstream source delayed (fix: event-based trigger instead of schedule)."*

---

### B8. Production pipeline failure — debugging under pressure

**Answer Script:**
> *"I had a critical pipeline fail at 6 AM — 2 hours before the business needed the daily report.*
>
> *My approach: Stay calm, triage fast.*
>
> *Step 1: Check ADF for the error message — it showed 'JDBC connection timeout' to Oracle.*
>
> *Step 2: Confirmed Oracle DB was up — the issue was our JDBC connection pool being exhausted by a parallel job we hadn't coordinated.*
>
> *Step 3: Fix — added `connectionProperties` to limit max JDBC connections and re-ran the pipeline with the failed partition only (not a full reload).*
>
> *Step 4: Added Oracle connection monitoring to our alerting dashboard to prevent recurrence.*
>
> *Pipeline completed 45 minutes late — business impact minimal. And we documented the fix in our runbook."*

---

### B10. ⭐ Where do you see Data Engineering in 3–5 years?

**Answer Script:**
> *"I see three big shifts happening.*
>
> *First, the **open table format war is settling** — Iceberg and Delta are winning. Every major cloud provider is converging on these formats. Deep expertise in them is becoming table stakes.*
>
> *Second, **streaming is becoming the default** — not an advanced feature. Real-time pipelines on Lakehouse tables will be standard, not special.*
>
> *Third, **AI-augmented data engineering** — tools that auto-generate pipelines, suggest optimizations, detect anomalies. Data engineers who understand the fundamentals deeply will use these tools effectively; those who don't will struggle.*
>
> *I'm preparing by: deepening Iceberg expertise (already production experience), learning Spark Structured Streaming, and staying current on Databricks' AI/ML features."*

---

---

## 📋 MASTER QUICK REFERENCE

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  STRONG ONE-LINERS — USE THESE TO OPEN EVERY ANSWER                         │
├────────────────────────────────────────────────────────────────────────────  │
│ Spark        │ "Driver plans, Executors execute, CM allocates"               │
│ Shuffle      │ "Disk I/O + network + serialization — #1 performance killer"  │
│ AQE          │ "Static planner → dynamic optimizer at runtime"               │
│ Broadcast    │ "Zero shuffle on large side, immune to data skew"             │
│ UDF          │ "Black box to Catalyst — always prefer native functions"       │
│ Iceberg      │ "ACID + time travel on open S3 storage"                       │
│ Hidden Part. │ "Partition logic in metadata, not queries — auto pruning"      │
│ Delta        │ "Parquet + _delta_log = ACID + time travel"                    │
│ Medallion    │ "Raw in, quality out — each layer adds trust"                 │
│ MDM          │ "One trusted record from many source systems"                  │
│ Idempotency  │ "MERGE not INSERT — safe to re-run after failure"              │
│ Unity Cat.   │ "One governance layer for all Databricks workspaces"           │
│ Key Vault    │ "Zero secrets in code — non-negotiable security requirement"   │
│ CI/CD        │ "Automated tests on PR, automated deploy on merge"             │
│ Kafka        │ "Distributed log — offset model enables exactly-once"          │
└──────────────────────────────────────────────────────────────────────────────┘
```
---
Read nested JSON from S3 & flatten it
Use struct field access + explode() for arrays

```python
# H1 — Read nested JSON from S3 and flatten it
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.appName("FlattenJSON").getOrCreate()

# Read raw JSON from S3
# PERMISSIVE - tries to parse, puts bad records in _corrupt_record
# DROPMALFORMED - drop bad records | FAILFAST -throws an error on malformed records.
df = spark.read.option("multiLine", "true") \
               .option("mode", "PERMISSIVE") \  
               .option("allowSingleQuotes", "true") \
               .option("dateFormat", "yyyy-MM-dd") \
               .json("s3://bucket/path/data.json")

# Access nested struct fields with dot notation
df_flat = df.select(
    col("id"),
    col("name"),
    col("address.city").alias("city"),
    col("address.zip").alias("zip"),
    explode(col("orders")).alias("order")   # explode array column
)

# Now flatten the exploded struct
df_final = df_flat.select(
    col("id"), col("name"), col("city"), col("zip"),
    col("order.order_id").alias("order_id"),
    col("order.amount").alias("amount")
)

df_final.show(5, truncate=False)

```

---


---
Read CSV from S3 with explicit schema (no inferSchema in production)
Never use inferSchema=True in production — it causes full file scan

```python

# H2 — Read CSV with explicit schema (production-safe)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType

# Define schema explicitly — avoids full scan + type guessing
schema = StructType([
    StructField("customer_id",  StringType(),  nullable=False),
    StructField("name",          StringType(),  nullable=True),
    StructField("age",           IntegerType(), nullable=True),
    StructField("signup_date",   DateType(),    nullable=True),
    StructField("revenue",       DoubleType(),  nullable=True)
])

df = spark.read \
    .option("header", "true") \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("mode", "PERMISSIVE")  \  # or FAILFAST / DROPMALFORMED
    .option("quote", "\") \ # "A,B" --> A,B
    .option("escape", "\") \
    .option("encoding", "UTF-8") # Handle special charator
    .option("multiline", "true")
    .schema(schema) \
    .csv("s3://bucket/path/customers.csv")

df.write.format("csv") \
    .option("header","true") \
    .mode("overwrite") \
    .save("s3://bucket/path/")

```

---

---
MERGE INTO Delta/Iceberg — upsert pattern
Insert new rows, update existing rows based on a key

```python

# H3 — MERGE INTO Delta table (upsert)
from delta.tables import DeltaTable

# Load target Delta table
target = DeltaTable.forPath(spark, "s3://bucket/delta/customers")

# Source: new/updated records from upstream
source_df = spark.read.parquet("s3://bucket/staging/customers_update")

# MERGE: match on customer_id
target.alias("tgt").merge(
    source_df.alias("src"),
    "tgt.customer_id = src.customer_id AND tgt.study_id = src.study_id"
).whenMatchedUpdate(
    set={
        "name": col("src.name"),
        "revenue": col("src.revenue"),
        "updated_at": col("src.updated_at")
    }
).whenMatchedDelete(
    condition=col("src.is_deleted") == True   # delete if flagged in source
).whenNotMatchedInsertAll() \
 .execute()

# Optional: also handle deletes
# .whenMatchedDelete()  <-- add a "is_deleted" flag in source

```

---

---
⭐ Implement SCD Type 2 using Delta MERGE + window functions
Track full history — close old rows, insert new rows

```python

# H4 — SCD Type 2 using Delta MERGE
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

target = DeltaTable.forPath(spark, "s3://bucket/delta/dim_customer")

# Source has new/changed records
source_df = spark.read.parquet("s3://bucket/staging/customers")

# Step 1 — close existing active rows that changed
target.alias("tgt").merge(
    source_df.alias("src"),
    "tgt.customer_id = src.customer_id AND tgt.is_current = true"
    " AND tgt.email != src.email"   # only if something changed
) \
.whenMatchedUpdate(set={
    "is_current":  "false",
    "end_date":    "current_timestamp()"
}) \
.execute()

# Step 2 — insert new version of changed rows + brand new rows
new_rows = source_df.withColumn("is_current", lit(True)) \
                    .withColumn("start_date", current_timestamp()) \
                    .withColumn("end_date", lit(None).cast("timestamp"))

new_rows.write.format("delta") \
        .mode("append") \
        .save("s3://bucket/delta/dim_customer")

```

---

---
⭐ PySpark JDBC read from Oracle with parallel partitions
Use partitionColumn + numPartitions to parallelize reads

```python

# H5 — JDBC Oracle read with parallel partitions

jdbc_url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLDB"

df = spark.read.format("jdbc") \
    .option("url",             jdbc_url) \
    .option("dbtable",        "SCHEMA.PATIENT_DATA") \
    .option("user",           "db_user") \
    .option("password",       "db_pass") \
    .option("driver",         "oracle.jdbc.driver.OracleDriver") \
    # Parallel partition options below
    .option("partitionColumn", "patient_id")  # numeric col to split on
    .option("lowerBound",      "1") \
    .option("upperBound",      "10000000") \
    .option("numPartitions",   "20") \     # 20 parallel DB connections
    .option("fetchsize",       "10000") \  # rows per fetch batch
    .load()

df.write.format("jdbc") \
    .option("url",             jdbc_url) \
    .option("user",           "db_user") \
    .option("password",       "db_pass") \
    .option("dbtable", "schema.outputtable") \
    .save()

```

**Why parallel?** Without partitionColumn, Spark opens ONE connection and pulls everything sequentially — very slow. With numPartitions=20, Spark opens 20 connections in parallel, each reading a chunk of patient_id range. fetchsize controls how many rows per network round trip.

---

---
⭐ Find new, updated and deleted records between source and target
Classic CDC pattern using join + condition logic

```python

# H6 — Detect new / updated / deleted records
from pyspark.sql.functions import col

source = spark.read.parquet("s3://bucket/source/customers")
target = spark.read.parquet("s3://bucket/target/customers")

# NEW — in source but not in target
new_records = source.join(target, on="customer_id", how="left_anti")

# UPDATED — in both, but a column value changed
updated_records = source.alias("src").join(
    target.alias("tgt"),
    on="customer_id",
    how="inner"
).filter(
    col("src.email") != col("tgt.email")
    | (col("src.revenue") != col("tgt.revenue"))
).select("src.*")

# DELETED — in target but not in source
deleted_records = target.join(source, on="customer_id", how="left_anti")

# Print counts for validation
print(f"New: {new_records.count()}")
print(f"Updated: {updated_records.count()}")
print(f"Deleted: {deleted_records.count()}")

```

**💡 left_anti join** is the cleanest way to find records in one dataset that don't exist in another — no NULL checks needed. For updates, use hash comparison (md5 of all columns) instead of checking each column individually when there are many columns.

---

---
⭐ Two-phase salted aggregation for skewed groupBy
Prevent data skew by adding a salt key before aggregating

```python

# H7 — Salted aggregation to handle skewed groupBy
from pyspark.sql.functions import col, floor, rand, sum as _sum

SALT_BUCKETS = 10

# Step 1 — Add salt key to distribute skewed keys across buckets
df_salted = df.withColumn(
    "salt", (rand() * SALT_BUCKETS).cast("int")
).withColumn(
    "salted_key", col("region").cast("string") + lit("_") + col("salt").cast("string")
)

# Step 2 — First aggregation: on salted key (distributed)
df_partial = df_salted.groupBy("salted_key", "region") \
                      .agg(_sum("revenue").alias("partial_sum"))

# Step 3 — Second aggregation: on original key (small, no skew)
df_final = df_partial.groupBy("region") \
                     .agg(_sum("partial_sum").alias("total_revenue"))

df_final.show()

```

**💡 When to use salting?** When one groupBy key has millions of rows (e.g. region='US'), one executor gets overloaded. Salting splits that key into 10 sub-keys, spreads load, then combines. Two-phase = pre-aggregate with salt, then final aggregate without salt.

---

---
⭐ Comprehensive Data Quality check function
Null, duplicate, range, and referential integrity checks

```python

# H8 — Reusable Data Quality check function
from pyspark.sql.functions import col, count, countDistinct, when, isnull

def run_dq_checks(df, table_name: str, pk_col: str, ref_df=None, ref_col=None):
    results = []

    # 1. NULL checks on all columns
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        results.append({"check": f"null_{c}", "passed": null_count == 0, "count": null_count})

    # 2. Duplicate check on primary key
    total     = df.count()
    distinct  = df.select(pk_col).distinct().count()
    dup_count = total - distinct
    results.append({"check": f"duplicate_{pk_col}", "passed": dup_count == 0, "count": dup_count})

    # 3. Range check — revenue must be > 0
    if "revenue" in df.columns:
        bad_range = df.filter(col("revenue") < 0).count()
        results.append({"check": "range_revenue_positive", "passed": bad_range == 0, "count": bad_range})

    # 4. Referential integrity — FK must exist in reference table
    if ref_df is not None:
        orphans = df.join(ref_df, on=ref_col, how="left_anti").count()
        results.append({"check": f"ref_integrity_{ref_col}", "passed": orphans == 0, "count": orphans})

    # Print results
    for r in results:
        status = "✅ PASS" if r["passed"] else "❌ FAIL"
        print(f"{status} | {table_name} | {r['check']} | count={r['count']}")

    return results

```

**💡 Interview tip:** DQ has 4 pillars — Completeness (nulls), Uniqueness (duplicates), Validity (range/format), Referential integrity (FK). Always make DQ a reusable function, not inline checks. Return results so you can log them to a monitoring table.

---

---
⭐ Full Medallion Architecture: Bronze → Silver → Gold with DQ gates
End-to-end pipeline with quality checkpoints at each layer

```python

# H10 — Bronze → Silver → Gold Medallion Pipeline
from pyspark.sql.functions import col, current_timestamp, lit, md5, concat_ws

BRONZE_PATH = "s3://bucket/bronze/patients"
SILVER_PATH = "s3://bucket/silver/patients"
GOLD_PATH   = "s3://bucket/gold/patient_summary"

# ── BRONZE: Raw ingestion — no transformation, just load + metadata
def ingest_bronze(source_path):
    df = spark.read.option("header", "true").csv(source_path)
    df = df.withColumn("ingested_at", current_timestamp()) \
           .withColumn("source_file",  lit(source_path))
    df.write.format("delta").mode("append").save(BRONZE_PATH)
    print(f"Bronze: {df.count()} rows loaded")

# ── DQ Gate — fail if too many nulls
def dq_gate(df, col_name, threshold=0.05):
    null_pct = df.filter(col(col_name).isNull()).count() / df.count()
    if null_pct > threshold:
        raise ValueError(f"DQ FAIL: {col_name} null% {null_pct:.2%} > threshold {threshold:.2%}")
    print(f"DQ PASS: {col_name} null% = {null_pct:.2%}")

# ── SILVER: Clean, deduplicate, standardize
def transform_silver():
    df = spark.read.format("delta").load(BRONZE_PATH)
    dq_gate(df, "patient_id")                     # Gate 1: no nulls on PK

    df_clean = df.dropDuplicates(["patient_id"]) \
                 .filter(col("patient_id").isNotNull()) \
                 .withColumn("dob", col("dob").cast("date")) \
                 .withColumn("gender", col("gender").upper())

    dq_gate(df_clean, "dob")                       # Gate 2: check after clean
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print(f"Silver: {df_clean.count()} rows written")

# ── GOLD: Aggregate for business consumption
def build_gold():
    df = spark.read.format("delta").load(SILVER_PATH)
    df_gold = df.groupBy("region", "gender") \
                .agg(count("patient_id").alias("patient_count"),
                     avg("age").alias("avg_age"))
    df_gold.write.format("delta").mode("overwrite").save(GOLD_PATH)

# ── Run the full pipeline
ingest_bronze("s3://bucket/raw/patients_20240601.csv")
transform_silver()
build_gold()

```

💡 Key difference: Python UDF = row-by-row serialization (slow, ~10x overhead). Pandas UDF = Apache Arrow columnar batches (vectorized, near-native speed). Always prefer Pandas UDF. Only use Python UDF for very complex logic that can't be vectorized.
---

---
⭐ Pandas UDF vs Python UDF — write both, explain performance
Row-by-row vs vectorized — critical performance difference

```python
# H11 — Python UDF vs Pandas UDF (performance comparison)
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, DoubleType
import pandas as pd

# ── OPTION 1: Regular Python UDF — row by row (slow)
# Spark serializes EACH ROW to Python, processes, returns to JVM
@udf(returnType=StringType())
def clean_name_python_udf(name: str) -> str:
    if name is None:
        return None
    return name.strip().title()

# ── OPTION 2: Pandas UDF — vectorized (fast)
# Spark sends a whole BATCH (Arrow column) at once to Python
@pandas_udf(StringType())
def clean_name_pandas_udf(names: pd.Series) -> pd.Series:
    return names.str.strip().str.title().fillna("")

# Usage — both called the same way
df.withColumn("clean_name_slow", clean_name_python_udf(col("name"))).show()
df.withColumn("clean_name_fast", clean_name_pandas_udf(col("name"))).show()

# ── Pandas UDF for aggregation
@pandas_udf(DoubleType())
def mean_udf(v: pd.Series) -> float:
    return v.mean()

df.groupBy("region").agg(mean_udf(col("revenue"))).show()

```

💡 Key difference: Python UDF = row-by-row serialization (slow, ~10x overhead). Pandas UDF = Apache Arrow columnar batches (vectorized, near-native speed). Always prefer Pandas UDF. Only use Python UDF for very complex logic that can't be vectorized.
---

---
⭐ Incremental load with watermark + MERGE into Iceberg
Only process new records using last-loaded timestamp

```python

# H12 — Incremental load with watermark + MERGE into Iceberg
from pyspark.sql.functions import col, max as _max, lit, current_timestamp

ICEBERG_TABLE = "catalog.db.patients"
WATERMARK_PATH = "s3://bucket/metadata/watermark.txt"

# ── Step 1: Read last watermark
def get_watermark() -> str:
    try:
        wm = spark.read.text(WATERMARK_PATH).collect()[0][0]
        return wm
    except:
        return "1900-01-01 00:00:00"     # first run — load all

# ── Step 2: Read only NEW rows from source since watermark
last_wm = get_watermark()
print(f"Loading records updated after: {last_wm}")

source_df = spark.read.parquet("s3://bucket/source/patients") \
    .filter(col("updated_at") > lit(last_wm))

print(f"New/updated records: {source_df.count()}")

# ── Step 3: MERGE into Iceberg
source_df.createOrReplaceTempView("source_data")

spark.sql(f"""
    MERGE INTO {ICEBERG_TABLE} AS tgt
    USING source_data AS src
    ON tgt.patient_id = src.patient_id
    WHEN MATCHED THEN
        UPDATE SET tgt.name       = src.name,
                   tgt.updated_at = src.updated_at
    WHEN NOT MATCHED THEN
        INSERT *
""")

# ── Step 4: Save new watermark (max updated_at from this batch)
new_wm = source_df.agg(_max("updated_at")).collect()[0][0]
spark.createDataFrame([[str(new_wm)]]).coalesce(1) \
     .write.mode("overwrite").text(WATERMARK_PATH)
print(f"Watermark updated to: {new_wm}")


```

**💡 Watermark pattern:** Store the max(updated_at) from the last successful run. Next run reads only records where updated_at > watermark. Then MERGE to upsert. Finally update the watermark. This is how real incremental pipelines work in production.

---

*Interview Answer Scripts | Mayuresh Patil | May 2026*
*Part 2: Delta Lake · Architecture · Governance · Azure · SQL · Kafka · Project*
