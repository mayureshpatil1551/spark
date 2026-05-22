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
> Don't har
