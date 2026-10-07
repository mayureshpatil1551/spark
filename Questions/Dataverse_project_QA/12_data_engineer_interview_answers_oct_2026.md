# Data Engineer Interview Q&A — Mayuresh Patil

Format: each question has a **Short Answer** (30-second version) and a **Detailed Answer** (interview script, first person).
Tip: replace the numbers with your exact figures if they differ. Never quote a metric you can't explain.

---

## Q1. How do you decide the partition granularity to balance query performance and data management overhead?

**Short Answer**
I partition on the column that is most frequently used in filters and has low-to-medium cardinality, usually a date. I aim for partitions of roughly 128 MB to 1 GB of data. Too many small partitions create small-file and metadata overhead, and too few lose pruning benefits.

**Detailed Answer**
"I start from the query patterns. If most reports filter by date, I partition by date. Then I check data volume per partition. If daily data is only a few MB, daily partitioning creates thousands of tiny files, so I move to monthly or use a coarser grain. If a day holds hundreds of GB, I add a second level or use hidden partitioning.

My rules of thumb:
- Partition column should have low cardinality. Never something like customer_id.
- Each partition should be around 128 MB to 1 GB at minimum.
- Partitions that are too small cause small-file problems, slow listing and heavy metadata.
- For high-cardinality filter columns, I don't partition. I use Z-Ordering or clustering instead.

In Iceberg I used hidden partitioning like `days(event_ts)`, so I could change granularity later without rewriting queries. In Delta, I partition by date for large fact tables and use Z-Order on columns like account or business unit. I validate with the Spark UI by checking files scanned and data skipped before and after."

```python
# Delta: partition by date, then check partition sizes
(df.write.format("delta")
   .partitionBy("load_date")
   .mode("overwrite")
   .save(path))

spark.sql("DESCRIBE DETAIL delta.`{}`".format(path)).select("numFiles", "sizeInBytes").show()
```

```sql
-- Iceberg hidden partitioning
CREATE TABLE sales (id BIGINT, event_ts TIMESTAMP, amount DOUBLE)
USING iceberg
PARTITIONED BY (days(event_ts));
```

---

## Q2. Tell me about your experience with CI/CD pipelines, specifically using GitHub for data engineering projects.

**Short Answer**
I use Git branching with pull requests for code review, and GitHub-based CI to run linting and unit tests on PySpark code. Merges to main trigger deployment of notebooks, jobs and ADF artifacts to higher environments, with secrets kept in Key Vault.

**Detailed Answer**
"In my projects, all code lives in GitHub: PySpark modules, Databricks notebooks, ADF configs and SQL scripts. We follow a feature branch workflow. A developer raises a pull request into `develop`. On every PR, CI runs flake8 or black for linting and pytest unit tests on transformation functions using a local Spark session. Another engineer must approve before merge.

After merge to `develop`, the pipeline deploys to the Dev/QA workspace. Promotion to Prod happens from `main` with an approval gate. Environment-specific values such as storage paths, cluster configs and connection details are parameterized, and credentials come from Azure Key Vault, never from the repo.

The benefits were consistent deployments, fewer manual errors, and a clear audit trail, which matters in regulated environments."

```yaml
# .github/workflows/ci.yml
name: ci
on:
  pull_request:
    branches: [develop, main]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install -r requirements.txt pytest flake8
      - run: flake8 src/
      - run: pytest tests/ -q
```

---

## Q3. How do you automate deployment of data pipelines and ensure continuous delivery from development to production?

**Short Answer**
I keep everything as code, parameterize per environment, and use a CI/CD pipeline that tests, packages and deploys to Dev, then QA, then Prod with approvals. Secrets come from Key Vault, and I add post-deploy smoke checks and a rollback path.

**Detailed Answer**
"My approach has four parts.

1. **Everything as code.** Notebooks, PySpark packages, ADF pipelines, job definitions, and config files all sit in Git.
2. **Environment parameterization.** One codebase, with separate config for Dev, QA and Prod. In ADF I use global parameters and linked services per environment. In Databricks I use job parameters and config files.
3. **Pipeline stages.** Lint and unit tests, then deploy to Dev, run an integration test on a sample dataset, deploy to QA, then manual approval for Prod.
4. **Safety.** Secrets come from Key Vault through secret scopes. After deployment I run a smoke test, such as a row-count or schema check. For rollback, I redeploy the previous tagged release since every release is versioned.

For ADF, I publish ARM templates from the collaboration branch and deploy them with parameter overrides per environment. For Databricks, I deploy jobs and notebooks with the Databricks CLI or Asset Bundles."

```bash
# Example deployment step
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

---

## Q4. What considerations do you take into account when deciding whether to broadcast a join, especially with varying data sizes?

**Short Answer**
I broadcast when one side is small enough to fit comfortably in executor and driver memory, typically under a few hundred MB, and the other side is large. It avoids shuffling the big table. I check actual size after filters and watch for skew and memory limits.

**Detailed Answer**
"A broadcast join sends the small table to every executor, so the large table doesn't need to be shuffled. That's a big saving. Before broadcasting I consider:

- **Size after filters and column pruning**, not the raw table size.
- **Memory available** on the driver and executors, since the table is collected on the driver first.
- **Join type.** The broadcast side has restrictions depending on join type, e.g. you can't broadcast the preserved side of an outer join.
- **Stability of size.** If the table can grow, a hard-coded broadcast can break later.

By default Spark auto-broadcasts under `spark.sql.autoBroadcastJoinThreshold` (10 MB). With AQE enabled, Spark can also convert a sort-merge join to a broadcast join at runtime based on actual shuffle statistics. For clearly small lookup tables I use an explicit hint."

```python
from pyspark.sql.functions import broadcast
result = fact_df.join(broadcast(lookup_df), "product_id", "left")
```

---

## Q5. How do you define a "small" lookup table, and what are the drawbacks if you broadcast a table that isn't truly small?

**Short Answer**
Small means it fits comfortably in driver and executor memory, usually tens to a couple hundred MB in memory, not on disk. If it's larger, you risk driver OOM, executor memory pressure, slow broadcast, and failed jobs.

**Detailed Answer**
"I define 'small' by in-memory size, not file size. Parquet is compressed, so a 50 MB file can expand to several hundred MB in memory. My working rule is: a lookup is small if its deserialized size is well under the driver and executor memory available, typically under about 100 to 200 MB in my projects, though the default threshold is only 10 MB.

If you broadcast a table that isn't small:
- The driver collects the whole table first, which can cause **driver OOM**.
- Every executor holds a full copy, which increases **executor memory pressure** and GC.
- Broadcast **timeouts** can occur since the data must be shipped to all nodes.
- Instead of a speedup, the job becomes slower or fails.

So I measure first, filter and select only needed columns, and if it's borderline, I let Spark choose or I use a sort-merge join."

---

## Q6. The driver tries to load a non-small table and hits OOM. How do you mitigate this and decide the broadcast threshold when data sizes are dynamic?

**Short Answer**
Don't hard-code broadcast hints for tables that can grow. Rely on AQE and a sensible threshold, reduce the table before broadcasting, and fall back to sort-merge join when size exceeds a limit. Also size driver memory properly.

**Detailed Answer**
"To mitigate the risk:

1. **Remove forced hints on tables with variable size.** Let Spark decide using `autoBroadcastJoinThreshold` and AQE, which uses real runtime statistics.
2. **Shrink the table first.** Apply filters, select only join and needed columns, and deduplicate.
3. **Set limits.** Keep `spark.sql.autoBroadcastJoinThreshold` modest, increase `spark.driver.memory` and `spark.driver.maxResultSize` if needed, and tune `spark.sql.broadcastTimeout`.
4. **Add a size guard.** For dynamic tables I check size at runtime and broadcast only if below my limit, otherwise use a normal join.
5. **Monitor.** I watch the Spark UI for broadcast size and driver memory, and set alerts on failures.

To pick the threshold, I test with representative data, measure in-memory size, and keep it at a fraction, say less than a third, of driver and executor memory."

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50 MB

# Size guard for dynamic tables
size_bytes = lookup_df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
if size_bytes < 100 * 1024 * 1024:
    result = fact_df.join(broadcast(lookup_df), "key")
else:
    result = fact_df.join(lookup_df, "key")
```

---

## Q7. How did you achieve the 40% reduction in query latency?

**Short Answer**
By optimizing the physical layout of the Delta tables: Z-Ordering on commonly filtered columns, file compaction to fix small files, and better clustering and partitioning. That improved data skipping and cut the data scanned by reports.

**Detailed Answer**
"In FinOps Dataverse, 300+ business reports ran on Delta tables. I first profiled slow queries in the Spark UI and found two problems: too many small files from incremental loads, and queries scanning far more data than needed.

I fixed it in steps:
1. **File compaction** using `OPTIMIZE` to merge small files into larger ones.
2. **Z-Ordering** on the columns most used in report filters and joins, which improved data skipping.
3. **Right partitioning** so common date filters pruned partitions.
4. **Maintenance** with scheduled OPTIMIZE and VACUUM so performance didn't degrade over time.

I measured before and after using the same report queries. Latency dropped by about 40%, and files scanned dropped significantly."

```sql
OPTIMIZE finance.fact_ledger
ZORDER BY (company_code, posting_date);

VACUUM finance.fact_ledger RETAIN 168 HOURS;
```

---

## Q8. What other techniques, like Z-ordering or clustering, did you use to optimize Delta Lake workloads?

**Short Answer**
Besides Z-Ordering and clustering, I used OPTIMIZE compaction, partition pruning, data skipping through statistics, auto-optimize settings, caching where useful, and VACUUM for cleanup. I also tuned Spark for joins and shuffles.

**Detailed Answer**
"Apart from Z-Order and clustering, I used:

- **OPTIMIZE and auto compaction** to control small files, with optimized writes enabled on frequently written tables.
- **Partition pruning** through sensible partition columns.
- **Data skipping stats.** Delta collects min and max for the first columns, so I keep filter columns early in the schema when appropriate.
- **VACUUM** to remove old files and control storage.
- **Efficient MERGE.** I added partition filters in the merge condition to limit files touched.
- **Spark tuning.** Broadcast joins for small dimensions, AQE for skew and shuffle partitions, and avoiding unnecessary wide transformations.
- **Selective columns** and pushing filters early.

Z-Order helps with multi-column filters, while liquid clustering is more flexible when query patterns change, since clustering keys can be changed without rewriting the whole table."

```sql
ALTER TABLE finance.fact_ledger SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

MERGE INTO tgt t USING src s
ON t.id = s.id AND t.load_date = s.load_date   -- partition filter limits scan
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

## Q9. Tell me about a challenging data migration you worked on and how you approached it.

**Short Answer**
I migrated 20+ TB of pharmaceutical data to Apache Iceberg on Amazon S3. I handled it in phases with schema mapping, parallel validation, incremental loads and a controlled cutover, so there was no data loss and little downtime.

**Detailed Answer**
"The most challenging one was the e-Clinical Data Platform migration, modernizing 20+ TB of pharmaceutical data on S3 using PySpark and Apache Iceberg.

**Challenges:** large volume, multiple source systems, strict data accuracy needs, and minimal downtime.

**Approach:**
1. **Assessment.** Profiled sources, mapped schemas and identified dependencies.
2. **Design.** Chose Iceberg for ACID, schema evolution and hidden partitioning.
3. **Historical load.** Migrated in batches by table and date range, with parallelism tuned so we didn't overload sources.
4. **Incremental sync.** Used incremental loads to capture changes during migration.
5. **Validation.** Row counts, checksums and sample comparisons between source and target.
6. **Cutover.** Ran old and new in parallel, then switched consumers after sign-off.

The result was a successful migration with verified data, and a platform that supported schema changes and faster processing afterward."

---

## Q10. How do you ensure data integrity across sources like Oracle, Snowflake and REST APIs, especially with millions of records daily?

**Short Answer**
I use schema validation, reconciliation checks such as row counts and checksums, idempotent loads, deduplication on keys, and audit logging with alerts. For APIs I add pagination, retries and incremental watermarks.

**Detailed Answer**
"With 10M+ records daily from Oracle, Snowflake, REST APIs and Smartsheet, I built integrity checks into every stage:

- **Schema validation** at ingestion, with rejected records routed to a quarantine location.
- **Reconciliation.** Compare source and target row counts, and checksums or aggregates on key columns.
- **Idempotent loads.** Using MERGE or overwrite-by-partition so a rerun doesn't create duplicates.
- **Deduplication** using business keys and ordering by timestamp.
- **Incremental watermarks** so we neither miss nor reload records.
- **REST APIs.** Pagination, retries with backoff, and checking total counts when available.
- **Audit tables** logging run id, counts, status and duration, with alerts on failures or mismatches.

This is how we kept pipeline availability at about 99.9%."

```python
src_count = src_df.count()
tgt_count = spark.table("silver.orders").where(f"load_date = '{run_date}'").count()
if src_count != tgt_count:
    raise Exception(f"Count mismatch: source={src_count}, target={tgt_count}")
```

---

## Q11. What challenges did you face implementing ETL pipelines in regulated environments like pharma, and how did you overcome them?

**Short Answer**
The main challenges were data accuracy, traceability, access control and change management. I addressed them with validation checks, lineage and audit logs, role-based access with secrets management, and controlled, reviewed deployments.

**Detailed Answer**
"In pharma, data must be accurate, traceable and secure. The challenges and my solutions:

1. **Data accuracy.** Added validation and reconciliation at each layer so errors were caught early.
2. **Traceability.** Maintained audit logs and lineage, so we could show where a record came from and how it changed. Iceberg snapshots and time travel also helped.
3. **Access control.** Role-based access, least privilege and no hard-coded credentials.
4. **Change control.** All changes went through Git, code review and approvals before production.
5. **Master data consistency.** Golden Record and XREF logic resolved the same entity across systems.
6. **Reliability.** Retries, monitoring and alerting to keep availability high.

The result was reliable pipelines that stood up to audits and gave downstream teams trusted data."

---

## Q12. How does the Python automation for SQL configuration generation work, and what were the benefits?

**Short Answer**
I built a Python generator that reads a metadata or config file for a new source and produces the SQL and pipeline configuration automatically from templates. It removed repetitive manual work and cut onboarding time for new pipelines.

**Detailed Answer**
"Earlier, onboarding a new table meant manually writing SQL for extraction and transformation plus config entries, which was repetitive and error-prone.

I automated it:
1. **Input.** A simple metadata file (CSV/JSON/Excel) listing source table, columns, keys, load type and target.
2. **Templates.** Jinja2 templates for SQL and config structures.
3. **Generation.** A Python script reads metadata, validates it (for example with Pydantic), and renders the SQL and config files.
4. **Output.** Ready-to-use files that plug into the ingestion framework.

**Benefits:** much less manual effort, consistent standards, fewer human errors, and faster onboarding. Adding a new table became a metadata change rather than new code."

```python
from jinja2 import Template

tpl = Template("""
SELECT {{ cols | join(', ') }}
FROM {{ source_table }}
{% if load_type == 'incremental' %}WHERE {{ watermark }} > '{{ last_run }}'{% endif %}
""")

sql = tpl.render(cols=["id", "name", "updated_at"],
                 source_table="SALES.ORDERS",
                 load_type="incremental",
                 watermark="updated_at",
                 last_run="2026-01-01")
```

---

## Q13. How do you manage migration of SAP financial data to both Iceberg and Snowflake, ensuring consistency?

**Short Answer**
I read the data once, apply the same transformation logic, and write to both targets from that single dataframe in the same run. Then I reconcile counts and aggregates between the two targets, and drive both from one config and run id.

**Detailed Answer**
"The key principle is a **single source of truth for transformation**. I extract the SAP data once, apply the transformations once, and then write the same dataframe to both targets. That avoids logic drifting between two separate pipelines.

Steps:
1. **Read and transform once**, cached or persisted so both writes use identical data.
2. **Write to Iceberg** (S3) and **Snowflake** using the same run id and load timestamp.
3. **Same schema mapping and data types**, defined in one config, with care for type differences such as decimals and timestamps.
4. **Reconcile** after load: row counts, sums of key financial columns, and min/max dates on both targets.
5. **Idempotency.** Use merge or partition overwrite so reruns are safe.
6. **Audit** results in a log table, with alerts on mismatch."

```python
df = transform(read_sap()).persist()

df.writeTo("iceberg_catalog.finance.gl_entries").using("iceberg").append()

(df.write.format("snowflake")
   .options(**sf_options)
   .option("dbtable", "GL_ENTRIES")
   .mode("append").save())
```

---

## Q14. How do you handle discrepancies or validation challenges when migrating to two targets at once?

**Short Answer**
I reconcile both targets against the source and against each other using counts, aggregates and checksums. If a mismatch appears, I identify which target diverged, fix and reload that target only, and rely on idempotent writes so reruns are safe.

**Detailed Answer**
"Writing to two targets means one can succeed while the other fails, so I plan for partial failure.

**Prevention**
- Same dataframe and same run id for both writes.
- Explicit type mapping, because Iceberg and Snowflake treat decimals, timestamps and nulls differently.

**Validation**
- Compare source vs Iceberg vs Snowflake: row counts, sums on amount columns, distinct key counts, and min/max dates.
- Row-level hash comparison for sampled or high-risk tables.

**Handling discrepancies**
1. Check the audit log to see which target failed or diverged.
2. Find the cause: type conversion, rounding, nulls, duplicates, or a partial write.
3. Reload only the affected target for the affected partition, using merge or partition overwrite so there are no duplicates.
4. Re-run validation and record the outcome.

I also set alerts so mismatches are caught the same day, not weeks later."

```sql
-- Compare the two targets
SELECT 'iceberg' AS tgt, COUNT(*) AS cnt, SUM(amount) AS total FROM iceberg.finance.gl_entries WHERE load_id = :run_id
UNION ALL
SELECT 'snowflake', COUNT(*), SUM(amount) FROM GL_ENTRIES WHERE LOAD_ID = :run_id;
```

---

## Quick Reminders Before the Interview

- Lead with the **problem**, then **action**, then **result with a number**.
- Be ready to explain *why* for each choice (why Iceberg, why Z-Order, why broadcast).
- Your resume says 3.5+ years, so say "3.5+ years" or "close to 4 years" consistently.
- If you are asked about something you did not personally build, say "my team did X, my part was Y."
