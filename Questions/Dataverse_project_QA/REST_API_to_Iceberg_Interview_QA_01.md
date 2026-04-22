# Interview Q&A: REST API → Iceberg Ingestion using PySpark

**For:** Data Engineers with 3–4 years experience  
**Topic:** "I ingested data from a REST API into an Iceberg table using PySpark"

---

## 1. REST API Concepts

---

### Q1. What is a REST API and how did you call it in PySpark?

**Answer:**

A REST API is a web service that follows HTTP standards. You interact with it using methods like GET, POST, PUT, DELETE. Each endpoint returns structured data (usually JSON).

In PySpark, the Spark driver calls the API using Python's `requests` library (not on workers — because REST calls need coordination, state, and token management which doesn't fit the distributed worker model).

```python
import requests

headers = {"Authorization": "Bearer <token>", "Accept": "*/*"}
response = requests.get("https://api.example.com/v1/data", headers=headers)

if response.status_code == 200:
    data = response.json()
```

---

### Q2. What is the difference between GET and POST requests? When did you use each?

**Answer:**

| Method | Purpose | Has Body? |
|--------|---------|-----------|
| GET    | Read/fetch data | No |
| POST   | Send data / trigger action | Yes (JSON body) |

In the ingestion pipeline:

- **GET** was used to fetch metadata like instances and study lists — no body needed, just URL parameters.
- **POST** was used to fetch protocol deviation records — because filters (like `{"statuses": ["APPROVED"]}`) had to be sent in the request body.

Headers also differ: POST requests need `Content-Type: application/json`; GET requests don't.

---

### Q3. How did you handle authentication with the REST API?

**Answer:**

The API used **OAuth2 password grant** (username + password + client credentials). The flow:

1. POST to the `/token` endpoint with credentials to get a short-lived `access_token`
2. Attach it to every subsequent request as `Authorization: Bearer <token>`
3. On a 401 (Unauthorized) response, wait and refresh the token, then retry

Credentials were not hardcoded. They were fetched securely from a credentials vault (Nabu/Fireshots) at runtime using a service token, so nothing sensitive lived in the codebase.

---

### Q4. How did you handle token expiry during a long-running API job?

**Answer:**

Access tokens have a short TTL (e.g. 30–60 minutes). For a job fetching hundreds of studies, mid-run expiry is common.

The approach:

1. On a **401 response**, don't immediately fail — enter a retry loop
2. Sleep for N seconds (e.g. 30s) to allow the token service to issue a new token
3. Call `get_token()` and compare the new token with the previous one
4. Only retry the API call if the token actually changed (if it's the same, keep waiting)
5. After `max_retries` attempts, raise a custom `TokenExpiredException` to cleanly propagate the failure

A custom exception class was used (instead of generic Exception) so the caller could distinguish token failures from other errors and handle them specifically in logging and reporting.

---

### Q5. What is pagination and how did you implement it?

**Answer:**

Most APIs don't return all records in one response — they split data across pages. Each page has a fixed size (e.g. 50 records), and you must loop through pages until you've fetched everything.

Implementation:

```python
page = 1
all_pages = []

while True:
    url = f"{base_url}?page={page}&per_page=50"
    resp = requests.get(url, headers=headers)
    data = resp.json()
    records = data.get("data", {}).get("records", [])
    
    if not records:
        break  # No more data
    
    all_pages.append(data)
    page += 1
```

The exit condition was an empty `records` list, or an explicit `"No data found."` message in the pagination metadata — whichever came first.

---

### Q6. What is an API rate limit and how did you handle it?

**Answer:**

Rate limiting is when an API restricts how many requests you can make in a time window (e.g. 100 requests/minute). Exceeding it returns a **429 Too Many Requests** error.

Handling:

- Added a configurable `api_rate_delay` (e.g. 3–5 seconds) using `time.sleep()` between each study's API call
- The delay was externalized in Spark config so it could be tuned per environment without code changes
- In production, a lower delay risks throttling; a higher delay is safer but slower

Real-world tip: Some APIs return `Retry-After` in the 429 response header — you can use that value as the sleep duration instead of a fixed delay.

---

## 2. PySpark

---

### Q7. Why did you run the API calls on the Spark driver and not on workers?

**Answer:**

REST API calls require:
- Stateful token management (refresh logic, retry loops)
- Sequential pagination (page N depends on the result of page N-1)
- A single authenticated session per API

Spark workers are stateless, distributed, and don't share memory. If you pushed API calls to workers, each task would need its own token, you'd have no coordination between pages, and failures would be hard to recover.

The correct pattern: driver fetches all data → collects it into a Python list → creates a Spark DataFrame for distributed processing (transformations, deduplication, writing).

---

### Q8. How did you convert the API JSON response into a Spark DataFrame?

**Answer:**

API responses were collected as a Python list of JSON pages. Each page had a nested structure like:

```json
{"api_param": "study_123", "data": {"records": [{...}, {...}]}}
```

Steps:
1. Pre-process nested fields: convert dict/list fields to JSON strings using `json.dumps()` so they fit a flat schema
2. Load a predefined schema from a `.schema` file (`StructType.fromJson()`)
3. Create DataFrame: `spark.createDataFrame(list_response, schema=struct_schema)`
4. Explode the records array: `explode("data.records")` to get one row per record

Using a pre-defined schema (instead of schema inference) ensures consistency across runs and avoids silent type mismatches.

---

### Q9. What is explode() and when did you use it?

**Answer:**

`explode()` transforms an array column into multiple rows — one row per array element.

In this pipeline, each API page response contained an array of records. After creating the DataFrame, you'd have one row per page with a nested array. `explode()` flattened it:

```python
from pyspark.sql.functions import explode, col

df_exploded = df.select(
    col("api_param"),
    explode("data.records").alias("records")
)

df_flat = df_exploded.select(
    col("api_param").alias("study_id"),
    "records.*"
)
```

Before explode: 10 rows (one per page), each with 50-element array
After explode: 500 rows, one per record — ready for transformation and loading.

Use `explode_outer()` if the array can be null/empty and you want to preserve those rows.

---

### Q10. How did you detect incremental (new/changed) records?

**Answer:**

Used PySpark's `subtract()` — a set difference operation:

```python
# Drop audit columns before comparison
existing_df = spark.sql("SELECT * FROM raw_db.target_table") \
    .drop("dv_insrt_ts", "dv_updt_ts", "dv_is_deleted", "dv_uuid")

# New rows = rows in fresh data that don't exist in current table
incremental_df = fresh_df.subtract(existing_df)
incremental_df = incremental_df.withColumn("dv_is_deleted", lit("N"))
```

`subtract()` compares entire rows. Audit columns are dropped before comparison so that system-generated timestamps don't cause every row to appear as new.

**Limitation:** `subtract()` treats any column difference as a new row. It doesn't tell you *which* column changed. The actual upsert (handling updates properly) is done in the downstream MERGE step using primary keys.

---

### Q11. How did you detect deleted records (soft deletes)?

**Answer:**

Records that existed in the target table but were no longer returned by the API are considered deleted.

Detection used a **left anti-join**:

```python
deleted_df = existing_df.join(
    fresh_df, on="dv_composite_key", how="leftanti"
).withColumn("dv_is_deleted", lit("Y"))
```

A left anti-join returns rows from the left DataFrame that have no match in the right. Here: rows in the existing table with no matching key in the fresh API data = deleted records.

These were written to staging with `dv_is_deleted = "Y"`, and the MERGE step propagated the flag to the raw table. This is a **soft delete** pattern — data is retained for audit/compliance but flagged as inactive.

---

### Q12. What is a composite key and why did you need one?

**Answer:**

A composite key uniquely identifies a record using multiple business columns combined into one string:

```python
from pyspark.sql.functions import concat_ws, coalesce, col, lit

df = df.withColumn(
    "dv_composite_key",
    concat_ws("|", coalesce(col("id"), lit("")),
                   coalesce(col("study_id"), lit("")),
                   coalesce(col("site_id"), lit("")))
)
# Result: "101|STUDY_A|SITE_3"
```

`coalesce` handles NULLs — replacing them with empty string so NULL columns don't break the key.

The composite key was used for:
- Left anti-join for delete detection
- MERGE condition in the Iceberg upsert
- Ensuring uniqueness when no single column was a reliable primary key

---

### Q13. What is the staging table pattern and why use it?

**Answer:**

Instead of writing directly to the raw/target Iceberg table, incremental data goes to a staging table first:

```
API → Driver (Python) → Staging Table → MERGE → Raw/Target Table
```

Reasons:

- **Atomicity:** The MERGE reads from staging as a clean, complete batch. If anything fails before the MERGE, the raw table is untouched.
- **Isolation:** Staging is truncated before each load, so it always contains only the current run's data — no stale rows.
- **Debugging:** You can inspect staging data before it's merged into production.
- **Performance:** Iceberg MERGEs on small staging datasets are faster than merging into a large table row by row.

Staging is truncated with `TRUNCATE TABLE` before each insert, making it stateless across runs.

---

## 3. Apache Iceberg

---

### Q14. What is Apache Iceberg and why was it chosen over a regular Parquet/Hive table?

**Answer:**

Apache Iceberg is an open table format for large analytic datasets. It sits on top of file formats like Parquet and adds:

| Feature | Hive / Parquet | Iceberg |
|--------|---------------|---------|
| ACID transactions | No | Yes |
| UPSERT / MERGE | No | Yes |
| Time travel | No | Yes |
| Schema evolution | Limited | Full |
| Concurrent reads/writes | Unsafe | Safe |
| Partition evolution | Rewrite table | In-place |

For this pipeline, Iceberg was chosen because:
- The MERGE statement needed ACID guarantees (upsert + soft delete in one operation)
- Audit requirements needed time travel (point-in-time snapshots)
- The table schema evolves as the API adds new fields

---

### Q15. What is an Iceberg MERGE statement and how did you use it?

**Answer:**

MERGE (upsert) handles inserts and updates in a single atomic SQL statement:

```sql
MERGE INTO raw_db.protocol_deviation AS target
USING (
    SELECT * FROM stg_db.protocol_deviation_stg
) AS source
ON target.dv_composite_key = source.dv_composite_key

WHEN MATCHED AND source.dv_is_deleted = 'Y' THEN
    UPDATE SET target.dv_is_deleted = 'Y', target.dv_updt_ts = current_timestamp()

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
```

This handles three cases in one pass:
- **New records** (not in target) → INSERT
- **Changed records** (key matches but row differs) → UPDATE
- **Deleted records** (flagged Y in staging) → soft-delete UPDATE

The MERGE SQL was stored as a template file and had a job UUID injected at runtime for traceability.

---

### Q16. What is Iceberg table maintenance and why is it needed?

**Answer:**

Every MERGE/INSERT in Iceberg creates a new **snapshot** (version) of the table and generates new Parquet files. Without maintenance, over time:

- Old snapshot metadata accumulates → slower planning
- Orphan files (unreferenced Parquet) → wasted storage
- Many small files → slow query scans

Maintenance operations run after each job:

```python
# Expire old snapshots (keep last N days)
spark.sql("CALL catalog.system.expire_snapshots('db.table', TIMESTAMP 'now - 7 days')")

# Remove orphan files
spark.sql("CALL catalog.system.remove_orphan_files('db.table')")

# Compact small files into larger ones
spark.sql("CALL catalog.system.rewrite_data_files('db.table')")
```

These run in `post_run()` — after the main ingestion — so they don't block the critical path.

---

### Q17. What is time travel in Iceberg? How would you use it?

**Answer:**

Iceberg keeps a history of every snapshot (version) of the table. You can query any past snapshot:

```sql
-- Query data as it was 2 days ago
SELECT * FROM raw_db.protocol_deviation
TIMESTAMP AS OF '2024-06-01 00:00:00';

-- Query a specific snapshot by ID
SELECT * FROM raw_db.protocol_deviation
VERSION AS OF 12345678;
```

Real-world use cases:
- **Audit/compliance:** Reproduce exactly what the table looked like on a specific date
- **Debugging:** Compare current data vs yesterday to find what changed
- **Rollback:** Restore to a previous snapshot if a bad load corrupted data

In this pipeline, time travel was valuable because clinical trial data has strict regulatory audit requirements.

---

## 4. Design Patterns & Architecture

---

### Q18. How did you ensure the job doesn't run twice simultaneously?

**Answer:**

At the start of `run()`, the job checks a job audit table to see if a previous run is still in progress:

```python
if self.is_last_job_running():
    self.logger.error("Previous job is still running.")
    return False
```

This is a **concurrency lock** pattern using a database table as the lock store. Each run inserts a record with status `RUNNING`. On success or failure, it updates to `SUCCESS` or `FAILED`.

Without this, two concurrent runs could:
- Both truncate and insert to staging simultaneously
- Both execute the MERGE, creating duplicates
- Produce incorrect audit counts

This is common in production pipelines triggered by schedulers like Airflow or cron — where a slow run can overlap with the next scheduled trigger.

---

### Q19. How did you track job success/failure for monitoring?

**Answer:**

The job used a **job audit table** pattern:

1. On start: `insert_into_job_audit(job_id)` — records that the job started
2. On success: `update_job_status_with_success(job_id, incremental_count, total_count)` — records counts and marks success
3. On failure: `update_job_status_with_error(job_id, error_message)` — records the exception message

Each run has a unique `job_id = uuid.uuid4()` so individual runs are traceable in the audit table even if multiple jobs run for the same table.

The summary log at the end reports:
- Total studies attempted
- Successful studies
- Failed studies with failure reasons

This gives the ops team a clear picture without having to read raw Spark logs.

---

### Q20. How did you handle partial failures — e.g., one study's API call fails?

**Answer:**

The per-study loop used a try-except block to isolate failures:

```python
for study_id in study_ids:
    try:
        pages = fetch_pd_for_study(study_id, headers)
        if pages is None:
            failed_studies.append((study_id, "API returned None"))
            continue
        list_response.extend(pages)
        successful_studies.append(study_id)
    except TokenExpiredException as te:
        failed_studies.append((study_id, "TokenExpired"))
    except Exception as ex:
        failed_studies.append((study_id, str(ex)))
```

Key design decisions:
- A `continue` after failure means one bad study doesn't stop the entire job
- Failed studies are collected with their reason — not silently dropped
- At the end, a summary is logged: "50 studies attempted, 48 succeeded, 2 failed"
- The job still returns data for the 48 successful studies — partial success is better than total failure

---

## 5. Real-World Hands-On Scenarios

---

### Scenario 1: The API returns 200 but the data is empty for some studies. How do you handle this?

**Answer:**

Empty records (`[]`) from a 200 response is a valid business case — a study may genuinely have no data. The code handles this separately from an API error:

```python
pages = fetch_pd_for_study(study_id, headers)

if pages is None:
    # API call itself failed (non-200, exception)
    failed_studies.append((study_id, "API returned None"))
elif pages == []:
    # API succeeded, no records exist
    successful_studies.append(study_id)
    logger.info(f"No records for study {study_id} — this is normal")
else:
    list_response.extend(pages)
    successful_studies.append(study_id)
```

Do **not** treat empty data as a failure. If you did, your monitoring would fire false alerts for every study that legitimately has no deviations.

---

### Scenario 2: The API schema changes — a new field is added. What breaks and how do you fix it?

**Answer:**

If the pipeline uses a hardcoded schema file (`.schema`), the new field won't be in the StructType. Spark will either:
- Silently ignore the new field (if createDataFrame is lenient), or
- Throw a schema mismatch error if the new field is not nullable

Fix:
1. Update the `.schema` file to add the new field (as nullable StringType to be safe)
2. Re-deploy the job

Iceberg makes this easy because it supports **schema evolution** — you can `ALTER TABLE ADD COLUMN` without rewriting the table. Existing rows just return NULL for the new column.

Proactive approach: periodically compare the API's response schema to the registered table schema and alert on drift before it causes a job failure.

---

### Scenario 3: The job ran successfully but you notice duplicate records in the target table. How do you debug?

**Answer:**

Step-by-step investigation:

1. **Check the job audit table** — did two runs execute at the same time? (Missing concurrency lock?)
2. **Check the staging table** — does it have duplicates before the MERGE? If yes, the issue is upstream (API returned duplicates, or composite key is not unique enough)
3. **Check the MERGE logic** — is the ON condition matching on a unique key? A non-unique key causes one source row to match multiple target rows
4. **Check subtract logic** — were audit columns (`dv_insrt_ts`) included in the comparison? If yes, they'd make every row look "new" on each run

```sql
-- Find duplicates in target table
SELECT dv_composite_key, COUNT(*) as cnt
FROM raw_db.protocol_deviation
GROUP BY dv_composite_key
HAVING COUNT(*) > 1;
```

Most common root cause at this experience level: composite key not unique, or audit timestamp columns included in the `subtract()` comparison.

---

### Scenario 4: The job runs daily. Today it took 3x longer than usual. How do you investigate?

**Answer:**

Possible causes and investigation steps:

| Possible Cause | How to Check |
|----------------|-------------|
| API is slow / rate-limited | Check API response times in logs |
| More records than usual (data volume spike) | Compare record counts in job audit table |
| Small file problem in Iceberg | Check file count with `FILES` system table |
| Iceberg MERGE scanning too many files | Check Spark UI → SQL tab → scan metrics |
| Token expiry causing long waits | Check logs for retry/sleep messages |
| Staging table not truncated — MERGE processing old rows | Check staging count vs expected |

First action: check Spark UI for the slow stage. If it's the MERGE step, run `rewrite_data_files` maintenance. If it's the API fetch loop, check API logs for throttling.

---

### Scenario 5: How would you re-run the job for a specific date range if the API supports it?

**Answer:**

If the API supports date filters (e.g. `?from_date=2024-06-01&to_date=2024-06-07`), you'd:

1. Add `from_date` and `to_date` as Spark config parameters
2. Pass them into the API URL in the fetch loop
3. Use the same staging → MERGE pattern — the MERGE handles upserts correctly regardless of whether rows are new or already exist

For a full historical backfill, you'd run the job iteratively over date ranges (daily or weekly batches) to avoid API timeouts on large ranges.

Iceberg makes re-runs safe because MERGE is idempotent — re-inserting an existing record (same composite key, same data) results in no change to the table.

---

### Scenario 6: You need to add a new source table (e.g., Adverse Events) alongside Protocol Deviations. How do you scale the pipeline?

**Answer:**

The pipeline was designed for reusability. Key components to parameterize:

- `target_table`, `target_db`, `staging_db` — all come from Spark config, not hardcoded
- `primary_keys` — configurable per table
- Schema file path and MERGE file path — built from `job_name` in Spark config
- The same class (`AlexIngestion`) can be reused by changing the Spark submit config

To add Adverse Events:
1. Create a new schema file: `code/files/alex_adverse_events/alex_adverse_events.schema`
2. Create a new merge file: `code/files/alex_adverse_events/alex_adverse_events.merge`
3. Submit the job with `job_name=alex_adverse_events`, `target_table=adverse_events`, etc.
4. No code changes needed — just config

This is the **convention over configuration** pattern commonly used in enterprise data pipelines.

---

*Prepared for Data Engineering interviews — 3.5 years experience level*
