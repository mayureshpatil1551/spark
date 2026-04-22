# Interview Q&A: Salesforce (Electra) REST API Ingestion using PySpark

**For:** Data Engineers with 3–4 years experience  
**Statement to interviewer:** "I ingested data from REST APIs like Electra (Salesforce) using PySpark"

---

## 1. Salesforce API & SOQL

---

### Q1. What is the Salesforce REST API and how is it different from a regular REST API?

**Answer:**

Salesforce REST API is a standard HTTP-based API, but it has Salesforce-specific conventions:

| Feature | Regular REST API | Salesforce REST API |
|--------|-----------------|---------------------|
| Query language | URL params / body | SOQL (Salesforce Object Query Language) |
| Auth | OAuth2 / API Key | OAuth2 Bearer token via connected app |
| Pagination | Varies | `nextRecordsUrl` in response |
| Endpoint | Custom | `/services/data/vXX.0/queryAll` |
| Batch size | Custom | Controlled via `Sforce-Query-Options: batchSize` header |

In the pipeline, queries were built using SOQL — which looks like SQL but operates on Salesforce objects (tables). The query is passed as a `q` parameter in the GET request.

---

### Q2. What is SOQL and how did you use it dynamically in this pipeline?

**Answer:**

SOQL (Salesforce Object Query Language) is Salesforce's query language. It looks like SQL:

```sql
SELECT Id, Name, SystemModstamp FROM Account WHERE SystemModstamp >= 2024-01-01T00:00:00.000+0000
```

In the pipeline, the SOQL query was not hardcoded. It was stored in a config file (`_conf.toml`) as a template with placeholders:

```toml
SQL = SELECT Id, Name, CreatedDate, SystemModstamp FROM Account
SQL_FILTER_CONDITION = WHERE SystemModstamp >= "{min_value}" AND SystemModstamp < "{max_value}"
```

At runtime, `min_value` = last checkpoint timestamp, `max_value` = current timestamp. The pipeline builds the complete query dynamically:

```python
complete_sql = f"{source_api_sql} {sql_filter_condition}"
```

This makes the pipeline **incremental** — every run only fetches records modified since the last successful run, not the full dataset.

---

### Q3. What is the `queryAll` endpoint vs `query` in Salesforce?

**Answer:**

- `/query` — returns only active (non-deleted) records
- `/queryAll` — returns both active AND soft-deleted records (records in Salesforce's Recycle Bin)

The pipeline used `/queryAll` to capture the full picture — including records deleted in Salesforce — so the raw layer stays in sync with the source and soft deletes can be propagated downstream.

---

### Q4. What is the `Sforce-Query-Options: batchSize=2000` header for?

**Answer:**

Salesforce returns paginated results. The `batchSize` header controls how many records are returned per page (max 2000). Setting it explicitly:

- Ensures predictable page sizes for pagination math
- Avoids Salesforce's default (which may be smaller)
- Allows tuning: smaller batches = less memory per API call; larger = fewer network round trips

The pipeline calculates total pages as `ceil(totalSize / records_per_page)` and builds a DataFrame of all remaining page URLs.

---

### Q5. How does Salesforce pagination work with `nextRecordsUrl`?

**Answer:**

The first API response includes:

```json
{
  "totalSize": 5200,
  "done": false,
  "nextRecordsUrl": "/services/data/v58.0/queryAll/01gXXXXXXXXXXXX-2000",
  "records": [...]
}
```

The URL pattern is `<base_url>-<offset>`. Each subsequent page is fetched by incrementing the offset:

```
/queryAll/01gXXX-2000    → page 2 (records 2001–4000)
/queryAll/01gXXX-4000    → page 3 (records 4001–5200)
```

The pipeline extracts the base URL by finding the `-` separator:

```python
underscore_index = api_response["nextRecordsUrl"].find("-")
next_page_url = api_response["nextRecordsUrl"][:underscore_index]
```

Then builds all page URLs mathematically — no need to call each page sequentially to discover the next one. This enables **parallel fetching** using Spark UDFs.

---

## 2. PySpark — Advanced Patterns

---

### Q6. Why did you use a UDF to make API calls on Spark workers? What are the risks?

**Answer:**

The first page was fetched on the driver (sequentially). For all remaining pages, the pipeline built a DataFrame of URLs and applied a UDF that each Spark worker executes in parallel — distributing the API load across the cluster.

```python
@udf(returnType=self.dataset_schema)
def udf_execute_api(url, headers):
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    response = session.get(url=url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.text)
    return None
```

**Why it works here:**
- Salesforce page URLs are independent — page 3 does not depend on page 2's response
- Parallel fetching is safe because each call is stateless

**Risks:**
- UDFs are black boxes to Spark — no query optimization possible
- If the API rate-limits, many workers hitting simultaneously triggers 429 errors
- Error handling inside UDF must use `pprint` (not logger) because workers don't have the driver's logger context
- Returning `None` on failure silently drops data — needs downstream null checks
- Serialization: the `headers` dict (with token) is serialized and sent to every worker — token must not expire mid-job

---

### Q7. Explain the repartition strategy: `.coalesce(1).repartition(N)`. Why both?

**Answer:**

```python
df = (self.spark
      .createDataFrame(data=total_api_calls_needed, schema=schema)
      .coalesce(1)
      .withColumn("id", monotonically_increasing_id())
      .repartition(self.request_api_repartition_num)
      )
```

Step by step:

1. `createDataFrame` — creates the DataFrame with default partitions (could be many small ones)
2. `.coalesce(1)` — merges all partitions into 1 before adding the ID column. This ensures `monotonically_increasing_id()` generates globally sequential IDs rather than partition-local IDs
3. `.repartition(N)` — re-distributes rows across N partitions so multiple workers execute API calls in parallel

Without `coalesce(1)` first: `monotonically_increasing_id()` produces IDs like `[0, 1, 8589934592, 8589934593, ...]` (partition-local), not clean sequential IDs.

Without `repartition(N)` after: all API calls would run on one worker — no parallelism benefit.

---

### Q8. What is `monotonically_increasing_id()` and what are its guarantees?

**Answer:**

`monotonically_increasing_id()` generates a unique 64-bit integer for each row. Key properties:

- **Unique** across the entire DataFrame — guaranteed
- **Monotonically increasing** within each partition — guaranteed
- **Not consecutive** across partitions — not guaranteed (gaps exist between partition blocks)
- **Not reproducible** — calling it twice on the same data may give different values

```python
# Example output with 2 partitions:
# Partition 0: 0, 1, 2, 3
# Partition 1: 8589934592, 8589934593, 8589934594
```

In this pipeline it's used purely for ordering/identification of API call rows, so the non-consecutive nature doesn't matter.

---

### Q9. How did you handle the case where some API pages fail inside the UDF?

**Answer:**

The UDF returns `None` for failed pages:

```python
if response.status_code == 200:
    return json.loads(response.text)
else:
    return None
```

Downstream, `.withColumn("response", explode("results.records"))` would fail or produce no rows if `results` is null. A more robust approach would be:

```python
from pyspark.sql.functions import col, when

future_response_df = future_response_df.filter(col("results").isNotNull())
```

This was an area for improvement — in production you'd want to capture which URLs returned null and either retry them or alert on partial data loss.

---

### Q10. Why was the first page fetched on the driver and remaining pages on workers?

**Answer:**

The first page is **required to determine** how many total records and pages exist (`totalSize`, `records per page`). You can't build the parallel request DataFrame without knowing this.

Flow:
```
Driver:
  1. Fetch page 1 → get totalSize=5200, records_per_page=2000
  2. Calculate: 3 total pages, 2 remaining
  3. Build DataFrame: [url_page2, url_page3]

Workers (parallel):
  4. Worker 1 → fetches url_page2
  5. Worker 2 → fetches url_page3

Driver:
  6. Union(page1_df, pages2_3_df) → final DataFrame
```

This is a **divide and conquer** pattern — driver handles coordination, workers handle execution.

---

### Q11. What is `HTTPAdapter` with `Retry` and why is it important?

**Answer:**

`HTTPAdapter` with `Retry` adds automatic retry logic to HTTP sessions:

```python
retry = Retry(
    total=5,              # max 5 total retries
    backoff_factor=1,     # sleep: 1s, 2s, 4s, 8s, 16s between retries
    status_forcelist=(500, 502, 504)  # retry on these server errors
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
```

Without this, a single transient 502 (Bad Gateway) would fail the entire UDF call for that page, losing those records.

`backoff_factor=1` means exponential backoff: wait 1s after 1st failure, 2s after 2nd, 4s after 3rd, etc. This prevents hammering an already-stressed API.

Note: 401 and 429 are NOT in `status_forcelist` — those need custom handling because retrying immediately won't help (need token refresh or rate limit wait).

---

## 3. Checkpoint & Incremental Loading

---

### Q12. What is a checkpoint in this pipeline and how does it work?

**Answer:**

A checkpoint records the timestamp of the latest successfully processed record. It drives incremental loading — each run only fetches data modified after the last checkpoint.

Implementation:

```python
# Write checkpoint after successful run
checkpoint_df = (self.spark
    .createDataFrame([(latest_modified_date,)], schema="checkpoint string")
    .withColumn("created_ts", current_timestamp())
)
checkpoint_df.coalesce(1).write.parquet(checkpoint_file_path, mode="overwrite")

# Read checkpoint at start of next run
df = self.spark.read.parquet(checkpoint_file_path)
checkpoint = df.orderBy(col("created_ts").desc()).select("checkpoint").limit(1).collect()[0][0]
```

The checkpoint value is the `MAX(SystemModstamp)` from the staging table after a successful run. Next run uses it as `min_value` in the SOQL WHERE clause.

If the checkpoint file doesn't exist (first run), it falls back to a default minimum date from config (e.g. `1970-01-01T00:00:00.000+0000`), which triggers a full load.

---

### Q13. What happens if the job fails halfway? Does the checkpoint get updated?

**Answer:**

No — and this is intentional. The checkpoint is only written **after** a successful run (after staging insert, MERGE, and all processing completes).

```python
# In run():
try:
    # ... all processing ...
    checkpoint_df.write.parquet(...)  # Only reached on success
except Exception as exp:
    self.update_job_status_with_error(...)
    raise exp
```

If the job fails mid-run:
- Checkpoint stays at the previous value
- Next run will re-fetch from the same starting point
- The staging table gets truncated before insert, so partial data from the failed run is wiped

This makes the pipeline **idempotent on failure** — re-running always produces the correct result.

---

### Q14. What is the `incremental_column` config and why have a fallback?

**Answer:**

Different Salesforce objects use different timestamp columns for tracking changes:
- Most objects use `SystemModstamp` (Salesforce's standard last-modified field)
- Some custom objects may use a custom field like `LastUpdatedDate__c`

The config allows specifying this per dataset:

```toml
incremental_column = "SystemModstamp"
```

If not set, the pipeline falls back to `SystemModstamp` — a safe default since it exists on all standard Salesforce objects. This makes the same ingestion class reusable across multiple Salesforce objects without code changes.

---

### Q15. Why is the checkpoint stored as Parquet and not in a database table?

**Answer:**

Parquet on cloud storage (S3/ADLS/GCS) is:
- **Fast to write:** A single-row Parquet file writes in milliseconds
- **Portable:** No dependency on a running database
- **Versionable:** `mode="overwrite"` replaces it cleanly; old versions can be kept for audit
- **Readable by Spark:** `spark.read.parquet()` is native — no JDBC driver needed

The alternative (database table) would require a database connection, credentials management, and a running service — adding operational complexity for a single-value store.

---

## 4. Security & Token Management

---

### Q16. How was the Salesforce access token stored and retrieved securely?

**Answer:**

The token was NOT hardcoded or stored in plaintext. The flow:

1. A separate session management job writes the Salesforce access token to an **encrypted column** in the `NABU_EXTRACTION_SESSION_TABLE` (a managed table)
2. At runtime, the ingestion job reads the encrypted token from the table
3. A cipher key is generated using `generate_cipher_key(nabu_endpoint, token, postgres_cred_name)` — this calls the Nabu credentials API
4. The token is decrypted: `decrypt_session(cipher_key, encrypted_token)`
5. The decrypted token is used only in memory — never logged or written to disk

```python
encrypted_access_token = df.collect()[0][0]
cipher_key = generate_cipher_key(self.nabu_endpoint, self.token, self.postgres_cred_name)
access_token = decrypt_session(cipher_key, encrypted_access_token)
```

This is a **vault pattern** — credentials are encrypted at rest and decrypted only at runtime using a separate secret (the cipher key) that itself lives in a secrets manager.

---

### Q17. The access token is sent in the UDF to Spark workers. What are the security implications?

**Answer:**

This is a real security concern in production:

- The token is serialized as part of the `headers` dict and distributed to all worker nodes via Spark's task serialization
- It appears in Spark's task descriptions and potentially in logs
- If Spark UI is accessible, headers may be visible

Mitigations:
- Ensure Spark UI authentication is enabled
- Mask sensitive headers in logs
- Use short-lived tokens (Salesforce tokens expire in ~1 hour by default)
- Consider passing only the token endpoint + credentials to workers, and having each worker fetch its own token (though this increases API calls to the token service)

For this pipeline, the token is short-lived and the cluster is in a private VPC, which reduces the practical risk.

---

## 5. Configuration & Reusability

---

### Q18. What is the TOML config pattern and why use it instead of hardcoding?

**Answer:**

Each dataset (Salesforce object) has its own `.toml` config file:

```
code/electra/config_files/account/account_conf.toml
code/electra/config_files/opportunity/opportunity_conf.toml
```

Each config contains:

```toml
[DEFAULT]
SQL = SELECT Id, Name, SystemModstamp FROM Account
SQL_FILTER_CONDITION = WHERE SystemModstamp >= "{min_value}" AND SystemModstamp < "{max_value}"
TIMESTAMP_FIELDS = ["CreatedDate", "SystemModstamp"]
DATE_FIELDS = ["CloseDate"]
incremental_column = "SystemModstamp"
DEFAULT_MIN_CHECKPOINT = 1970-01-01T00:00:00.000+0000
```

Benefits:
- **One codebase, many datasets:** The same `RawDataIngestion` class handles Account, Opportunity, Contact etc. — just point it at a different config
- **No code deploy for new fields:** Adding a column = update config file
- **Non-engineers can update configs** without touching Python code
- **Environment-specific configs** can be maintained separately

---

### Q19. Why are timestamp and date columns cast separately, and how?

**Answer:**

Salesforce API returns all fields as strings. PySpark can't infer Salesforce's timestamp format (`2024-01-15T10:30:00.000`) automatically, so explicit casting is needed:

```python
# Timestamp columns: "2024-01-15T10:30:00.000"
for tc in self.timestamp_columns:
    df = df.withColumn(tc, to_timestamp(col(tc), "yyyy-MM-dd'T'HH:mm:ss.SSS"))

# Date columns: "2024-01-15"
for dc in self.date_columns:
    df = df.withColumn(dc, to_date(col(dc), "yyyy-MM-dd"))
```

The columns to cast are read from config (`TIMESTAMP_FIELDS`, `DATE_FIELDS`) as JSON arrays — so adding new date fields requires only a config change, not a code change.

`spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")` was set before insertion to avoid Spark 3.x strict timestamp parsing rejecting older Salesforce date formats.

---

### Q20. The schema is loaded from a `.schema` file. Why not use Spark's schema inference?

**Answer:**

Spark's schema inference (`spark.read.json(rdd)` without schema) reads the data to guess types. Problems:

- **Inconsistent:** If a field is null in all sampled rows, Spark infers it as `string` — might be wrong
- **Slow:** Spark has to scan data twice (once to infer, once to read)
- **Fragile:** If one record has an int and another has a string in the same field, inference fails or picks wrong type
- **Non-reproducible:** Schema can drift between runs as data changes

With a `.schema` file (`StructType.fromJson()`):
- Schema is locked — guaranteed consistency across runs
- Any schema mismatch from the API is caught immediately as an error, not silently corrupted
- Supports complex nested types that inference handles poorly

---

## 6. Real-World Coding Questions

---

### Coding Q1. Write a PySpark UDF that calls a REST API and returns a flat row of data.

**Answer:**

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import json

# Define return schema
schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("status", StringType())
])

@udf(returnType=schema)
def call_api_udf(url, token):
    try:
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return (data.get("id"), data.get("name"), data.get("status"))
        return None
    except Exception:
        return None

# Usage
df_with_results = url_df.withColumn("result", call_api_udf("url", "token"))
df_final = df_with_results.filter("result IS NOT NULL").select("result.*")
```

Key points to mention:
- Return type must exactly match the schema
- Return `None` on failure so the row is skipped rather than crashing
- `timeout=30` prevents a hanging worker task
- `select("result.*")` flattens the struct column into individual columns

---

### Coding Q2. Implement checkpoint read/write logic in PySpark.

**Answer:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, max as spark_max

spark = SparkSession.builder.getOrCreate()
CHECKPOINT_PATH = "s3://bucket/checkpoints/account/"

def get_checkpoint(default="1970-01-01T00:00:00.000+0000"):
    try:
        df = spark.read.parquet(CHECKPOINT_PATH)
        checkpoint = (df
            .orderBy(col("created_ts").desc())
            .select("checkpoint")
            .limit(1)
            .collect()[0][0]
        )
        return checkpoint
    except Exception:
        return default  # First run — full load

def save_checkpoint(stg_table, col_name="SystemModstamp"):
    latest = (spark
        .sql(f"SELECT MAX({col_name}) as max_ts FROM {stg_table}")
        .withColumn("checkpoint", date_format(col("max_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'+0000'"))
        .select("checkpoint")
        .collect()[0][0]
    )
    checkpoint_df = (spark
        .createDataFrame([(latest,)], schema="checkpoint string")
        .withColumn("created_ts", current_timestamp())
    )
    checkpoint_df.coalesce(1).write.parquet(CHECKPOINT_PATH, mode="overwrite")
    return latest

# Usage
min_ts = get_checkpoint()
max_ts = "2024-06-01T00:00:00.000+0000"
query = f"SELECT * FROM Account WHERE SystemModstamp >= '{min_ts}' AND SystemModstamp < '{max_ts}'"
# ... run ingestion ...
save_checkpoint("staging_db.account_stg")
```

---

### Coding Q3. Build the Salesforce pagination URL list and create a DataFrame of API calls.

**Answer:**

```python
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, MapType
from math import ceil

def build_pagination_df(spark, first_response, base_url, headers, num_partitions=10):
    total_records = first_response["totalSize"]
    records_per_page = len(first_response["records"])
    total_pages = ceil(total_records / records_per_page)

    # Extract base URL (strip the "-offset" suffix)
    next_url_raw = first_response["nextRecordsUrl"]
    dash_idx = next_url_raw.find("-")
    next_url_base = next_url_raw[:dash_idx]

    # Build all page URLs (page 2 onwards)
    RequestRow = Row("url", "headers")
    rows = [
        RequestRow(
            f"{base_url}{next_url_base}-{records_per_page * i}",
            headers
        )
        for i in range(1, total_pages)
    ]

    schema = StructType([
        StructField("url", StringType()),
        StructField("headers", MapType(StringType(), StringType()))
    ])

    return (spark
        .createDataFrame(rows, schema=schema)
        .repartition(num_partitions)
    )

# Example usage
first_resp = {
    "totalSize": 6000,
    "records": [{}] * 2000,
    "nextRecordsUrl": "/services/data/v58.0/queryAll/01gXXX-2000"
}
df = build_pagination_df(spark, first_resp, "https://myorg.salesforce.com", {"Authorization": "Bearer abc"})
df.show(truncate=False)
# Output:
# /queryAll/01gXXX-2000  → page 2
# /queryAll/01gXXX-4000  → page 3
```

---

### Coding Q4. Write a function to dynamically cast timestamp and date columns from a config list.

**Answer:**

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, to_date
from typing import List

def cast_temporal_columns(
    df: DataFrame,
    timestamp_cols: List[str],
    date_cols: List[str],
    ts_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSS",
    date_format_str: str = "yyyy-MM-dd"
) -> DataFrame:
    for tc in timestamp_cols:
        if tc in df.columns:
            df = df.withColumn(tc, to_timestamp(col(tc), ts_format))
        else:
            print(f"Warning: timestamp column '{tc}' not found in DataFrame")

    for dc in date_cols:
        if dc in df.columns:
            df = df.withColumn(dc, to_date(col(dc), date_format_str))
        else:
            print(f"Warning: date column '{dc}' not found in DataFrame")

    return df

# Usage
timestamp_cols = ["CreatedDate", "SystemModstamp", "LastModifiedDate"]
date_cols = ["CloseDate", "ActivityDate"]
df = cast_temporal_columns(df, timestamp_cols, date_cols)
```

The `if tc in df.columns` guard prevents `AnalysisException` when the API drops a field — the job continues instead of crashing.

---

### Coding Q5. Write a retry wrapper for a REST API call with exponential backoff.

**Answer:**

```python
import time
import requests
from typing import Optional

def call_api_with_retry(
    url: str,
    headers: dict,
    max_retries: int = 5,
    backoff_factor: float = 1.0,
    retry_status_codes: tuple = (429, 500, 502, 503, 504)
) -> Optional[dict]:

    attempt = 0
    while attempt <= max_retries:
        try:
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code == 200:
                return resp.json()

            elif resp.status_code == 401:
                print(f"[{url}] 401 Unauthorized — token expired, cannot retry")
                return None  # Caller must refresh token

            elif resp.status_code in retry_status_codes:
                wait_time = backoff_factor * (2 ** attempt)
                # Honour Retry-After header if present (e.g. 429)
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait_time = float(retry_after)
                print(f"[{url}] {resp.status_code} — retrying in {wait_time}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
                attempt += 1

            else:
                print(f"[{url}] Unrecoverable error {resp.status_code}: {resp.text}")
                return None

        except requests.exceptions.ConnectionError as e:
            wait_time = backoff_factor * (2 ** attempt)
            print(f"[{url}] Connection error: {e}. Retrying in {wait_time}s")
            time.sleep(wait_time)
            attempt += 1

    print(f"[{url}] All {max_retries} retries exhausted")
    return None
```

Key points to mention:
- Exponential backoff: `2^attempt * factor` → 1s, 2s, 4s, 8s, 16s
- Honours `Retry-After` header for rate limiting (429)
- Separates 401 (token expired, different fix) from server errors (can retry)
- `requests.exceptions.ConnectionError` catches network-level failures, not just HTTP errors

---

### Coding Q6. The pipeline unions two DataFrames (page 1 and pages 2+). What could go wrong and how do you prevent it?

**Answer:**

```python
# Potential issues with this union:
df = df_current.union(df_future)
```

**Problem 1: Schema mismatch**
`union()` matches columns by position, not name. If `df_current` and `df_future` have columns in different order, data silently goes into wrong columns.

Fix:
```python
# Use unionByName instead
df = df_current.unionByName(df_future, allowMissingColumns=True)
```

**Problem 2: Null rows from failed UDF calls**
If some UDF calls returned `None`, `df_future` will have null rows after `explode`.

Fix:
```python
df_future = (future_response_df
    .filter(col("results").isNotNull())   # Drop failed pages
    .withColumn("response", explode("results.records"))
    .select("response.*")
    .drop("attributes")
)
```

**Problem 3: Column present in one but not the other**
If page 1 had a column that page 2+ didn't (Salesforce API quirk with null-only columns), union fails.

Fix: Use `allowMissingColumns=True` in `unionByName` — fills missing columns with null.

---

### Scenario Q7. Your checkpoint file is corrupted. The job falls back to 1970 and reloads 10 years of data. How do you prevent this?

**Answer:**

Root cause: `spark.read.parquet()` fails → falls back to `DEFAULT_MIN_CHECKPOINT = 1970-01-01T00:00:00.000+0000` → full reload.

Prevention strategies:

**1. Validate checkpoint value after reading:**
```python
from datetime import datetime, timezone

def get_safe_checkpoint(checkpoint: str, max_lookback_days: int = 7) -> str:
    try:
        parsed = datetime.strptime(checkpoint, "%Y-%m-%dT%H:%M:%S.%f%z")
        lookback_limit = datetime.now(timezone.utc).replace(
            day=datetime.now().day - max_lookback_days
        )
        if parsed < lookback_limit:
            raise ValueError(f"Checkpoint {checkpoint} is too old — possible corruption")
        return checkpoint
    except Exception as e:
        raise RuntimeError(f"Invalid checkpoint: {e}")
```

**2. Keep a backup checkpoint:**
```python
# Write to both primary and backup path
checkpoint_df.coalesce(1).write.parquet(primary_path, mode="overwrite")
checkpoint_df.coalesce(1).write.parquet(backup_path, mode="overwrite")
```

**3. Use a database table instead of Parquet for atomic writes with history:**
```sql
CREATE TABLE checkpoint_log (
    job_name STRING,
    checkpoint TIMESTAMP,
    created_ts TIMESTAMP,
    status STRING
);
```

**4. Alert on suspiciously large data volumes:**
If `increment_data_count > threshold`, send an alert before merging — gives a chance to abort a bad full reload.

---

### Scenario Q8. Salesforce adds a new column to the object. Your schema file doesn't have it. What happens and how do you handle it?

**Answer:**

What happens:
- `createDataFrame(data, schema=struct_schema)` — Spark enforces the schema strictly
- The new column from the API response is **silently dropped** if it's not in the schema
- If the new field is nested or changes a type, it may cause an `AnalysisException`

Handling approaches:

**Short-term fix (no downtime):**
Add the new field to the `.schema` file as nullable StringType — broadest compatible type:
```json
{"name": "NewField__c", "type": "string", "nullable": true, "metadata": {}}
```
Then `ALTER TABLE target_table ADD COLUMN NewField__c STRING` in Iceberg.

**Long-term: Schema drift detection:**
```python
def detect_schema_drift(api_response_keys: set, schema_keys: set):
    new_fields = api_response_keys - schema_keys
    missing_fields = schema_keys - api_response_keys
    if new_fields:
        logger.warning(f"New fields in API not in schema: {new_fields}")
    if missing_fields:
        logger.warning(f"Fields in schema missing from API: {missing_fields}")
```

Run this comparison at the start of each job. Alert (but don't fail) on drift so engineers are notified before it becomes a problem.

---

*Prepared for Data Engineering interviews — 3.5 years experience level*  
*Pipeline: Electra (Salesforce) → PySpark UDF → Staging → Iceberg MERGE*
