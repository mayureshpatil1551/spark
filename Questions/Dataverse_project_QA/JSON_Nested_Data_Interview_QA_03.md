# Interview Q&A: JSON Responses, Nested Data & Real-World Scenarios

**For:** Data Engineers with 3–4 years experience  
**Context:** REST API ingestion (Electra/Salesforce + ALEX) using PySpark → Iceberg

---

## 1. API Response Format Questions

---

### Q1. In what format does a REST API return data? What did you receive in your project?

**Answer:**

REST APIs return data in **JSON (JavaScript Object Notation)** format. It is the industry standard because:
- Human-readable
- Language-independent
- Natively supported by Python (`json` module) and PySpark (`spark.read.json`)

In your project, both APIs returned JSON responses:

**Salesforce (Electra) response:**
```json
{
  "totalSize": 5200,
  "done": false,
  "nextRecordsUrl": "/services/data/v58.0/queryAll/01gXXX-2000",
  "records": [
    {
      "attributes": { "type": "Account", "url": "/services/data/v58.0/sobjects/Account/001XX" },
      "Id": "001XX000003GYk1",
      "Name": "Acme Corp",
      "SystemModstamp": "2024-05-01T10:30:00.000+0000",
      "CloseDate": "2024-06-30"
    }
  ]
}
```

**ALEX (Clinical) response:**
```json
{
  "api_param": "study_123",
  "data": {
    "pagination": { "message": "", "total": 150 },
    "records": [
      {
        "id": 1,
        "study_id": "S001",
        "site_id": "ST01",
        "additional_details": { "category": "Minor", "notes": "xyz" },
        "raised_by_details": { "user_id": "U01", "name": "John" }
      }
    ]
  }
}
```

Both are **nested JSON** — the actual records are buried inside wrapper keys (`data.records`, `records`), and some fields are themselves nested objects.

---

### Q2. What is nested JSON? How is it different from flat JSON?

**Answer:**

**Flat JSON** — all values are primitives (strings, numbers, booleans):
```json
{ "id": "001", "name": "Acme", "status": "Active", "revenue": 50000 }
```

**Nested JSON** — values can be objects (dicts) or arrays:
```json
{
  "id": "001",
  "name": "Acme",
  "address": {
    "city": "Chicago",
    "state": "IL",
    "zip": "60601"
  },
  "contacts": [
    { "name": "Alice", "role": "Manager" },
    { "name": "Bob",   "role": "Engineer" }
  ]
}
```

When you load nested JSON into PySpark, the schema reflects the nesting:

```
root
 |-- id: string
 |-- name: string
 |-- address: struct
 |    |-- city: string
 |    |-- state: string
 |    |-- zip: string
 |-- contacts: array
 |    |-- element: struct
 |         |-- name: string
 |         |-- role: string
```

You access nested fields with dot notation: `col("address.city")`, and array fields with `explode()`.

---

### Q3. How did you read nested JSON in PySpark? Walk me through step by step.

**Answer:**

**Method 1 — From API response collected on driver (your project's approach):**

```python
import json
from pyspark.sql.functions import col, explode

# Step 1: API response is a Python dict (already parsed)
response = {
    "totalSize": 3,
    "records": [
        {"Id": "001", "Name": "Acme", "address": {"city": "Chicago"}},
        {"Id": "002", "Name": "Beta", "address": {"city": "NYC"}},
        {"Id": "003", "Name": "Gamma", "address": {"city": "LA"}}
    ]
}

# Step 2: Serialize back to JSON string, parallelize as RDD
rdd = spark.sparkContext.parallelize([json.dumps(response)])

# Step 3: Read JSON with defined schema
df = spark.read.json(rdd, schema=your_struct_schema)

# Step 4: At this point df has ONE row — the whole response
# Schema: totalSize(int), records(array<struct>)
df.printSchema()
# root
#  |-- totalSize: long
#  |-- records: array
#  |    |-- element: struct
#  |         |-- Id: string
#  |         |-- Name: string
#  |         |-- address: struct
#  |              |-- city: string
```

**Step 5 — Explode the records array:**
```python
df_exploded = df.withColumn("data", explode(col("records")))
# Now df_exploded has ONE ROW PER RECORD

# Step 6 — Flatten the struct
df_flat = df_exploded.select(col("data.*"))
# Columns: Id, Name, address (still nested struct)

# Step 7 — Access nested struct fields
df_final = df_flat.select(
    col("Id"),
    col("Name"),
    col("address.city").alias("city")
)
```

**Method 2 — From file/S3 path:**
```python
df = spark.read.option("multiline", "true").json("s3://bucket/api_response.json")
```

---

### Q4. What is `explode()` in PySpark? Explain with an example from your project.

**Answer:**

`explode()` takes an **array column** and creates one row per array element. The rest of the columns are duplicated for each element.

**Before explode:**

| api_param  | data.records (array)               |
|------------|-------------------------------------|
| study_123  | [{id:1, name:A}, {id:2, name:B}]   |

```python
df_exploded = df.select(
    col("api_param"),
    explode(col("data.records")).alias("record")
)
```

**After explode:**

| api_param  | record              |
|------------|---------------------|
| study_123  | {id:1, name:A}      |
| study_123  | {id:2, name:B}      |

```python
# Flatten the struct column using select("record.*")
df_final = df_exploded.select(
    col("api_param"),
    col("record.*")
)
```

**Final result:**

| api_param  | id | name |
|------------|-----|------|
| study_123  | 1   | A    |
| study_123  | 2   | B    |

In the Salesforce pipeline the same pattern was used:
```python
df = (spark.read.json(rdd, schema=dataset_schema)
      .withColumn("data", explode(col("records")))
      .select(col("data.*"))
      .drop("totalSize", "done", "records", "attributes")
      )
```

---

### Q5. What is the difference between `explode()` and `explode_outer()`?

**Answer:**

| Function | Behaviour when array is NULL or empty |
|----------|---------------------------------------|
| `explode()` | Row is **dropped** entirely |
| `explode_outer()` | Row is **kept** with NULL values |

```python
from pyspark.sql.functions import explode, explode_outer

data = [
    ("S001", [{"id": 1}, {"id": 2}]),
    ("S002", []),          # empty array
    ("S003", None)         # null array
]

df = spark.createDataFrame(data, ["study_id", "records"])

# explode — drops S002 and S003
df.select("study_id", explode("records")).show()
# +--------+------+
# |study_id|   col|
# +--------+------+
# |    S001| {1}  |
# |    S001| {2}  |

# explode_outer — keeps all rows
df.select("study_id", explode_outer("records")).show()
# +--------+------+
# |study_id|   col|
# +--------+------+
# |    S001| {1}  |
# |    S001| {2}  |
# |    S002| null |   ← kept
# |    S003| null |   ← kept
```

**When to use each:**
- `explode()` — when you only care about rows with data (most common)
- `explode_outer()` — when missing/empty arrays are meaningful (e.g. you need to report "study S002 returned no records")

In the ALEX pipeline, `explode_outer()` was used for some optional nested fields to avoid silently losing records where those fields were null.

---

### Q6. The API returns a field `additional_details` as a nested JSON object. How do you handle it?

**Answer:**

In the ALEX pipeline, fields like `additional_details`, `raised_by_details` were nested objects:

```json
{
  "id": 1,
  "additional_details": {
    "category": "Minor",
    "notes": "Protocol deviation in visit 3",
    "severity": "Low"
  }
}
```

**Option 1 — Serialize to string (used in project):**

Before creating the DataFrame, convert the nested dict to a JSON string:
```python
for rec in page["data"]["records"]:
    rec["additional_details"] = json.dumps(rec.get("additional_details"))
```

Result: `additional_details` column becomes a string `'{"category":"Minor","notes":"..."}'`

Why: The `.schema` file defines it as `StringType`. This avoids schema conflicts when the nested structure varies between records.

**Option 2 — Keep as struct and access with dot notation:**
```python
# If schema defines additional_details as StructType
df.select(
    col("id"),
    col("additional_details.category"),
    col("additional_details.notes")
)
```

**Option 3 — Parse string back to struct later with `from_json()`:**
```python
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType

detail_schema = StructType([
    StructField("category", StringType()),
    StructField("notes", StringType()),
    StructField("severity", StringType())
])

df = df.withColumn(
    "additional_details_parsed",
    from_json(col("additional_details"), detail_schema)
)
df.select("additional_details_parsed.category").show()
```

Option 1 (stringify) is safest for raw layer ingestion — preserve the data exactly as received, parse in a downstream layer when schema is stable.

---

### Q7. What is `from_json()` and `to_json()` in PySpark? When would you use them?

**Answer:**

**`to_json()`** — converts a struct/map/array column into a JSON string:
```python
from pyspark.sql.functions import to_json, struct

df.withColumn(
    "address_json",
    to_json(struct("city", "state", "zip"))
).show()
# address_json = '{"city":"Chicago","state":"IL","zip":"60601"}'
```

Use case: storing a complex struct as a single string in a table that only supports flat types.

**`from_json()`** — parses a JSON string column into a struct:
```python
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("city", StringType()),
    StructField("state", StringType())
])

df.withColumn("address_struct", from_json(col("address_json"), schema)).show()
```

Use case: a raw table stored JSON as string → downstream transformation layer parses it into queryable columns.

**In your project:** `additional_details` was stored as string (using `json.dumps`) in raw layer. A downstream curated layer would use `from_json()` to parse it into typed columns.

---

### Q8. The API response has an `attributes` field in every Salesforce record. Why did you drop it?

**Answer:**

Salesforce adds a metadata `attributes` field to every record:
```json
{
  "attributes": {
    "type": "Account",
    "url": "/services/data/v58.0/sobjects/Account/001XX000003GYk1"
  },
  "Id": "001XX000003GYk1",
  "Name": "Acme Corp"
}
```

It contains the Salesforce object type and the record's API URL — useful for navigating the API, but not business data. It's dropped because:
- It's not part of the target table schema
- It wastes storage on every row
- It could cause schema conflicts if the URL format changes

```python
df.drop("attributes")
```

---

## 2. Real-World JSON & Data Handling Scenarios

---

### Scenario 1. The API sometimes returns `additional_details` as a dict, and sometimes as a string (inconsistent). Your job crashes. How do you fix it?

**Answer:**

This is a real production problem — APIs sometimes return inconsistent types for the same field.

**Detection:**
```python
# Check the actual type
for rec in records:
    ad = rec.get("additional_details")
    print(type(ad))  # <class 'dict'> or <class 'str'>
```

**Fix — normalize before DataFrame creation:**
```python
import json

def safe_json_string(value):
    if value is None:
        return None
    if isinstance(value, dict) or isinstance(value, list):
        return json.dumps(value)   # dict → string
    if isinstance(value, str):
        return value               # already a string
    return str(value)              # fallback

for rec in page["data"]["records"]:
    rec["additional_details"] = safe_json_string(rec.get("additional_details"))
```

**PySpark-side fix using `when()`:**
```python
from pyspark.sql.functions import when, col, to_json

df = df.withColumn(
    "additional_details",
    when(
        col("additional_details").startswith("{"),
        col("additional_details")            # already a JSON string
    ).otherwise(
        to_json(col("additional_details"))   # struct → JSON string
    )
)
```

---

### Scenario 2. You need to flatten a deeply nested JSON with 3 levels. How do you approach it?

**Answer:**

Example deeply nested response:
```json
{
  "study_id": "S001",
  "visits": [
    {
      "visit_id": "V1",
      "date": "2024-01-15",
      "assessments": [
        { "type": "Lab", "result": "Normal" },
        { "type": "ECG",  "result": "Abnormal" }
      ]
    }
  ]
}
```

**Step-by-step flattening:**

```python
from pyspark.sql.functions import explode, col

# Level 1 — explode visits array
df_visits = df.select(
    col("study_id"),
    explode(col("visits")).alias("visit")
)
# Columns: study_id, visit (struct: visit_id, date, assessments[])

# Level 2 — explode assessments array inside visit
df_assessments = df_visits.select(
    col("study_id"),
    col("visit.visit_id"),
    col("visit.date"),
    explode(col("visit.assessments")).alias("assessment")
)
# Columns: study_id, visit_id, date, assessment (struct: type, result)

# Level 3 — flatten the final struct
df_flat = df_assessments.select(
    col("study_id"),
    col("visit_id"),
    col("date"),
    col("assessment.type").alias("assessment_type"),
    col("assessment.result").alias("assessment_result")
)

df_flat.show()
# +--------+--------+-----------+-----------------+------------------+
# |study_id|visit_id|       date|  assessment_type|assessment_result |
# +--------+--------+-----------+-----------------+------------------+
# |    S001|      V1| 2024-01-15|              Lab|            Normal|
# |    S001|      V1| 2024-01-15|              ECG|          Abnormal|
```

Rule of thumb: one `explode()` per array level. Never chain two `explode()` calls in the same `select()` — that creates a cartesian product.

---

### Scenario 3. You got 10,000 records from the API but after explode your DataFrame shows only 9,800. What happened?

**Answer:**

Most likely cause: some records had a **null or empty array** in the field you exploded, and `explode()` (not `explode_outer()`) was used — dropping those rows.

**Investigation:**
```python
# Check how many records had null/empty arrays before explode
from pyspark.sql.functions import size, col

df.withColumn("record_count", size(col("data.records"))) \
  .groupBy("record_count") \
  .count() \
  .show()
# record_count | count
#            0 |   150   ← these 150 pages had empty arrays
#           50 |   194
```

**Fix:**
```python
# Option 1: Use explode_outer to keep all rows
df_exploded = df.select(
    col("api_param"),
    explode_outer(col("data.records")).alias("record")
)

# Option 2: Filter empty arrays before exploding (if you don't need them)
df_filtered = df.filter(size(col("data.records")) > 0)
df_exploded = df_filtered.select(explode(col("data.records")).alias("record"))
```

Also check: were there duplicate `api_param` values? A join before explode can multiply rows unexpectedly.

---

### Scenario 4. Your nested JSON has 50 fields inside `records`. You don't want to write all column names manually. How do you select all of them?

**Answer:**

Use `select("record.*")` — the wildcard flattens all fields of a struct into individual columns:

```python
# After explode, 'record' is a struct column
df_flat = df_exploded.select("record.*")
# All 50 fields of record become individual columns automatically
```

For programmatic column inspection:
```python
# List all fields of a struct column
from pyspark.sql.types import StructType

record_schema = df_exploded.schema["record"].dataType
field_names = [f.name for f in record_schema.fields]
print(field_names)
# ['id', 'study_id', 'site_id', 'subject_id', 'additional_details', ...]
```

To keep only specific fields while using wildcard:
```python
# Keep all record fields but also bring in the parent key
df_flat = df_exploded.select(
    col("api_param").alias("study_id"),
    col("record.*")   # all 50 fields from record
)
```

---

### Scenario 5. The API returns a JSON array at the top level, not a JSON object. How do you read it?

**Answer:**

Most APIs wrap records in an object. But some return a bare array:

```json
[
  {"id": "001", "name": "Acme"},
  {"id": "002", "name": "Beta"},
  {"id": "003", "name": "Gamma"}
]
```

`spark.read.json()` expects one JSON object per line by default. For a top-level array:

**Option 1 — `multiLine` option (for files):**
```python
df = spark.read.option("multiLine", "true").json("s3://bucket/response.json")
# Spark auto-detects the array and creates one row per element
```

**Option 2 — Python-side conversion (for API response in memory):**
```python
import json

response = requests.get(url, headers=headers).json()
# response is a list: [{"id":"001",...}, {"id":"002",...}]

# Parallelize each element as its own JSON string
rdd = spark.sparkContext.parallelize([json.dumps(rec) for rec in response])
df = spark.read.json(rdd, schema=your_schema)
# One row per record — no explode needed
```

**Option 3 — Wrap in an object then explode:**
```python
wrapped = {"records": response}
rdd = spark.sparkContext.parallelize([json.dumps(wrapped)])
df = spark.read.json(rdd).withColumn("rec", explode("records")).select("rec.*")
```

---

### Scenario 6. After reading nested JSON, you see column names like `data.records.id` in the schema. How do you rename and flatten them cleanly?

**Answer:**

PySpark represents nested columns with dot notation in the schema. To flatten:

```python
from pyspark.sql.functions import col

# Method 1: Explicit select with aliases
df_flat = df.select(
    col("data.records.id").alias("id"),
    col("data.records.name").alias("name"),
    col("data.records.study_id").alias("study_id")
)

# Method 2: Select star from struct (after explode)
df_exploded = df.withColumn("rec", explode("data.records"))
df_flat = df_exploded.select("rec.*")   # all nested fields flattened

# Method 3: Recursive flattening function (handles any depth)
def flatten_df(df):
    from pyspark.sql.types import StructType
    
    complex_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
    
    while complex_cols:
        for c in complex_cols:
            nested_fields = [
                col(f"{c}.{field.name}").alias(f"{c}_{field.name}")
                for field in df.schema[c].dataType.fields
            ]
            df = df.select(
                [col(x) for x in df.columns if x != c] + nested_fields
            )
        complex_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
    return df

df_fully_flat = flatten_df(df)
# address_city, address_state, address_zip instead of address.city etc.
```

---

### Scenario 7. The API returns the same record twice (duplicates in response). How do you deduplicate?

**Answer:**

APIs can return duplicates for several reasons:
- Pagination overlap (record updated mid-pagination shifts the offset window)
- API bug
- Same record appears under different parent objects

**Detection:**
```python
from pyspark.sql.functions import count, col

df.groupBy("id").agg(count("*").alias("cnt")) \
  .filter(col("cnt") > 1) \
  .show()
```

**Deduplication:**
```python
# Simple: keep first occurrence per primary key
df_deduped = df.dropDuplicates(["id"])

# Better: keep most recently modified record
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window = Window.partitionBy("id").orderBy(desc("SystemModstamp"))
df_deduped = (df
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)
```

The window function approach is preferred when duplicates have different timestamps — you explicitly keep the latest version, not just a random one.

---

### Scenario 8. How do you convert a MapType column (key-value pairs from JSON) into individual columns?

**Answer:**

Some APIs return dynamic key-value fields as a JSON object with unpredictable keys:

```json
{ "id": "001", "metadata": { "region": "US", "tier": "Gold", "score": "92" } }
```

In PySpark this becomes a `MapType(StringType, StringType)` column:

```python
from pyspark.sql.functions import map_keys, map_values, col, explode

# Option 1: Access a known key
df.withColumn("region", col("metadata")["region"]).show()

# Option 2: Explode map into key-value rows
df_expanded = df.select(
    col("id"),
    explode(col("metadata")).alias("key", "value")
)
# id    | key    | value
# 001   | region | US
# 001   | tier   | Gold
# 001   | score  | 92

# Option 3: Pivot key-value rows into columns (if keys are known)
df_pivoted = df_expanded.groupBy("id").pivot("key").agg(first("value"))
# id  | region | score | tier
# 001 | US     | 92    | Gold
```

In your pipeline, this pattern would apply to fields like `additional_details` once they're parsed from string using `from_json()` with a `MapType` schema.

---

### Scenario 9. You need to write a PySpark function that reads a JSON API response, explodes nested records, and returns a clean flat DataFrame. Write it.

**Answer:**

```python
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, explode_outer, to_timestamp, to_date
from pyspark.sql.types import StructType
from typing import List

def api_response_to_flat_df(
    spark: SparkSession,
    api_response: dict,
    schema: StructType,
    records_path: str = "records",       # dot-separated path: "data.records"
    timestamp_cols: List[str] = [],
    date_cols: List[str] = [],
    drop_cols: List[str] = ["attributes", "totalSize", "done"]
) -> DataFrame:
    """
    Converts a nested API JSON response dict into a flat PySpark DataFrame.

    Args:
        spark: SparkSession
        api_response: raw API response as Python dict
        schema: StructType schema for the response
        records_path: dot-separated path to the records array (e.g. "data.records")
        timestamp_cols: columns to cast to TimestampType
        date_cols: columns to cast to DateType
        drop_cols: columns to drop from final DataFrame

    Returns:
        Flat PySpark DataFrame, one row per record
    """
    # Step 1: Serialize and parallelize
    rdd = spark.sparkContext.parallelize([json.dumps(api_response)])

    # Step 2: Read JSON with schema
    df = spark.read.json(rdd, schema=schema)

    # Step 3: Explode nested records array
    df = df.withColumn("_record", explode_outer(col(records_path)))

    # Step 4: Flatten struct
    df = df.select("_record.*")

    # Step 5: Drop unwanted columns
    existing_drop = [c for c in drop_cols if c in df.columns]
    df = df.drop(*existing_drop)

    # Step 6: Cast timestamps
    for tc in timestamp_cols:
        if tc in df.columns:
            df = df.withColumn(tc, to_timestamp(col(tc), "yyyy-MM-dd'T'HH:mm:ss.SSS"))

    # Step 7: Cast dates
    for dc in date_cols:
        if dc in df.columns:
            df = df.withColumn(dc, to_date(col(dc), "yyyy-MM-dd"))

    return df


# Usage — Salesforce response
sf_response = {
    "totalSize": 2, "done": True,
    "records": [
        {"attributes": {"type": "Account"}, "Id": "001", "Name": "Acme",
         "SystemModstamp": "2024-05-01T10:30:00.000+0000", "CloseDate": "2024-06-30"},
        {"attributes": {"type": "Account"}, "Id": "002", "Name": "Beta",
         "SystemModstamp": "2024-05-02T08:00:00.000+0000", "CloseDate": "2024-07-15"}
    ]
}

df = api_response_to_flat_df(
    spark=spark,
    api_response=sf_response,
    schema=your_schema,
    records_path="records",
    timestamp_cols=["SystemModstamp"],
    date_cols=["CloseDate"],
    drop_cols=["attributes", "totalSize", "done"]
)
df.show()
# +----+----+-----------------------+-----------+
# | Id |Name|        SystemModstamp |  CloseDate|
# +----+----+-----------------------+-----------+
# | 001|Acme|2024-05-01 10:30:00.000| 2024-06-30|
# | 002|Beta|2024-05-02 08:00:00.000| 2024-07-15|
```

---

### Scenario 10. The API returns paginated responses. You need to collect ALL pages and return a single unified DataFrame. Write it.

**Answer:**

```python
import json
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from typing import Optional
import time

def fetch_all_pages_to_df(
    spark: SparkSession,
    base_url: str,
    headers: dict,
    schema: StructType,
    records_key: str = "records",
    next_page_key: str = "nextRecordsUrl",
    per_page: int = 50,
    rate_delay: float = 1.0,
    max_pages: int = 500   # safety limit
) -> Optional[DataFrame]:
    """
    Fetches all paginated API pages sequentially and returns unified DataFrame.
    """
    all_records = []
    page = 1
    url = base_url

    while url and page <= max_pages:
        try:
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code == 200:
                data = resp.json()
                records = data.get(records_key, [])

                if not records:
                    print(f"Page {page}: No records. Stopping.")
                    break

                all_records.extend(records)
                print(f"Page {page}: fetched {len(records)} records. Total so far: {len(all_records)}")

                # Check for next page
                next_url = data.get(next_page_key)
                url = f"{base_url_root}{next_url}" if next_url else None
                page += 1

                if rate_delay > 0:
                    time.sleep(rate_delay)

            elif resp.status_code == 401:
                print("Token expired — stopping pagination")
                break
            else:
                print(f"Page {page}: Error {resp.status_code}. Stopping.")
                break

        except Exception as e:
            print(f"Page {page}: Exception {e}. Stopping.")
            break

    if not all_records:
        return None

    # Build DataFrame from all collected records
    rdd = spark.sparkContext.parallelize(
        [json.dumps({"records": all_records})]
    )
    df = (spark.read.json(rdd, schema=schema)
          .withColumn("rec", explode("records"))
          .select("rec.*")
          )

    print(f"Total records fetched: {len(all_records)}")
    return df
```

---

### Scenario 11. How do you handle a JSON field that is sometimes an object `{}` and sometimes an array `[]`?

**Answer:**

This is a type inconsistency problem — real APIs do this. Example:

```json
// Record 1: contacts is an object
{"id": "001", "contacts": {"name": "Alice"}}

// Record 2: contacts is an array
{"id": "002", "contacts": [{"name": "Bob"}, {"name": "Carol"}]}
```

PySpark will fail schema enforcement or silently nullify one of them.

**Fix — normalize on the Python side before creating DataFrame:**
```python
def normalize_contacts(value):
    if value is None:
        return []
    if isinstance(value, dict):
        return [value]         # wrap single object in a list
    if isinstance(value, list):
        return value           # already a list
    return []

for rec in records:
    rec["contacts"] = normalize_contacts(rec.get("contacts"))
```

Now `contacts` is always a list — `explode()` works consistently.

**Alternative — store as JSON string:**
```python
for rec in records:
    rec["contacts"] = json.dumps(rec.get("contacts"))
```

Parse and normalize in a downstream layer after the raw ingestion is complete.

---

### Scenario 12. Write PySpark code to read a JSON file from S3, handle multi-line JSON, and explode a nested array.

**Answer:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, to_timestamp

spark = SparkSession.builder.appName("JsonIngestion").getOrCreate()

# Read multi-line JSON from S3
# multiLine=true: treats the entire file as one JSON object (not line-by-line)
df_raw = (spark.read
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")       # don't fail on malformed records
    .option("columnNameOfCorruptRecord", "_corrupt_record")  # capture bad rows
    .json("s3://my-bucket/api-responses/2024-06-01/*.json")
)

df_raw.printSchema()
# root
#  |-- study_id: string
#  |-- data: struct
#  |    |-- pagination: struct
#  |    |-- records: array
#  |         |-- element: struct
#  |              |-- id: long
#  |              |-- name: string
#  |              |-- created_at: string

# Log corrupted records before dropping them
corrupt = df_raw.filter(col("_corrupt_record").isNotNull())
print(f"Corrupt records: {corrupt.count()}")
corrupt.show(truncate=False)

# Process clean records
df_clean = df_raw.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

# Explode nested records array
df_exploded = df_clean.select(
    col("study_id"),
    explode_outer(col("data.records")).alias("record")
)

# Flatten struct + cast types
df_final = (df_exploded
    .select(
        col("study_id"),
        col("record.*")
    )
    .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
)

df_final.printSchema()
df_final.show(5, truncate=False)
```

Key options explained:
- `multiLine=true` — for JSON files where a single object spans multiple lines
- `mode=PERMISSIVE` — don't crash on malformed rows; put them in `_corrupt_record`
- `columnNameOfCorruptRecord` — captures the raw bad JSON string for debugging

---

*Prepared for Data Engineering interviews — 3.5 years experience level*  
*Topics: JSON response formats, nested JSON, explode patterns, real-world scenarios*
