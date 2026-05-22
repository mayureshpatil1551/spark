# 🎯 Interview Prep — IT Applications Engineer II - Data (IC2)
> **Interview Tomorrow | Tailored for Mayuresh Patil**
> **Focus: Python · SQL · ADF · Databricks · Delta Lake · CI/CD · Agile**

---

## 📋 What This Role Is Looking For

Based on the JD, they want someone who can:
- Write and debug **Python** for data manipulation
- Work with **SQL** for extraction, transformation, analysis
- Use **Azure Data Factory (ADF)** for orchestration
- Work with **Databricks, Delta Lake, Lakehouse**
- Follow **CI/CD, Agile, Git** practices
- **Communicate progress**, escalate issues, collaborate cross-functionally
- Support **testing (unit, CIT, UAT)** and **documentation**

**Your Resume Match:** Strong. You have direct experience in every preferred area.

---

## Table of Contents
1. [Introduction & Behavioral](#1-introduction--behavioral)
2. [Python](#2-python)
3. [SQL](#3-sql)
4. [Azure Data Factory (ADF)](#4-azure-data-factory-adf)
5. [Databricks & Delta Lake](#5-databricks--delta-lake)
6. [CI/CD, Agile & Git](#6-cicd-agile--git)
7. [Data Engineering Concepts](#7-data-engineering-concepts)
8. [Testing & Documentation](#8-testing--documentation)
9. [Real-World Scenario Questions](#9-real-world-scenario-questions)
10. [Questions to Ask the Interviewer](#10-questions-to-ask-the-interviewer)

---

# 1. INTRODUCTION & BEHAVIORAL

---

**Q: Tell me about yourself.**

> *"I'm Mayuresh Patil, a Data Engineer at Cognizant working on the AbbVie pharmaceutical data platform. Over the past 2+ years, I've built PySpark pipelines, designed Medallion Architecture on Apache Iceberg and AWS S3, and integrated data from Oracle, Salesforce, Smartsheet, and Snowflake. I've worked across the full data lifecycle — ingestion, transformation, DQ checks, orchestration via ADF, and deployment using CI/CD on Azure DevOps. I recently led a 20TB legacy data migration and reduced pipeline execution time by 30% through Spark and query optimization. This role excites me because it aligns with my ADF, Databricks, and Python experience — and I'm looking for a team where I can grow technically while contributing to real-world data solutions."*

---

**Q: Why are you interested in this role?**

> *"This role directly matches what I've been doing at AbbVie — Python for data processing, ADF for orchestration, Databricks and Delta Lake for transformation and storage. I'm especially excited about the DevOps and Agile focus because I've worked in Azure DevOps CI/CD pipelines and Agile sprints with Jira. I want to be in a role where I can both deliver solid technical work and grow into more senior responsibilities over time."*

---

**Q: Describe a time you solved a business problem using data engineering.**

> *"At AbbVie, the analytics team was getting duplicate patient records from three different source systems — Oracle, Salesforce, and Snowflake. The same patient had three different IDs across systems, making reports unreliable.*
>
> *I built a cross-reference (XREF) mapping system in PySpark that linked all source IDs to a single master ID. Then I applied survivorship rules to create a Golden Record — one trusted record per patient. I delivered this as a pipeline integrated into our Medallion Architecture.*
>
> *Result: Analytics teams now work from a single trusted source. Duplicate-related support tickets dropped to zero."*

---

**Q: Tell me about a time you missed a deadline or made a mistake. What did you learn?**

> *"Early in the AbbVie project, I underestimated the time needed to handle schema drift from the Salesforce API — new fields appeared mid-sprint that broke our ingestion pipeline in production.*
>
> *I learned to always build schema-resilient pipelines using `mergeSchema=true` and to add schema drift detection alerts. I also started over-communicating risks to the team during sprint planning — not waiting until a blocker happens."*

---

**Q: How do you handle working cross-functionally with non-technical stakeholders?**

> *"I translate technical issues into business impact. If a pipeline is delayed, I don't just say 'there's a Spark shuffle issue' — I say 'the daily report will be ready 2 hours late due to a data processing bottleneck, and here's what I'm doing to fix it and prevent it.'*
>
> *I document issues, root causes, and resolutions clearly in Confluence so business teams have full visibility."*

---

# 2. PYTHON

---

**Q: How do you use Python for data manipulation and processing?**

> *"Python is my primary language for data engineering. I use PySpark for large-scale distributed data processing — reading from Oracle JDBC, transforming records, applying DQ checks, and writing to Iceberg on S3. For utility scripts, I use pandas for small datasets, boto3 for S3 operations, and requests for REST API integration.*
>
> *At AbbVie, I also built Python scripts that dynamically generated 300+ SQL configuration files using Jinja2 templating — eliminating 15+ hours/week of manual setup."*

---

**Q: Write Python code to read a CSV, filter rows, and write output.**

```python
import pandas as pd

# Read CSV
df = pd.read_csv("s3://bucket/data/patients.csv")

# Filter rows where age > 18 and status is active
df_filtered = df[(df["age"] > 18) & (df["status"] == "active")]

# Drop duplicates on patient_id
df_clean = df_filtered.drop_duplicates(subset=["patient_id"])

# Write output
df_clean.to_csv("s3://bucket/output/active_patients.csv", index=False)
print(f"Records written: {len(df_clean)}")
```

---

**Q: How do you handle exceptions in a Python pipeline?**

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_data(source_path, target_path):
    try:
        df = spark.read.csv(source_path, header=True, inferSchema=True)
        
        if df.count() == 0:
            logger.warning(f"Empty file at {source_path} — skipping.")
            return
        
        df.write.mode("overwrite").parquet(target_path)
        logger.info(f"Successfully written {df.count()} records to {target_path}")
        
    except FileNotFoundError as e:
        logger.error(f"Source file not found: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in ingest_data: {e}")
        raise
```

> *"I always separate exception types — handle known issues like missing files differently from unknown errors. I also use logging rather than print statements in production so logs are captured in Databricks job logs and ADF monitor."*

---

**Q: What is a Python decorator? Have you used one?**

> *"A decorator is a function that wraps another function to add behavior without modifying it. Common use cases are logging, timing, and retry logic.*

```python
import time
import functools

def retry(max_attempts=3, delay=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    print(f"Attempt {attempt+1} failed: {e}. Retrying...")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry(max_attempts=3, delay=5)
def call_salesforce_api(endpoint):
    # API call that might fail transiently
    response = requests.get(endpoint, headers=headers)
    response.raise_for_status()
    return response.json()
```

> *"I used this pattern in AbbVie for Salesforce and Veeva API calls — they occasionally hit rate limits and needed automatic retry with backoff."*

---

**Q: What is the difference between a list, tuple, set, and dictionary?**

| Type | Mutable | Ordered | Duplicates | Use Case |
|---|---|---|---|---|
| List | ✅ Yes | ✅ Yes | ✅ Yes | General sequence of items |
| Tuple | ❌ No | ✅ Yes | ✅ Yes | Fixed data, function returns |
| Set | ✅ Yes | ❌ No | ❌ No | Unique values, membership check |
| Dict | ✅ Yes | ✅ (Python 3.7+) | Keys: No | Key-value lookup |

> *"In my pipelines, I use dicts for config parameters, sets for checking unique values efficiently, and lists for sequences I need to iterate or append to."*

---

**Q: How do you read config files in Python for a metadata-driven pipeline?**

```python
import yaml
import json

# Read YAML config
with open("pipeline_config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Access config values
source_tables = config["tables"]
target_path   = config["iceberg"]["target_path"]
load_type     = config["load_type"]  # "full" or "incremental"

# Example config structure:
# tables:
#   - name: PATIENT_RECORDS
#     primary_key: patient_id
#     partition_col: record_date
#     load_type: incremental
```

> *"This is the core of our metadata-driven framework at AbbVie — one config file drives ingestion for 500+ tables without hardcoded logic."*

---

**Q: How do you use boto3 to interact with S3?**

```python
import boto3

s3 = boto3.client("s3",
    aws_access_key_id     = dbutils.secrets.get("scope", "aws-key"),
    aws_secret_access_key = dbutils.secrets.get("scope", "aws-secret")
)

# List files in a bucket prefix
response = s3.list_objects_v2(Bucket="abbvie-data", Prefix="raw/patients/")
files = [obj["Key"] for obj in response.get("Contents", [])]

# Check if file exists
def file_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

# Copy file
s3.copy_object(
    CopySource={"Bucket": "source-bucket", "Key": "raw/file.csv"},
    Bucket="target-bucket",
    Key="processed/file.csv"
)
```

---

# 3. SQL

---

**Q: Write SQL to find the top 3 customers by total purchase amount per region.**

```sql
SELECT region, customer_id, customer_name, total_purchase, region_rank
FROM (
    SELECT
        c.region,
        c.customer_id,
        c.customer_name,
        SUM(o.purchase_amount) AS total_purchase,
        RANK() OVER (
            PARTITION BY c.region
            ORDER BY SUM(o.purchase_amount) DESC
        ) AS region_rank
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.region, c.customer_id, c.customer_name
) ranked
WHERE region_rank <= 3
ORDER BY region, region_rank;
```

---

**Q: What is the difference between WHERE and HAVING?**

> *"WHERE filters rows BEFORE aggregation — it cannot use aggregate functions. HAVING filters AFTER aggregation — it works on grouped results.*

```sql
-- WHERE: filter before grouping
SELECT region, SUM(revenue) AS total
FROM sales
WHERE status = 'completed'      -- Filter individual rows first
GROUP BY region;

-- HAVING: filter after grouping
SELECT region, SUM(revenue) AS total
FROM sales
GROUP BY region
HAVING SUM(revenue) > 100000;   -- Filter aggregated results
```

---

**Q: What are window functions? Write an example.**

```sql
-- Running total of revenue per customer, ordered by date
SELECT
    customer_id,
    order_date,
    revenue,
    SUM(revenue)  OVER (PARTITION BY customer_id ORDER BY order_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    LAG(revenue, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_revenue,
    RANK()        OVER (PARTITION BY region ORDER BY revenue DESC) AS revenue_rank
FROM orders;
```

> *"I use window functions heavily in my pipelines — especially `ROW_NUMBER()` for deduplication and `LAG/LEAD` for change detection."*

---

**Q: How would you find duplicate records in a table?**

```sql
-- Method 1: Find duplicates by count
SELECT customer_id, COUNT(*) AS cnt
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Method 2: Get all duplicate rows with ROW_NUMBER
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
    FROM customers
) t
WHERE rn > 1;

-- Method 3: Delete duplicates, keep latest
DELETE FROM customers
WHERE id NOT IN (
    SELECT MAX(id)
    FROM customers
    GROUP BY customer_id
);
```

---

**Q: Write SQL for incremental data extraction — only new records since last run.**

```sql
-- Get records updated after the last pipeline run watermark
SELECT *
FROM source_table
WHERE updated_at > (
    SELECT MAX(last_run_timestamp)
    FROM pipeline_audit_log
    WHERE pipeline_name = 'patient_ingestion'
    AND status = 'SUCCESS'
)
ORDER BY updated_at;
```

---

**Q: What is a JOIN? Explain types with examples.**

```sql
-- INNER JOIN: only matching rows from both tables
SELECT c.name, o.order_id
FROM customers c INNER JOIN orders o ON c.id = o.customer_id;

-- LEFT JOIN: all rows from left, matching from right (NULL if no match)
SELECT c.name, o.order_id
FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;

-- LEFT ANTI JOIN (NOT IN pattern): rows in left NOT in right
SELECT c.name
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.customer_id IS NULL;  -- Customers with NO orders

-- FULL OUTER JOIN: all rows from both, NULLs where no match
SELECT c.name, o.order_id
FROM customers c FULL OUTER JOIN orders o ON c.id = o.customer_id;
```

---

**Q: Optimize this slow query:**

```sql
-- SLOW version
SELECT *
FROM orders o
WHERE customer_id IN (
    SELECT customer_id FROM customers WHERE region = 'APAC'
);

-- FAST version — replace subquery with JOIN
SELECT o.*
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
WHERE c.region = 'APAC';
```

> *"Subqueries in IN() run once per row — O(n²). JOIN with filter is O(n) — always convert when possible."*

---

# 4. AZURE DATA FACTORY (ADF)

---

**Q: What is ADF? How have you used it?**

> *"Azure Data Factory is Azure's cloud ETL and orchestration service. It lets you build data pipelines visually or with JSON — connecting sources, applying transformations, and loading targets.*
>
> *At AbbVie, I use ADF to: trigger Databricks notebooks when files arrive on S3 (event-based trigger), schedule daily Oracle JDBC ingestion jobs (schedule trigger), and orchestrate multi-step pipeline runs with dependency management between activities.*
>
> *ADF handles the 'when and what' — Databricks handles the 'how' — the actual PySpark computation."*

---

**Q: What are the trigger types in ADF?**

| Trigger Type | When It Fires | My Use Case |
|---|---|---|
| **Schedule** | Fixed cron time | Daily Oracle load at 2 AM |
| **Event-based** | File arrives/deleted in ADLS/S3 | CSV/XML file drop triggers pipeline |
| **Tumbling Window** | Fixed non-overlapping time windows | Hourly API ingestion with backfill support |
| **Manual** | On-demand | Emergency re-run or backfill |

---

**Q: What are Linked Services and Datasets in ADF?**

> *"A **Linked Service** is a connection definition — like a JDBC connection string to Oracle, or an S3 access key. It's the 'how to connect' definition.*
>
> *A **Dataset** is a reference to data within a linked service — like 'the PATIENT_RECORDS table in the Oracle linked service' or 'the raw/patients/ folder in the S3 linked service'.*
>
> *Activities in a pipeline use datasets as input/output — the pipeline logic is decoupled from connection details."*

---

**Q: ADF pipeline is failing — how do you debug it?**

> *"Step-by-step:*
>
> *1. Open **ADF Monitor** → find the failed run → check the red error icon*
> *2. Click the failed **activity** — the error message tells you exactly what failed (JDBC timeout, file not found, schema mismatch, etc.)*
> *3. If it's a Databricks notebook failure — open the notebook's output log or Databricks job run details*
> *4. Common root causes I've resolved:*
>   - *Oracle JDBC timeout → increase `queryTimeout` in the linked service*
>   - *File not found → source file delayed; switch trigger from schedule to event-based*
>   - *Schema mismatch → source added a new column → add `mergeSchema=true`*
>   - *Permission error → ADLS Gen2 access control → fix RBAC or SAS token*
>
> *5. After fixing — use ADF's **re-run from failed activity** to avoid full pipeline re-execution."*

---

**Q: How does ADF handle incremental loads?**

> *"Two approaches I've used:*
>
> *1. **Watermark pattern** — ADF pipeline reads the last successful run timestamp from a lookup activity (querying an audit table), passes it as a parameter to the Databricks notebook, which uses it to query only new/changed records.*
>
> *2. **Event-based trigger** — trigger fires only when a new file arrives in ADLS. The pipeline processes only that file — inherently incremental.*
>
> *For Oracle, I use the watermark approach — the JDBC query has a `WHERE updated_at > @last_run_ts` parameter injected by ADF."*

---

**Q: Difference between Copy Activity and Databricks Notebook Activity in ADF?**

| | Copy Activity | Databricks Notebook Activity |
|---|---|---|
| Purpose | Move data between systems | Run PySpark transformation logic |
| Transformation | Basic (mapping, type conversion) | Full PySpark — any complexity |
| Compute | ADF's integration runtime | Databricks cluster |
| Best for | Simple file copies, table-to-table | Complex ETL, ML, data engineering |

> *"I use Copy Activity only for simple file moves — like S3 to ADLS. For any real transformation, I trigger a Databricks Notebook Activity."*

---

# 5. DATABRICKS & DELTA LAKE

---

**Q: What is Databricks and how have you used it?**

> *"Databricks is a unified analytics platform built on Apache Spark. It provides managed Spark clusters, collaborative notebooks, job scheduling, MLflow for ML, and Unity Catalog for governance.*
>
> *At AbbVie, I use Databricks for all PySpark transformation work — reading from Iceberg on S3, applying Medallion Architecture transformations, writing back to Iceberg, and running optimization jobs. I use Databricks Workflows for pipeline orchestration and Unity Catalog for access control on sensitive patient data."*

---

**Q: What is Delta Lake? What makes it different from plain Parquet?**

> *"Delta Lake adds a `_delta_log` transaction log on top of Parquet files. This gives you:*
> - *ACID transactions — writes are atomic, all-or-nothing*
> - *Time travel — query any historical version*
> - *Schema enforcement — bad schemas are rejected*
> - *DML support — UPDATE, DELETE, MERGE work correctly*
>
> *Plain Parquet has none of these — it's just a file format with no transactional guarantees."*

---

**Q: How do you MERGE data into a Delta table? (UPSERT)**

```python
from delta.tables import DeltaTable
import pyspark.sql.functions as F

# Target Delta table
delta_table = DeltaTable.forPath(spark, "abfss://container@storage.dfs.core.windows.net/delta/patients/")

# Source: incoming batch
df_source = spark.read.parquet("s3://staging/patients_daily/")

# MERGE: update if exists, insert if new
delta_table.alias("target").merge(
    df_source.alias("source"),
    "target.patient_id = source.patient_id"
).whenMatchedUpdate(
    condition="source.updated_at > target.updated_at",
    set={
        "target.name":       "source.name",
        "target.email":      "source.email",
        "target.updated_at": "source.updated_at"
    }
).whenNotMatchedInsertAll().execute()
```

> *"MERGE is how I handle CDC and incremental loads — it's idempotent, so re-running after a failure doesn't create duplicates."*

---

**Q: What is the Medallion Architecture?**

```
RAW (Bronze)     → Ingest as-is from source. No transformation. Full history.
TRUSTED (Silver) → Clean, validate, deduplicate. DQ checks. Quarantine bad records.
CURATED (Gold)   → Business-level aggregations. Optimized for analytics consumption.
```

> *"At AbbVie, I refactored legacy SQL scripts into this three-layer pattern. It gave us clear data lineage, easier debugging when something goes wrong, and a clean separation between raw source data and business-ready data."*

---

**Q: What is Z-Ordering and when would you use it?**

> *"Z-Ordering is a multi-dimensional data clustering technique in Delta Lake and Iceberg. It co-locates related data within Parquet files so that queries filtering on those columns can skip more files using min/max statistics.*
>
> *Use it for high-cardinality columns that you frequently filter on but can't partition by — like `customer_id` or `patient_id`. These have too many distinct values for partitioning, but Z-ordering still enables effective data skipping.*

```sql
OPTIMIZE schema.patient_records
ZORDER BY (patient_id, facility_id);
```

---

**Q: What is Unity Catalog?**

> *"Unity Catalog is Databricks' centralized governance layer. It manages data permissions, lineage, and discovery across all workspaces.*
>
> *Key features: fine-grained access control (row and column level), automatic data lineage tracking, centralized audit logs.*
>
> *At AbbVie, I used it to restrict PHI column access — only the clinical team could see patient PII. Everyone else got masked values automatically through column masking policies."*

---

# 6. CI/CD, AGILE & GIT

---

**Q: What is CI/CD? How have you implemented it?**

> *"CI/CD — Continuous Integration and Continuous Deployment — automates testing and deployment of code changes.*
>
> *My implementation on Azure DevOps:*
> - *Developers work on feature branches*
> - *On PR to main: Azure DevOps pipeline runs automated pytest unit tests*
> - *On merge to main: release pipeline deploys Databricks notebooks and updates job configs*
>
```yaml
# azure-pipelines.yml
- script: pip install pytest pyspark
- script: pytest tests/ -v
- script: |
    databricks workspace import_dir ./notebooks /Shared/prod --overwrite
    databricks jobs reset --job-id $(JOB_ID) --json-file job_config.json
```
>
> *Secrets are stored in Azure DevOps Variable Groups linked to Key Vault — never in YAML."*

---

**Q: What Git branching strategy do you follow?**

```
main        → Production (protected, requires PR + review)
  └── dev   → Integration branch (all features merge here first)
        └── feature/oracle-incremental-load   (your work branch)
        └── feature/salesforce-api-pagination
        └── hotfix/jdbc-timeout-fix
```

> *"I follow GitFlow. I never commit directly to main. Every change goes through a PR with code review. Hotfixes get their own branch and are merged to both main and dev."*

---

**Q: What is Agile? How do you work in sprints?**

> *"Agile is an iterative development methodology — work is broken into 2-week sprints, each delivering working software.*
>
> *My workflow at AbbVie:*
> - *Sprint Planning: pick user stories from backlog, break into tasks in Jira*
> - *Daily standups: share progress, blockers, next steps*
> - *Sprint Review: demo completed work to stakeholders*
> - *Retrospective: what went well, what to improve*
>
> *I create Jira subtasks for each pipeline component — ingestion, transformation, DQ checks, testing, deployment — so progress is visible to the whole team."*

---

**Q: What is the difference between Waterfall and Agile?**

| | Waterfall | Agile |
|---|---|---|
| Delivery | One big release at end | Incremental per sprint |
| Requirements | Fixed upfront | Evolve through sprints |
| Feedback | After delivery | Every sprint |
| Risk | High (late discovery) | Low (early feedback) |
| Best for | Fixed-scope projects | Evolving requirements |

> *"At AbbVie, we follow Agile — requirements from AbbVie's data team evolve frequently. Sprints let us adapt quickly without a full project restart."*

---

**Q: How do you write unit tests for PySpark code?**

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from my_pipeline import apply_dq_checks, clean_patient_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("unit_tests") \
        .getOrCreate()

def test_null_filter(spark):
    """Test that records with null patient_id are removed."""
    data = [(1, "Alice"), (None, "Bob"), (3, "Charlie")]
    df = spark.createDataFrame(data, ["patient_id", "name"])
    
    result = apply_dq_checks(df)
    
    assert result.count() == 2
    assert result.filter(F.col("patient_id").isNull()).count() == 0

def test_deduplication(spark):
    """Test that duplicates on patient_id are removed."""
    data = [(1, "Alice", "2024-01-01"), (1, "Alice", "2024-01-02"), (2, "Bob", "2024-01-01")]
    df = spark.createDataFrame(data, ["patient_id", "name", "updated_at"])
    
    result = clean_patient_data(df)
    
    assert result.count() == 2  # Only unique patient_ids
```

---

# 7. DATA ENGINEERING CONCEPTS

---

**Q: What is ETL vs ELT?**

> *"ETL (Extract, Transform, Load) — data is transformed before loading into the target. Traditional pattern for data warehouses.*
>
> *ELT (Extract, Load, Transform) — data is loaded raw first, then transformed inside the target system. Modern pattern for data lakes and Lakehouse — load raw to Bronze, transform to Silver and Gold.*
>
> *At AbbVie, I follow ELT — I load everything raw to Iceberg first (Bronze), then transform in place using PySpark and write to Silver and Gold layers. This preserves raw data for reprocessing if transformation logic changes."*

---

**Q: What is idempotency in data pipelines?**

> *"An idempotent pipeline produces the same result whether run once or multiple times. It's safe to re-run after failures.*
>
> *I achieve this using MERGE INTO instead of INSERT — so re-running updates existing records instead of duplicating them. For partition overwrites, I use `replaceWhere` on specific partition columns instead of overwriting the whole table.*
>
> *This is critical in production — ADF retries failed pipelines automatically, so idempotency prevents data corruption on retries."*

---

**Q: How do you handle schema drift?**

> *"Schema drift = source system adds, renames, or removes columns unexpectedly.*
>
> *My three-layer defense:*
>
> *1. Use `mergeSchema=true` for append operations — new columns are automatically added to the target schema*
>
> *2. Build schema drift detection in the pipeline — compare expected vs actual columns and alert:*
```python
expected = {"patient_id", "name", "dob", "facility_id"}
actual   = set(df.columns)
new_cols = actual - expected
if new_cols:
    send_alert(f"New columns in source: {new_cols}")
```
>
> *3. Keep raw data in Bronze — if downstream schemas break, I can re-derive from raw with the new schema logic."*

---

# 8. TESTING & DOCUMENTATION

---

**Q: What types of testing are you familiar with?**

> *"Three types relevant to my work:*
>
> *1. **Unit Testing** — test individual Python functions in isolation using pytest. I test DQ check functions, transformation logic, and utility helpers.*
>
> *2. **Component Integration Testing (CIT)** — test that pipeline components work together. Run the full pipeline against a small dev dataset and verify: row counts match, schema is correct, DQ checks pass.*
>
> *3. **User Acceptance Testing (UAT)** — business users validate that the output data matches their expectations. I support this by providing data profiles, sample records, and count reconciliation reports.*
>
> *At AbbVie, I also do reconciliation testing — compare source record count vs target record count after every pipeline run."*

---

**Q: How do you document your pipelines?**

> *"Three levels of documentation I maintain:*
>
> *1. **Code-level** — docstrings on every function explaining parameters, return values, and examples*
> *2. **Pipeline-level** — Confluence pages with architecture diagrams, data flow, source-to-target mappings, and known issues*
> *3. **Operational** — runbooks for common failure scenarios with step-by-step resolution guides*
>
> *I also maintain a data dictionary — column names, types, descriptions, and business definitions for every table."*

---

# 9. REAL-WORLD SCENARIO QUESTIONS

---

**🔴 Scenario 1: Pipeline fails in production at 3 AM. Business needs the report by 8 AM. What do you do?**

> *"First, don't panic — diagnose fast.*
>
> *Step 1: Check ADF Monitor for the error type. Is it: source not available, transformation failure, or a write error?*
>
> *Step 2: Based on error — triage the fix:*
> - *If source file missing → check S3/ADLS for file, contact upstream team, run manual trigger when file arrives*
> - *If Spark OOM → increase executor memory in cluster config and re-trigger*
> - *If schema mismatch → add `mergeSchema=true` and re-run*
> - *If JDBC timeout → increase timeout config and re-run*
>
> *Step 3: Re-run only from the failed step using ADF's partial re-run — not the entire pipeline from scratch.*
>
> *Step 4: Communicate to stakeholders: 'Issue identified as X. Fix deployed at 4:30 AM. Report will be ready by 6:30 AM.'*
>
> *Step 5: After resolution — document root cause and add monitoring alert to catch it earlier next time."*

---

**🔴 Scenario 2: A source system started sending 30% more columns than expected. Your pipeline is breaking. How do you fix it?**

> *"This is schema drift — a common real-world problem.*
>
> *Immediate fix:*
```python
# Add mergeSchema to handle new columns gracefully
df_source.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save(target_path)
```
>
> *Then investigate: which columns are new? Are they needed for downstream consumers?*
>
> *Add schema drift detection going forward:*
```python
expected_cols = set(spark.read.format("delta").load(target_path).columns)
actual_cols   = set(df_source.columns)
new_cols      = actual_cols - expected_cols
if new_cols:
    log_alert(f"Schema drift detected: new columns {new_cols}")
```
>
> *Update data dictionary and notify downstream teams of new fields.*
>
> *Root layer (Bronze) always captures everything — so even if I missed the new columns today, the raw data is preserved for reprocessing."*

---

**🔴 Scenario 3: Your SQL query joins 3 large tables and takes 45 minutes. How do you optimize it?**

> *"My approach:*
>
> *Step 1: Run EXPLAIN — identify the most expensive operations. Full table scans? Missing partition pruning?*
>
> *Step 2: Check for filter pushdown — are WHERE conditions on partition columns? If not, add them to enable partition pruning.*
>
> *Step 3: Review join order — always join the most filtered (smallest result) tables first.*
>
> *Step 4: Replace subqueries with JOINs — correlated subqueries run O(n²).*
>
> *Step 5: In Spark/Databricks — check if small tables (< 100MB) can be broadcast.*
>
> *Example: In AbbVie, a query joining patient_records (20TB) × facilities (5MB) × drug_codes (2MB) was doing Sort-Merge Joins on all three. I broadcast facilities and drug_codes — eliminated 2 shuffle stages — query went from 45 min to 8 min."*

---

**🔴 Scenario 4: Stakeholder says "The numbers in the report are wrong." How do you investigate?**

> *"Systematic investigation:*
>
> *Step 1: Get specifics — which metric? Which time period? How different?*
>
> *Step 2: Check audit logs in Nabu — when did the last successful pipeline run? How many rows were written?*
>
> *Step 3: Trace backwards through layers:*
> - *Gold layer — is the aggregation logic correct?*
> - *Silver layer — were DQ checks passing? Any records quarantined?*
> - *Bronze layer — did the source data change?*
>
> *Step 4: Use time travel on Delta/Iceberg to compare current data with the previous version:*
```sql
-- Compare today vs yesterday's Gold table
SELECT 'today' as version, SUM(revenue) FROM gold.revenue_summary
UNION ALL
SELECT 'yesterday', SUM(revenue) FROM gold.revenue_summary
TIMESTAMP AS OF CURRENT_TIMESTAMP - INTERVAL 1 DAY;
```
>
> *Step 5: If source data changed (upstream bug), reprocess the affected date partition.*
>
> *Communicate finding and fix timeline to stakeholder as soon as root cause is identified."*

---

**🔴 Scenario 5: You need to add a new data source (new Salesforce object) to the existing pipeline. How do you approach it?**

> *"My approach follows the metadata-driven framework:*
>
> *Step 1: Understand the source — schema, volume, update frequency, primary key, incremental column.*
>
> *Step 2: Add a config entry — no new code if the framework is generic enough:*
```yaml
- name: SF_NEW_OBJECT
  source: salesforce
  object: NewObject__c
  primary_key: Id
  incremental_col: LastModifiedDate
  load_type: incremental
  target_table: iceberg.bronze.sf_new_object
```
>
> *Step 3: If the existing framework handles it — just add config, deploy, test.*
>
> *Step 4: If new logic needed — create a feature branch, develop, write unit tests, PR review, deploy via CI/CD.*
>
> *Step 5: Test end-to-end in dev → staging → prod with row count reconciliation.*
>
> *Step 6: Update documentation — data dictionary, pipeline architecture diagram."*

---

**🔴 Scenario 6: You discover a pipeline has been loading duplicate records for the past 5 days. How do you fix it?**

> *"Step 1: Assess impact — how many duplicates? Which tables? Downstream consumers affected?*
>
> *Step 2: Stop the pipeline immediately — prevent more duplicates from accumulating.*
>
> *Step 3: Use time travel to restore to the clean version before the duplication started:*
```sql
RESTORE TABLE schema.my_table TO TIMESTAMP AS OF '5 days ago';
```
>
> *Step 4: Fix the root cause — likely missing deduplication or MERGE was replaced by INSERT accidentally.*
>
> *Step 5: Re-run the pipeline with the fix for the affected 5-day window as a backfill.*
>
> *Step 6: Add a duplicate count check to the pipeline's DQ gates — alert if duplicates exceed threshold.*
>
> *Step 7: Communicate clearly — what happened, when, impact, and resolution timeline."*

---

**🔴 Scenario 7: A business user wants data from a new Oracle table to be available in Databricks by Monday. It's Thursday. What's your plan?**

> *"Thursday: Gather requirements — table name, schema, volume, update frequency, what business wants to do with it.*
>
> *Thursday afternoon: Check access — can we connect to that Oracle table via JDBC? Request access if not.*
>
> *Friday morning: Develop pipeline in dev environment:*
> - *JDBC read from Oracle with parallel partitions*
> - *Basic DQ checks (nulls on primary key, no future dates)*
> - *Write to Iceberg Bronze*
> - *Write unit tests*
>
> *Friday afternoon: Test in staging, verify row counts match source.*
>
> *Friday EOD: Deploy to production via CI/CD pipeline. Run validation.*
>
> *Monday: Pipeline runs on schedule. Data available. Communicate to business user with row count confirmation.*
>
> *Risk communication: If Oracle access takes longer, set expectation early — don't wait until Monday."*

---

# 10. QUESTIONS TO ASK THE INTERVIEWER

> These show you're serious and have thought about the role:

1. *"What does the data stack look like — are you using Delta Lake, Iceberg, or something else for the Lakehouse layer?"*

2. *"How is the team structured — is this role primarily engineering pipelines, or is there also analytics/BI support?"*

3. *"What does the CI/CD process look like today — are there automated tests for data pipelines?"*

4. *"What's the biggest data engineering challenge the team is currently working through?"*

5. *"What does success look like for this role in the first 90 days?"*

---

## 📋 Last-Minute Revision Cheat Sheet

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    KEY POINTS TO REMEMBER                               │
├─────────────────────────────────────────────────────────────────────────┤
│ Python     │ Exception handling, decorators, config-driven design       │
│ SQL        │ Window functions, JOINs, correlated → JOIN rewrite         │
│ ADF        │ 4 trigger types, Linked Service vs Dataset, debug flow     │
│ Databricks │ Delta MERGE, Medallion, Unity Catalog, Z-ordering          │
│ CI/CD      │ Git branching, pytest, Azure DevOps YAML pipeline          │
│ Agile      │ Sprints, Jira, standups, retros, iterative delivery        │
│ Testing    │ Unit (pytest), CIT (pipeline), UAT (business validation)   │
│ Scenarios  │ Always: diagnose → fix → communicate → prevent recurrence  │
├─────────────────────────────────────────────────────────────────────────┤
│ STRONG ONE-LINERS                                                       │
│  ADF      → "Orchestration engine — triggers Databricks for compute"   │
│  Delta    → "Parquet + _delta_log = ACID + time travel"                │
│  MERGE    → "Idempotent upsert — safe to re-run after failures"         │
│  CI/CD    → "Automated tests on PR, automated deploy on merge"          │
│  Agile    → "2-week sprints with daily standups and continuous delivery"│
│  Schema   → "mergeSchema=true + drift detection = resilient pipelines" │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Final Tips for Tomorrow

- **Be specific with numbers** — 20TB, 10M+ records, 30% improvement, 15hrs/week saved
- **Connect every answer to AbbVie** — real experience beats theoretical knowledge
- **For things you don't know** — say "I haven't used that specifically, but here's how I'd approach it based on my experience with X"
- **Ask clarifying questions** in scenario Qs — shows structured thinking
- **Energy matters** — you have exactly the right experience for this role. Be confident.

---

**All the best tomorrow, Mayuresh! 🚀**

*Interview Prep | IT Applications Engineer II - Data (IC2) | May 2026*
