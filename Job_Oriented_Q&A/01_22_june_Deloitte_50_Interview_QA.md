# Deloitte Data Engineer II — 50 Real World Interview Questions & Answers
### Mapped to Your Profile: ECDP | FinOps PoC | Azure | Databricks | Snowflake | PySpark

> **How to use:** Read question → cover answer → speak it out loud for 2 minutes.
> Answers are written in first-person so you can say them directly.
> Each answer ties back to your real projects wherever possible.

---

## TABLE OF CONTENTS

| # | Topic |
|---|-------|
| Q1–Q8 | Azure Data Factory (ADF) |
| Q9–Q13 | ADLS Gen2, Event Hubs & Event-Driven Architecture |
| Q14–Q18 | Databricks & Delta Lake |
| Q19–Q22 | Medallion Architecture |
| Q23–Q26 | Data Formats — Parquet, Avro, Delta, Iceberg |
| Q27–Q30 | Data Governance, Security & Fine-Grained Access Control |
| Q31–Q34 | ETL, Migration & Informatica |
| Q35–Q39 | SQL Mastery |
| Q40–Q43 | Python — PySpark, Pandas, API Ingestion |
| Q44–Q46 | Shell Scripting & Automation |
| Q47–Q50 | Architecture, Design & Stakeholder Scenarios |

---
## 1. What is CI/CD, and how have you implemented it in your project?

CI/CD stands for Continuous Integration and Continuous Deployment. In my project, I used Azure DevOps with YAML pipelines to automate deployment of ADF pipelines and Databricks notebooks across environments. Whenever a developer pushes code to a feature branch, the CI pipeline triggers automatically—it validates the ADF ARM template export and runs basic checks. Once that's merged to the main branch, the CD pipeline picks it up and promotes the changes through DEV, then UAT, then PROD, with approval gates between each stage so nothing reaches production without sign-off.


## 2. What is an ARM template, and why do you use it for deployment?

An ARM template is a JSON file that defines Azure resources and their configuration in a declarative way—basically infrastructure as code. In my case, ADF generates an ARM template representing the entire pipeline, linked services, and dataset definitions when I publish. I use that template to deploy consistently across DEV, UAT, and PROD instead of manually recreating pipelines in each environment, which removes human error and keeps environments in sync. I also use parameter files alongside the template so environment-specific values—like the UAT Key Vault URL versus the PROD one—get swapped in automatically during deployment without touching the template itself.

## 3. How do you manage credentials and secrets in your CI/CD pipeline?

I never hardcode credentials anywhere in code or pipeline configuration. All secrets—Oracle passwords, Databricks tokens, storage keys—live in Azure Key Vault, and ADF's managed identity is granted access to retrieve them at runtime. For the DevOps pipeline itself, if I need a secret during deployment (say, a service principal credential for authenticating to Azure), I link the Key Vault into the pipeline as a variable group in Azure DevOps Library, so the actual secret value is never visible in logs or YAML files.

## 4. Walk me through your branching and environment promotion strategy.

I follow a standard DEV to UAT to PROD flow. Developers work on feature branches, raise a pull request into the main/collaboration branch, and that triggers the CI build. From there, the release pipeline deploys first to DEV automatically for sanity testing, then requires manual approval to push to UAT for broader testing, and another approval gate before PROD. Each environment has its own Key Vault, storage account, and ADF instance, and the ARM template parameter files keep the configuration isolated per environment so nothing from DEV accidentally points to a PROD resource.

## 5. How do you handle a failed deployment or pipeline run—do you have a rollback strategy?

If a deployment fails partway through, Azure DevOps release pipelines let me redeploy the last known-good ARM template version, since every successful deployment is versioned and stored as a release artifact. For pipeline run failures specifically—like a Databricks notebook activity failing mid-run—I rely on the activity dependency chain in ADF combined with the watermark table; since the watermark only updates after a successful copy, a failed run just gets retried from the same starting point next execution without double-processing or skipping data. I also have failure-path activities wired up to send alerts via Teams or email so issues get caught quickly rather than silently failing.

---

---

## SECTION 1 — Azure Data Factory (ADF)
### Q1–Q8

---

### Q1. Walk me through how you designed a metadata-driven pipeline in ADF. Why is it better than one pipeline per table?

**Answer:**

In ECDP, we had 25+ source tables from Oracle that all needed the same ingestion pattern — extract, land to ADLS Bronze, trigger Databricks transformation. The naive approach would be one ADF pipeline per table — 25 pipelines, all nearly identical.

Instead I built a **metadata-driven pattern**:

I created a control table in Azure SQL:
```sql
CREATE TABLE pipeline_config (
    config_id       INT IDENTITY PRIMARY KEY,
    source_table    VARCHAR(100),   -- e.g. 'GL_ACCOUNT'
    target_path     VARCHAR(200),   -- ADLS Bronze path
    watermark_col   VARCHAR(50),    -- e.g. 'UPDATED_AT'
    load_type       VARCHAR(10),    -- 'FULL' or 'INCREMENTAL'
    notebook_path   VARCHAR(200),   -- Databricks notebook to trigger
    is_active       BIT DEFAULT 1
);
```

The ADF master pipeline has three activities:
1. **Lookup Activity** — reads the control table: `SELECT * FROM pipeline_config WHERE is_active = 1`
2. **ForEach Activity** — iterates over each row, passes config as parameters
3. **Execute Pipeline Activity** — calls a single parameterized child pipeline per table

The child pipeline accepts parameters like `@pipeline().parameters.source_table` and uses them in dataset definitions and Databricks notebook calls.

**Why it's better:**
- Adding a new source = adding one row to the control table, zero pipeline changes
- A bug fix or policy change (e.g., new retry count) happens once in the child pipeline, applies to all 25 tables
- You can disable a table by flipping `is_active = 0` without touching any pipeline
- The control table gives you a self-documenting inventory of all data sources

---

### Q2. How do you handle pipeline failures and retries in ADF — what's your retry strategy?

**Answer:**

ADF has built-in retry settings at the activity level, but I go beyond that with a multi-layer strategy:

**Layer 1 — Activity-level retry (transient failures):**
For Copy Activities hitting JDBC or REST APIs, I set:
- Retry count: 3
- Retry interval: 30 seconds
This handles transient network blips, brief source system unavailability.

**Layer 2 — Pipeline-level failure handling:**
I connect the failure output port of each critical activity to an error-handling activity chain:
```
Copy Activity → [On Failure] → Stored Procedure Activity (log error to audit table)
                             → Web Activity (call Logic App to send email alert)
```

The audit table captures:
```sql
INSERT INTO pipeline_audit (
    pipeline_name, run_id, activity_name,
    status, error_message, rows_copied, run_time
)
```

**Layer 3 — Re-run safety (idempotency):**
The most important layer. Before retrying, the pipeline checks the audit table — if a batch already has `status = SUCCESS`, it skips that batch entirely. This means a retry never double-processes data.

**Layer 4 — Watermark protection:**
The watermark in the control table is updated only after a successful pipeline run — never at the start. So if a run fails midway, the next retry picks up from the same watermark, not a later one.

---

### Q3. How did you implement incremental loads using watermarking in ADF?

**Answer:**

In ECDP, most source tables needed incremental loads — pulling only new/changed records since the last run.

**The pattern I implemented:**

**Step 1 — Lookup current watermark:**
```
Lookup Activity → SELECT last_watermark FROM pipeline_config WHERE source_table = @table_name
```

**Step 2 — Extract new records using watermark:**
The Copy Activity source query uses a parameterized SQL:
```sql
SELECT * FROM GL_TRANSACTIONS
WHERE UPDATED_AT > '@{activity('LookupWatermark').output.firstRow.last_watermark}'
  AND UPDATED_AT <= GETDATE()
```

**Step 3 — Copy to Bronze:**
Copy Activity writes to ADLS Bronze with partition path `year=YYYY/month=MM/day=DD`.

**Step 4 — Update watermark only on success:**
After Copy Activity succeeds, a Stored Procedure Activity updates the watermark:
```sql
UPDATE pipeline_config
SET last_watermark = GETDATE(),
    last_run_status = 'SUCCESS',
    last_run_time = GETDATE()
WHERE source_table = @table_name
```

**Challenge I faced:**
One Oracle table had no `UPDATED_AT` column. Fix: I worked with the DBA to add a **database trigger** that populates an audit timestamp on every INSERT/UPDATE. As an interim, I used a **hash-based comparison** — hash key columns and compare against previous load's hashes stored in a staging table.

---

### Q4. How did you parameterize ADF datasets and linked services for DEV/UAT/PROD environments?

**Answer:**

In ECDP, we needed the same pipeline to work across three environments but point to different resources.

**Approach — ARM template parameter files:**

Each environment has its own parameter file checked into Git:
```
/arm-templates/
    parameters.dev.json
    parameters.uat.json
    parameters.prod.json
```

Each parameter file defines environment-specific values:
```json
{
  "parameters": {
    "adls_account_name": { "value": "ecdpstoragedev" },
    "oracle_connection_string": { "value": "jdbc:oracle:thin://dev-oracle:1521/ECDB" },
    "databricks_workspace_url": { "value": "https://adb-dev-xxxx.azuredatabricks.net" },
    "key_vault_name": { "value": "ecdp-kv-dev" }
  }
}
```

**ADF Global Parameters** store environment-level config that all pipelines reference:
- `env_name` → "DEV" / "UAT" / "PROD"
- `base_adls_path` → full ADLS container URL
- `log_table_connection` → Azure SQL connection for audit logging

**Linked Services** reference Key Vault for credentials instead of inline values — so the parameter file only needs the Key Vault name, not the actual secret. Key Vault name differs per environment; secrets are the same key name with different values.

**Azure DevOps CI/CD** deploys using:
```yaml
- task: AzureResourceManagerTemplateDeployment@3
  inputs:
    deploymentScope: 'Resource Group'
    overrideParameters: '-factoryName ecdp-adf-$(env)'
    csmFile: '$(Build.ArtifactStagingDirectory)/ARMTemplateForFactory.json'
    csmParametersFile: '$(Build.ArtifactStagingDirectory)/parameters.$(env).json'
```

---

### Q5. How did you trigger Databricks notebooks from ADF and pass parameters to them?

**Answer:**

ADF has a native **Databricks Notebook Activity** that I used in ECDP.

**Setup in ADF:**
```json
{
  "type": "DatabricksNotebook",
  "typeProperties": {
    "notebookPath": "/pipelines/silver/transform_gl_accounts",
    "baseParameters": {
      "batch_date": "@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')",
      "source_table": "@pipeline().parameters.source_table",
      "batch_id": "@pipeline().RunId",
      "env": "@pipeline().globalParameters.env_name"
    }
  }
}
```

**Receiving parameters in the Databricks notebook:**
```python
# Access ADF-passed parameters via dbutils.widgets
dbutils.widgets.text("batch_date", "")
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("env", "dev")

batch_date = dbutils.widgets.get("batch_date")
source_table = dbutils.widgets.get("source_table")
batch_id = dbutils.widgets.get("batch_id")
env = dbutils.widgets.get("env")

print(f"Processing {source_table} for batch {batch_id} on {batch_date}")
```

**Returning status back to ADF:**
The notebook's exit value is captured by ADF:
```python
# At end of notebook — ADF reads this as activity output
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "rows_processed": df.count(),
    "batch_id": batch_id
}))
```

In the next ADF activity, reference this output:
`@activity('TransformNotebook').output.runOutput`

---

### Q6. Have you worked with ADF's mapping data flows? When would you use them vs Databricks notebooks?

**Answer:**

Yes, I've used Mapping Data Flows in ADF, though I primarily used Databricks notebooks for complex transformations in ECDP.

**Mapping Data Flows are good for:**
- Simple to moderate transformations that a data analyst (not just engineers) needs to understand or modify
- No-code/low-code visual transformations — business users can read the flow diagram
- Transformations that need to be built quickly without writing PySpark code
- When you don't have a Databricks cluster available and want serverless compute

**Databricks notebooks are better for:**
- Complex logic that's hard to express visually (multi-level nested XML parsing, salting for skew, ML feature engineering)
- Large data volumes — Databricks clusters are more powerful than ADF's Spark-based data flows
- Code reusability — notebook functions can be packaged, versioned, unit tested
- When you need full PySpark API control (custom UDFs, RDD operations, Spark UI visibility)

**In ECDP:** I used Mapping Data Flows for simple lookups and type conversions on smaller reference tables (< 1GB), and Databricks notebooks for the heavy Oracle → Delta transformations (15GB+ tables).

**Rule of thumb I follow:**
If a junior analyst needs to modify the transformation logic without touching code → Data Flow.
If the transformation requires a data engineer and involves > 5GB of data → Databricks notebook.

---

### Q7. How did you monitor ADF pipelines in production? What metrics did you track?

**Answer:**

**In ADF Monitor (built-in):**
- Pipeline run history — status, duration, start/end time
- Activity-level drill-down — which activity failed, error message, input/output
- Trigger history — did the scheduled trigger fire correctly?

**Custom audit table (what I built in ECDP):**
Every pipeline writes to an Azure SQL audit table after each run:
```sql
CREATE TABLE pipeline_audit (
    run_id          VARCHAR(50) PRIMARY KEY,  -- ADF RunId
    pipeline_name   VARCHAR(100),
    source_table    VARCHAR(100),
    start_time      DATETIME,
    end_time        DATETIME,
    duration_secs   INT,
    rows_extracted  BIGINT,
    rows_loaded     BIGINT,
    status          VARCHAR(20),  -- SUCCESS / FAILED / PARTIAL
    error_message   VARCHAR(MAX),
    batch_date      DATE
);
```

**Azure Monitor Alerts I configured:**
- Alert if any pipeline fails → email notification via Action Group
- Alert if pipeline duration exceeds threshold (e.g., normally 30 min but takes > 90 min) → might indicate data volume spike or performance degradation

**Key metrics I tracked weekly:**
- Pipeline success rate (target > 99%)
- Average duration trend — increasing duration often signals growing data volume or performance degradation
- Rows loaded vs rows expected (source row count vs target row count)
- Retry count — high retries indicate flaky source system connectivity

---

### Q8. How did you handle ADF's integration runtime — self-hosted vs Azure IR?

**Answer:**

**Azure Integration Runtime (Azure IR):**
Managed by Microsoft, serverless, no setup needed. Works for cloud-to-cloud connections — ADLS to ADLS, Azure SQL to ADLS, REST APIs over the internet.

I used Azure IR for:
- ADLS Gen2 to Databricks (all cloud)
- REST API ingestion from external vendors
- Azure SQL (control table) reads/writes

**Self-Hosted Integration Runtime (SHIR):**
A Windows agent you install on a VM inside your corporate network. Required for on-premises sources that aren't directly accessible from the internet.

In ECDP, the Oracle database was on-premises behind a corporate firewall. Azure IR can't reach it. I installed SHIR on a dedicated VM in the client's on-prem network:

1. Downloaded the SHIR installer from ADF
2. Registered it with the ADF instance using an authentication key
3. Configured firewall rules: VM needs outbound HTTPS to Azure (port 443), and outbound to Oracle (port 1521)
4. Installed Oracle JDBC driver on the SHIR VM

**SHIR high availability:**
For production, I set up two SHIR nodes on two VMs — if one goes down, the other takes over automatically. ADF balances load across nodes.

**Challenge:** SHIR VMs need to be maintained (patched, monitored). I set up Azure Monitor alerts on SHIR node status — if a node goes offline, alert fires immediately.

---

## SECTION 2 — ADLS Gen2, Event Hubs & Event-Driven Architecture
### Q9–Q13

---

### Q9. How did you structure your ADLS Gen2 storage — containers, folders, access control?

**Answer:**

In ECDP, I designed the ADLS Gen2 structure following medallion architecture principles:

**Container structure:**
```
ecdp-storage/
├── raw/                    ← Bronze — immutable raw landing zone
│   ├── oracle/
│   │   ├── gl_accounts/year=2024/month=01/day=15/
│   │   └── transactions/year=2024/month=01/day=15/
│   └── sap/
│       └── gl_postings/year=2024/
├── processed/              ← Silver — cleaned, conformed Delta tables
│   ├── gl_accounts/
│   └── transactions/
├── curated/                ← Gold — business-ready aggregations
│   └── cost_center_summary/
└── archive/                ← Expired raw data moved here after retention period
```

**Access Control using ADLS Gen2 ACLs (not just RBAC):**

ADLS Gen2 supports POSIX-style ACLs at the file/folder level, not just at the storage account level:

- **ADF Managed Identity**: Read/Write on `/raw/` only — can land data but can't modify Silver/Gold
- **Databricks Service Principal**: Read on `/raw/`, Read/Write on `/processed/` and `/curated/`
- **Data Analysts (AD Group)**: Read-only on `/curated/` only
- **Data Engineers (AD Group)**: Read/Write on all containers

**Hierarchical Namespace must be enabled** — this is what makes ADLS Gen2 different from Blob Storage and enables ACLs + efficient directory operations (rename, move at folder level without copying files).

**Lifecycle management policy:**
Raw files older than 90 days move to Cool tier automatically (cost saving). Files older than 365 days move to Archive tier.

---

### Q10. What is Event Hub and how would you use it in a data pipeline? Have you worked with event-driven architecture?

**Answer:**

**What Azure Event Hubs is:**
Event Hubs is a fully managed, real-time event streaming platform — think of it as a high-throughput message queue designed for millions of events per second. It's Azure's equivalent of Apache Kafka (and actually supports the Kafka protocol).

**Core concepts:**
- **Producer**: Sends events to Event Hub (IoT device, application, API)
- **Event Hub**: Stores events in partitions for a configurable retention period (1–90 days)
- **Consumer Group**: A logical group of consumers that read from the event stream independently
- **Partition**: Events are distributed across partitions for parallelism

**How I would use it in a data pipeline (event-driven architecture):**

**Scenario — Real-time financial transaction ingestion:**
```
Bank API → Event Hub → Azure Function (validate + route) → ADLS Bronze
                                                         ↓
                                              Databricks Structured Streaming → Silver Delta
```

**Databricks reading from Event Hub:**
```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

schema = StructType([...])  # transaction schema

df_stream = spark.readStream \
    .format("eventhubs") \
    .options(**eventhub_conf) \
    .load() \
    .select(from_json(col("body").cast("string"), schema).alias("data")) \
    .select("data.*")

df_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(silver_path)
```

**In my projects:** ECDP was batch-based (Oracle → ADLS via ADF). But in FinOps PoC discussions, I proposed Event Hubs as the Phase 2 architecture for near-real-time SAP IDOC streaming — SAP can publish IDOCs to Event Hub via a message adapter, replacing the daily XML file dump.

---

### Q11. How would you design a serverless event-driven pipeline using Azure Functions and Event Grid?

**Answer:**

**Use case — file arrival triggers processing automatically:**

In a batch pipeline, you either poll for new files (inefficient — run every 5 minutes even if no files arrive) or use a fixed schedule (data sits for up to 24 hours waiting for the next run).

Event-driven solves this: as soon as a file lands in ADLS, processing starts automatically.

**Architecture:**
```
Source drops file to ADLS Bronze
    ↓
Event Grid detects BlobCreated event
    ↓
Event Grid routes to Azure Function (HTTP trigger / Event Grid trigger)
    ↓
Azure Function validates file (size check, name pattern, schema check)
    ↓
If valid: Function calls ADF REST API to trigger pipeline OR publishes to Queue Storage
If invalid: Function moves file to quarantine folder + sends alert email
    ↓
ADF pipeline runs → Databricks processes → Silver Delta updated
```

**Azure Function code (Event Grid trigger):**
```python
import azure.functions as func
import json
import requests

def main(event: func.EventGridEvent):
    data = event.get_json()
    file_url = data['url']
    file_name = data['url'].split('/')[-1]
    
    # Validate file matches expected pattern
    if not file_name.startswith('GL_POSTING_') or not file_name.endswith('.xml'):
        move_to_quarantine(file_url)
        send_alert(f"Unexpected file received: {file_name}")
        return
    
    # Trigger ADF pipeline via REST API
    trigger_adf_pipeline(
        pipeline_name="ingest_sap_gl",
        parameters={"file_path": file_url, "file_name": file_name}
    )
```

**Why this is better than scheduled triggers:**
- Zero polling overhead
- Processing starts within seconds of file arrival
- No wasted compute on empty schedule runs
- Scales automatically — 1 file or 1000 files, Functions scale out

---

### Q12. What is the difference between Event Grid, Event Hubs, and Service Bus? When do you use each?

**Answer:**

This is a common Azure architect question. All three are messaging services but for very different purposes:

| | Event Grid | Event Hubs | Service Bus |
|--|-----------|-----------|------------|
| **Purpose** | React to events (what happened) | Stream high-volume telemetry | Reliable message queue (guaranteed delivery) |
| **Volume** | Millions of events/sec | Millions of events/sec | Thousands of messages/sec |
| **Retention** | 24 hours (fire-and-forget) | 1–90 days (replay possible) | Until consumed (or TTL) |
| **Ordering** | Not guaranteed | Per-partition ordering | FIFO with sessions |
| **Use case** | File arrival → trigger function; Resource state changes | IoT telemetry, click streams, real-time analytics | Order processing, payment transactions, workflow steps |

**When I'd use each in data engineering:**
- **Event Grid**: Detect when a new file lands in ADLS and trigger an Azure Function or ADF pipeline — exactly what I described in Q11
- **Event Hubs**: Ingest high-frequency streaming data (millions of financial ticks/sec, IoT sensor readings) into Databricks Structured Streaming
- **Service Bus**: Orchestrate multi-step data workflows where each step must complete before the next starts, with guaranteed exactly-once delivery — e.g., extract → validate → transform → load, where each step consumes from a queue

---

### Q13. How would you ingest data from a REST API into your data lake? Walk me through your approach.

**Answer:**

I've built API ingestion pipelines in Python for FinOps Dataverse where we pulled data from a Salesforce-like API.

**Full approach:**

**Step 1 — Authentication:**
Most enterprise APIs use OAuth2. Get a bearer token first:
```python
import requests

def get_access_token(client_id, client_secret, token_url):
    response = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    })
    return response.json()["access_token"]
```
Store `client_id` and `client_secret` in Key Vault — never hardcode.

**Step 2 — Handle pagination:**
APIs rarely return all data in one call. Most use cursor-based or page-based pagination:
```python
def fetch_all_pages(base_url, token, page_size=500):
    all_records = []
    page = 1
    
    while True:
        response = requests.get(
            f"{base_url}?page={page}&limit={page_size}",
            headers={"Authorization": f"Bearer {token}"}
        )
        data = response.json()
        records = data.get("records", [])
        all_records.extend(records)
        
        if not data.get("has_more", False):
            break
        page += 1
    
    return all_records
```

**Step 3 — Rate limiting:**
```python
import time
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(multiplier=1, min=2, max=30),
       stop=stop_after_attempt(5))
def safe_api_call(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 429:  # Rate limited
        retry_after = int(response.headers.get("Retry-After", 10))
        time.sleep(retry_after)
        raise Exception("Rate limited")
    return response
```

**Step 4 — Write to ADLS Bronze:**
```python
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

df = pd.DataFrame(all_records)
parquet_buffer = df.to_parquet(index=False)

# Write to ADLS Gen2
datalake_client.get_file_client(container, f"raw/api/{today}/response.parquet") \
    .upload_data(parquet_buffer, overwrite=True)
```

**Step 5 — Orchestration:**
Azure Function on a timer trigger (every 15 min) calls this Python script. Or ADF Web Activity calls the API for simpler cases.

---

## SECTION 3 — Databricks & Delta Lake
### Q14–Q18

---

### Q14. Explain Delta Lake's transaction log (_delta_log). How does it enable ACID transactions?

**Answer:**

This is a deep internals question. The `_delta_log` is the heart of Delta Lake.

**What it is:**
Every Delta table has a hidden `_delta_log/` directory alongside the data files. It contains JSON files (and periodically Parquet checkpoint files) that record every operation ever performed on the table.

```
/silver/gl_accounts/
├── _delta_log/
│   ├── 00000000000000000000.json   ← Version 0: table created
│   ├── 00000000000000000001.json   ← Version 1: first data write
│   ├── 00000000000000000002.json   ← Version 2: OPTIMIZE run
│   ├── 00000000000000000010.checkpoint.parquet  ← Checkpoint every 10 versions
│   └── _last_checkpoint
├── part-00000-xxx.parquet
└── part-00001-xxx.parquet
```

**Each log entry records:**
```json
{
  "add": {
    "path": "part-00000-xxx.parquet",
    "size": 52428800,
    "partitionValues": {"fiscal_year": "2024"},
    "stats": {"numRecords": 150000, "minValues": {...}, "maxValues": {...}}
  }
}
```
OR
```json
{
  "remove": {
    "path": "part-00000-old.parquet",
    "deletionTimestamp": 1706000000000
  }
}
```

**How ACID works:**

- **Atomicity**: A write either adds a new log entry (commit) or doesn't. If the job crashes mid-write, partial Parquet files exist but no log entry is added — they're invisible to readers (orphan files, cleaned by VACUUM).
- **Consistency**: Schema validation happens at commit time. Wrong schema = no log entry = no commit.
- **Isolation**: Optimistic concurrency — two writers read the current version, both write their Parquet files, then race to write the log entry. The second writer detects a conflict and retries (or fails with a conflict error for incompatible operations).
- **Durability**: Log entry in cloud storage is durable by definition.

**Time travel works because:**
To read version N, Delta reads log entries 0 through N and reconstructs which Parquet files were "alive" at that point.

---

### Q15. What is Z-Ordering in Delta Lake and when did you use it in your project?

**Answer:**

**What it is:**
ZORDER is a data layout optimization that co-locates related data in the same Parquet files. When you query with a filter on a ZORDERed column, Delta reads far fewer files (data skipping), dramatically reducing I/O.

**How it works:**
ZORDER uses a space-filling curve (Z-curve) to map multi-dimensional data into a 1D sequence that preserves locality. Files are rewritten so records with similar values on the ZORDER columns end up in the same files.

**In ECDP — real usage:**

The Silver GL Transactions table was 15GB. The Gold aggregation job queried it with:
```sql
WHERE fiscal_year = 2024 AND cost_center = 'CC_1001'
```

Without ZORDER: Spark scanned all 15GB to find matching rows.

After running:
```sql
OPTIMIZE silver.gl_transactions
ZORDER BY (fiscal_year, cost_center);
```

Delta's min/max statistics per file now allow it to skip files that don't contain the queried `fiscal_year` and `cost_center` values. The same query went from scanning 15GB to scanning ~800MB — 95% less I/O.

**ZORDER vs Partitioning — key difference:**
- **Partition** on `fiscal_year` → entire directories skipped at the folder level. Best for high-cardinality filters on few distinct values (year, month).
- **ZORDER** on `cost_center` → within each partition, files are sorted so related cost center records cluster together. Best for high-cardinality columns with many distinct values (10,000+ cost centers) where partitioning would create too many small directories.

**Rule:** Partition on date/year/month. ZORDER on the columns you filter on within those partitions.

---

### Q16. What challenges did you face with Delta Lake MERGE and how did you solve them?

**Answer:**

I covered this partially in earlier answers but here's the full story:

**Challenge 1 — MERGE was slow (45 min on 2GB dataset):**

Root cause: MERGE does a full table scan to find matching records. When the target table had 50 million rows and no data skipping in place, it read every file.

**Fix:**
```sql
-- Step 1: ZORDER target table on the merge key
OPTIMIZE silver.gl_transactions ZORDER BY (account_id, transaction_date);

-- Step 2: Enable low shuffle merge
SET spark.databricks.delta.merge.enableLowShuffle = true;

-- Step 3: The MERGE itself
MERGE INTO silver.gl_transactions AS t
USING new_data AS s
ON t.account_id = s.account_id AND t.transaction_date = s.transaction_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```
After ZORDER, MERGE skipped 90% of files and ran in 6 minutes.

**Challenge 2 — Concurrent write conflicts:**

Two ADF pipeline runs triggered within seconds of each other (trigger timing bug). Both tried to MERGE into the same Delta table simultaneously.

Error: `ConcurrentAppendException: Files were added to the target table by a concurrent update`

**Fix:**
- Set pipeline trigger concurrency = 1 in ADF (only one run active at a time)
- Added retry with exponential backoff in the Databricks notebook for transient conflict errors:
```python
from delta.exceptions import ConcurrentModificationException
import time

def merge_with_retry(target, source_df, condition, max_retries=3):
    for attempt in range(max_retries):
        try:
            target.alias("t").merge(source_df.alias("s"), condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            return
        except ConcurrentModificationException:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # 1s, 2s, 4s backoff
```

---

### Q17. How do you handle slowly changing dimensions (SCD Type 1 and 2) in Delta Lake?

**Answer:**

**SCD Type 1 — Overwrite (no history kept):**
Simply update the existing record when something changes. Use MERGE:
```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/silver/dim_cost_center")

target.alias("t").merge(
    source_df.alias("s"),
    "t.cost_center_key = s.cost_center_key"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

**SCD Type 2 — Full history (track all changes with effective dates):**

This is more complex. The pattern I implemented in ECDP for dimension tables:

**Target table schema:**
```python
schema = StructType([
    StructField("surrogate_key", LongType()),       # Auto-generated unique key
    StructField("cost_center_key", StringType()),   # Business key
    StructField("cost_center_name", StringType()),
    StructField("department", StringType()),
    StructField("effective_date", DateType()),
    StructField("end_date", DateType()),            # NULL = currently active
    StructField("is_current", BooleanType())
])
```

**SCD2 MERGE logic:**
```python
# Step 1: Find records that changed
changed_records = source_df.alias("s").join(
    target_df.filter("is_current = true").alias("t"),
    "cost_center_key"
).filter("s.cost_center_name != t.cost_center_name OR s.department != t.department")

# Step 2: Expire old records (set end_date and is_current=false)
target.alias("t").merge(
    changed_records.alias("s"),
    "t.cost_center_key = s.cost_center_key AND t.is_current = true"
).whenMatchedUpdate(set={
    "is_current": "false",
    "end_date": "current_date()"
}).execute()

# Step 3: Insert new versions of changed records + new records
new_versions = changed_records.select(
    source_df["*"],
    lit(True).alias("is_current"),
    current_date().alias("effective_date"),
    lit(None).cast(DateType()).alias("end_date")
)

new_versions.write.format("delta").mode("append").save(target_path)
```

**Time travel for SCD2 audit:**
With Delta Lake time travel, I can also look at any point-in-time state of the dimension without needing the SCD2 history — it's a useful backup validation tool.

---

### Q18. What is Delta Live Tables (DLT) and how does it differ from standard Databricks notebooks?

**Answer:**

**Standard Databricks notebooks:**
You write Python/SQL transformation code, manage dependencies manually (run notebook A before notebook B), handle errors yourself, rebuild broken pipelines manually, and manage data quality checks in ad-hoc code.

**Delta Live Tables (DLT):**
A managed pipeline framework built on top of Delta Lake. You declare *what* your tables should look like — DLT figures out *how* and *when* to run them.

```python
import dlt
from pyspark.sql.functions import *

# Bronze — raw ingestion
@dlt.table(name="bronze_gl_transactions",
           comment="Raw GL transactions from Oracle")
def bronze_gl_transactions():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .load("/raw/oracle/gl_transactions/")

# Silver — with data quality expectations
@dlt.table(name="silver_gl_transactions")
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount != 0")
@dlt.expect_or_fail("valid_doc_number", "doc_number IS NOT NULL")
def silver_gl_transactions():
    return dlt.read_stream("bronze_gl_transactions") \
        .withColumn("amount", col("amount").cast(DecimalType(18,2))) \
        .dropDuplicates(["doc_number", "line_num"])
```

**Key DLT advantages:**
- **Automatic dependency resolution**: DLT builds the DAG — you don't manually sequence notebook runs
- **Built-in data quality**: `@dlt.expect` decorators enforce quality rules; violations are tracked automatically
- **Auto-restart on failure**: If Silver fails, DLT retries only Silver (not Bronze again)
- **Lineage**: DLT shows exactly which Bronze table feeds which Silver table, automatically

**When I'd use DLT vs notebooks:**
- DLT: greenfield projects, streaming pipelines, when you want managed orchestration and built-in DQ
- Notebooks: complex one-off transformations, existing projects, when you need maximum PySpark flexibility, or when DLT's cost model doesn't fit (DLT has a premium cluster surcharge)

---

## SECTION 4 — Medallion Architecture
### Q19–Q22

---

### Q19. Walk me through exactly how you implemented Bronze, Silver, Gold in your project. Be specific.

**Answer:**

In ECDP, here's the exact implementation per layer:

**BRONZE — Raw, immutable landing zone:**
```
Purpose: Preserve source data exactly as received. Never transform here.
Format: Delta (for ACID and time travel) OR raw Parquet/JSON (I used Delta)
Partitioning: By ingestion date (year/month/day) — NOT business date
Schema: Schema-on-read, permissive mode — accept whatever comes in
```

ADF Copy Activity lands Oracle data to Bronze:
```python
# Bronze write — append only, never overwrite
df_raw.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingest_year", "ingest_month", "ingest_day") \
    .option("mergeSchema", "true") \  # Accept new columns from source
    .save("/bronze/oracle/gl_transactions/")
```

**SILVER — Cleaned, conformed, deduplicated:**
```
Purpose: Single source of truth for data engineers and analysts.
Format: Delta
Partitioning: By business date (fiscal_year, fiscal_period)
Schema: Enforced, explicit — schema changes require code change
```

Databricks notebook processes Bronze → Silver:
```python
df_bronze = spark.read.format("delta").load("/bronze/oracle/gl_transactions/") \
    .filter(col("ingest_date") == today)

df_silver = df_bronze \
    .withColumn("amount", col("amount_raw").cast(DecimalType(18,2))) \
    .withColumn("posting_date", to_date(col("posting_date_raw"), "yyyyMMdd")) \
    .filter(col("doc_number").isNotNull()) \
    .dropDuplicates(["doc_number", "line_num"])

# Upsert to Silver — not append, to handle reprocessing
DeltaTable.forPath(spark, "/silver/gl_transactions") \
    .alias("t").merge(df_silver.alias("s"),
        "t.doc_number = s.doc_number AND t.line_num = s.line_num") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

**GOLD — Business-ready aggregations:**
```
Purpose: Optimized for reporting queries. Aggregated, enriched with dimensions.
Format: Delta (consumed by Snowflake via Spark connector, Power BI via Databricks SQL)
Partitioning: By fiscal_year (reporting always filters on year)
```

```python
df_gold = spark.read.format("delta").load("/silver/gl_transactions/") \
    .join(broadcast(dim_cost_center), "cost_center_key", "left") \
    .groupBy("fiscal_year", "fiscal_period", "department", "region") \
    .agg(
        sum("amount").alias("total_spend"),
        count("doc_number").alias("transaction_count"),
        countDistinct("cost_center_key").alias("active_cost_centers")
    )

df_gold.write.format("delta").mode("overwrite") \
    .option("replaceWhere", f"fiscal_year = {current_year}") \
    .save("/gold/cost_center_summary/")
```

---

### Q20. How do you handle data quality failures at each medallion layer?

**Answer:**

Each layer has a different quality philosophy:

**Bronze — Accept everything, flag bad records:**
Bronze's job is to capture raw data. I never reject records at Bronze:
```python
df_with_dq = df_raw \
    .withColumn("_dq_null_doc", col("doc_number").isNull()) \
    .withColumn("_dq_negative_amount", col("amount") < 0) \
    .withColumn("_dq_flags", array_join(
        array(
            when(col("_dq_null_doc"), lit("NULL_DOC")).otherwise(lit(None)),
            when(col("_dq_negative_amount"), lit("NEGATIVE_AMOUNT")).otherwise(lit(None))
        ), ","
    ))

# Write everything to Bronze including flagged records
df_with_dq.write.format("delta").mode("append").save(bronze_path)
```

**Silver — Quarantine bad records, don't block good ones:**
```python
# Split into good and quarantine
df_good = df_bronze.filter(col("doc_number").isNotNull() & (col("amount") != 0))
df_bad = df_bronze.filter(col("doc_number").isNull() | (col("amount") == 0))

# Write good records to Silver
df_good.write.format("delta").mode("append").save(silver_path)

# Write bad records to quarantine with rejection reason
df_bad.withColumn("rejection_reason", lit("NULL_DOC_OR_ZERO_AMOUNT")) \
      .withColumn("rejection_time", current_timestamp()) \
      .write.format("delta").mode("append").save(quarantine_path)
```

**Gold — Hard stop on critical failures:**
```python
# Critical checks — fail the pipeline
silver_count = spark.read.format("delta").load(silver_path).count()
gold_count = df_gold.count()

# Gold should never have more rows than Silver (aggregation)
assert gold_count <= silver_count, "Gold row count exceeds Silver — data duplication suspected"

# Business rule: total spend should be within 5% of yesterday's total
yesterday_spend = get_yesterday_total_spend()
today_spend = df_gold.agg(sum("total_spend")).collect()[0][0]
variance = abs(today_spend - yesterday_spend) / yesterday_spend

if variance > 0.05:
    raise ValueError(f"Spend variance {variance:.1%} exceeds 5% threshold — investigate before publishing")
```

---

### Q21. How did you manage schema evolution across Bronze, Silver, and Gold layers?

**Answer:**

Schema evolution is one of the trickiest parts of lakehouse maintenance. Here's my layered approach:

**Bronze — Permissive, accepts new columns:**
```python
# mergeSchema=true allows new columns to be added automatically
df.write.format("delta").option("mergeSchema", "true").mode("append").save(bronze_path)
```
New columns from source appear in Bronze automatically. This is intentional — Bronze should be a complete mirror of source.

**Silver — Controlled evolution, alerts on new columns:**
```python
expected_schema = spark.read.format("delta").load(silver_path).schema
incoming_schema = df_bronze.schema

new_cols = set(incoming_schema.fieldNames()) - set(expected_schema.fieldNames())
dropped_cols = set(expected_schema.fieldNames()) - set(incoming_schema.fieldNames())

if dropped_cols:
    # Source dropped a column — this is critical, fail the pipeline
    raise Exception(f"Source dropped columns: {dropped_cols}. Pipeline halted.")

if new_cols:
    # New columns from source — log and alert, but don't fail
    log_schema_change(new_cols, "NEW_COLUMNS_DETECTED")
    send_alert(f"New columns detected in Silver source: {new_cols}")
    # Only add to Silver after explicit review — use overwriteSchema manually
```

**Gold — Strictest, no automatic evolution:**
Gold is consumed by Snowflake and BI tools. Any schema change breaks downstream reports. Gold schema changes require:
1. PR review
2. Snowflake table ALTER if applicable
3. BI dashboard validation
4. Explicit `overwriteSchema=true` only after all above steps

---

### Q22. How did you handle late-arriving data in your medallion pipeline?

**Answer:**

Late-arriving data means a record with `business_date = 2024-01-10` arrives in the pipeline on `2024-01-13`. If you partition by ingestion date, this record lands in the January 13 partition, not January 10 — breaking all reports that query by business date.

**Solution I implemented:**

**Bronze**: Partition by ingestion date (when data arrived) — this is correct for Bronze because you want to replay from a specific ingestion point. Also store `business_date` as a column.

**Silver**: Partition by business date, use MERGE:
```python
# Late record for 2024-01-10 arriving on 2024-01-13
# MERGE puts it in the correct business_date partition
DeltaTable.forPath(spark, silver_path) \
    .alias("t").merge(
        df_late.alias("s"),
        "t.doc_number = s.doc_number AND t.line_num = s.line_num"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```
MERGE inserts the record into the correct partition regardless of when it arrived.

**Gold**: Reprocess affected partitions:
```python
# If a late record arrives for 2024-01, recompute Gold for that month
affected_partition = df_late.select("fiscal_year", "fiscal_period").distinct()

for row in affected_partition.collect():
    recompute_gold_for_period(row.fiscal_year, row.fiscal_period)
```

Used `replaceWhere` for efficient partition-only recompute:
```python
df_gold_recomputed.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "fiscal_year = 2024 AND fiscal_period = 1") \
    .save(gold_path)
```

---

## SECTION 5 — Data Formats
### Q23–Q26

---

### Q23. Compare Parquet, Avro, JSON, CSV, and Delta for data engineering use cases. When do you use each?

**Answer:**

| Format | Structure | Best For | Avoid When |
|--------|-----------|----------|------------|
| **CSV** | Row-based, plain text | Small data exchange, Excel-friendly, human-readable | Large datasets (slow, no compression benefit, no schema) |
| **JSON** | Semi-structured, row-based | APIs, nested/flexible schema, config files | Analytics on large flat datasets (verbose, slow to parse) |
| **Parquet** | Columnar, compressed | Analytical queries (read few columns from many rows), data lakes, Spark | Frequent single-row updates, write-heavy workloads |
| **Avro** | Row-based, binary, schema in file | Schema evolution, Kafka streaming (compact, fast serialization), data exchange between systems | Analytics queries (row-based = full row reads) |
| **Delta** | Parquet + transaction log | Lakehouse storage, ACID writes, upserts, time travel, streaming | Simple one-time batch that never needs updates (overkill) |
| **Iceberg** | Similar to Delta | Multi-engine (Spark + Trino + Flink + Snowflake on same table), hidden partitioning | Pure Databricks stack where Delta is simpler |

**In my projects:**
- **Bronze layer**: Delta (for time travel and ACID on raw data)
- **SAP XML intermediate**: Parquet (after parsing, before Silver write)
- **Streaming from Event Hubs**: Avro (Event Hubs default serialization)
- **API responses**: JSON (raw, then converted to Parquet at Bronze write)
- **All Silver/Gold**: Delta Lake

---

### Q24. What is the small files problem and how do you prevent and fix it?

**Answer:**

**What it is:**
Spark writes one output file per partition task. If you have 200 shuffle partitions and write 100MB of data, you get 200 files of 512KB each. Over hundreds of daily runs, a Delta table accumulates tens of thousands of tiny files.

**Why it's bad:**
- **Read performance**: Spark opens one file per task. Opening 50,000 files has 50,000 file system metadata operations — each costs ~1-5ms. That's 50-250 seconds of overhead before any data is read.
- **Driver memory**: The file listing (all 50,000 file paths) must fit in driver memory.
- **Cloud storage costs**: Cloud object stores (ADLS, S3) charge per operation. 50,000 files = 50,000 LIST operations per query.

**Prevention:**
```python
# 1. Coalesce before writing small datasets
df.coalesce(5).write.format("delta").mode("append").save(path)

# 2. Tune shuffle partitions for data size
spark.conf.set("spark.sql.shuffle.partitions", "20")  # Not default 200 for small data

# 3. Enable Delta auto-optimize (Databricks)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

**Fix for existing problem:**
```sql
-- Compact existing small files into ~1GB files
OPTIMIZE silver.gl_transactions;

-- With ZORDER for query optimization simultaneously
OPTIMIZE silver.gl_transactions ZORDER BY (fiscal_year, cost_center);

-- Remove old file versions (default 7-day retention)
VACUUM silver.gl_transactions RETAIN 168 HOURS;
```

**In ECDP — real numbers:**
After 3 months of daily incremental runs, our Silver table had 48,000 files, average 15KB each. A Gold aggregation query that took 2 minutes initially now took 22 minutes. After `OPTIMIZE` + `ZORDER`, query time went back to 2.5 minutes.

---

### Q25. Explain partitioning strategies — how did you choose partition columns in your projects?

**Answer:**

Partitioning physically separates data into subfolders by column value. A query with `WHERE fiscal_year = 2024` skips all partitions except `fiscal_year=2024/` — reading 1/5 of the data if you have 5 years.

**Rules I follow for choosing partition columns:**

**Rule 1 — Low cardinality:**
Partition columns should have few distinct values. `fiscal_year` (5-10 values) = good partition. `doc_number` (millions of values) = terrible partition — millions of tiny files.

**Rule 2 — Commonly filtered:**
Partition column should appear in `WHERE` clauses of most queries. In ECDP, almost every query has `WHERE fiscal_year = X` — so partitioning by `fiscal_year` eliminates massive amounts of I/O.

**Rule 3 — Not too granular:**
`year + month + day` partitioning creates 365+ partitions per year. For a table with 50GB/year, that's ~137MB per partition — fine. But for a 1GB/year table, that's ~2.7MB per partition — too small, causes small files problem.

**What I used in ECDP:**
```python
# Silver GL Transactions (50GB/year, queries always filter on fiscal_year + fiscal_period)
.partitionBy("fiscal_year", "fiscal_period")  # 12 partitions per year, ~4GB each

# Bronze (partitioned by ingestion for reprocessing purposes)
.partitionBy("ingest_year", "ingest_month", "ingest_day")

# Gold (quarterly reporting)
.partitionBy("fiscal_year")  # Gold is small (aggregated), year is enough
```

**ZORDER for high-cardinality columns:**
For `cost_center` (10,000+ distinct values), I used ZORDER instead of partitioning — ZORDER clusters related data without creating thousands of tiny partition folders.

---

### Q26. What is schema evolution and how do Delta Lake and Iceberg handle it differently?

**Answer:**

Schema evolution is the ability of a table to accommodate changes in the structure of incoming data over time — new columns added, columns renamed, data types changed.

**Delta Lake schema evolution:**

Adding columns (additive change) — easy:
```python
df_with_new_col.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append").save(path)
```

Renaming columns or changing types (breaking change) — requires explicit override:
```python
df_different_schema.write.format("delta") \
    .option("overwriteSchema", "true") \  # Drops all old data and rebuilds
    .mode("overwrite").save(path)
```

Delta supports column mapping (rename without rewrite) in newer versions, but it's not fully seamless for all operations.

**Iceberg schema evolution:**
Iceberg was designed with schema evolution as a first-class feature. It uses column IDs (not names) internally — so renaming a column is a metadata-only operation that doesn't rewrite any data files:
- Add column: metadata only
- Drop column: metadata only (data still in files, just not read)
- Rename column: metadata only (ID stays the same)
- Change type: supported for compatible types (int → long, float → double)

**Hidden partitioning in Iceberg:**
Iceberg separates partition scheme from table schema. You can change how data is partitioned (e.g., from monthly to daily) without rewriting existing data — a major advantage over Delta/Hive partitioning.

**When to choose Iceberg over Delta:**
- Multi-engine environment: Snowflake + Spark + Trino all reading the same table — Iceberg has broader engine support
- Frequent partition scheme changes
- Regulatory requirement for column-level drop/rename without data rewrite

**In my projects:** I used Delta because we were 100% on Databricks. If we had Snowflake reading the same underlying lake files directly, I'd evaluate Iceberg.

---

## SECTION 6 — Data Governance & Security
### Q27–Q30

---

### Q27. How do you implement column-level and row-level security in Databricks and Snowflake?

**Answer:**

**Column-level security — Databricks:**

Using Unity Catalog (Databricks' governance layer):
```sql
-- Grant access to specific columns only
GRANT SELECT (doc_number, posting_date, cost_center, fiscal_year)
ON TABLE silver.gl_transactions
TO `analyst_group@company.com`;

-- Mask sensitive column (amount) for non-finance users
CREATE OR REPLACE FUNCTION mask_amount(amount DOUBLE, user_groups ARRAY<STRING>)
RETURNS DOUBLE
RETURN IF(ARRAY_CONTAINS(user_groups, 'finance_team'), amount, NULL);

ALTER TABLE silver.gl_transactions
ALTER COLUMN amount SET MASK mask_amount
USING COLUMNS (current_groups());
```

**Row-level security — Databricks Unity Catalog:**
```sql
-- Create row filter: each user sees only their region's data
CREATE OR REPLACE FUNCTION region_filter(region STRING)
RETURNS BOOLEAN
RETURN IS_ACCOUNT_GROUP_MEMBER(region);  -- e.g., APAC user → sees APAC rows only

ALTER TABLE silver.gl_transactions
ADD ROW FILTER region_filter ON (region);
```

**Column-level security — Snowflake:**
```sql
-- Dynamic data masking policy
CREATE MASKING POLICY mask_financial_amount AS (val NUMBER)
RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('FINANCE_ROLE', 'EXECUTIVE_ROLE') THEN val
        ELSE NULL  -- Other roles see NULL
    END;

ALTER TABLE gl_transactions
MODIFY COLUMN amount SET MASKING POLICY mask_financial_amount;
```

**Row-level security — Snowflake:**
```sql
-- Row Access Policy: each analyst sees only their department
CREATE ROW ACCESS POLICY dept_policy AS (dept_col VARCHAR)
RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ADMIN_ROLE'
    OR EXISTS (
        SELECT 1 FROM user_department_mapping
        WHERE user = CURRENT_USER() AND department = dept_col
    );

ALTER TABLE gl_transactions
ADD ROW ACCESS POLICY dept_policy ON (department);
```

---

### Q28. What is data lineage and how did you implement or track it in your projects?

**Answer:**

**What data lineage is:**
Data lineage tracks where data came from, how it was transformed, and where it went — the full ancestry of every data element. It answers: "This Gold report shows $50M revenue — which source tables fed this number, and what transformations happened along the way?"

**Why it matters:**
- **Debugging**: When a report shows wrong numbers, lineage tells you exactly which pipeline step introduced the error
- **Impact analysis**: "If the Oracle source schema changes, which downstream reports are affected?"
- **Regulatory compliance**: GDPR, SOX, HIPAA require knowing where sensitive data flows
- **Trust**: Business users trust data more when they can see its origin

**How I tracked lineage in ECDP:**

**1. Databricks Unity Catalog (automated lineage):**
Unity Catalog automatically captures column-level lineage for SQL queries and Python DataFrame operations in Databricks. No code needed — it records which source table/column maps to which target table/column.

Access via:
```python
# REST API or UI: Catalog → Table → Lineage tab
# Shows: Oracle GL_TRANSACTIONS → Silver gl_transactions → Gold cost_center_summary
```

**2. Custom audit table (manual lineage for ADF + Databricks integration):**
```sql
CREATE TABLE data_lineage (
    lineage_id      BIGINT IDENTITY,
    source_system   VARCHAR(50),   -- 'ORACLE', 'SAP', 'REST_API'
    source_object   VARCHAR(100),  -- 'GL_TRANSACTIONS'
    target_layer    VARCHAR(20),   -- 'BRONZE', 'SILVER', 'GOLD'
    target_path     VARCHAR(200),
    pipeline_name   VARCHAR(100),
    run_id          VARCHAR(50),
    rows_read       BIGINT,
    rows_written    BIGINT,
    transformation  VARCHAR(500),  -- Brief description of logic applied
    run_timestamp   DATETIME
);
```

Every Databricks notebook writes a lineage entry at start and end, creating an audit trail from source to Gold.

---

### Q29. How do you handle encryption for data at rest and in transit?

**Answer:**

**Data at rest — ADLS Gen2:**
- **Storage Service Encryption (SSE)**: Enabled by default — Microsoft encrypts all data using AES-256. Zero config needed.
- **Customer-Managed Keys (CMK)**: For stricter compliance, the client's own Azure Key Vault key is used for encryption instead of Microsoft-managed keys. In ECDP, the client's security team required CMK because they needed the ability to revoke encryption keys (for data destruction compliance).

**Data at rest — Databricks:**
- Databricks clusters use Azure-managed disks for shuffle data. Enable encryption at host for DBFS and cluster disks.
- Delta Lake files in ADLS inherit ADLS encryption — no separate setup needed.

**Data in transit:**
- All communication between ADF, Databricks, ADLS, and Azure SQL uses HTTPS/TLS 1.2+ by default
- For Oracle JDBC connection from SHIR: enable JDBC encrypted connection using Oracle's native encryption (`oracle.net.encryption_client=REQUIRED`)
- Snowflake Spark connector: all data staged to Azure Blob uses HTTPS, all Snowflake connections use TLS

**Secrets management:**
- All credentials in Azure Key Vault
- Databricks accesses Key Vault via secret scopes backed by Key Vault:
```python
password = dbutils.secrets.get(scope="kv-backed-scope", key="oracle-password")
```
- ADF Linked Services reference Key Vault secrets by name — actual secret value never stored in ADF
- Key Vault access logged in Azure Monitor — every secret access is audited

---

### Q30. Have you worked with data cataloging tools? How did you manage data discovery in your projects?

**Answer:**

**What a data catalog does:**
A data catalog is a searchable inventory of all data assets — tables, columns, descriptions, owners, data types, sample values, lineage, and quality metrics. It enables data discovery: a new analyst can find "what table has cost center data?" without asking an engineer.

**Tools I've worked with / know:**

**Databricks Unity Catalog:**
In ECDP, I used Unity Catalog as the primary catalog layer. It provides:
- Three-level namespace: `catalog.schema.table` (e.g., `ecdp.silver.gl_transactions`)
- Table and column descriptions (added via `COMMENT ON TABLE/COLUMN`)
- Automated lineage (as described in Q28)
- Access control at the catalog/schema/table/column level
- Data search via Databricks UI or REST API

```sql
-- Document tables for discovery
COMMENT ON TABLE silver.gl_transactions IS
    'Cleaned GL transaction data from Oracle. Updated daily at 7 AM.
     Contact: data-engineering@company.com. Source: Oracle ECDP_DB.GL_TRANSACTIONS';

COMMENT ON COLUMN silver.gl_transactions.amount IS
    'Transaction amount in local currency. See currency_code for denomination.
     For USD-converted amount, see gold.cost_center_summary.amount_usd';
```

**Microsoft Purview (Azure native):**
Microsoft Purview connects to ADLS, ADF, Databricks, SQL databases and automatically scans and catalogs all data assets. It shows lineage across the full pipeline (ADF → ADLS → Databricks → Snowflake) in a visual graph.

In ECDP, I configured Purview to:
- Auto-scan ADLS containers weekly and catalog all Delta tables
- Classify sensitive columns automatically (financial data, PII detection)
- Show cross-system lineage from Oracle source to Snowflake reports

---

## SECTION 7 — ETL, Migration & SQL
### Q31–Q39

---

### Q31. Have you worked with Informatica? How does it compare to ADF for ETL?

**Answer:**

I have studied Informatica PowerCenter as part of my understanding of legacy ETL tools, though my hands-on production ETL experience is primarily with ADF and Databricks. Here's an honest, comparative answer:

**Informatica PowerCenter (legacy, on-premises):**
- Visual, drag-and-drop ETL designer — non-programmers can build mappings
- Mature, 20+ year-old tool with connectors to virtually every enterprise system (SAP, Oracle, Teradata, mainframe)
- Strong data quality and transformation capabilities built-in
- Requires on-premises infrastructure, licenses are expensive
- Session-based parallelism — not designed for cloud-scale distributed processing

**Informatica IICS (Intelligent Cloud Services):**
- Cloud-native version of PowerCenter
- Serverless compute for transformations
- Better cloud integration (AWS, Azure, Salesforce connectors)
- Still visual/low-code, good for business analysts

**ADF comparison:**
| | Informatica PowerCenter | ADF |
|--|------------------------|-----|
| Paradigm | On-prem ETL | Cloud orchestration + ETL |
| Coding | Minimal (visual) | Medium (JSON config + some code) |
| Scale | Session-based | Cloud-native, scales automatically |
| Cost | Expensive license | Pay-per-use |
| Best for | Legacy enterprise, complex transformations | Azure-native pipelines, modern cloud ETL |

**Migration scenario:**
In a Informatica → ADF migration, the typical approach is: map each Informatica mapping to an equivalent ADF pipeline + Databricks notebook, rewrite transformation logic in PySpark, validate output row counts and checksums match Informatica's output.

---

### Q32. You're migrating data from Oracle on-premises to Azure. Walk me through your full migration approach.

**Answer:**

This is exactly what I did in ECDP. Here's the full approach:

**Phase 1 — Assessment (2-3 weeks):**
- Inventory all Oracle objects: tables, views, stored procedures, sequences, constraints
- Profile data: row counts, data types, null rates, duplicate rates, size per table
- Identify dependencies: which tables have FK relationships, which ETL jobs read from Oracle
- Classify tables: hot (queried daily), warm (weekly), cold (monthly/archival)
- Document complex Oracle-specific features that need migration: partitioning, LOBs, materialized views

**Phase 2 — Schema migration:**
- Convert Oracle DDL to Azure SQL / Delta Lake schema
- Oracle → Azure type mappings:
  - `NUMBER(10)` → `INT`
  - `VARCHAR2(200)` → `VARCHAR(200)`
  - `DATE` → `DATETIME2` (Oracle DATE includes time; Azure DATE doesn't)
  - `CLOB` → `VARCHAR(MAX)` or store in ADLS as separate files
  - `RAW` → `VARBINARY`

**Phase 3 — Historical data migration (one-time load):**
- Use ADF Copy Activity with JDBC parallel read to extract Oracle tables
- Land to ADLS Bronze as Parquet
- Transform and load to Silver Delta tables
- Validate: row counts match, checksums match, sample records match

**Phase 4 — Incremental sync (parallel run period):**
- Run Oracle and Azure pipeline in parallel for 2-4 weeks
- Daily reconciliation: compare Oracle totals vs Azure totals per table
- Fix any discrepancies found
- This is the "dual-run" period — business continues using Oracle, Azure is validated silently

**Phase 5 — Cutover:**
- Final incremental sync to capture any last-minute Oracle changes
- Switch application/reporting connections from Oracle to Azure
- Keep Oracle in read-only mode for 30 days as rollback option
- Decommission Oracle after 30-day stability period

**Challenges I faced in ECDP:**
- Oracle `DATE` columns interpreted as midnight when they should have included time → fixed by casting `DATE` to `TIMESTAMP` in Oracle extract query
- LOB (CLOB) columns couldn't be read via ADF JDBC directly → extracted via Oracle external tables to flat files, then loaded separately

---

### Q33. How do you handle a data migration where you can't afford downtime?

**Answer:**

This is a zero-downtime migration pattern — the business keeps running on the old system while you migrate to the new one.

**Strategy — Change Data Capture (CDC) with parallel run:**

**Step 1 — Initial historical load:**
Extract all historical data from Oracle to Azure. This can take hours/days — that's fine because Oracle is still serving production traffic. Azure is the dark copy at this point.

**Step 2 — Enable CDC on Oracle:**
Oracle LogMiner or GoldenGate captures every INSERT/UPDATE/DELETE that happens on Oracle after the historical load started. These changes are queued.

**Step 3 — Apply CDC changes to Azure:**
While historical load runs, CDC changes are continuously applied to Azure in near-real-time. Azure catches up to Oracle's current state.

**Step 4 — Parallel run validation:**
Once Azure is caught up (lag < minutes), run both Oracle and Azure in parallel. Every query that hits Oracle also hits Azure — compare results. Run for 1-2 weeks to build confidence.

**Step 5 — Cutover window (minutes, not hours):**
- Stop CDC feed
- Apply final remaining changes to Azure (lag is now seconds to minutes)
- Flip DNS/connection string to Azure
- Monitor for 30 minutes
- Total user-visible downtime: 0-5 minutes (just the connection flip)

**Step 6 — Rollback plan:**
Oracle remains live in read-only mode. If Azure has issues in the first 24 hours, flip connection string back to Oracle. This is the safety net.

---

### Q34. What shell scripting have you done for data engineering automation?

**Answer:**

I've used Bash scripting for operational automation in data engineering contexts:

**File transfer and landing zone management:**
```bash
#!/bin/bash
# Automated SFTP file pickup and landing to ADLS

SOURCE_DIR="/sftp/inbound/sap_exports/"
ADLS_PATH="abfss://raw@storage.dfs.core.windows.net/sap/"
LOG_FILE="/var/log/sap_pickup_$(date +%Y%m%d).log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a $LOG_FILE
}

# Find files modified in last 24 hours
FILES=$(find $SOURCE_DIR -name "*.xml" -mtime -1)

for FILE in $FILES; do
    FILENAME=$(basename $FILE)
    log "Processing: $FILENAME"
    
    # Copy to ADLS using azcopy
    azcopy copy "$FILE" "${ADLS_PATH}${FILENAME}" --overwrite=false
    
    if [ $? -eq 0 ]; then
        log "SUCCESS: $FILENAME uploaded"
        mv "$FILE" "/sftp/processed/${FILENAME}"
    else
        log "ERROR: Failed to upload $FILENAME"
        mv "$FILE" "/sftp/failed/${FILENAME}"
        # Send alert
        echo "File upload failed: $FILENAME" | mail -s "ALERT: SAP File Upload Failed" ops@company.com
    fi
done
```

**Environment setup for Databricks cluster init scripts:**
```bash
#!/bin/bash
# Cluster init script — runs on every cluster node at startup

# Install Oracle JDBC driver
wget -q https://storage-account.blob.core.windows.net/drivers/ojdbc8.jar \
    -O /opt/oracle/ojdbc8.jar

# Set environment variables
echo "export ORACLE_HOME=/opt/oracle" >> /etc/environment
echo "export LD_LIBRARY_PATH=$ORACLE_HOME/lib" >> /etc/environment

# Install Python packages not in base image
pip install --quiet spark-xml==0.15.0 snowflake-connector-python==3.0.0

echo "Init script completed successfully"
```

**Job scheduling via cron:**
```bash
# crontab entry — run SAP file pickup daily at 5 AM
0 5 * * * /home/dataeng/scripts/sap_file_pickup.sh >> /var/log/cron.log 2>&1

# Weekly OPTIMIZE run on Delta tables (Sunday 2 AM)
0 2 * * 0 /home/dataeng/scripts/delta_optimize.sh >> /var/log/optimize.log 2>&1
```

---

## SECTION 8 — SQL Mastery
### Q35–Q39

---

### Q35. Write a query to find the top 3 spending cost centers per region for each fiscal year.

**Answer:**

```sql
WITH ranked_spend AS (
    SELECT
        fiscal_year,
        region,
        cost_center,
        SUM(amount) AS total_spend,
        RANK() OVER (
            PARTITION BY fiscal_year, region
            ORDER BY SUM(amount) DESC
        ) AS spend_rank
    FROM gl_transactions gt
    JOIN dim_cost_center dc ON gt.cost_center_key = dc.cost_center_key
    GROUP BY fiscal_year, region, cost_center
)
SELECT fiscal_year, region, cost_center, total_spend, spend_rank
FROM ranked_spend
WHERE spend_rank <= 3
ORDER BY fiscal_year, region, spend_rank;
```

**Why RANK and not ROW_NUMBER:**
If two cost centers have identical spend, RANK gives both rank 1 and skips rank 2 — correct behavior for "top 3 spenders." ROW_NUMBER would arbitrarily assign 1,2,3 to tied records.

---

### Q36. Write a query to find all accounts that had transactions every month for the past 12 months (no gaps).

**Answer:**

```sql
WITH monthly_activity AS (
    SELECT
        account_id,
        DATE_TRUNC('month', transaction_date) AS txn_month
    FROM transactions
    WHERE transaction_date >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY account_id, DATE_TRUNC('month', transaction_date)
),
account_month_count AS (
    SELECT
        account_id,
        COUNT(DISTINCT txn_month) AS active_months
    FROM monthly_activity
    GROUP BY account_id
)
SELECT account_id
FROM account_month_count
WHERE active_months = 12  -- Present in all 12 months
ORDER BY account_id;
```

---

### Q37. Explain query tuning. How would you optimize a slow SQL query?

**Answer:**

When I get a slow query, I follow this systematic approach:

**Step 1 — Get the execution plan:**
```sql
EXPLAIN PLAN FOR SELECT ...;  -- Oracle
EXPLAIN SELECT ...;            -- PostgreSQL / Snowflake
```
Look for: full table scans, hash joins on large tables, sort operations on large result sets, cardinality estimates vs actual rows.

**Step 2 — Check indexes:**
Is the WHERE clause column indexed? Is the join column indexed?
```sql
-- If not indexed and frequently queried:
CREATE INDEX idx_gl_posting_date ON gl_transactions(posting_date);
CREATE INDEX idx_gl_account_date ON gl_transactions(account_id, posting_date); -- Composite for multi-column filter
```

**Step 3 — Reduce data early:**
Push filters as early as possible. Don't JOIN first then filter — filter first then JOIN:
```sql
-- Bad: Join all records, then filter
SELECT * FROM transactions t JOIN accounts a ON t.account_id = a.id
WHERE t.posting_date >= '2024-01-01';

-- Better: Filter before join (optimizer usually does this, but explicit is clearer)
SELECT * FROM (SELECT * FROM transactions WHERE posting_date >= '2024-01-01') t
JOIN accounts a ON t.account_id = a.id;
```

**Step 4 — Avoid functions on indexed columns in WHERE:**
```sql
-- Bad: Function on column prevents index use
WHERE YEAR(posting_date) = 2024

-- Good: Range filter uses index
WHERE posting_date >= '2024-01-01' AND posting_date < '2025-01-01'
```

**Step 5 — Snowflake-specific: clustering and result cache:**
- If repeatedly filtering on `fiscal_year`, define cluster key: `ALTER TABLE ... CLUSTER BY (fiscal_year)`
- Snowflake caches query results for 24 hours — identical queries are instant if underlying data hasn't changed

---

### Q38. Write a query to calculate a 7-day rolling average of daily transaction amounts per account.

**Answer:**

```sql
SELECT
    account_id,
    transaction_date,
    daily_amount,
    AVG(daily_amount) OVER (
        PARTITION BY account_id
        ORDER BY transaction_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW  -- Current day + 6 prior days = 7 days
    ) AS rolling_7day_avg
FROM (
    SELECT
        account_id,
        transaction_date,
        SUM(amount) AS daily_amount
    FROM transactions
    GROUP BY account_id, transaction_date
) daily_totals
ORDER BY account_id, transaction_date;
```

**Key syntax explanation:**
- `PARTITION BY account_id` — separate rolling window per account
- `ORDER BY transaction_date` — ordered chronologically
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` — physical row-based window (6 prior rows + current = 7 rows)
- Use `ROWS` not `RANGE` here — `RANGE BETWEEN 6 PRECEDING` would mean 6 units of the ORDER BY value (days), which handles gaps but behaves differently than `ROWS`

---

### Q39. How do you handle slowly growing query performance in Snowflake? What do you investigate?

**Answer:**

When Snowflake queries that previously ran in 5 seconds now take 45 seconds, I investigate in this order:

**1. Check if the warehouse suspended and resumed:**
A cold warehouse (just auto-resumed) takes extra time for the first query. Check warehouse activity history — if it was suspended, the slowdown is a one-time warmup cost.

**2. Check result cache:**
Snowflake caches results for 24 hours. If the underlying table changed (new data loaded), the cache is invalidated and the query re-executes. Is this slowdown after a data load?

**3. Check clustering depth:**
As data accumulates, a clustered table's clustering quality degrades:
```sql
SELECT SYSTEM$CLUSTERING_INFORMATION('gl_transactions', '(fiscal_year)');
-- If average_depth >> 1, reclustering is needed
ALTER TABLE gl_transactions RECLUSTER;
```

**4. Check query profile (Snowflake UI):**
The query profile shows per-step execution time. Look for:
- `TableScan` step processing far more rows than expected (partition pruning not working)
- `Sort` or `Join` steps consuming most time
- Spill to disk (yellow warning) — warehouse is too small for data being processed

**5. Check for missing filters on cluster key:**
```sql
-- Bad: No filter on fiscal_year (cluster key) — full table scan
SELECT * FROM gl_transactions WHERE cost_center = 'CC_001';

-- Good: Filter on cluster key first — prunes partitions
SELECT * FROM gl_transactions WHERE fiscal_year = 2024 AND cost_center = 'CC_001';
```

**6. Warehouse size:**
If data volume grew significantly (2x year-over-year), the same warehouse size processes more data. Upgrading from Small to Medium doubles compute and can halve query time.

---

## SECTION 9 — Python & PySpark
### Q40–Q43

---

### Q40. How do you ingest data from a paginated REST API in Python and write it to ADLS?

**Answer:**

```python
import requests
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from tenacity import retry, wait_exponential, stop_after_attempt
import json
from datetime import datetime

class APIIngestion:
    def __init__(self, base_url, client_id, client_secret, token_url):
        self.base_url = base_url
        self.token = self._get_token(client_id, client_secret, token_url)
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def _get_token(self, client_id, client_secret, token_url):
        resp = requests.post(token_url, data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        })
        resp.raise_for_status()
        return resp.json()["access_token"]
    
    @retry(wait=wait_exponential(min=2, max=30), stop=stop_after_attempt(5))
    def _fetch_page(self, endpoint, params):
        resp = requests.get(f"{self.base_url}/{endpoint}",
                          headers=self.headers, params=params, timeout=30)
        if resp.status_code == 429:
            raise Exception("Rate limited")
        resp.raise_for_status()
        return resp.json()
    
    def fetch_all(self, endpoint, page_size=500):
        all_records = []
        cursor = None
        
        while True:
            params = {"limit": page_size}
            if cursor:
                params["cursor"] = cursor
            
            data = self._fetch_page(endpoint, params)
            all_records.extend(data.get("records", []))
            
            cursor = data.get("next_cursor")
            if not cursor or not data.get("records"):
                break
        
        return all_records
    
    def write_to_adls(self, records, storage_account, container, path):
        df = pd.DataFrame(records)
        parquet_bytes = df.to_parquet(index=False)
        
        credential = DefaultAzureCredential()
        client = DataLakeServiceClient(
            account_url=f"https://{storage_account}.dfs.core.windows.net",
            credential=credential
        )
        
        file_client = client.get_file_client(container, path)
        file_client.upload_data(parquet_bytes, overwrite=True)
        print(f"Uploaded {len(records)} records to {path}")


# Usage
ingestion = APIIngestion(
    base_url="https://api.example.com",
    client_id=dbutils.secrets.get("kv", "api-client-id"),
    client_secret=dbutils.secrets.get("kv", "api-client-secret"),
    token_url="https://auth.example.com/token"
)

records = ingestion.fetch_all("transactions", page_size=500)
today = datetime.now().strftime("%Y/%m/%d")
ingestion.write_to_adls(
    records,
    storage_account="ecdpstorage",
    container="raw",
    path=f"api/transactions/{today}/data.parquet"
)
```

---

### Q41. How do you use Pandas for data transformation when PySpark is overkill?

**Answer:**

For small to medium datasets (< 1GB), Pandas is faster and simpler than spinning up a Spark cluster:

```python
import pandas as pd
import numpy as np

# Read from ADLS using pandas directly (small reference file)
df = pd.read_parquet("abfss://raw@storage.dfs.core.windows.net/ref/cost_centers.parquet")

# Common transformations I've done:

# 1. Handle duplicates
df = df.drop_duplicates(subset=["cost_center_key"], keep="last")

# 2. Standardize strings
df["cost_center_name"] = df["cost_center_name"].str.strip().str.upper()

# 3. Handle nulls
df["department"] = df["department"].fillna("UNASSIGNED")
df = df.dropna(subset=["cost_center_key"])  # Required field

# 4. Type casting
df["effective_date"] = pd.to_datetime(df["effective_date"], format="%Y%m%d")
df["amount"] = pd.to_numeric(df["amount_raw"].str.lstrip("0"), errors="coerce")

# 5. Conditional column
df["region_group"] = np.where(df["region"].isin(["APAC", "ANZ"]), "ASIA_PACIFIC", "OTHER")

# 6. Apply complex business logic row-by-row (only for small data)
def classify_cost_center(row):
    if row["cost_center_key"].startswith("CC_CORP"):
        return "CORPORATE"
    elif row["amount"] > 1_000_000:
        return "MAJOR"
    return "STANDARD"

df["classification"] = df.apply(classify_cost_center, axis=1)

# Write back to ADLS Silver
df.to_parquet("abfss://processed@storage.dfs.core.windows.net/ref/cost_centers.parquet",
              index=False)
```

**When to use Pandas vs PySpark:**
- Pandas: < 1GB, quick reference data transforms, rapid prototyping
- PySpark: > 1GB, cluster required anyway, complex joins across large tables

---

### Q42. How do you unit test PySpark transformation code?

**Answer:**

Testing Spark code is important and often neglected. Here's how I structured tests in ECDP:

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from datetime import date
from myproject.transformations import clean_gl_transactions  # function under test

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for tests"""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("unit_tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

class TestGLTransformations:
    
    def test_null_doc_numbers_are_filtered(self, spark):
        """Records with null doc_number should be excluded from Silver"""
        data = [
            ("DOC001", "CC_001", "1000.00"),
            (None, "CC_002", "2000.00"),   # Should be filtered
            ("DOC003", "CC_003", "3000.00")
        ]
        schema = ["doc_number", "cost_center", "amount_raw"]
        df_input = spark.createDataFrame(data, schema)
        
        df_result = clean_gl_transactions(df_input)
        
        assert df_result.count() == 2
        assert df_result.filter("doc_number IS NULL").count() == 0
    
    def test_amount_cast_strips_leading_zeros(self, spark):
        """SAP amounts like '0000050000' should cast to 50000.00"""
        data = [("DOC001", "CC_001", "0000050000")]
        df_input = spark.createDataFrame(data, ["doc_number", "cost_center", "amount_raw"])
        
        df_result = clean_gl_transactions(df_input)
        result_amount = df_result.select("amount").collect()[0][0]
        
        assert float(result_amount) == 50000.00
    
    def test_duplicate_doc_line_deduplicated(self, spark):
        """Duplicate doc_number + line_num should keep only the latest record"""
        data = [
            ("DOC001", "1", "2024-01-15", "1000.00"),
            ("DOC001", "1", "2024-01-16", "1100.00"),  # Same doc+line, later date
        ]
        df_input = spark.createDataFrame(data, ["doc_number", "line_num", "updated_at", "amount_raw"])
        
        df_result = clean_gl_transactions(df_input)
        
        assert df_result.count() == 1
        assert df_result.collect()[0]["amount_raw"] == "1100.00"  # Latest kept
```

**Run tests:**
```bash
pytest tests/test_transformations.py -v --tb=short
```

---

### Q43. How did you use PySpark for a complex transformation in your project — walk through actual code?

**Answer:**

In ECDP, I had a Silver → Gold transformation that needed: join GL transactions with cost center dimension, calculate period-over-period spend variance, and flag unusual spend.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, lag, when, abs, round, current_timestamp,
    broadcast, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

spark = SparkSession.builder.getOrCreate()

# Read Silver
df_txn = spark.read.format("delta").load("/silver/gl_transactions/")
df_cc = spark.read.format("delta").load("/silver/dim_cost_center/")

# Aggregate to monthly cost center spend (Silver → intermediate)
df_monthly = df_txn \
    .groupBy("fiscal_year", "fiscal_period", "cost_center_key") \
    .agg(
        sum("amount").alias("total_amount"),
        sum("amount_usd").alias("total_amount_usd")
    )

# Join with cost center dimension (broadcast — small table)
df_enriched = df_monthly.join(
    broadcast(df_cc.select("cost_center_key", "department", "region", "manager")),
    "cost_center_key",
    "left"
)

# Calculate period-over-period variance using window function
window_spec = Window \
    .partitionBy("cost_center_key") \
    .orderBy("fiscal_year", "fiscal_period")

df_with_variance = df_enriched \
    .withColumn("prev_period_amount", lag("total_amount_usd", 1).over(window_spec)) \
    .withColumn(
        "variance_pct",
        when(col("prev_period_amount").isNull(), lit(None))
        .when(col("prev_period_amount") == 0, lit(None))
        .otherwise(
            round(
                (col("total_amount_usd") - col("prev_period_amount"))
                / abs(col("prev_period_amount")) * 100,
                2
            )
        )
    ) \
    .withColumn(
        "variance_flag",
        when(col("variance_pct") > 20, lit("HIGH_INCREASE"))
        .when(col("variance_pct") < -20, lit("HIGH_DECREASE"))
        .otherwise(lit("NORMAL"))
    )

# Write to Gold
df_with_variance \
    .withColumn("loaded_at", current_timestamp()) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "fiscal_year = 2024") \
    .partitionBy("fiscal_year") \
    .save("/gold/cost_center_monthly_spend/")

print(f"Gold layer updated: {df_with_variance.count()} rows")
```

---

## SECTION 10 — Architecture, Design & Stakeholder Scenarios
### Q47–Q50

---

### Q44. How do you mentor junior data engineers? Give a specific example.

**Answer:**

In ECDP, I worked alongside two junior engineers who were new to PySpark. Here's a specific example:

A junior engineer had written a transformation that used `.collect()` to pull all records to the driver, iterate in Python, and then create a new DataFrame:

```python
# What they wrote — BAD pattern
rows = df.collect()  # Pulls 5M rows to driver
result = []
for row in rows:
    if row['amount'] > 1000:
        result.append(row)
new_df = spark.createDataFrame(result)
```

Instead of just saying "this is wrong," I sat with them and:

1. **Explained why it fails**: collect() on 5M rows causes driver OOM — I showed them the Spark UI memory spike
2. **Showed the correct pattern**:
```python
# Correct — stays distributed, never moves to driver
new_df = df.filter(col("amount") > 1000)
```
3. **Connected it to a mental model**: "Think of Spark as 20 workers in a warehouse. collect() is like asking all 20 workers to carry everything to your desk. filter() lets each worker do the check themselves."

4. **Added a PR checklist item**: After this, I added "no `.collect()` except for row count validation or small result sets < 1000 rows" to our team's PR review checklist.

5. **Gave them the next challenge**: Asked them to refactor two more notebooks applying the same principle — learning by doing.

---

### Q45. How do you estimate effort for a data pipeline that involves a source you've never worked with before?

**Answer:**

Unknown sources are the biggest estimation risk. I handle this with a **spike / discovery phase**:

**Week 1 — Discovery spike (timeboxed):**
- Get sample data from the source (even 1000 rows is enough to start)
- Assess: data quality issues, schema complexity, volume/frequency, access mechanism (JDBC, API, file, CDC)
- Build a prototype end-to-end — just Bronze write, minimal transformation, no production-quality error handling
- This spike produces an informed estimate, not a guess

**Estimation breakdown I use:**
| Component | Multiplier |
|-----------|-----------|
| Simple source (flat, clean, small) | 1x base |
| Complex source (nested, dirty, large) | 2-3x base |
| New source type (never done before) | +1 week discovery |
| Third-party API with rate limits | +50% for retry/pagination logic |
| DEV → UAT → PROD deployment | +1 week per environment |
| Unknown data quality | +25% buffer |

**Base estimate for a typical incremental pipeline:**
- Design + review: 2 days
- Bronze ingestion: 1-2 days
- Silver transformation: 3-5 days
- Gold aggregation: 2-3 days
- Testing (DEV): 2 days
- UAT deployment + validation: 2 days
- PROD deployment: 1 day

**Total: ~2.5-3 weeks** for a standard pipeline from a known source.
Add 1 week for discovery if source is new. Add 1 week if data quality is poor.

I always present estimates as a range with assumptions, not a single number — and I flag what could cause the estimate to grow.

---

### Q46. A pipeline is loading data correctly but business says the numbers don't match their Excel report. How do you debug this?

**Answer:**

This is a reconciliation problem. I treat it systematically rather than guessing.

**Step 1 — Understand the discrepancy:**
Get specifics from the business user: which report, which metric, which date range, what the difference is. "Numbers don't match" is too vague — I need: "Pipeline shows $48.2M total spend for Jan 2024, Excel shows $49.7M — difference of $1.5M."

**Step 2 — Reproduce the business calculation:**
Get the Excel file. Understand the exact formula — which source data did the Excel pull from? Which filters are applied? (This is often where the issue is — different filter assumptions.)

**Step 3 — Trace from Gold back to Source:**

```python
# Step A: Compare Gold total to Silver total
gold_total = spark.read.format("delta").load(gold_path) \
    .filter("fiscal_year=2024 AND fiscal_period=1") \
    .agg(sum("total_spend")).collect()[0][0]

silver_total = spark.read.format("delta").load(silver_path) \
    .filter("fiscal_year=2024 AND fiscal_period BETWEEN 1 AND 1") \
    .agg(sum("amount")).collect()[0][0]

print(f"Gold: {gold_total}, Silver: {silver_total}")
# If these differ → issue in Gold aggregation logic
# If they match → look further upstream
```

**Common root causes I've found:**
1. **Filter difference**: Excel includes records that pipeline excludes (e.g., pipeline excludes reversals, Excel doesn't)
2. **Currency handling**: Excel uses one FX rate, pipeline uses a different rate
3. **Date boundary**: Excel uses posting_date, pipeline uses document_date — different fields
4. **Intercompany eliminations**: Excel nets out intercompany, pipeline doesn't (or vice versa)
5. **Timing**: Excel pulled data at 9 AM, pipeline ran at 6 AM — 3 hours of new transactions not yet in pipeline

**How I communicated the resolution in ECDP:**
Found the issue was filter-based — Excel was including transactions with `document_type = 'SA'` but the pipeline was excluding them (classified as system adjustments). Confirmed with Finance team that SA documents should be included for their report. Updated the filter, reran Gold, numbers matched.

---

### Q47. How do you approach building a data pipeline from scratch for a new client?

**Answer:**

I follow a structured discovery-to-delivery approach:

**Phase 1 — Discovery (Week 1-2):**
- Understand the business question: "What decision will this data support?" drives all technical choices
- Inventory data sources: formats, volumes, frequencies, access mechanisms, data quality
- Understand consumers: who reads this data, using what tools, with what query patterns
- Document existing ETL/reports that this replaces or complements

**Phase 2 — Architecture Design (Week 2-3):**
Choose the right tools for the job:
- Orchestration: ADF for Azure-native, Airflow for multi-cloud/on-prem
- Storage: Delta Lake for ACID + time travel, Iceberg for multi-engine
- Processing: Databricks for heavy PySpark, Synapse for SQL-heavy
- Serving: Snowflake for SQL analytics, Databricks SQL for lakehouse queries, Power BI for self-service

Produce: architecture diagram, data flow diagram, data dictionary, SLA commitments.

**Phase 3 — PoC (Week 3-4):**
Build the hardest part end-to-end on a small dataset. This de-risks the project and validates architectural choices before full investment.

**Phase 4 — Build (Week 4-8):**
Following medallion architecture, build Bronze → Silver → Gold with:
- Metadata-driven pipelines (not one-per-table)
- Data quality at each layer
- Comprehensive logging and alerting
- CI/CD from day one (not retrofitted at the end)

**Phase 5 — Validate and handover:**
Parallel run, reconciliation against existing reports, documentation, knowledge transfer to operations team.

---

### Q48. What would you do if your pipeline starts failing every day at the same time?

**Answer:**

A consistent time-based failure pattern usually points to a few specific causes.

**Hypothesis 1 — Source system maintenance window:**
Many production systems have a nightly maintenance window (e.g., 2-4 AM) where they're unavailable or slow. If our pipeline runs at 2:30 AM, it'll fail every night.

**Check**: Query source system availability logs for that time window. Fix: shift pipeline trigger time by 2 hours.

**Hypothesis 2 — Resource contention:**
Another heavy job runs at the same time (end-of-day reporting, another ETL, database backup). Our pipeline competes for cluster resources or database connections.

**Check**: Look at cluster utilization in Databricks or Azure Monitor — is the cluster hitting CPU/memory ceiling at exactly that time? Fix: stagger job schedules, increase cluster size, or add a separate cluster for our pipeline.

**Hypothesis 3 — Data volume spike at that time:**
Source system batch-commits a large number of transactions at end-of-business (e.g., 5 PM close of business). Our incremental pipeline that normally processes 10K records suddenly processes 500K — and times out.

**Check**: Look at row count trend in pipeline audit table — is the count much higher on failing runs? Fix: increase cluster size, tune `spark.sql.shuffle.partitions`, or increase ADF activity timeout.

**Hypothesis 4 — Token/certificate expiry:**
OAuth tokens and SSL certificates expire. A token with a 24-hour lifetime that was issued at 9 AM fails at 9 AM the next day if not refreshed.

**Check**: Is the error message authentication-related? Fix: implement token refresh logic, or increase token lifetime.

**How I'd debug in practice:**
```python
# Check audit table for failure pattern
SELECT DATE(start_time), COUNT(*), AVG(duration_secs), MAX(error_message)
FROM pipeline_audit
WHERE status = 'FAILED'
GROUP BY DATE(start_time)
ORDER BY DATE(start_time);
```
If failures are every day at the exact same time → resource/schedule conflict.
If failures are intermittent but cluster to the same time → data volume spike.

---

### Q49. How do you balance pipeline speed vs cost in a cloud environment?

**Answer:**

This is a constant tension in cloud data engineering — more compute = faster pipelines but higher cost.

**Framework I use — classify pipelines by SLA:**

| SLA Tier | Requirement | Strategy |
|----------|------------|---------|
| **Critical** (finance close, regulatory) | < 1 hour, 99.9% reliability | Dedicated cluster, larger size, no auto-terminate |
| **Standard** (daily analytics) | < 4 hours | Job cluster, right-sized, auto-terminate |
| **Non-critical** (weekly reports) | < 24 hours | Spot/preemptible instances (60-80% cheaper), longer timeout |

**Cost optimization techniques I've used:**

**1. Spot instances for batch jobs:**
Databricks spot instances cost 60-80% less than on-demand. For non-critical batch pipelines, I use spot with an on-demand fallback:
- Primary: 80% spot workers
- Fallback: 20% on-demand workers (so the job doesn't die if spots are reclaimed)

**2. Auto-scaling:**
Set min/max workers — cluster scales up for heavy processing, scales down (or terminates) when idle:
```
min_workers = 2, max_workers = 10, auto_terminate = 30 min
```

**3. Cluster pools:**
Pre-warm a pool of VMs. When a job cluster needs nodes, it gets them from the pool in 30 seconds instead of 5 minutes of cold start. Improves latency without paying for always-on clusters.

**4. Delta caching:**
Cache frequently-read Delta tables on SSDs of workers — avoids repeated reads from ADLS (which have per-operation costs). Especially valuable for Silver tables read by multiple Gold jobs.

**5. Right-size before production:**
Profile the job on DEV with production-scale data. Find the inflection point: at what worker count does adding more workers yield < 10% speedup? That's your right-size.

In ECDP, I reduced monthly Databricks spend by 35% by switching non-SLA pipelines to spot instances and reducing `spark.sql.shuffle.partitions` from 200 to 50 for datasets where 200 partitions caused excessive overhead.

---

### Q50. You have 30 minutes to present the architecture of your ECDP project to a new client. What do you cover?

**Answer:**

I'd structure a 30-minute presentation like this:

**Minutes 0-5 — Business context:**
"ECDP is a financial data platform for [client] processing 16TB+ of financial data from Oracle source systems. The goal: give finance teams access to clean, trusted data within 4 hours of business day close. Previously, analysts waited 2-3 days for manual extracts."

**Minutes 5-10 — Architecture overview:**
Show a single-page diagram:
```
Oracle (on-prem) → [ADF + SHIR] → ADLS Gen2 Bronze → [Databricks PySpark] → Silver → Gold → Snowflake → Power BI
                                        ↑                      ↑
                                   Metadata-driven          Delta Lake
                                   pipelines                ACID + Time Travel
```

**Minutes 10-15 — Key design decisions + why:**
- Medallion architecture: "Bronze preserves raw data for reprocessing. Silver is the single source of truth. Gold is optimized for consumption."
- Metadata-driven pipelines: "One pipeline template serves 25 source tables. Adding a new source is a config table update, not a code change."
- Delta Lake: "ACID transactions mean a failed mid-run job never leaves the table in a corrupt state. Time travel lets us answer 'what did the data look like last week?'"
- Dual target (Delta + Snowflake): "Data engineers use Delta directly. Finance teams use Snowflake — familiar SQL, existing BI tool connectors."

**Minutes 15-20 — Reliability and governance:**
- Pipeline monitoring, audit table, alerting
- Key Vault for all credentials, no hardcoded secrets
- CI/CD: DEV → UAT → PROD with ARM templates and YAML pipelines
- Row/column-level security in both Databricks Unity Catalog and Snowflake

**Minutes 20-25 — Results:**
- Pipeline runtime: went from 4 hours to 58 minutes after optimization
- Data freshness: from 2-3 days manual to 4-hour automated
- Reliability: 99.2% pipeline success rate over 3-month production period
- Cost: 35% reduction in Databricks compute cost after right-sizing

**Minutes 25-30 — Q&A + what I'd do differently:**
"In Phase 2, I'd add Delta Live Tables for managed pipeline orchestration with built-in data quality, Event Hubs for near-real-time ingestion for time-sensitive financial data, and Microsoft Purview for automated cross-system data lineage."

---

## QUICK REFERENCE — YOUR KEY NUMBERS

| Metric | Value |
|--------|-------|
| ECDP data volume | 16TB+ financial data |
| Pipeline runtime improvement | 4 hours → 58 minutes |
| Spark job improvement | 3 hours → 35 minutes |
| JDBC read improvement | 2 hours → 18 minutes |
| MERGE improvement | 45 minutes → 6 minutes |
| Small files query improvement | 22 minutes → 2.5 minutes |
| FinOps PoC revenue generated | $5,000 immediate |
| Databricks cost reduction | 35% via spot + tuning |
| Source tables handled | 25+ (ECDP), 15+ (FinOps) |
| Pipeline success rate | 99.2% over 3 months |

---

*Prepared for Deloitte Data Engineer II Interview — covers all JD requirements mapped to your profile*




**Azure Event Hubs**

Think of it as a massive, super-fast mailbox that can receive millions of messages per second. It's used when you have a constant stream of events coming in — like financial transactions, IoT sensor readings, or clickstream data — and you need to capture all of them without losing any, then process them in near real-time.

Interview-ready definition: "Event Hubs is a real-time event streaming service. Producers push events into it, and consumers (like Databricks Structured Streaming) read from it continuously. It retains events for a configurable period — say 7 days — so if a consumer goes down, it can replay from where it left off."

Real use case to mention: "If a bank wanted real-time fraud detection on transactions, instead of waiting for a nightly batch, transactions would stream into Event Hub the moment they happen, and Databricks would process them within seconds using Structured Streaming."

Key term to drop: it works on the same protocol as Apache Kafka, so if someone knows Kafka, you can say "it's Azure's managed, Kafka-compatible alternative."

---

**Azure Synapse Analytics**

This is Azure's big data analytics and data warehousing service. The simplest way to describe it: it combines SQL-based data warehousing (like a traditional data warehouse) with big data processing (like Spark) in one workspace.

Interview-ready definition: "Synapse is an analytics platform that lets you run SQL queries on huge volumes of structured data using dedicated SQL pools, and also run Spark jobs for big data processing — all in a single workspace with shared metadata."

Where it fits vs Databricks: "Databricks is generally stronger for heavy ETL, machine learning, and complex PySpark transformations. Synapse is strong when your team is SQL-first and you want a data warehouse experience with serverless SQL queries directly over your data lake, plus tight integration with Power BI."

If asked "did you use Synapse" and you haven't hands-on: be honest — "My direct production experience is with Databricks for the heavy lift transformation work, but I understand Synapse serves a similar role with a stronger SQL-warehouse angle, often used by teams that want a more SQL-native experience or need Synapse's dedicated SQL pools for very large structured reporting workloads."

---

**Azure Functions**

This is serverless compute — you write a small piece of code (a "function"), and Azure runs it only when triggered, then shuts it down. You don't manage any servers.

Interview-ready definition: "Azure Functions let you run code in response to an event — a file landing in storage, an HTTP request, a message in a queue, or a timer — without provisioning or managing infrastructure. You pay only for the time the function actually runs."

Real use case to mention: "I'd use a Functions to validate a file the moment it lands in ADLS — check the filename pattern, size, and basic structure — and then trigger the ADF pipeline only if it passes validation. Without Functions, you'd have to run a scheduled check every few minutes even when nothing has changed, which wastes compute."

Key point: Functions are event-driven and short-lived (typically seconds to a few minutes), which is different from Databricks notebooks that handle the heavy data processing.

---

**Logic Apps**

This is a visual, low-code workflow orchestration tool — think of it as connecting different services together (Outlook, Teams, SharePoint, databases, APIs) using a drag-and-drop designer instead of writing code.

Interview-ready definition: "Logic Apps is a workflow automation service used to orchestrate business processes and integrate different systems with minimal code. It has hundreds of pre-built connectors — Outlook, Slack, Salesforce, SQL Server, REST APIs."

Real use case to mention: "I'd use Logic Apps for the alerting layer of a pipeline — when an ADF pipeline fails, it calls a Logic App via a Web Activity, and the Logic App sends an email to the on-call engineer, posts a message to Teams, and logs the incident to a ticketing system — all without writing a single line of orchestration code."

Functions vs Logic Apps — the question they might ask: "Functions is for when you need to write actual code logic — like custom validation or transformation. Logic Apps is for when you're stitching together existing services and don't need complex code, just configuration."

---

**Queue Storage / Event Grid**

These are two different things often mentioned together, so separate them clearly.

**Queue Storage** is a simple message queue — think of it as a to-do list that different parts of your system can add tasks to and pick tasks off of, one at a time, in order.

Interview-ready definition: "Queue Storage is a simple, reliable message queue used to decouple parts of a system. A producer adds a message, a consumer picks it up and processes it, then deletes it. It's simple, cheap, and great for basic task queuing."

**Event Grid** is different — it's a pub-sub event routing service. It doesn't carry the message payload itself usually; it tells you that something happened and where to go look.

Interview-ready definition: "Event Grid is an event routing service — when something happens in Azure, like a new file landing in Blob Storage or a VM being created, Event Grid detects that event and pushes a notification to whatever is subscribed — an Azure Function, Logic App, or another endpoint. It's the glue that makes event-driven architecture possible without polling."

The comparison they'll likely ask: "Queue Storage is for task processing — work items waiting to be picked up. Event Grid is for event notification — telling subscribers that something happened, in near real-time, with very high scale, like millions of events per second."

---

**How to tie all five together if asked "design an event-driven pipeline":**

"A file lands in ADLS → Event Grid detects the BlobCreated event → triggers an Azure Function to do quick validation → if valid, the Function either calls ADF directly or drops a message in Queue Storage for a downstream process to pick up → meanwhile, if anything fails, a Logic App handles sending the alert email or Teams message → and if this were a high-volume streaming scenario instead of file-based, Event Hubs would be the ingestion layer instead of file landing, feeding directly into Databricks Structured Streaming → and Synapse could be the SQL-based reporting layer on top of the final curated data, alongside or instead of Snowflake."

That one sentence shows you understand how all five pieces fit together in a real architecture, which is usually exactly what they're testing for.
