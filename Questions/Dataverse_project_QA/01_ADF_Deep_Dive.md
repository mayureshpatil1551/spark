# ADF Deep Dive — CSV Ingestion, Key Vault, Batch Tables, Parameterization
**Mayuresh | Data Engineer | Cognizant Technology Solutions**

---

## Part - 0

---

### Tell me about yourself / Give me a brief introduction.

Hi, good afternoon, my name is Mayuresh Patil.
I am a Data Engineer with about 3 years of experience building scalable pipelines and lakehouse architectures across cloud ecosystems.

Currently, I’m working at Cognizant, where I’ve contributed to two major enterprise projects. 
The one was a e-clinical data plateform, in that we are pharmaceutical data migration into Apache Iceberg on AWS S3. I built ingestion pipelines from sources like Oracle, CSV, XML, and Salesforce APIs, and optimized Spark jobs to improve query speeds by 40% and reduce pipeline execution time by 30%.

The second one was an FinOps Dataverse for 16TB+ of financial data on Azure. I designed Bronze–Silver–Gold architecture on ADLS Gen2, developed ADF pipelines, wrote PySpark transformation notebooks, and set up CI/CD pipelines for automated deployments.

My technical expertise includes PySpark, SQL, Python, Databricks, ADF, and ADLS Gen2, and I’m certified in Databricks and Microsoft Azure Fundamentals. Beyond the technical side, I follow Medallion architecture principles to ensure strong data lineage and compliance.

In short, I bring a mix of hands‑on engineering, performance optimization, and cloud expertise, and I’m passionate about designing reliable, efficient data solutions that directly support business outcomes.

---

### Explain your ECDP — eClinical Data Platform project.

FinOps Dataverse was a Big Data migration project in the pharmaceutical domain. It handled 20TB+ of legacy data across multiple source systems.

My role was Data Engineer, and we followed a Medallion architecture — Raw, Transformation Layer (TL), ATL, and Outbound.

Let me walk you through each layer:

RAW LAYER (Ingestion)
We ingested data from six different source types — CSV, JSON, XML files from S3; Oracle tables; Snowflake tables; Salesforce REST API; and Smartsheet API. For each source type, I wrote separate generic PySpark code so the same framework could be reused for any table or file in that source system. We landed everything into Apache Iceberg tables in the Raw layer. We supported both full load and incremental merge, and handled token expiry for APIs automatically with retry logic.

TRANSFORMATION LAYER (TL & ATL)
From Raw, we applied data cleansing, type casting, deduplication, and business validations before loading into the TL Iceberg tables — always using incremental merge. I also resolved Spark performance issues here — skew handling, small file problem, partitioning, and bucketing. This reduced pipeline execution time by 30% and improved 300+ analytical query speeds by 40%.

OUTBOUND LAYER
Finally, we synced the processed data to Oracle tables and CSV flat files for downstream consumers — with zero manual intervention.

One key achievement I'm proud of: I built Python scripts that auto-generated SQL configuration files, which saved the team 15+ hours per week of manual setup work.

We also delivered the SAP financial hierarchy migration — which was estimated at 8 weeks — in just 2.5 weeks, 60% faster than planned. That PoC secured $5,000 in immediate revenue for the business unit.

All code was managed in GitHub, and orchestration was done via Nabu Modak, an enterprise pipeline orchestration tool.

---

### Explain your FinOps Dataverse project.?
ECDP is an Enterprise Cloud Data Platform that processes 16TB+ of financial data on Azure. My role was Data Engineer, and the key technologies were ADF, Databricks, Delta Lake, ADLS Gen2, and CI/CD through Azure DevOps.

We followed the Medallion architecture — Bronze, Silver, and Gold layers.

ARCHITECTURE OVERVIEW
Bronze layer is the raw landing zone on ADLS Gen2 — raw files land here as-is with no transformation. Silver layer holds cleaned, validated Delta tables on Databricks. Gold layer holds aggregated, business-ready Delta tables used by BI tools.

BRONZE LAYER — DATA INGESTION (via ADF)
I built ADF pipelines to ingest data from two source systems: CSV flat files and Oracle database tables. For incremental load, we used a watermark-based incremental pattern — storing the last updated timestamp in a control table and fetching only changed rows on each run. All raw data was loaded into ADLS Gen2 in Parquet format.

SILVER LAYER — TRANSFORMATION (via Databricks + PySpark)
ADF calls Databricks notebooks using the Databricks Linked Service. In the notebooks, I read from Bronze Parquet files, applied schema enforcement, data type casting, null handling, and deduplication. The final write to Silver is always using Delta MERGE — so the load is incremental and idempotent, meaning even if a pipeline reruns, data is not duplicated.

GOLD LAYER — AGGREGATION
From Silver Delta tables, I created Gold-layer aggregated tables for analytics and reporting. I applied Delta OPTIMIZE and Z-ORDER on high-filter columns to speed up BI queries significantly.

CI/CD PIPELINE
This is something I'm proud of here. I set up CI/CD using Azure DevOps and GitHub Actions. ADF pipelines are exported as ARM templates and stored in GitHub. Databricks notebooks are also version-controlled. On every merge to the main branch, the CI/CD pipeline automatically deploys to production — Dev, UAT, and Prod each have their own parameter files, so the same code deploys to all three environments without any manual changes.

Overall, this project gave me deep hands-on experience with the full Azure Data Engineering stack — from ingestion to transformation to deployment automation.



## PART 1 — CSV (ADLS Gen2) → Bronze Delta (Incremental Load, End to End)

---

### Big Picture Flow

```
ADLS Gen2                    ADF                          ADLS Gen2               Databricks
─────────────────            ─────────────────────────    ─────────────────────   ──────────────────────
landing/                     Get Metadata Activity   →    staging/                bronze_csv_to_delta
  claims_20240115.csv   →    ForEach Activity        →      claims_20240115/          notebook
  claims_20240116.csv        Copy Activity           →        *.parquet           writes Delta files to
                             Delete Activity                                       bronze/delta/claims/
                             (archive processed file)
```

---

### Step 1 — ADLS Gen2 Folder Structure

```
adls-container/
│
├── landing/                          ← source drops CSV files here (external team / upstream system)
│   ├── claims/
│   │   ├── claims_20240115.csv
│   │   └── claims_20240116.csv
│   └── patients/
│       └── patients_20240115.csv
│
├── staging/                          ← ADF stages Parquet here before Databricks reads
│   └── claims/
│       └── run_date=2024-01-15/
│           └── part-00000.parquet
│
├── archive/                          ← processed CSV files moved here (audit trail)
│   └── claims/
│       └── 2024-01-15/
│           └── claims_20240115.csv
│
└── delta/                            ← final Bronze Delta table (Databricks writes here)
    └── claims/
        ├── run_date=2024-01-15/
        │   └── part-00000.snappy.parquet
        └── _delta_log/
```

---

### Step 2 — ADF Linked Services Needed

```
1. ADLS Gen2 Linked Service         → to read landing/, write staging/, delete/archive files
2. Azure Databricks Linked Service  → to trigger notebook
3. Azure Key Vault Linked Service   → to fetch secrets (covered in Part 2)
```

---

### Step 3 — ADF Datasets

**Dataset 1 — Source CSV Dataset (Parameterized)**
```
Name:       DS_CSV_Source
Type:       Delimited Text (CSV)
Linked Service: LS_ADLS_Gen2
Parameters:
  - p_folder_path   (string)   e.g. landing/claims
  - p_file_name     (string)   e.g. claims_20240115.csv

File Path:  @{dataset().p_folder_path}/@{dataset().p_file_name}
First row as header: Yes
```

**Dataset 2 — Sink Parquet Dataset (Parameterized)**
```
Name:       DS_Parquet_Staging
Type:       Parquet
Linked Service: LS_ADLS_Gen2
Parameters:
  - p_entity        (string)   e.g. claims
  - p_run_date      (string)   e.g. 2024-01-15

File Path:  staging/@{dataset().p_entity}/run_date=@{dataset().p_run_date}/
```

---

### Step 4 — ADF Pipeline: csv_incremental_to_bronze

```
Pipeline Parameters:
  - p_entity     (string)   e.g. "claims"
  - p_run_date   (string)   e.g. "2024-01-15"   (passed by master pipeline or trigger)
```

#### Activity 1 — Get Metadata (list all CSV files in landing folder)

```
Activity Name:  ACT_GetMetadata_LandingFiles
Type:           Get Metadata
Dataset:        DS_CSV_Source
  p_folder_path = landing/@{pipeline().parameters.p_entity}
  p_file_name   = *                          ← wildcard to list all files
Field List:
  - childItems                               ← returns array of {name, type} objects
```

Output example:
```json
{
  "childItems": [
    { "name": "claims_20240115.csv", "type": "File" },
    { "name": "claims_20240116.csv", "type": "File" }
  ]
}
```

---

#### Activity 2 — If Condition (skip if no files)

```
Activity Name:  ACT_IfFiles_Exist
Type:           If Condition
Expression:     @greater(length(activity('ACT_GetMetadata_LandingFiles').output.childItems), 0)

True path  → proceed to ForEach
False path → Web Activity to log "no files found" and exit
```

---

#### Activity 3 — ForEach (loop through each CSV file)

```
Activity Name:  ACT_ForEach_Files
Type:           ForEach
Items:          @activity('ACT_GetMetadata_LandingFiles').output.childItems
IsSequential:   false          ← parallel processing (set batchCount: 4 for 4 parallel)
```

**Inside ForEach — 3 child activities:**

---

#### Activity 3a — Copy Activity (CSV → Parquet staging)

```
Activity Name:  ACT_Copy_CSV_to_Parquet
Type:           Copy

SOURCE:
  Dataset:      DS_CSV_Source
  Parameters:
    p_folder_path = landing/@{pipeline().parameters.p_entity}
    p_file_name   = @{item().name}            ← current file from ForEach loop
  Additional columns:
    source_file_name = @{item().name}         ← adds filename as extra column in output

SINK:
  Dataset:      DS_Parquet_Staging
  Parameters:
    p_entity    = @{pipeline().parameters.p_entity}
    p_run_date  = @{pipeline().parameters.p_run_date}
  Copy behavior: MergeFiles                   ← merge all row groups into fewer files

SETTINGS:
  Fault tolerance: Skip incompatible rows     ← don't fail on single bad row
  Log incompatible rows: Yes → write to ADLS error log path
```

---

#### Activity 3b — Delete Activity (move file to archive)

```
Activity Name:  ACT_Delete_LandingFile
Type:           Delete
Dataset:        DS_CSV_Source
  p_folder_path = landing/@{pipeline().parameters.p_entity}
  p_file_name   = @{item().name}

Runs after:     ACT_Copy_CSV_to_Parquet → Success only
                ↑ critical — only delete if copy succeeded
```

> Note: ADF Delete Activity deletes permanently. If you want archive (keep copy),
> add a Copy Activity first that copies to archive/ path, THEN delete from landing/.

**Archive copy (optional but recommended):**
```
ACT_Archive_CSV:
  Source: landing/@{entity}/@{item().name}
  Sink:   archive/@{entity}/@{run_date}/@{item().name}
  Run before: ACT_Delete_LandingFile
```

---

#### Activity 4 — Databricks Notebook Activity (Parquet → Bronze Delta)

```
Activity Name:  ACT_Databricks_Bronze_Write
Type:           Azure Databricks Notebook
Linked Service: LS_Databricks
Notebook path:  /Repos/ecdp/notebooks/bronze/csv_to_delta
Parameters:
  entity    = @{pipeline().parameters.p_entity}
  run_date  = @{pipeline().parameters.p_run_date}

Runs after: ACT_ForEach_Files → Success only
            ↑ entire ForEach must complete before Databricks runs
```

---

#### Activity 5 — Web Activity (failure notification)

```
Activity Name:  ACT_Notify_Failure
Type:           Web Activity
URL:            https://outlook.office.com/webhook/...  (Teams webhook)
Method:         POST
Body:           {
                  "text": "Pipeline @{pipeline().Pipeline} failed. 
                           Entity: @{pipeline().parameters.p_entity}.
                           Run: @{pipeline().RunId}"
                }
Runs after:     Any activity → Failed
```

---

### Step 5 — Databricks Notebook: CSV Parquet → Bronze Delta

```python
# ── Parameters from ADF ────────────────────────────────────────────────────────
dbutils.widgets.text("entity",   "claims")
dbutils.widgets.text("run_date", "2024-01-15")

entity   = dbutils.widgets.get("entity")
run_date = dbutils.widgets.get("run_date")

# ── Paths ──────────────────────────────────────────────────────────────────────
storage_account = "ecdpstorage"
staging_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/staging/{entity}/run_date={run_date}/"
bronze_path  = f"abfss://bronze@{storage_account}.dfs.core.windows.net/delta/{entity}/"

# ── Read Parquet staged by ADF ─────────────────────────────────────────────────
from pyspark.sql.functions import current_timestamp, lit, input_file_name, to_date

df = spark.read.format("parquet").load(staging_path)

print(f"Records from staging: {df.count()}")

# ── Add Bronze audit columns ───────────────────────────────────────────────────
df = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system",       lit("csv")) \
    .withColumn("entity",              lit(entity)) \
    .withColumn("run_date",            lit(run_date)) \
    .withColumn("source_file",         col("source_file_name"))  # passed from ADF Copy

# ── Write to Bronze Delta (APPEND only, no table registration) ─────────────────
df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("run_date") \
    .option("mergeSchema", "true") \
    .save(bronze_path)

print(f"Bronze write complete: {df.count()} records → {bronze_path}")

# ── Cleanup staging files after successful Bronze write ────────────────────────
dbutils.fs.rm(staging_path, recurse=True)
```

---

### Full CSV Incremental Pipeline Flow (Visual)

```
TRIGGER (Tumbling Window daily)
    │
    ▼
csv_incremental_to_bronze pipeline
    │
    ├── ACT_GetMetadata ──→ [claims_20240115.csv, claims_20240116.csv]
    │
    ├── ACT_IfCondition ──→ files exist? YES
    │
    ├── ACT_ForEach (parallel, 4 at a time)
    │   ├── File 1: claims_20240115.csv
    │   │   ├── ACT_Archive_CSV  → archive/claims/2024-01-15/claims_20240115.csv
    │   │   ├── ACT_Copy         → staging/claims/run_date=2024-01-15/*.parquet
    │   │   └── ACT_Delete       → removes claims_20240115.csv from landing/
    │   │
    │   └── File 2: claims_20240116.csv
    │       ├── ACT_Archive_CSV  → archive/claims/2024-01-16/claims_20240116.csv
    │       ├── ACT_Copy         → staging/claims/run_date=2024-01-16/*.parquet
    │       └── ACT_Delete       → removes claims_20240116.csv from landing/
    │
    └── ACT_Databricks_Bronze_Write
            ↓
        reads staging/claims/run_date=2024-01-15/
        appends to delta/claims/ (partitioned by run_date)
```

---

## PART 2 — Azure Key Vault for Oracle and CSV Ingestion

---

### Why Key Vault?

Never store credentials in ADF pipelines, notebooks, or code.

```
BAD:  username = "oracle_user"
      password = "MyP@ssw0rd123"      ← hardcoded in notebook, visible in GitHub

GOOD: password = dbutils.secrets.get(scope="ecdp-kv", key="oracle-password")
                                       ← fetched from Key Vault at runtime, never in code
```

---

### Step 1 — Store Secrets in Azure Key Vault

Create these secrets in Azure Key Vault (done once by infra/DevOps team):

```
For Oracle:
  Secret Name: oracle-jdbc-url       Value: jdbc:oracle:thin:@//oracle-host:1521/ECDPDB
  Secret Name: oracle-username       Value: ecdp_reader
  Secret Name: oracle-password       Value: MySecureP@ss123

For ADLS Gen2 (CSV source storage):
  Secret Name: adls-storage-account-key    Value: base64encodedstoragekey==
  Secret Name: adls-account-name          Value: ecdpstorage

For Databricks:
  Secret Name: databricks-pat-token   Value: dapi1234abcd...
```

---

### Step 2 — ADF Linked Service to Key Vault

Create one Key Vault Linked Service in ADF. All other Linked Services reference this.

```
Name:         LS_AzureKeyVault
Type:         Azure Key Vault
Base URL:     https://ecdp-keyvault.vault.azure.net/
Auth:         System Assigned Managed Identity
              (ADF's managed identity must have "Key Vault Secrets User" role in KV)
```

---

### Step 3 — Oracle Linked Service (using Key Vault)

```
Name:         LS_Oracle
Type:         Oracle
Connection:
  Host:       oracle-host.database.windows.net   ← hardcoded (not a secret)
  Port:       1521
  SID:        ECDPDB

Authentication:
  Username:   Key Vault secret
    AKV Linked Service: LS_AzureKeyVault
    Secret name:        oracle-username

  Password:   Key Vault secret
    AKV Linked Service: LS_AzureKeyVault
    Secret name:        oracle-password
```

**At runtime:** ADF fetches `oracle-username` and `oracle-password` from Key Vault before opening the JDBC connection. These values are never stored in ADF or visible in pipeline logs.

---

### Step 4 — ADLS Gen2 Linked Service (using Key Vault)

```
Name:         LS_ADLS_Gen2
Type:         Azure Data Lake Storage Gen2
URL:          https://ecdpstorage.dfs.core.windows.net

Authentication:  Account Key
  Account Key: Key Vault secret
    AKV Linked Service: LS_AzureKeyVault
    Secret name:        adls-storage-account-key
```

> Preferred alternative: Use Managed Identity instead of account key.
> Then no secret needed at all — ADF's identity gets RBAC "Storage Blob Data Contributor"
> role on the ADLS account. Even more secure.

---

### Step 5 — Databricks Linked Service (using Key Vault)

```
Name:         LS_Databricks
Type:         Azure Databricks
Workspace URL: https://adb-xxxx.azuredatabricks.net

Authentication:
  Access Token: Key Vault secret
    AKV Linked Service: LS_AzureKeyVault
    Secret name:        databricks-pat-token

Cluster type: New job cluster     ← auto-created per run, auto-terminated after
Cluster config:
  Node type:  Standard_DS3_v2
  Min workers: 2
  Max workers: 8
```

---

### Step 6 — Databricks Notebook accessing Key Vault

In Databricks, create a Secret Scope backed by Azure Key Vault (one-time setup):

```
Databricks UI → Settings → Secret Scopes → Create
  Scope name:       ecdp-kv
  Manage principal: All Users
  DNS Name:         https://ecdp-keyvault.vault.azure.net/
  Resource ID:      /subscriptions/.../resourceGroups/.../providers/Microsoft.KeyVault/vaults/ecdp-keyvault
```

Now notebooks access secrets like this:

```python
# Oracle JDBC connection in notebook (Bronze or Silver)
jdbc_url  = dbutils.secrets.get(scope="ecdp-kv", key="oracle-jdbc-url")
username  = dbutils.secrets.get(scope="ecdp-kv", key="oracle-username")
password  = dbutils.secrets.get(scope="ecdp-kv", key="oracle-password")

df_oracle = spark.read \
    .format("jdbc") \
    .option("url",      jdbc_url) \
    .option("dbtable",  "CLAIMS") \
    .option("user",     username) \
    .option("password", password) \
    .option("driver",   "oracle.jdbc.driver.OracleDriver") \
    .load()

# ADLS Gen2 access from notebook (if not using Unity Catalog / cluster config)
storage_key = dbutils.secrets.get(scope="ecdp-kv", key="adls-storage-account-key")
spark.conf.set(
    "fs.azure.account.key.ecdpstorage.dfs.core.windows.net",
    storage_key
)
# Now abfss:// paths work automatically
df = spark.read.format("delta").load("abfss://bronze@ecdpstorage.dfs.core.windows.net/delta/claims/")
```

---

### Key Vault Summary

```
Component         How it accesses Key Vault              What it fetches
──────────────    ───────────────────────────────────    ──────────────────────────────
ADF               Managed Identity → LS_AzureKeyVault    Oracle credentials, ADLS key, PAT
Databricks        Secret Scope (KV-backed)               Oracle JDBC, ADLS key, any secret
GitHub Actions    Azure CLI / OIDC                       ADF ARM deployment credentials
Local dev         az keyvault secret show (CLI)          Developer testing only
```

---

## PART 3 — Many Tables in Same DB: Individual vs Batch Pipelines

---

### Problem

You have 20 Oracle tables to ingest daily:
`claims, patients, products, providers, invoices, regions, ...`

**Naive approach — 20 individual pipelines:**
```
pipeline_oracle_claims_to_bronze
pipeline_oracle_patients_to_bronze
pipeline_oracle_products_to_bronze
... × 20
```

Problems: 20 pipelines to maintain, 20 triggers to manage, any config change requires 20 updates, no single view of all ingestion status.

**Correct approach — 1 generic pipeline + metadata config table.**

---

### Step 1 — Config/Metadata Control Table

Create this in Azure SQL Database (same place as watermark control table):

```sql
CREATE TABLE ingestion_config (
    entity_name         VARCHAR(100) PRIMARY KEY,
    source_type         VARCHAR(20),      -- 'oracle' or 'csv'
    source_schema       VARCHAR(100),     -- Oracle schema name
    source_table        VARCHAR(100),     -- Oracle table name / CSV folder name
    watermark_column    VARCHAR(100),     -- column to track incremental changes
    last_watermark      DATETIME,         -- last successful run timestamp
    load_type           VARCHAR(20),      -- 'incremental' or 'full'
    is_active           BIT,              -- 1 = enabled, 0 = disabled
    partition_column    VARCHAR(100),     -- Bronze partition column
    batch_size          INT               -- max records per run (optional)
);
```

Populated with:

```sql
INSERT INTO ingestion_config VALUES
('claims',    'oracle', 'ECDP', 'CLAIMS',    'UPDATED_AT', '2020-01-01', 'incremental', 1, 'run_date', NULL),
('patients',  'oracle', 'ECDP', 'PATIENTS',  'UPDATED_AT', '2020-01-01', 'incremental', 1, 'run_date', NULL),
('products',  'oracle', 'ECDP', 'PRODUCTS',  'MODIFIED_DT','2020-01-01', 'incremental', 1, 'run_date', NULL),
('providers', 'oracle', 'ECDP', 'PROVIDERS', NULL,         '2020-01-01', 'full',        1, 'run_date', NULL),
('invoices',  'csv',    NULL,   NULL,         NULL,         '2020-01-01', 'incremental', 1, 'run_date', NULL);
-- is_active = 0 to temporarily disable a table without deleting its config
```

---

### Step 2 — Generic Oracle Batch Pipeline

```
Pipeline Name: oracle_batch_to_bronze
Pipeline Parameters:
  - p_run_date  (string)  e.g. "2024-01-15"
```

#### Activity 1 — Lookup (fetch all active Oracle tables)

```
Activity Name:  ACT_Lookup_Config
Type:           Lookup
Dataset:        DS_AzureSQL_ControlDB
Query:
  SELECT
    entity_name,
    source_schema,
    source_table,
    watermark_column,
    last_watermark,
    load_type
  FROM ingestion_config
  WHERE source_type = 'oracle'
  AND   is_active   = 1

First row only: OFF    ← return ALL rows, not just first
```

Output:
```json
{
  "value": [
    { "entity_name": "claims",   "source_table": "CLAIMS",   "watermark_column": "UPDATED_AT", "last_watermark": "2024-01-14 23:59:59", "load_type": "incremental" },
    { "entity_name": "patients", "source_table": "PATIENTS",  "watermark_column": "UPDATED_AT", "last_watermark": "2024-01-14 23:59:59", "load_type": "incremental" },
    { "entity_name": "providers","source_table": "PROVIDERS", "watermark_column": null,          "last_watermark": "2020-01-01",          "load_type": "full"        }
  ]
}
```

---

#### Activity 2 — ForEach (loop through each table)

```
Activity Name:  ACT_ForEach_Tables
Type:           ForEach
Items:          @activity('ACT_Lookup_Config').output.value
IsSequential:   false      ← run tables in parallel
BatchCount:     5           ← max 5 tables running simultaneously
```

**Inside ForEach — child activities:**

---

#### Activity 2a — If Condition (incremental vs full load)

```
Activity Name:  ACT_If_LoadType
Type:           If Condition
Expression:     @equals(item().load_type, 'incremental')

True  → incremental Copy Activity
False → full load Copy Activity
```

---

#### Activity 2b (True) — Copy Activity for Incremental Load

```
Activity Name:  ACT_Copy_Oracle_Incremental
Type:           Copy

SOURCE:
  Linked Service: LS_Oracle
  Query (dynamic):
    SELECT *
    FROM @{item().source_schema}.@{item().source_table}
    WHERE @{item().watermark_column}
          > '@{item().last_watermark}'
    AND   @{item().watermark_column}
          <= '@{pipeline().parameters.p_run_date} 23:59:59'

SINK:
  Linked Service: LS_ADLS_Gen2
  File path:      staging/oracle/@{item().entity_name}/run_date=@{pipeline().parameters.p_run_date}/
  Format:         Parquet
```

---

#### Activity 2c (False) — Copy Activity for Full Load

```
Activity Name:  ACT_Copy_Oracle_Full
Type:           Copy

SOURCE:
  Query:  SELECT * FROM @{item().source_schema}.@{item().source_table}

SINK:    staging/oracle/@{item().entity_name}/run_date=@{pipeline().parameters.p_run_date}/
```

---

#### Activity 2d — Databricks Notebook Activity (per table)

```
Activity Name:  ACT_Databricks_Per_Table
Notebook:       /Repos/ecdp/notebooks/bronze/oracle_to_delta_generic
Parameters:
  entity   = @{item().entity_name}
  run_date = @{pipeline().parameters.p_run_date}
  load_type = @{item().load_type}

Runs after: ACT_Copy_Oracle_Incremental OR ACT_Copy_Oracle_Full → Success
```

---

#### Activity 2e — Update Watermark (per table, LAST step)

```
Activity Name:  ACT_Update_Watermark
Type:           Stored Procedure / Script Activity
Dataset:        DS_AzureSQL_ControlDB
Stored Procedure: usp_update_watermark
Parameters:
  @entity_name    = @{item().entity_name}
  @new_watermark  = @{pipeline().parameters.p_run_date} 23:59:59

Runs after: ACT_Databricks_Per_Table → Success only
            ↑ watermark updated ONLY if Databricks succeeded
```

```sql
-- Stored procedure definition
CREATE PROCEDURE usp_update_watermark
    @entity_name   VARCHAR(100),
    @new_watermark DATETIME
AS
BEGIN
    UPDATE ingestion_config
    SET    last_watermark = @new_watermark
    WHERE  entity_name    = @entity_name
END
```

---

### Batch Pipeline — Full Visual

```
oracle_batch_to_bronze (p_run_date = '2024-01-15')
│
├── ACT_Lookup_Config → returns 5 tables
│
└── ACT_ForEach_Tables (parallel, 5 at a time)
    │
    ├── Table: claims
    │   ├── IF incremental → ACT_Copy_Oracle_Incremental (WHERE UPDATED_AT > watermark)
    │   ├── ACT_Databricks_Per_Table (entity=claims, run_date=2024-01-15)
    │   └── ACT_Update_Watermark (claims → 2024-01-15 23:59:59)
    │
    ├── Table: patients
    │   ├── IF incremental → ACT_Copy_Oracle_Incremental
    │   ├── ACT_Databricks_Per_Table (entity=patients)
    │   └── ACT_Update_Watermark (patients → 2024-01-15 23:59:59)
    │
    └── Table: providers
        ├── IF full → ACT_Copy_Oracle_Full (SELECT * no filter)
        ├── ACT_Databricks_Per_Table (entity=providers, load_type=full)
        └── ACT_Update_Watermark (providers → 2024-01-15 23:59:59)
```

---

### Generic Databricks Notebook (handles any entity)

```python
dbutils.widgets.text("entity",    "claims")
dbutils.widgets.text("run_date",  "2024-01-15")
dbutils.widgets.text("load_type", "incremental")

entity    = dbutils.widgets.get("entity")
run_date  = dbutils.widgets.get("run_date")
load_type = dbutils.widgets.get("load_type")

from pyspark.sql.functions import current_timestamp, lit

staging_path = f"abfss://bronze@ecdpstorage.dfs.core.windows.net/staging/oracle/{entity}/run_date={run_date}/"
bronze_path  = f"abfss://bronze@ecdpstorage.dfs.core.windows.net/delta/{entity}/"

df = spark.read.format("parquet").load(staging_path)

df = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system",       lit("oracle")) \
    .withColumn("entity",              lit(entity)) \
    .withColumn("load_type",           lit(load_type)) \
    .withColumn("run_date",            lit(run_date))

# Full load: overwrite today's partition. Incremental: append.
write_mode = "overwrite" if load_type == "full" else "append"

df.write \
    .format("delta") \
    .mode(write_mode) \
    .option("mergeSchema", "true") \
    .partitionBy("run_date") \
    .save(bronze_path)

print(f"Written {df.count()} records to {bronze_path} | mode={write_mode}")

# Cleanup staging
dbutils.fs.rm(staging_path, recurse=True)
```

---

## PART 4 — Parameterized Datasets and Linked Services

---

### Why Parameterize?

Without parameterization:
```
DS_Oracle_Claims    ← hardcoded for claims only
DS_Oracle_Patients  ← hardcoded for patients only
DS_Oracle_Products  ← hardcoded for products only
... × 20 datasets
```

With parameterization:
```
DS_Oracle_Generic   ← one dataset, any table, any schema
DS_ADLS_Parquet     ← one dataset, any path, any entity
```

---

### Parameterized Dataset — Oracle Source

```
Dataset Name: DS_Oracle_Generic
Type:         Oracle Table
Linked Service: LS_Oracle

Parameters (defined on dataset):
  - p_schema   (string)   default: "ECDP"
  - p_table    (string)   default: "CLAIMS"

Table name:   @{dataset().p_schema}.@{dataset().p_table}
```

**How pipeline uses it:**
```
Copy Activity → Source Dataset: DS_Oracle_Generic
  p_schema = @{item().source_schema}     ← from ForEach config table row
  p_table  = @{item().source_table}
```

---

### Parameterized Dataset — ADLS Gen2 Parquet Sink

```
Dataset Name: DS_ADLS_Parquet_Generic
Type:         Parquet
Linked Service: LS_ADLS_Gen2

Parameters:
  - p_container  (string)   e.g. "bronze"
  - p_entity     (string)   e.g. "claims"
  - p_run_date   (string)   e.g. "2024-01-15"

File path:
  Container:  @{dataset().p_container}
  Directory:  staging/oracle/@{dataset().p_entity}/run_date=@{dataset().p_run_date}
  File:       *.parquet
```

**How pipeline uses it:**
```
Copy Activity → Sink Dataset: DS_ADLS_Parquet_Generic
  p_container = "bronze"
  p_entity    = @{item().entity_name}
  p_run_date  = @{pipeline().parameters.p_run_date}
```

---

### Parameterized Dataset — CSV Source

```
Dataset Name: DS_CSV_Generic
Type:         DelimitedText (CSV)
Linked Service: LS_ADLS_Gen2

Parameters:
  - p_folder_path  (string)   e.g. "landing/claims"
  - p_file_name    (string)   e.g. "claims_20240115.csv"

File path:  @{dataset().p_folder_path}/@{dataset().p_file_name}
First row as header: Yes
Column delimiter:    ,
Quote character:     "
```

---

### Parameterized Linked Service (Dynamic Connections)

Use when you connect to multiple databases on the same DB server — e.g., DEV Oracle, QA Oracle, PROD Oracle with different SIDs.

```
Linked Service Name: LS_Oracle_Dynamic
Type:                Oracle

Parameters:
  - p_host    (string)   e.g. oracle-dev.host.com
  - p_port    (string)   e.g. 1521
  - p_sid     (string)   e.g. ECDPDEV

Connection string (dynamic):
  jdbc:oracle:thin:@//@{linkedService().p_host}:@{linkedService().p_port}/@{linkedService().p_sid}

Username:  Key Vault → oracle-username
Password:  Key Vault → oracle-password
```

**Pipeline references it:**
```
Copy Activity → Source Linked Service: LS_Oracle_Dynamic
  p_host = @{pipeline().globalParameters.oracle_host}    ← env-specific global parameter
  p_port = 1521
  p_sid  = @{pipeline().globalParameters.oracle_sid}
```

ADF Global Parameters per environment:
```
DEV:  oracle_host = oracle-dev.host.com  | oracle_sid = ECDPDEV
QA:   oracle_host = oracle-qa.host.com   | oracle_sid = ECDPQA
PROD: oracle_host = oracle-prod.host.com | oracle_sid = ECDPPROD
```

This way one Linked Service definition covers all environments — the ARM template deploys the same LS everywhere, only Global Parameters differ.

---

### Parameterized Databricks Notebook (widgets)

Databricks receives all configuration as widget parameters from ADF:

```python
# Define all widgets at top of notebook
dbutils.widgets.text("entity",          "claims")
dbutils.widgets.text("run_date",        "2024-01-15")
dbutils.widgets.text("load_type",       "incremental")
dbutils.widgets.text("env",             "dev")          # dev / qa / prod
dbutils.widgets.text("source_type",     "oracle")       # oracle / csv
dbutils.widgets.text("partition_col",   "run_date")

# Read all widgets
entity        = dbutils.widgets.get("entity")
run_date      = dbutils.widgets.get("run_date")
load_type     = dbutils.widgets.get("load_type")
env           = dbutils.widgets.get("env")
source_type   = dbutils.widgets.get("source_type")

# Resolve environment-specific storage account
storage_accounts = {
    "dev":  "ecdpdevstore",
    "qa":   "ecdpqastore",
    "prod": "ecdpprodstore"
}
storage_account = storage_accounts[env]

# Resolve paths dynamically
staging_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/staging/{source_type}/{entity}/run_date={run_date}/"
bronze_path  = f"abfss://bronze@{storage_account}.dfs.core.windows.net/delta/{entity}/"

# One notebook handles any entity, any environment, any source type
```

---

### Full Parameterization Summary

```
Component                  What is Parameterized                         Benefit
─────────────────────────  ─────────────────────────────────────────     ─────────────────────────────────────
Dataset DS_Oracle_Generic  schema name, table name                       One dataset for all Oracle tables
Dataset DS_ADLS_Parquet    container, entity, run_date                   One dataset for any ADLS path
Dataset DS_CSV_Generic     folder path, file name                        One dataset for any CSV landing folder
LS_Oracle_Dynamic          host, port, SID                               One LS for dev/qa/prod Oracle
Pipeline                   entity, run_date, env                         One pipeline for all tables/environments
Databricks notebook        entity, run_date, env, load_type, source      One notebook for any ingestion scenario
Config table               all entity metadata in rows                   Add new table = add one DB row, no code
```

---

### Interview Q&A for This Section

| Question | Answer |
|---|---|
| **Why not create one pipeline per Oracle table?** | "At 20+ tables, individual pipelines are unmaintainable. Any change — adding a column, changing archive path — requires updating 20 pipelines. One generic pipeline driven by a config table means adding a new table is just inserting one row in the control DB. Zero pipeline changes." |
| **Where do you store secrets in ADF?** | "We never store secrets in ADF directly. ADF Linked Services reference Azure Key Vault secrets by name via a Key Vault Linked Service. ADF's Managed Identity has 'Key Vault Secrets User' role — it fetches secrets at runtime. Nothing sensitive in ADF or GitHub." |
| **How does Databricks access Key Vault?** | "We created a Databricks Secret Scope backed by Azure Key Vault. Notebooks call `dbutils.secrets.get(scope='ecdp-kv', key='oracle-password')`. The value is fetched at runtime and never printed in logs or stored in the notebook." |
| **What is a parameterized Linked Service?** | "A Linked Service where connection properties like host or database name are parameters instead of hardcoded values. This lets one Linked Service definition connect to dev, QA, and prod databases by passing different parameter values from the pipeline — no duplicate Linked Services per environment." |
| **How do you handle a new table being added to Oracle?** | "We add one row to the `ingestion_config` table with the entity name, source table, watermark column, and load type. The generic batch pipeline picks it up automatically on the next run — no pipeline changes, no new datasets, no new notebooks." |

---

*Last Updated: June 2026*
