# Hands-On Lab (Part 1): Oracle → Bronze → Silver → Gold on Azure Free Trial

This is a from-scratch, click-by-click build using your 30-day Azure free trial. Designed to mirror a real production setup but kept lean enough to fit free-trial limits.

> **Cost-control rule of thumb for the whole lab:** Stop/delete the Oracle VM and Databricks cluster whenever you're not actively working. ADF and Key Vault cost almost nothing when idle; VMs and Databricks clusters burn credit by the hour.

---

## Phase 0: Naming Convention & Plan

Use a consistent prefix so resources are easy to find/clean up later, e.g. `myproj`:

| Resource | Name (dev) | Name (test/prod later) |
|---|---|---|
| Resource Group | `rg-myproj-dev` | `rg-myproj-test` |
| Key Vault | `kv-myproj-dev` | `kv-myproj-test` |
| Storage Account | `stmyprojdev` | `stmyprojtest` |
| Data Factory | `adf-myproj-dev` | `adf-myproj-test` |
| Databricks | `dbx-myproj-dev` | `dbx-myproj-test` |
| Oracle VM | `vm-oracle-dev` | (shared / not duplicated) |

Key Vault and Storage Account names must be **globally unique** — add random digits if needed.

---

## Phase 1: Create the Resource Group

Portal: **Create a resource → Resource Group**
- Subscription: your free trial subscription
- Resource group: `rg-myproj-dev`
- Region: pick one close to you (e.g., Central India / East US) — **use the SAME region for everything** to avoid cross-region charges/latency.

Or via Cloud Shell (Azure CLI, opens in portal — top-right `>_` icon):
```bash
az group create --name rg-myproj-dev --location centralindia
```

---

## Phase 2: Oracle Source Database

You need a real Oracle DB to practice against. Cheapest realistic option on a free trial: **Oracle Database XE on a small Azure VM**.

### 2.1 Create the VM
Portal: **Create a resource → Virtual Machine**
- Resource group: `rg-myproj-dev`
- Name: `vm-oracle-dev`
- Image: **Ubuntu Server 22.04 LTS**
- Size: `Standard_B2s` (2 vCPU, 4GB RAM — minimum for Oracle XE)
- Authentication: SSH key pair (download the private key) or password
- Allow inbound ports: SSH (22) — you'll restrict this later

### 2.2 Install Oracle XE
SSH into the VM, then:
```bash
sudo apt update
wget https://download.oracle.com/otn-pub/otn_software/db-express/oracle-database-xe-21c-1.0-1.ol8.x86_64.rpm
# Convert/install via alien or use Docker (simpler):
sudo apt install docker.io -y
sudo docker run -d -p 1521:1521 -p 5500:5500 -e ORACLE_PWD=YourStrongPwd123 \
  --name oracle-xe gvenzl/oracle-xe:21-slim
```
Wait ~2-3 minutes for it to initialize (`docker logs -f oracle-xe` until you see "DATABASE IS READY TO USE").

### 2.3 Create Sample Schema + Incremental Test Data
Connect via SQL*Plus (inside the container):
```bash
sudo docker exec -it oracle-xe sqlplus system/YourStrongPwd123@//localhost:1521/XEPDB1
```

```sql
CREATE USER salesuser IDENTIFIED BY SalesPwd123;
GRANT CONNECT, RESOURCE, DBA TO salesuser;
ALTER USER salesuser QUOTA UNLIMITED ON USERS;

CONNECT salesuser/SalesPwd123@//localhost:1521/XEPDB1;

CREATE TABLE customer_master (
    customer_id     NUMBER PRIMARY KEY,
    customer_name   VARCHAR2(100),
    region          VARCHAR2(50),
    segment         VARCHAR2(50),
    last_updated_dttm TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE watermark_control (
    table_name      VARCHAR2(100),
    watermark_col   VARCHAR2(100),
    last_loaded_val VARCHAR2(100)
);

INSERT INTO customer_master VALUES (1,'Alice','West','Retail',TIMESTAMP '2024-01-01 00:00:00');
INSERT INTO customer_master VALUES (2,'Bob','East','Enterprise',TIMESTAMP '2024-01-02 00:00:00');
INSERT INTO watermark_control VALUES ('CUSTOMER_MASTER','LAST_UPDATED_DTTM','2024-01-01 00:00:00');
COMMIT;
```

Later, to test incremental loads, you'll run:
```sql
UPDATE customer_master SET region='North', last_updated_dttm=SYSTIMESTAMP WHERE customer_id=1;
INSERT INTO customer_master VALUES (3,'Carol','South','Enterprise',SYSTIMESTAMP);
COMMIT;
```
…and re-run the pipeline — only rows 1 (updated) and 3 (new) should flow through.

---

## Phase 3: Azure Key Vault

Portal: **Create a resource → Key Vault**
- Resource group: `rg-myproj-dev`
- Name: `kv-myproj-dev-<random>`
- Pricing tier: Standard

### Add Secrets
Go to your Key Vault → **Secrets → Generate/Import**, create:
| Secret Name | Value |
|---|---|
| `oracle-db-username` | `salesuser` |
| `oracle-db-password` | `SalesPwd123` |
| `oracle-db-host` | the VM's **private/public IP** |
| `databricks-pat-token` | (you'll generate this in Phase 6) |

### Access Policy (do this AFTER creating ADF in Phase 5)
Key Vault → **Access policies** (or **Access control (IAM)** if using RBAC model) → grant ADF's Managed Identity the **"Key Vault Secrets User"** role (or Get/List under the old Access Policy model).

---

## Phase 4: ADLS Gen2 Storage Account

Portal: **Create a resource → Storage Account**
- Resource group: `rg-myproj-dev`
- Name: `stmyprojdev<random>`
- Performance: Standard
- Redundancy: LRS (cheapest)
- **Advanced tab → Enable hierarchical namespace = ON** (this is what makes it "ADLS Gen2", required)

### Create Containers
Storage Account → **Containers** → create:
- `bronze`
- `silver` (optional — if Silver/Gold use external Delta tables backed by storage)
- `gold`

---

## Phase 5: Azure Data Factory — Resources & Pipeline

### 5.1 Create ADF
Portal: **Create a resource → Data Factory**
- Resource group: `rg-myproj-dev`
- Name: `adf-myproj-dev`
- **Git configuration: skip for now** (we'll connect Git in Part 2 for CI/CD) — or configure it now pointing to a GitHub repo with a `dev` collaboration branch. Either is fine.

After creation, note that ADF gets a **System-Assigned Managed Identity** automatically (Settings → Identity). Use this identity's name (same as the ADF name) when granting Key Vault access in Phase 3.

### 5.2 Self-Hosted Integration Runtime (to reach the Oracle VM)
Since your Oracle DB is on a VM (private network), ADF's Azure IR (cloud-only) can't reach it directly unless the VM has a public IP with firewall rules open to ADF's IP ranges. For production-style setup, use a **Self-Hosted Integration Runtime (SHIR)**:

1. ADF Studio → **Manage → Integration Runtimes → New → Self-Hosted**
2. Name it `SHIR-OnPrem` → it gives you an auth key
3. On the Oracle VM (or a separate small VM), download and install the **"Microsoft Integration Runtime"** Windows package — note: SHIR requires **Windows**, so if your Oracle VM is Ubuntu, create a tiny separate `Standard_B1s` Windows VM just for this, in the same VNet
4. Install the Oracle client / ODP.NET driver on that SHIR machine (required for Oracle connector)
5. Register the SHIR using the auth key from step 2

> If you want to avoid the Windows-VM hassle for this lab, you can instead give the Oracle VM a public IP, restrict the NSG to ADF's documented Azure IR IP ranges for your region, and use Azure IR. For a "production approach" demo, mentioning SHIR is the correct/expected answer even if you simplify for the lab.

### 5.3 Global Parameters
ADF Studio → **Manage → Global Parameters → New**:

| Name | Type | Value |
|---|---|---|
| `gp_storage_account` | String | `stmyprojdev` |
| `gp_bronze_container` | String | `bronze` |
| `gp_environment` | String | `dev` |
| `gp_databricks_notebook_b2s` | String | `/Repos/dev/notebooks/bronze_to_silver` |
| `gp_databricks_notebook_s2g` | String | `/Repos/dev/notebooks/silver_to_gold` |

### 5.4 Linked Services
ADF Studio → **Manage → Linked Services → New**

**a) Azure Key Vault**
- Name: `LS_AzureKeyVault`
- Type: Azure Key Vault
- Base URL: select your `kv-myproj-dev`

**b) Oracle**
- Name: `LS_OracleDB`
- Type: Oracle
- Connect via: `SHIR-OnPrem` (or Azure IR if you went the public-IP route)
- Host / Port / Service name: fill from your VM's IP and `XEPDB1`
- User name: select **Azure Key Vault** → `LS_AzureKeyVault` → secret `oracle-db-username`
- Password: same, secret `oracle-db-password`
- Test connection before saving

**c) ADLS Gen2**
- Name: `LS_ADLS_Bronze`
- Type: Azure Data Lake Storage Gen2
- Authentication: Account Key (simplest for lab) or Managed Identity (grant ADF's identity "Storage Blob Data Contributor" role on the storage account — preferred for production)

**d) Azure Databricks** — create after Phase 6 once the workspace exists.

### 5.5 Datasets
ADF Studio → **Author → Datasets → New**

**a) `DS_Oracle_Source`**
- Linked service: `LS_OracleDB`
- Type: Oracle table — leave table blank, add dataset parameters `p_schema` and `p_table` (used only for documentation; actual extraction uses a query)

**b) `DS_Watermark_Control`**
- Same linked service, points to `WATERMARK_CONTROL` table

**c) `DS_Bronze_Parquet`**
- Linked service: `LS_ADLS_Bronze`
- Format: Parquet
- Add dataset parameters: `p_table_name`, `p_year`, `p_month`, `p_day`
- File path, using dynamic content:
```
@concat('bronze/', dataset().p_table_name, '/year=', dataset().p_year, '/month=', dataset().p_month, '/day=', dataset().p_day, '/')
```

### 5.6 Pipeline Parameters
Create pipeline `PL_Oracle_to_Bronze_Incremental`, add parameters:

| Name | Type | Default |
|---|---|---|
| `pTableName` | String | `CUSTOMER_MASTER` |
| `pSchemaName` | String | `SALESUSER` |
| `pWatermarkColumn` | String | `LAST_UPDATED_DTTM` |

### 5.7 Build the Pipeline (activities, in order)

1. **Lookup — `Lookup_OldWatermark`**
   - Source: `DS_Watermark_Control`
   - Query:
   ```sql
   SELECT LAST_LOADED_VAL FROM WATERMARK_CONTROL WHERE TABLE_NAME = '@{pipeline().parameters.pTableName}'
   ```
   - First row only ✅

2. **Lookup — `Lookup_NewWatermark`**
   - Source: `DS_Oracle_Source`
   - Query:
   ```sql
   SELECT TO_CHAR(MAX(@{pipeline().parameters.pWatermarkColumn}), 'YYYY-MM-DD HH24:MI:SS') AS NEW_WATERMARK
   FROM @{pipeline().parameters.pSchemaName}.@{pipeline().parameters.pTableName}
   ```

3. **Copy Data — `Copy_Oracle_To_Bronze`**
   - Source (`DS_Oracle_Source`, query mode):
   ```sql
   SELECT * FROM @{pipeline().parameters.pSchemaName}.@{pipeline().parameters.pTableName}
   WHERE @{pipeline().parameters.pWatermarkColumn} > TO_TIMESTAMP('@{activity('Lookup_OldWatermark').output.firstRow.LAST_LOADED_VAL}','YYYY-MM-DD HH24:MI:SS')
   AND @{pipeline().parameters.pWatermarkColumn} <= TO_TIMESTAMP('@{activity('Lookup_NewWatermark').output.firstRow.NEW_WATERMARK}','YYYY-MM-DD HH24:MI:SS')
   ```
   - Sink (`DS_Bronze_Parquet`):
     - `p_table_name` = `@pipeline().parameters.pTableName`
     - `p_year` = `@formatDateTime(utcnow(),'yyyy')`
     - `p_month` = `@formatDateTime(utcnow(),'MM')`
     - `p_day` = `@formatDateTime(utcnow(),'dd')`

4. **If Condition — `If_RowsCopied`**
   - Expression: `@greater(activity('Copy_Oracle_To_Bronze').output.rowsCopied, 0)`
   - **True branch:**
     - **Script activity** `Update_Watermark` (against `LS_OracleDB`):
       ```sql
       UPDATE WATERMARK_CONTROL
       SET LAST_LOADED_VAL = '@{activity('Lookup_NewWatermark').output.firstRow.NEW_WATERMARK}'
       WHERE TABLE_NAME = '@{pipeline().parameters.pTableName}'
       ```
     - **Databricks Notebook** `NB_Bronze_to_Silver` (added after Phase 6) — runs on Success of `Update_Watermark`
     - **Databricks Notebook** `NB_Silver_to_Gold` — runs on Success of the above

5. **Failure handling**: add a **Web Activity** or **Office 365 Outlook / Logic App** activity connected via "On Failure" from `Copy_Oracle_To_Bronze` to send an alert.

Save and **Debug** to test this part before moving to Phase 6.

---

## Phase 6: Azure Databricks

### 6.1 Create Workspace
Portal: **Create a resource → Azure Databricks**
- Resource group: `rg-myproj-dev`
- Name: `dbx-myproj-dev`
- Pricing Tier: **Standard** is enough for this lab (Premium needed for full Unity Catalog + fine-grained access control — optional stretch goal; Standard works fine for Delta tables via Hive metastore)

### 6.2 Generate a PAT Token
Workspace → click your user icon → **User Settings → Developer → Access tokens → Generate new token**
- Copy it immediately → paste into Key Vault secret `databricks-pat-token` (Phase 3)

### 6.3 Create a Cluster
Workspace → **Compute → Create Cluster**
- Name: `cluster-dev`
- Databricks Runtime: latest LTS (e.g., 14.3 LTS)
- Node type: smallest available (`Standard_DS3_v2` or similar)
- **Terminate after 30 minutes of inactivity** ✅ (critical for cost control)
- Workers: 1 (single-node mode is fine for this lab)

### 6.4 Give Databricks Access to ADLS
Simplest for lab: use the storage account key directly in notebooks via Spark config (not ideal for prod, but workable):
```python
spark.conf.set(
  "fs.azure.account.key.stmyprojdev.dfs.core.windows.net",
  dbutils.secrets.get(scope="myproj-scope", key="storage-account-key")
)
```
For this, create a **Databricks Secret Scope backed by Azure Key Vault**:
- Go to `https://<your-workspace-url>#secrets/createScope`
- Scope name: `myproj-scope`
- DNS Name + Resource ID: from your Key Vault's **Properties** page
- Add a secret `storage-account-key` in Key Vault (from Storage Account → Access Keys)

Production-grade alternative: use a **Service Principal + OAuth (ABFS)** instead of account keys, or Unity Catalog external locations with managed identities — mention this as the "real" approach in interviews even if you use the simpler method for the lab.

### 6.5 Create Notebooks
In Databricks workspace, create folder `/Repos/dev/notebooks/` (or `/Shared/notebooks/` if not using Repos), and create:
- `bronze_to_silver` — paste the code from the earlier SCD/MERGE guide, reading from `abfss://bronze@stmyprojdev.dfs.core.windows.net/...`
- `silver_to_gold` — same, reading Silver Delta table and writing Gold (with or without SCD2)

Test each notebook manually first with hardcoded widget values before wiring into ADF.

### 6.6 Connect ADF → Databricks
Back in ADF: **Manage → Linked Services → New → Azure Databricks**
- Name: `LS_Databricks`
- Workspace URL: auto-filled if you pick "From Azure subscription"
- Cluster: "New Job Cluster" (spins up only for the run — cheaper than "existing interactive cluster" for production) — pick same node type as your dev cluster
- Access token: From Azure Key Vault → secret `databricks-pat-token`

Now go back to the pipeline (Phase 5.7, step 4) and add the two **Notebook activities**, pointing to `/Repos/dev/notebooks/bronze_to_silver` and `silver_to_gold`, with Base Parameters as discussed in the earlier guide.

---

## Phase 7: End-to-End Test

1. Run the pipeline manually (Debug, then Trigger now)
2. Check `bronze` container → Parquet file should appear under `customer_master/year=2026/month=06/day=12/`
3. Check Databricks → query `SELECT * FROM silver.customer_master` and `SELECT * FROM gold.customer_dim`
4. Go back to Oracle, run an UPDATE + INSERT (from Phase 2.3), re-trigger the pipeline
5. Confirm: only the changed/new rows appear in the new Bronze file, and Silver/Gold reflect the update via MERGE

---

## Phase 8: Free-Trial Cost Management

- **Stop the Oracle VM** and **terminate Databricks clusters** when not actively working (Portal → VM → Stop; Databricks auto-terminates per your setting)
- Set a **Budget alert**: Cost Management → Budgets → alert at $20/$50/$100 thresholds
- Delete the whole resource group at the end of each session if you're done for the day, and recreate via a script (see Part 2 — Infrastructure as Code) — or just stop compute resources, since storage/ADF/Key Vault cost very little

---

**Continue to Part 2** for: connecting ADF to Git, promoting to a Test/Prod environment, and CI/CD with parameterized Linked Services per environment.
