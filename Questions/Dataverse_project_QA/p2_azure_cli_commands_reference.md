# Azure CLI Commands & Scripts Reference (Companion to Part 1 & 2)

Run these in **Azure Cloud Shell** (Bash) — click the `>_` icon in the Azure Portal top bar. Replace placeholder values (`<...>`) with your own. Run commands in order; later commands depend on earlier resources existing.

---

## 0. Set Variables (run this first, every session)

```bash
# ---- EDIT THESE ----
LOCATION="centralindia"
RG="rg-myproj-dev"
SUFFIX="0612"          # any short unique string, e.g. today's date

# ---- Derived names ----
KV="kv-myproj-$SUFFIX"
STORAGE="stmyprojdev$SUFFIX"
ADF="adf-myproj-dev"
VM_ORACLE="vm-oracle-dev"
DBX="dbx-myproj-dev"

echo "RG=$RG KV=$KV STORAGE=$STORAGE ADF=$ADF VM=$VM_ORACLE DBX=$DBX"
```

---

## 1. Resource Group

```bash
az group create --name $RG --location $LOCATION
```

---

## 2. Oracle VM (Ubuntu) + Oracle XE via Docker

### 2.1 Create the VM
```bash
az vm create \
  --resource-group $RG \
  --name $VM_ORACLE \
  --image Ubuntu2204 \
  --size Standard_B2s \
  --admin-username azureuser \
  --generate-ssh-keys \
  --public-ip-sku Standard
```

### 2.2 Open port 1521 (Oracle listener) — restrict to your IP only
```bash
MY_IP=$(curl -s https://api.ipify.org)

az vm open-port \
  --resource-group $RG \
  --name $VM_ORACLE \
  --port 1521 \
  --priority 900

# Tighten the rule to only your IP (recommended)
az network nsg rule update \
  --resource-group $RG \
  --nsg-name "${VM_ORACLE}NSG" \
  --name open-port-1521 \
  --source-address-prefixes "$MY_IP/32"
```

### 2.3 Get the VM's public IP (you'll need this for Linked Service + Key Vault secret)
```bash
az vm show -d -g $RG -n $VM_ORACLE --query publicIps -o tsv
```

### 2.4 SSH in and install Oracle XE (Docker)
```bash
ssh azureuser@<PUBLIC_IP>
```
Then on the VM:
```bash
sudo apt update -y
sudo apt install -y docker.io
sudo systemctl enable --now docker

sudo docker run -d -p 1521:1521 -p 5500:5500 \
  -e ORACLE_PWD=YourStrongPwd123 \
  --name oracle-xe \
  --restart unless-stopped \
  gvenzl/oracle-xe:21-slim

# Watch logs until "DATABASE IS READY TO USE"
sudo docker logs -f oracle-xe
```

### 2.5 Create schema + tables (run after DB is ready)
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
EXIT;
```

### 2.6 Stop/Start the VM (cost control)
```bash
az vm stop  --resource-group $RG --name $VM_ORACLE   # end of session
az vm start --resource-group $RG --name $VM_ORACLE   # next session
```

---

## 3. Azure Key Vault + Secrets

```bash
az keyvault create \
  --name $KV \
  --resource-group $RG \
  --location $LOCATION \
  --enable-rbac-authorization true

ORACLE_IP=$(az vm show -d -g $RG -n $VM_ORACLE --query publicIps -o tsv)

az keyvault secret set --vault-name $KV --name "oracle-db-username" --value "salesuser"
az keyvault secret set --vault-name $KV --name "oracle-db-password" --value "SalesPwd123"
az keyvault secret set --vault-name $KV --name "oracle-db-host"     --value "$ORACLE_IP"
```

> `databricks-pat-token` and `storage-account-key` are added in steps 5 and 6 below, after those resources exist.

---

## 4. ADLS Gen2 Storage Account + Containers

```bash
az storage account create \
  --name $STORAGE \
  --resource-group $RG \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Get a key (needed for container creation auth + Databricks secret scope)
STORAGE_KEY=$(az storage account keys list --account-name $STORAGE --resource-group $RG --query "[0].value" -o tsv)

for c in bronze silver gold; do
  az storage container create --name $c --account-name $STORAGE --account-key $STORAGE_KEY
done

# Store the storage key in Key Vault (used later by Databricks secret scope)
az keyvault secret set --vault-name $KV --name "storage-account-key" --value "$STORAGE_KEY"
```

---

## 5. Azure Data Factory + Permissions

```bash
az datafactory create \
  --resource-group $RG \
  --factory-name $ADF \
  --location $LOCATION
```

### 5.1 Get ADF's Managed Identity and grant it access

**Key Vault — "Key Vault Secrets User" role:**
```bash
ADF_PRINCIPAL_ID=$(az datafactory show --resource-group $RG --factory-name $ADF --query identity.principalId -o tsv)
KV_ID=$(az keyvault show --name $KV --resource-group $RG --query id -o tsv)

az role assignment create \
  --assignee $ADF_PRINCIPAL_ID \
  --role "Key Vault Secrets User" \
  --scope $KV_ID
```

**Storage account — "Storage Blob Data Contributor" role (so ADF can write to Bronze without account keys):**
```bash
STORAGE_ID=$(az storage account show --name $STORAGE --resource-group $RG --query id -o tsv)

az role assignment create \
  --assignee $ADF_PRINCIPAL_ID \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

> Role assignments can take a few minutes to propagate. If "Test Connection" fails in ADF Linked Service immediately after this, wait 5 minutes and retry.

---

## 6. Azure Databricks Workspace

```bash
az databricks workspace create \
  --resource-group $RG \
  --name $DBX \
  --location $LOCATION \
  --sku standard
```

After it's created, **get the workspace URL** (you'll need it for the PAT token step, done manually in the UI since PAT generation isn't a CLI operation):
```bash
az databricks workspace show --resource-group $RG --name $DBX --query workspaceUrl -o tsv
```

### 6.1 Generate PAT token (manual, UI step)
1. Open the workspace URL from above in a browser
2. User icon (top-right) → **User Settings → Developer → Access tokens → Generate new token**
3. Copy the token, then store it in Key Vault:
```bash
az keyvault secret set --vault-name $KV --name "databricks-pat-token" --value "<PASTE_TOKEN_HERE>"
```

### 6.2 Create Databricks Secret Scope backed by Key Vault (Databricks CLI)
Install/configure Databricks CLI in Cloud Shell:
```bash
pip install databricks-cli --quiet

DBX_HOST=$(az databricks workspace show --resource-group $RG --name $DBX --query workspaceUrl -o tsv)

databricks configure --token
# When prompted:
#   Databricks Host: https://<DBX_HOST>
#   Token: <paste the PAT token from 6.1>
```

```bash
KV_DNS=$(az keyvault show --name $KV --resource-group $RG --query properties.vaultUri -o tsv)
KV_RESOURCE_ID=$(az keyvault show --name $KV --resource-group $RG --query id -o tsv)

databricks secrets create-scope \
  --scope myproj-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id $KV_RESOURCE_ID \
  --dns-name $KV_DNS
```

Now in notebooks you can do:
```python
storage_key = dbutils.secrets.get(scope="myproj-scope", key="storage-account-key")
spark.conf.set(f"fs.azure.account.key.{STORAGE}.dfs.core.windows.net", storage_key)
```

---

## 7. ADF Linked Services — Connection String Reference

When creating Linked Services in ADF Studio UI, you'll need these values (all retrievable via CLI if needed):

```bash
echo "Oracle Host: $(az keyvault secret show --vault-name $KV --name oracle-db-host --query value -o tsv)"
echo "Storage Account: $STORAGE"
echo "Key Vault Base URL: $(az keyvault show --name $KV --resource-group $RG --query properties.vaultUri -o tsv)"
echo "Databricks Workspace URL: $(az databricks workspace show --resource-group $RG --name $DBX --query workspaceUrl -o tsv)"
```

Oracle connection string format for ADF Linked Service:
```
Host=<ORACLE_IP>;Port=1521;Sid=XEPDB1
```
(or use Service Name `XEPDB1` depending on the connector version's field labels)

---

## 8. Part 2 — Service Principal for CI/CD (GitHub Actions)

```bash
SUBSCRIPTION_ID=$(az account show --query id -o tsv)
RG_TEST="rg-myproj-test"

az group create --name $RG_TEST --location $LOCATION

az ad sp create-for-rbac \
  --name "sp-myproj-cicd" \
  --role Contributor \
  --scopes /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RG_TEST \
  --sdk-auth
```

Copy the **entire JSON output** — this goes into GitHub repo → **Settings → Secrets and variables → Actions → New repository secret** named `AZURE_CREDENTIALS`.

Also add:
- `AZURE_SUBSCRIPTION_ID` = output of `echo $SUBSCRIPTION_ID`

---

## 9. Test Environment Resources (Part 2)

```bash
KV_TEST="kv-myproj-test-$SUFFIX"
STORAGE_TEST="stmyprojtest$SUFFIX"
ADF_TEST="adf-myproj-test"

az keyvault create --name $KV_TEST --resource-group $RG_TEST --location $LOCATION --enable-rbac-authorization true

az storage account create \
  --name $STORAGE_TEST --resource-group $RG_TEST --location $LOCATION \
  --sku Standard_LRS --kind StorageV2 --hierarchical-namespace true

STORAGE_TEST_KEY=$(az storage account keys list --account-name $STORAGE_TEST --resource-group $RG_TEST --query "[0].value" -o tsv)
for c in bronze silver gold; do
  az storage container create --name $c --account-name $STORAGE_TEST --account-key $STORAGE_TEST_KEY
done

az datafactory create --resource-group $RG_TEST --factory-name $ADF_TEST --location $LOCATION

# Grant Test ADF identity access to Test Key Vault + Storage
ADF_TEST_PRINCIPAL_ID=$(az datafactory show --resource-group $RG_TEST --factory-name $ADF_TEST --query identity.principalId -o tsv)
KV_TEST_ID=$(az keyvault show --name $KV_TEST --resource-group $RG_TEST --query id -o tsv)
STORAGE_TEST_ID=$(az storage account show --name $STORAGE_TEST --resource-group $RG_TEST --query id -o tsv)

az role assignment create --assignee $ADF_TEST_PRINCIPAL_ID --role "Key Vault Secrets User" --scope $KV_TEST_ID
az role assignment create --assignee $ADF_TEST_PRINCIPAL_ID --role "Storage Blob Data Contributor" --scope $STORAGE_TEST_ID

# Copy the same secrets into Test Key Vault (point to whatever Test should use)
az keyvault secret set --vault-name $KV_TEST --name "oracle-db-username" --value "salesuser"
az keyvault secret set --vault-name $KV_TEST --name "oracle-db-password" --value "SalesPwd123"
az keyvault secret set --vault-name $KV_TEST --name "oracle-db-host"     --value "$ORACLE_IP"
```

---

## 10. Cleanup (End of Lab / Save Remaining Credit)

```bash
# Delete everything for this lab
az group delete --name $RG --yes --no-wait
az group delete --name $RG_TEST --yes --no-wait
```

⚠️ This permanently deletes all resources in both groups. Only run when you're fully done.

---

## 11. Test-Data Script for Re-Running Incremental Loads

Save this on the Oracle VM as `simulate_changes.sql` and run it (`sqlplus salesuser/SalesPwd123@//localhost:1521/XEPDB1 @simulate_changes.sql`) each time you want to test the incremental pipeline again:

```sql
-- Simulate an update to an existing record
UPDATE customer_master
SET region = 'North', last_updated_dttm = SYSTIMESTAMP
WHERE customer_id = 1;

-- Simulate a new record
INSERT INTO customer_master VALUES (
  (SELECT MAX(customer_id)+1 FROM customer_master),
  'NewCustomer_' || TO_CHAR(SYSDATE,'HH24MISS'),
  'South', 'Enterprise', SYSTIMESTAMP
);

COMMIT;
```
