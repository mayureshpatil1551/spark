# Hands-On Lab (Part 2): Environment Promotion & CI/CD (Dev → Test/Prod)

This part covers how a "production-grade" ADF + Databricks project moves code from Dev to Test/Prod — the part most tutorials skip, but the part interviewers ask about most.

> **Free trial note:** Setting up a second full environment (Test or Prod) costs more credit. You can do this conceptually with a **single ADF instance using "branches" as environments**, OR spin up a second lightweight resource group (`rg-myproj-test`) with just ADF + Key Vault + Storage (skip a second Oracle VM/Databricks — reuse Dev's for testing the deployed pipeline against the same data). Either way, the steps below are the same pattern used in real companies.

---

## 1. The Core Idea

In production, you **never edit pipelines directly in the Test/Prod Data Factory**. Instead:

```
Dev ADF (Git-connected, "collaboration branch")
   │  (developer builds & tests pipelines here)
   ▼
Pull Request → main/release branch
   ▼
ADF "Publish" → generates ARM templates (in adf_publish branch)
   ▼
CI/CD pipeline (Azure DevOps or GitHub Actions)
   ▼
Deploys ARM template to Test ADF → Prod ADF
   (with environment-specific parameter overrides)
```

The **pipeline logic stays identical** across environments. What changes between environments are **connection details** (Key Vault name, storage account name, Databricks workspace) — handled via **ARM template parameters** and **Global Parameters**.

---

## 2. Connect Dev ADF to Git

ADF Studio → **Manage → Git configuration → Configure**
- Repository type: **GitHub** (easiest) or Azure DevOps Git
- Authorize your GitHub account
- Repository name: e.g., `myproj-adf`
- Collaboration branch: `main`
- Root folder: `/`
- ADF auto-creates a `dev` branch for you to work in (or create your own feature branches)

From now on:
- You work in a feature branch (e.g., `feature/oracle-pipeline`) inside ADF Studio
- Click **"Save"** → commits JSON to Git
- Open a **Pull Request** in GitHub to merge into `main`
- Click **"Publish"** (only from `main`/collaboration branch) → this generates/updates the **`adf_publish`** branch containing ARM templates (`ARMTemplateForFactory.json` + a parameters file)

---

## 3. Why Global Parameters Matter for CI/CD

Recall from Part 1, Phase 5.3 — Global Parameters like `gp_storage_account`, `gp_environment`. These automatically become entries in the generated **ARM template parameters file**:

```json
{
  "Microsoft.DataFactory/factories/globalParameters_gp_storage_account": {
    "value": "stmyprojdev"
  },
  "Microsoft.DataFactory/factories/globalParameters_gp_environment": {
    "value": "dev"
  }
}
```

When you deploy this ARM template to the **Test** Data Factory, your CI/CD pipeline overrides these values:
```json
{
  "Microsoft.DataFactory/factories/globalParameters_gp_storage_account": {
    "value": "stmyprojtest"
  },
  "Microsoft.DataFactory/factories/globalParameters_gp_environment": {
    "value": "test"
  }
}
```

**This is the entire point of Global Parameters**: one codebase, environment-specific values injected at deploy time. Same logic applies to Linked Service properties (Key Vault name, Databricks workspace URL) — ADF lets you mark these as "parameterize in ARM template" so they also appear in the parameters file.

### Enable Linked Service Parameterization
ADF Studio → **Manage → ARM template → Edit parameter configuration** (or use the default `arm-template-parameters-definition.json` checked into your repo root). Make sure these are flagged as parameters:
- Key Vault Linked Service `baseUrl`
- Storage account name in `LS_ADLS_Bronze`
- Databricks workspace URL / cluster config in `LS_Databricks`
- All Global Parameters

---

## 4. Set Up Test Environment Resources

In `rg-myproj-test` (or reuse `rg-myproj-dev` with different names — your choice for the lab):

1. **Key Vault**: `kv-myproj-test` — add the **same secret names** (`oracle-db-username`, `oracle-db-password`, `databricks-pat-token`, `oracle-db-host`) but pointing to whatever Test should use (can be the same Oracle VM for this lab — just simulating separation)
2. **Storage Account**: `stmyprojtest` with `bronze`/`silver`/`gold` containers
3. **Data Factory**: `adf-myproj-test` — **do NOT connect this to Git**. Test/Prod factories receive deployments via ARM template only, never manual edits.
4. Grant `adf-myproj-test`'s Managed Identity access to `kv-myproj-test` (same as Phase 3 in Part 1)
5. (Optional) Separate Databricks workspace `dbx-myproj-test`, or reuse Dev's workspace with a different folder (`/Repos/test/notebooks/`) — cheaper for a lab

---

## 5. CI/CD Pipeline (GitHub Actions example)

Create `.github/workflows/deploy-adf-test.yml` in your repo:

```yaml
name: Deploy ADF to Test

on:
  push:
    branches:
      - adf_publish   # triggers when ADF auto-publishes ARM templates

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Azure Login
        uses: azure/login@v1
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}

      - name: Deploy ARM Template to Test ADF
        uses: azure/arm-deploy@v1
        with:
          subscriptionId: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
          resourceGroupName: rg-myproj-test
          template: ./ARMTemplateForFactory.json
          parameters: >
            ./ARMTemplateParametersForFactory.json
            factoryName=adf-myproj-test
            globalParameters_gp_storage_account=stmyprojtest
            globalParameters_gp_environment=test
```

### Setting up the secret
- `AZURE_CREDENTIALS`: a Service Principal JSON (`az ad sp create-for-rbac --sdk-auth`) with **Contributor** role on `rg-myproj-test`, stored in GitHub → repo **Settings → Secrets and variables → Actions**

This is the same conceptual flow Azure DevOps Release Pipelines use, just with the "ARM Template Deployment" task instead of `azure/arm-deploy`.

---

## 6. Promoting Databricks Notebooks

Databricks notebooks are promoted via **Databricks Repos** (Git-backed):

1. In Databricks, **Repos → Add Repo**, connect the same GitHub repo (notebooks live in a `/notebooks` folder)
2. Dev workspace: Repo checked out on `dev` branch → path `/Repos/dev/notebooks/...`
3. Test workspace (or same workspace, different Repo checkout): checked out on `main`/`test` branch → path `/Repos/test/notebooks/...`
4. Your ADF pipeline's Notebook activity path comes from the Global Parameter `gp_databricks_notebook_b2s`, which the CI/CD step overrides per environment — so **the same pipeline definition points to different notebook copies in each environment automatically**.

---

## 7. Promotion Flow Summary (What Actually Happens Day-to-Day)

1. Developer creates feature branch in ADF Studio, builds/edits pipeline
2. Tests using **Debug** (runs against Dev resources, using Dev's Global Parameter values)
3. Opens PR → reviewed → merged to `main`
4. ADF **Publish** button → generates ARM templates in `adf_publish` branch
5. CI/CD pipeline triggers automatically → deploys ARM template to Test ADF with Test's parameter values
6. QA validates in Test
7. A second CI/CD stage (often with manual approval gate) deploys the same ARM template to **Prod** ADF with Prod's parameter values (different Key Vault, storage account, Databricks workspace)

---

## 8. Production Extras Worth Mentioning in Interviews

- **Monitoring**: ADF → Monitor tab + Azure Monitor alerts on pipeline failures; Log Analytics workspace for centralized logging
- **Managed Identity everywhere** instead of account keys/PATs where possible (storage access, Key Vault access)
- **Triggers**: Tumbling Window triggers for incremental loads (gives `WindowStart`/`WindowEnd` automatically, avoids needing a manual watermark table in many cases)
- **Unity Catalog** (Databricks Premium) for centralized governance across Dev/Test/Prod workspaces — single metastore, catalog-per-environment (`dev_catalog`, `prod_catalog`)
- **Data quality checks**: a notebook/activity step that validates row counts, null checks, schema drift before promoting Silver → Gold
- **Parameter files per environment**: many teams keep a `config/dev.json`, `config/test.json`, `config/prod.json` checked into the repo, consumed by the CI/CD pipeline to populate ARM parameters — avoids hardcoding values in YAML

---

## 9. Wrap-Up Checklist

- ✅ Dev ADF connected to Git, pipeline built and tested via Debug
- ✅ Global Parameters used for all environment-dependent values
- ✅ Linked Services parameterized for ARM template export
- ✅ Test resource group with its own Key Vault, Storage, ADF (no Git connection)
- ✅ CI/CD workflow deploys ARM template + overrides parameters per environment
- ✅ Databricks notebooks promoted via Repos/Git, paths driven by Global Parameters
- ✅ Cost controls: clusters auto-terminate, VM stopped when idle, budget alert set
