# Interview Answer Script — Section A.1: Core Concepts (Azure Data Factory)

---

## L1 — Basic Questions

### 1. What is Azure Data Factory, and how is it different from SSIS?

**Short Answer:**
ADF is a cloud-based ETL/orchestration service on Azure. Unlike SSIS (which runs on-premises SQL Server and needs a server/VM), ADF is serverless, scales automatically, and connects natively to cloud sources like ADLS, Azure SQL, and REST APIs.

**Detailed Answer:**
"In my FinOps Dataverse project, I used ADF to build pipelines that pull data from Oracle and flat-file sources into ADLS Gen2. The reason we picked ADF over something like SSIS is that ADF is fully managed — I don't provision or patch a server, and it scales on its own when data volume goes up. It also has native connectors for Oracle, REST APIs, and Azure services, so I could build a configuration-driven ingestion framework instead of hardcoding per-source logic. If we had used SSIS instead, we'd need to manage our own VM/server, handle scaling manually, and lose the tight integration with ADLS Gen2 and Databricks that made our Medallion architecture work smoothly."

---

### 2. What are the building blocks of ADF — Pipeline, Activity, Dataset, Linked Service?

**Short Answer:**
- **Pipeline** = a container that groups a set of activities (a logical unit of work).
- **Activity** = a single step/task inside a pipeline (e.g., Copy Activity, Web Activity).
- **Dataset** = a pointer to the actual data (structure/location) you're reading or writing.
- **Linked Service** = the connection string/credentials to a data source or compute (like a "connector").

**Detailed Answer:**
"On my FinOps Dataverse pipelines, I used all four together: a Linked Service to connect to our Oracle database and to ADLS Gen2, a Dataset on top of that Linked Service to define the specific table or file, a Copy Activity to move the data, and all of this was wrapped inside a Pipeline that also had parameters so the same pipeline could be reused for multiple sources. If I skipped Linked Services and hardcoded connection details into each Dataset instead, I'd lose reusability — any credential change would mean editing every dataset instead of one central place."

---

### 3. What is a Trigger in ADF? What types of triggers exist?

**Short Answer:**
A Trigger is what starts a pipeline run. Main types: **Schedule Trigger** (runs on a fixed time/interval), **Tumbling Window Trigger** (runs on fixed, non-overlapping time slices and tracks dependency/state), **Event-based Trigger** (fires when a file lands in storage), and **Manual/On-demand Trigger** (triggered manually or via API).

**Detailed Answer:**
"In my pipelines, I mostly relied on Schedule Triggers to run ingestion jobs at a fixed daily cadence, since our finance data loads followed a predictable batch schedule. If I needed backfill-safe processing with awareness of 'which time slice has already run,' Tumbling Window would be the better fit because it maintains state — but for our use case, a straightforward schedule trigger was enough and simpler to monitor and troubleshoot."

---

### 4. What is the difference between a Dataset and a Linked Service?

**Short Answer:**
A Linked Service is the **connection** (like a login/connection string) to a data store or compute. A Dataset is the **data itself** — pointing to a specific table, file, or folder using that connection.

**Detailed Answer:**
"Think of Linked Service as 'how do I connect to Oracle,' and Dataset as 'which table in Oracle am I reading.' In my project, I had one Linked Service per source system (Oracle, ADLS Gen2), and multiple Datasets built on top of each — one per table or file pattern. This separation meant that if a password or endpoint changed, I updated it once in the Linked Service, and every Dataset using it kept working without changes."

---

### 5. What is Copy Activity and what are its main components?

**Short Answer:**
Copy Activity moves data from a source to a sink (destination). Main components: **Source** (where data comes from), **Sink** (where it lands), and **Mapping** (how source columns map to sink columns/schema).

**Detailed Answer:**
"I used Copy Activity extensively to move data from Oracle and flat files into ADLS Gen2 (Bronze layer) in the FinOps Dataverse project. The source was configured against our Oracle Linked Service/Dataset, the sink was ADLS Gen2 in Parquet/CSV format, and I used schema mapping to make sure column names and types lined up correctly before landing the data. Without proper mapping, you'd get schema mismatches downstream in Databricks when Spark tries to read the files — so getting this right at the Copy Activity stage saved rework later in the pipeline."

---

## L2 — Deeper Questions

### 6. Tumbling Window Trigger vs Schedule Trigger — when would you use Tumbling Window specifically?

**Short Answer:**
Schedule Trigger just fires the pipeline at set times, with no memory of previous runs. Tumbling Window Trigger fires on fixed, non-overlapping time windows AND tracks state/dependency, so you can reprocess a specific past window (backfill) reliably, and chain windows so one can't start until the previous one finishes.

**Detailed Answer:**
"In my project we used Schedule Triggers because our loads were simple daily batches without complex backfill requirements. But Tumbling Window is the better choice when you need guaranteed sequential processing of time slices — for example, if window 2 must not run until window 1 has completed successfully, or if you need to reprocess a specific day's data without disturbing others. The trade-off is that Tumbling Window is more complex to set up and monitor (it has its own retry/dependency model), so I'd only reach for it if the business genuinely needed ordered, resumable, and backfillable time-sliced processing — which wasn't a hard requirement in what I built, so Schedule Trigger was simpler and sufficient."

---

### 7. How does ADF handle dependency chaining between pipelines — Execute Pipeline activity, trigger dependencies, or both? Trade-off?

**Short Answer:**
Both exist. **Execute Pipeline activity** calls one pipeline from inside another (synchronous, parent-child). **Trigger dependency** (tumbling window dependency) lets a trigger only fire after another trigger's window succeeds, without nesting pipelines. Trade-off: Execute Pipeline gives tighter control and passes parameters directly, but can create long monolithic pipeline chains that are harder to debug; trigger dependency keeps pipelines decoupled and independently schedulable, but is limited to tumbling-window based orchestration.

**Detailed Answer:**
"In my configuration-driven onboarding framework, I used Execute Pipeline activity to call reusable child pipelines — for example, a generic ingestion pipeline that different source-specific parent pipelines would invoke with different parameters. This let me avoid duplicating logic across 20+ sources; new source onboarding just meant adding new parameters, not new pipeline code, which is part of how we cut onboarding time by 50%. The trade-off is that if you chain too many Execute Pipeline calls, monitoring and debugging get harder because failures can be buried a few levels deep. Trigger-based dependency chaining is more loosely coupled but I didn't need it here since our use case was about reusing logic, not sequencing independent pipelines."

---

### 8. Mapping Data Flow vs Wrangling Data Flow — when would you choose each?

**Short Answer:**
Mapping Data Flow is a visual, Spark-powered ETL tool for building transformation logic without writing code — good for production-grade, scalable transformations. Wrangling Data Flow is Power Query–based, meant for business users to do quick, exploratory data prep/cleansing — not built for heavy production pipelines.

**Detailed Answer:**
"On my projects, I didn't rely on Mapping Data Flow for the heavy transformation work — I used PySpark notebooks in Databricks instead for cleansing, deduplication, and business rule logic, because it gave me more control, better performance tuning options (like Z-Ordering and partitioning), and easier integration with our Medallion architecture. Mapping Data Flow is a reasonable option for teams that want low-code visual ETL directly in ADF without spinning up a separate Databricks environment. Wrangling Data Flow, on the other hand, is meant more for ad hoc, exploratory data prep by business/analyst users — I wouldn't use it for a production pipeline with 10M+ records daily like what I handled in eCDP, since it's not designed for that scale or repeatability."

---

### 9. How do you version control ADF pipelines and how does CI/CD deployment work?

**Short Answer:**
ADF integrates with Git (Azure DevOps or GitHub) so pipeline JSON definitions are version-controlled. Deployment across environments (Dev → Test → Prod) is done using ARM templates generated from the Git-connected "collaboration branch," published through Azure DevOps release pipelines.

**Detailed Answer:**
"I used GitHub for version control across my projects generally. For ADF specifically, the pattern is: you connect your ADF instance to a Git repo, work in feature branches, and when ready, publish to the collaboration branch which auto-generates an ARM template representing your pipelines, datasets, and linked services. That ARM template is then what gets deployed to Test/Prod environments via an Azure DevOps release pipeline, with environment-specific parameters (like different connection strings) swapped in at deployment time. This way, nothing is deployed directly by clicking 'Publish' in a shared environment — changes go through code review and a controlled release process, which matters a lot in a regulated environment like the financial data platform I worked on."

---

### 10. What's the difference between a "Global Parameter" and a "Pipeline Parameter" in ADF?

**Short Answer:**
Pipeline Parameters are scoped to a single pipeline — passed in when that pipeline runs. Global Parameters are defined once at the ADF factory level and can be reused across multiple pipelines without redefining them each time.

**Detailed Answer:**
"In my configuration-driven framework, I used Pipeline Parameters heavily — things like source table name, file path, or load type (full vs incremental) were passed into the pipeline at runtime so the same pipeline could serve multiple sources. Global Parameters are useful for values that are truly constant across the whole factory — like an environment name or a base storage account path — so you're not repeating the same parameter definition in every single pipeline. I'd reach for Global Parameters mainly to reduce repetition for values that don't change per-run, and Pipeline Parameters for anything that does change per execution."

---

*End of Section A.1 — send the next batch when ready.*
