# Interview Answer Script — Section A.2: Integration Runtime & A.3: Copy Activity / Performance

**Note on honesty:** A few questions below (SHIR high availability, network path setup) are usually owned by an infra/platform team, not a data engineer. Your resume shows you used **Azure Integration Runtime** (not Self-hosted IR) — so where I don't have evidence you personally configured something, I've marked the answer as "conceptual/general knowledge" so you don't accidentally overclaim in the interview. Use your judgment on whether to say "I haven't set this up myself, but here's how it works."

---

## A.2 — Integration Runtime (IR)

### L1

### 11. What is Integration Runtime in ADF, and what types exist?

**Short Answer:**
Integration Runtime (IR) is the compute engine ADF uses to actually move/transform data. Three types: **Azure IR** (fully managed, serverless, for cloud-to-cloud movement), **Self-hosted IR (SHIR)** (installed on your own VM/on-prem machine, used to reach private/on-prem data sources), and **Azure-SSIS IR** (used specifically to lift-and-shift and run existing SSIS packages in Azure).

**Detailed Answer:**
"In my FinOps Dataverse project, I used Azure Integration Runtime for moving data between cloud-native sources like Oracle (connected over the network) and ADLS Gen2 — it's serverless, so I didn't have to manage any compute for it. Self-hosted IR is used when the source system sits behind a private network or on-prem firewall that Azure can't reach directly — it acts like a secure bridge/agent installed inside that network. Azure-SSIS IR is really only relevant if a company is migrating existing SSIS packages to Azure without rewriting them, which wasn't part of my work since we built everything natively in ADF and Databricks."

---

### 12. Why would you need a Self-hosted Integration Runtime?

**Short Answer:**
You need SHIR when your data source is inside a private network — on-premises, or in a VPC/VNet that Azure's public IR can't reach directly. SHIR is installed as an agent on a machine inside that network and creates an outbound-only secure connection to ADF.

**Detailed Answer:**
"This is a conceptual answer rather than something I personally configured — in my project, our Oracle source was reachable over the network without needing a self-hosted agent, so I used Azure IR. But the general reason to use SHIR is security and network isolation: if a company's database sits behind a corporate firewall with no public endpoint, Azure's cloud-hosted IR simply cannot reach it. SHIR solves this by running as software inside that private network and only making outbound calls to ADF, so the firewall doesn't need to open any inbound ports."

---

### L2

### 13. How do you set up high availability for SHIR (multiple nodes)? What happens if a node goes down mid-pipeline?

**Short Answer (general knowledge, not personally implemented):**
You install SHIR on 2+ machines and register them under the same logical Integration Runtime — ADF automatically load-balances and fails over between nodes. If the active node processing a task goes down mid-run, ADF retries the task on another healthy node (though any in-flight activity on that node typically fails and needs to be retried/re-triggered).

**Detailed Answer:**
"I want to be upfront — I haven't personally set up a multi-node SHIR cluster, since my pipelines used Azure IR. But conceptually: you install the SHIR software on multiple VMs and link them to the same IR name in ADF. ADF treats them as one logical unit and distributes work across the nodes for both load balancing and high availability. If the node running an activity crashes mid-task, that specific task run fails, and depending on your retry policy configured on the activity, ADF will retry it — potentially on a different available node. It's not seamless mid-task failover; it's more like 'the job fails and gets retried,' which is why setting sensible retry counts and intervals on your activities matters."

---

### 14. Azure IR "Auto Resolve" vs explicit region — why does it matter for performance/cost?

**Short Answer:**
"Auto Resolve" lets ADF automatically pick the region closest to your sink (or source, depending on operation) to minimize latency. An explicit region locks the IR to a specific region regardless of where your data is, which can add latency or egress cost if it doesn't match your data's actual location.

**Detailed Answer:**
"In my pipelines, I generally let ADF use Auto Resolve since our ADLS Gen2 and Azure SQL resources were in the same region, and this avoided any unnecessary cross-region data movement. The reason this matters: if your IR runs in a different region than your data, you pay cross-region data egress costs and add network latency to every activity run. I'd only pick an explicit region deliberately — for example, if compliance required data processing to happen in a specific region regardless of where the ADF factory itself is hosted."

---

### 15. How does ADF connect securely to an on-prem Oracle/SQL Server — what's the actual network path?

**Short Answer (general knowledge):**
Typically either: (a) a Self-hosted IR installed inside the on-prem network making outbound calls to ADF, combined with (b) a secure network layer like a Site-to-Site VPN or ExpressRoute connecting the on-prem network to Azure, or a direct firewall-allowed connection if the DB has a reachable endpoint.

**Detailed Answer:**
"This is more of a network/infra-team decision than something I owned personally. In my project, our Oracle instance was accessible over the network directly, so I connected using Azure IR with proper credentials stored in Azure Key Vault — I didn't need to set up VPN or ExpressRoute myself. But conceptually, when a source truly sits in a private on-prem network, the standard pattern is: ExpressRoute or VPN provides the secure network tunnel between on-prem and Azure, and then a Self-hosted IR (installed on a machine inside that network) is what ADF actually talks to in order to pull data — the IR doesn't need an inbound port opened, since it initiates the connection outward to ADF."

---

## A.3 — Copy Activity & Performance

### L1

### 16. What is Copy Activity's "mapping" — how does source schema map to sink schema?

**Short Answer:**
Mapping defines how each column in the source lines up with a column in the sink — by name or by explicit position/type. It can be automatic (if names match) or manually configured when names/types differ.

**Detailed Answer:**
"In my FinOps Dataverse pipelines, when moving data from Oracle into ADLS Gen2, I used schema mapping to make sure Oracle column types translated correctly into the target format — this mattered especially for date and numeric fields where implicit type conversion could silently cause issues. Getting the mapping right at the Copy Activity stage meant Databricks could reliably read the Bronze layer data downstream without unexpected schema drift or type mismatches."

---

### 17. What file formats can Copy Activity read/write?

**Short Answer:**
CSV, JSON, Parquet, Avro, ORC, and more (plus binary/text formats). It supports both structured and semi-structured data across nearly all major file formats.

**Detailed Answer:**
"In my pipelines, I primarily worked with CSV and Parquet — landing raw data as CSV or Parquet in the Bronze layer of our Medallion architecture in ADLS Gen2. Parquet was preferred for larger datasets because it's columnar and compresses well, which helped downstream Spark jobs in Databricks read data faster compared to row-based formats like CSV."

---

### 18. What is fault tolerance in Copy Activity (skip incompatible rows)?

**Short Answer:**
Fault tolerance settings let Copy Activity skip rows that fail due to incompatible data (type mismatch, malformed data) instead of failing the entire copy job, and optionally log those skipped rows to a file for review.

**Detailed Answer:**
"This was useful in my project when ingesting flat files from less controlled sources — occasionally a row would have a malformed value that didn't match the expected schema. Rather than letting one bad row fail an entire pipeline run processing millions of records, I'd configure fault tolerance to skip and log incompatible rows, so the rest of the load succeeded and we could review the skipped rows separately for data quality follow-up, instead of blocking the whole day's load over a handful of bad records."

---

### L2

### 19. How do you tune Copy Activity performance — Degree of Copy Parallelism, Data Integration Units?

**Short Answer:**
**Data Integration Units (DIUs)** control the raw compute power (CPU/memory/network) allocated to a copy run. **Degree of Copy Parallelism** controls how many threads/partitions the copy operation is split into at the source. Increasing both boosts throughput, but each also increases cost.

**Detailed Answer:**
"In the FinOps Dataverse and eCDP work, performance tuning was one of the bigger levers I used — for example, cutting a pipeline's runtime from 4 hours to 58 minutes was largely through partition tuning at the Databricks/Spark layer, but the same principle applies in ADF: increasing DIUs gives Copy Activity more compute to push data faster, and increasing Degree of Copy Parallelism lets it split the source read into multiple concurrent streams instead of one sequential stream. The trade-off is cost — more DIUs and parallelism means faster runs but higher consumption charges, so I'd tune these based on actual bottlenecks rather than maxing them out by default."

---

### 20. Copy behavior options — Preserve Hierarchy, Flatten Hierarchy, Merge Files — what's the difference?

**Short Answer:**
- **Preserve Hierarchy**: keeps the source folder/file structure exactly as-is in the sink.
- **Flatten Hierarchy**: takes files from nested source folders and writes them all into a single flat destination folder.
- **Merge Files**: combines multiple source files into one single output file at the sink.

**Detailed Answer:**
"In my ingestion pipelines, Preserve Hierarchy was the default and most common choice for landing raw data into the Bronze layer, because keeping the original folder structure (e.g., by source system and date) made it easier to trace data lineage and troubleshoot issues back to the original file. Flatten Hierarchy would be useful if downstream tools expect a single flat folder without nested subfolders. Merge Files is handy when a source produces many small files (which hurts read performance in Spark due to the 'small files problem') and you want to consolidate them into fewer, larger files before further processing."

---

### 21. How would you copy 500 million rows from Oracle to ADLS efficiently — partitioned copy? What Oracle-side partition option does ADF support?

**Short Answer:**
Use ADF's **partitioned copy** feature for Oracle sources, which can split the read using a physical partition column (like an Oracle table partition) or a dynamic range partition (e.g., splitting by a numeric or date column range) so multiple parallel reads happen instead of one giant sequential query.

**Detailed Answer:**
"While I haven't personally moved a single 500-million-row table in one shot, this is directly the kind of problem I solved at scale — in eCDP, I worked with 10M+ records processed daily and reduced a core pipeline's runtime from 4 hours to 58 minutes through partition tuning. For a dataset that large from Oracle, I'd use ADF's dynamic range partitioning on a Copy Activity source — splitting the extract by a column like an ID range or date range so several parallel Copy sub-tasks run concurrently instead of a single-threaded full-table scan. I'd combine this with tuning DIUs and Degree of Copy Parallelism, and land the output as Parquet with sensible partitioning in ADLS so downstream Spark reads are also efficient — this mirrors the incremental loading and partition tuning approach I actually used to make our pipelines meet SLA."

---

### 22. Upsert/merge using Copy Activity + staging table vs Data Flow with Upsert sink?

**Short Answer:**
With **Copy Activity + staging table**: you land data into a staging/temp table first, then run a separate SQL stored procedure/script (e.g., `MERGE` statement) to upsert it into the target table. With a **Data Flow Upsert sink**: the merge/upsert logic is built directly into the Data Flow itself, so ADF handles the insert-or-update logic natively without a separate SQL step.

**Detailed Answer:**
"In my own pipelines, I actually handled upsert/merge logic in PySpark within Databricks rather than in ADF Data Flows — using Delta Lake's `MERGE INTO` capability to implement incremental loading, which is one of the things that helped reduce our pipeline execution time by 30%. But comparing the two ADF-native options conceptually: staging table + stored procedure gives you more control and is easier to debug since the merge logic is plain SQL you can test independently, but it means an extra hop (land to staging, then merge) and typically needs a SQL-based sink like Azure SQL Database. Data Flow's Upsert sink keeps everything inside one ADF Data Flow, which is more visual/low-code, but debugging complex merge conditions is harder since you're working within ADF's UI rather than plain SQL. Given the scale I worked at, I preferred handling merge logic in PySpark/Delta Lake directly, since it gave the most control and performance tuning options."

---

*End of Section A.2 & A.3 — send the next batch when ready.*
