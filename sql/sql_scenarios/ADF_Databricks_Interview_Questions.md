# Real Interview Questions — Azure Data Factory (ADF) & Azure Databricks

Standard questions actually asked in ADF/Databricks interviews (L1 = fundamentals/screening, L2 = deep/experienced-level). No scenario framing here — pure technical questions.

---

## PART A — Azure Data Factory (ADF)

### A.1 Core Concepts

**L1**
1. What is Azure Data Factory, and how is it different from SSIS?
2. What are the building blocks of ADF — Pipeline, Activity, Dataset, Linked Service? Explain each in one line.
3. What is a Trigger in ADF? What types of triggers exist (Schedule, Tumbling Window, Event-based, Manual)?
4. What is the difference between a Dataset and a Linked Service?
5. What is Copy Activity and what are its main components (source, sink, mapping)?

**L2**
6. What is the difference between a Tumbling Window Trigger and a Schedule Trigger? When would you use Tumbling Window specifically?
7. How does ADF handle dependency chaining between pipelines — via Execute Pipeline activity, trigger dependencies, or both? What's the trade-off?
8. Explain the difference between Mapping Data Flow and Wrangling Data Flow. When would you choose each?
9. How do you version control ADF pipelines, and how does CI/CD deployment work (ARM templates, Azure DevOps release pipelines)?
10. What's the difference between a "Global Parameter" and a "Pipeline Parameter" in ADF?

### A.2 Integration Runtime (IR)

**L1**
11. What is Integration Runtime in ADF, and what types exist (Azure IR, Self-hosted IR, Azure-SSIS IR)?
12. Why would you need a Self-hosted Integration Runtime?

**L2**
13. How do you set up high availability for a Self-hosted IR (multiple nodes)? What happens if the SHIR node goes down mid-pipeline?
14. What's the difference between Azure IR region selection ("Auto Resolve" vs explicit region) and why would it matter for performance/cost?
15. How does ADF connect securely to an on-prem Oracle/SQL Server database — what's the actual network path (VPN, ExpressRoute, SHIR)?

### A.3 Copy Activity & Performance

**L1**
16. What is Copy Activity's "mapping" — how does source schema map to sink schema?
17. What file formats can Copy Activity read/write (CSV, JSON, Parquet, Avro, ORC)?
18. What is fault tolerance in Copy Activity (skip incompatible rows)?

**L2**
19. How do you tune Copy Activity performance — what settings control parallelism (Degree of Copy Parallelism, Data Integration Units)?
20. What's the difference between "Copy behavior" options — Preserve Hierarchy, Flatten Hierarchy, Merge Files?
21. How would you copy 500 million rows from Oracle to ADLS efficiently — partitioned copy? What Oracle-side partition option does ADF support?
22. How do you implement upsert/merge behavior using Copy Activity + a staging table, versus using a Data Flow with an Upsert sink?

### A.4 Data Flows

**L1**
23. What is a Mapping Data Flow, and what compute does it run on (Spark under the hood)?
24. What transformations are available in Data Flow (Join, Lookup, Derived Column, Aggregate, Conditional Split, etc.)?

**L2**
25. How do you debug a slow Data Flow — what does the Data Flow monitoring/execution plan show you (partitioning, transformation stage times)?
26. Explain the difference between a "Lookup" transformation and a "Join" transformation in Data Flow.
27. How does Data Flow handle schema drift, and when would you enable "Allow schema drift"?

### A.5 Parameterization, Metadata-Driven Pipelines & Error Handling

**L1**
28. What is a parameterized pipeline in ADF, and why use one instead of hardcoding values?
29. What is the "Lookup" activity used for, and how is it different from "Get Metadata"?
30. What is "ForEach" activity, and what's the difference between sequential and parallel execution in ForEach?

**L2**
31. Design a metadata-driven ADF framework: one control table drives ingestion for 50 different source tables using a single generic pipeline. Walk through the design — what does the control table contain, and how does the pipeline read it?
32. How do you implement incremental loading in ADF using a watermark pattern — walk through the Lookup → Copy → Stored Procedure (watermark update) sequence.
33. How does ADF handle retries and error handling — activity-level retry policy vs. "On Failure" path in pipeline design?
34. How would you design a pipeline so a single source's failure doesn't block the other 49 sources in a metadata-driven framework?
35. What is "Get Metadata" activity commonly used for in file-based pipelines (e.g., checking if a file exists, listing files in a folder before a ForEach)?

### A.6 Security & Monitoring

**L1**
36. How does ADF authenticate to Azure services securely — Managed Identity vs. Service Principal vs. connection string?
37. How do you monitor pipeline runs in ADF — where do you see success/failure and duration?

**L2**
38. How do you integrate Azure Key Vault with ADF Linked Services so credentials are never hardcoded?
39. How would you set up alerting so the team is notified within minutes of a pipeline failure (Azure Monitor + Action Groups)?
40. What's the difference between Managed VNet Integration Runtime and a standard Azure IR from a security standpoint?

---

## PART B — Azure Databricks

### B.1 Core Concepts

**L1**
41. What is Databricks, and how is it different from plain open-source Apache Spark?
42. What is a Databricks Workspace, Cluster, and Notebook — explain each briefly.
43. What is the difference between an All-Purpose Cluster and a Job Cluster? When would you use each?
44. What is DBFS (Databricks File System)?
45. What is a Databricks Job, and how is it different from running a notebook interactively?

**L2**
46. What is Photon in Databricks, and how does it improve performance over standard Spark execution?
47. What's the difference between Databricks Runtime and open-source Spark runtime — what extra features does Databricks add?
48. How do cluster pools work, and why would you use them to reduce cluster startup time and cost?

### B.2 Delta Lake

**L1**
49. What is Delta Lake, and what problem does it solve that plain Parquet doesn't (ACID transactions)?
50. What is the Delta transaction log (`_delta_log`), and what does it store?
51. What is "time travel" in Delta Lake, and how do you query an older version of a table?
52. What's the difference between `MERGE INTO`, `INSERT`, and `overwrite` in Delta Lake?

**L2**
53. Explain how Delta Lake achieves ACID transactions on top of cloud object storage (which doesn't natively support transactions).
54. What is `OPTIMIZE` and `ZORDER` in Delta Lake — what problem do they each solve, and how are they different?
55. What is `VACUUM`, and what risk does running it with too short a retention period create?
56. Explain schema evolution vs. schema enforcement in Delta Lake — how do you intentionally allow a new column to be added (`mergeSchema`)?
57. What is Change Data Feed (CDF) in Delta Lake, and when would you use it?
58. Explain the small-file problem in Delta Lake and how auto-compaction / OPTIMIZE addresses it.
59. What's the difference between a Delta Live Table (DLT) pipeline and a regular scheduled notebook job?

### B.3 PySpark / Spark Internals

**L1**
60. What is the difference between a DataFrame and an RDD in Spark?
61. What is lazy evaluation in Spark, and why does Spark use it?
62. What's the difference between `repartition()` and `coalesce()`?
63. What is a Spark action vs. a transformation? Give one example of each.

**L2**
64. Explain the difference between a wide transformation and a narrow transformation, and why wide transformations (like `groupBy`, `join`) are more expensive.
65. What is data skew, and how do you detect and fix it in a Spark job (salting, broadcast join, adaptive query execution)?
66. What is Adaptive Query Execution (AQE) in Spark 3.x, and what problems does it solve automatically (skew join optimization, dynamic partition coalescing)?
67. Explain broadcast joins — when does Spark automatically broadcast, and when would you force it with a hint?
68. How do you read the Spark UI to diagnose a slow stage — what do you look for (task skew, shuffle read/write size, GC time)?
69. What is a shuffle in Spark, and why is minimizing shuffle important for performance?
70. Explain `cache()` vs `persist()` and when caching can actually hurt performance instead of helping.

### B.4 Unity Catalog & Governance

**L1**
71. What is Unity Catalog, and what problem does it solve (centralized governance across workspaces)?
72. What is the three-level namespace in Unity Catalog (catalog.schema.table)?

**L2**
73. What's the difference between a Managed Table and an External Table in Unity Catalog / Delta Lake?
74. How does Unity Catalog handle access control — what's the difference between granting access at the catalog, schema, and table level?
75. How do you access secrets securely in a Databricks notebook — Secret Scopes backed by Azure Key Vault vs. Databricks-backed secret scopes?

### B.5 Performance Tuning & Cost

**L1**
76. What factors affect Databricks cluster cost (cluster size, auto-scaling, DBU type, runtime version)?
77. What is auto-scaling in a Databricks cluster, and why use it?

**L2**
78. How do you decide optimal partition size for a large table — what's the "rule of thumb" for file size in Delta Lake (e.g., ~128MB-1GB per file)?
79. How do you tune the number of shuffle partitions (`spark.sql.shuffle.partitions`) for a workload, and what happens if it's set too high or too low?
80. How would you reduce cost on a Databricks job that runs daily but only needs to process a small incremental dataset — job cluster sizing, cluster pools, or spot instances?

### B.6 Orchestration & CI/CD

**L1**
81. How do you schedule a Databricks notebook to run daily — Databricks Jobs, or via ADF triggering a notebook activity?
82. What is a Databricks Job with multiple tasks (task orchestration within Databricks itself)?

**L2**
83. How do you integrate Databricks notebooks into a CI/CD pipeline (Databricks Repos + Azure DevOps/GitHub Actions)?
84. When would you trigger a Databricks notebook from ADF (Databricks Notebook Activity) versus running everything natively as a Databricks Workflow? What are the trade-offs?

---

## PART C — Questions That Combine ADF + Databricks (Very Common in Real Interviews)

**L1**
85. In a typical pipeline, what does ADF do and what does Databricks do — where's the line between orchestration and transformation?
86. Why would a project use both ADF and Databricks instead of just one of them?

**L2**
87. Design an end-to-end pipeline: ADF triggers ingestion from Oracle → lands raw data in ADLS (Bronze) → Databricks notebook transforms to Silver/Gold in Delta Lake → ADF triggers the Databricks job and monitors its status. Walk through how ADF passes parameters into the Databricks notebook and how it knows the notebook succeeded or failed.
88. How do you pass a dynamic parameter (like a watermark date) from an ADF pipeline into a Databricks notebook, and get a return value back into ADF?
89. If a Databricks job in the middle of an ADF pipeline fails, how does that failure propagate back to ADF, and how do you set up the pipeline to retry just that step?
90. In a metadata-driven framework spanning both tools, where does the "control table" logically live and get updated — ADF (via Stored Procedure activity) or Databricks (via a Delta table)? What are the pros/cons of each?

---

### How to use this list
Go topic by topic (A.1 → A.6, then B.1 → B.6), and for anything you can't answer in under 30 seconds without hesitating, that's your study priority. Part C is what senior/L2 interviewers usually save for last, since it tests whether you understand the *whole* pipeline, not just isolated tool knowledge.
