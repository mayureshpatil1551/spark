contect - https://www.linkedin.com/in/shirisha-siliveri-5047a524b/recent-activity/all/

-----

## 🚀 Databricks Data Engineer Interview Series | Day 1

📌 Topic: How to Ingest Continuously Arriving Files from ADLS Gen2 Using Auto Loader

🎯 Interview Question:
How would you use Databricks Auto Loader to ingest files arriving continuously in Azure Data Lake Storage Gen2 (ADLS Gen2)?

💡 Answer:
I would use Databricks Auto Loader (cloudFiles) to incrementally ingest files arriving in ADLS Gen2 into a Delta table.

Auto Loader automatically discovers new files, supports schema inference and evolution, and scales to handle large volumes of data efficiently.

🔹 Implementation Steps:
✅ Configure secure access to ADLS Gen2 using Unity Catalog storage credentials or managed identity.
✅ Configure Auto Loader using the cloudFiles format.
✅ Define the source path and schema location.
✅ Configure schema evolution to handle new columns.
✅ Transform the incoming data as required.
✅ Write the ingested data into a Delta table.
✅ Configure a checkpoint location to track streaming progress and support recovery.
✅ Run the pipeline continuously or schedule it using Lakeflow Jobs, depending on latency requirements.

🔥 Important Follow-Up Questions
❓ 1. How does Auto Loader discover new files?
Auto Loader supports directory listing and file notification modes. File notification mode can reduce directory-listing overhead at scale.
❓ 2. How do you prevent duplicate ingestion?
Auto Loader tracks discovered files using checkpoint state. Preserve the checkpoint across restarts and deployments to avoid reprocessing previously tracked files.
❓ 3. What happens when the stream restarts?
The stream resumes using its checkpoint, provided the checkpoint remains available and intact and the required source data is still accessible.
❓ 4. Does Auto Loader guarantee exactly-once processing?
Auto Loader supports exactly-once file processing when used with Delta Lake, but end-to-end guarantees depend on the complete pipeline, including transformations and the sink.


🎯 Key Interview Takeaway:
Auto Loader provides scalable, incremental file ingestion with schema evolution and checkpoint-based recovery. Understanding file discovery, schema management, checkpointing, and fault tolerance is essential for building reliable data pipelines.

💬 Interview Tip: Don't just memorize the code. Be prepared to explain why you chose Auto Loader, how it handles schema changes, and how you recover from failures.


---

🚀 Databricks Interview Series | Day 2: Unity Catalog

🎯 Topic: Unity Catalog in Databricks

💡 Main Interview Question: What is Unity Catalog in Databricks, and why do we use it?
✅ Answer:
Unity Catalog is Databricks' unified governance solution that helps organizations manage and secure data and AI assets across their Databricks environment.

It provides centralized capabilities for:
🔹 Access Control: Manage who can access data and what actions they can perform.
🔹 Data Discovery: Organize and discover tables, views, volumes, and other supported data assets.
🔹 Data Lineage: Track how data flows from source tables through transformations to downstream tables and reports.
🔹 Auditing: Monitor data access and governance-related activities through supported audit capabilities.
🔹 Centralized Governance: Apply governance policies across supported workspaces and data assets.

📌 Example:
Imagine an organization has three teams:
• Finance Team
• Marketing Team
• Data Engineering Team
All three teams use the same Databricks environment, but each team requires different levels of data access.
Using Unity Catalog, administrators can grant the Finance team access to financial data, the Marketing team access to marketing data, and the Data Engineering team the permissions needed to build and maintain data pipelines.
This helps enforce security while enabling teams to work with the data they need.

🔥 4 Follow-Up Interview Questions and Answers

Q1. What is the three-level namespace in Unity Catalog?
✅ Answer:
Unity Catalog organizes data using a three-level namespace:
catalog.schema.table
For example:
production.sales.orders

This structure helps organize data assets logically and manage permissions at different levels.

Q2. What is the difference between Unity Catalog and the Hive metastore?
✅ Answer:
The Hive metastore is a traditional metadata management service used to organize tables and their metadata.
Unity Catalog provides a more comprehensive governance framework, including centralized access control, data discovery, lineage, auditing, and governance across supported workspaces.
Unity Catalog is generally preferred for modern Databricks environments that require centralized governance.

Q3. What is the difference between a managed table and an external table in Unity Catalog?
✅ Answer:
Managed Table:
Databricks manages the table's data lifecycle.
The table's data is stored in a location managed according to the configured storage and governance setup.
Dropping the table generally removes its data after the applicable retention period and platform rules.
External Table:
The data is stored at an externally managed storage location.
Unity Catalog manages the table's metadata and access governance.
Dropping the table removes its metadata registration but generally leaves the underlying data files intact.

---

🚀 Databricks Interview Preparation - Day 3

How would you optimize a large Delta table that is frequently queried by date and customer ID?

Let's walk through a practical approach.
Imagine a sales table frequently queried using filters on order_date and customer_id.

Here are the key optimization techniques I would consider:

1️⃣ Data Skipping
Delta Lake collects file-level statistics that help skip files that cannot match the query filters. Ensure that important filter columns have useful statistics and that the table's physical layout supports effective data skipping.

2️⃣ Liquid Clustering
For workloads that frequently filter by multiple columns,  evaluate liquid clustering.

CREATE TABLE main.sales.orders ( 
order_id BIGINT, 
customer_id BIGINT, 
order_date DATE, 
amount DECIMAL(12, 2) ) 
USING DELTA 
CLUSTER BY (order_date, customer_id);

Liquid clustering helps organize data around commonly queried columns, improving data skipping and adapting to evolving query patterns.

3️⃣ Small File Compaction
A large number of small files can increase query overhead.

Use:
OPTIMIZE main.sales.orders;

This helps compact smaller files into larger files and improve scan efficiency.

4️⃣ Statistics Collection
Verify that statistics are collected for frequently filtered columns, such as order_date and customer_id, so Delta Lake can make better data-skipping decisions.

5️⃣ Execution Plan Analysis
Review the execution plan using:

EXPLAIN FORMATTED SELECT * FROM main.sales.orders WHERE order_date >= '2026-01-01'   AND customer_id = 101;

Look for:
Excessive file scans
Inefficient predicate pushdown
Large shuffles
Data skew
Unnecessary column reads
Expensive aggregations

6️⃣ Additional Optimizations
Depending on the workload, also consider:
Photon for supported workloads
Predictive optimization where applicable
Efficient join strategies
Materialized views or aggregate tables for repeated queries
Avoiding unnecessary columns in SELECT statements

What about partitioning?
I would not automatically partition by both date and customer ID. High-cardinality partitioning can create too many directories and small files.

For new table designs, I would evaluate liquid clustering against traditional partitioning based on the query workload and platform capabilities.

💡 Key Takeaway
Delta table optimization is not just about increasing cluster size. It is about understanding the query workload, reducing the amount of data scanned, minimizing file overhead, and avoiding unnecessary shuffling.

The best optimization strategy should always be driven by actual query patterns and execution-plan analysis.

#Databricks #DeltaLake #ApacheSpark #DataEngineering #PySpark #BigData #SQL #InterviewPreparation


24



2
Feed post
View Shirisha Siliveri’s profile, open to work
Shirisha Siliveri

 • 3rd+

Databricks|Snowflake |DBT| Microsoft Fabric |ResponsibleAI|AgenticAI|GenAI|LLM|RAG| MLWorkflows|AWS|Azure|GCP|Python |SQL | BigData|Healthcare|Retail|FinancialServices

2w


Follow

🚀 Databricks Interview Series | Day 2: Unity Catalog

🎯 Topic: Unity Catalog in Databricks

💡 Main Interview Question: What is Unity Catalog in Databricks, and why do we use it?
✅ Answer:
Unity Catalog is Databricks' unified governance solution that helps organizations manage and secure data and AI assets across their Databricks environment.

It provides centralized capabilities for:
🔹 Access Control: Manage who can access data and what actions they can perform.
🔹 Data Discovery: Organize and discover tables, views, volumes, and other supported data assets.
🔹 Data Lineage: Track how data flows from source tables through transformations to downstream tables and reports.
🔹 Auditing: Monitor data access and governance-related activities through supported audit capabilities.
🔹 Centralized Governance: Apply governance policies across supported workspaces and data assets.

📌 Example:
Imagine an organization has three teams:
• Finance Team
• Marketing Team
• Data Engineering Team
All three teams use the same Databricks environment, but each team requires different levels of data access.
Using Unity Catalog, administrators can grant the Finance team access to financial data, the Marketing team access to marketing data, and the Data Engineering team the permissions needed to build and maintain data pipelines.
This helps enforce security while enabling teams to work with the data they need.

🔥 4 Follow-Up Interview Questions and Answers

Q1. What is the three-level namespace in Unity Catalog?
✅ Answer:
Unity Catalog organizes data using a three-level namespace:
catalog.schema.table
For example:
production.sales.orders

This structure helps organize data assets logically and manage permissions at different levels.

Q2. What is the difference between Unity Catalog and the Hive metastore?
✅ Answer:
The Hive metastore is a traditional metadata management service used to organize tables and their metadata.
Unity Catalog provides a more comprehensive governance framework, including centralized access control, data discovery, lineage, auditing, and governance across supported workspaces.
Unity Catalog is generally preferred for modern Databricks environments that require centralized governance.

Q3. What is the difference between a managed table and an external table in Unity Catalog?
✅ Answer:
Managed Table:
Databricks manages the table's data lifecycle.
The table's data is stored in a location managed according to the configured storage and governance setup.
Dropping the table generally removes its data after the applicable retention period and platform rules.
External Table:
The data is stored at an externally managed storage location.
Unity Catalog manages the table's metadata and access governance.
Dropping the table removes its metadata registration but generally leaves the underlying data files intact.


----


🚀 Databricks Interview Preparation series : Day 4

Q. How does Lakeflow Declarative Pipelines handle incremental data processing?

Answer
Lakeflow Declarative Pipelines (formerly Delta Live Tables) supports incremental data processing through streaming tables and materialized views.

Instead of reprocessing the entire dataset every time, it processes new or changed data incrementally whenever possible. This helps reduce processing time, compute costs, and resource utilization.

I would explain it using the following approach:

1. Incremental Data Ingestion
For continuously arriving files in ADLS Gen2, I would use Auto Loader with cloudFiles to detect and ingest new files incrementally.

2. Streaming Tables
I would use streaming tables when processing continuously arriving data. They maintain streaming progress and process newly available records without repeatedly scanning all historical data.

3. Incremental Transformations
I would create downstream streaming tables to apply transformations, filtering, and data quality checks.

4. Materialized Views
For aggregated or analytical datasets, I would use materialized views. Lakeflow Declarative Pipelines manages their refresh and can incrementally update results when supported by the query and source changes.

For example, I could maintain daily sales summaries instead of recalculating the entire aggregation manually for every execution.

5. Automatic Dependency Management
Lakeflow Declarative Pipelines automatically identifies dependencies between datasets and manages execution order.
I don't need to manually orchestrate every transformation step.

6. Monitoring and Reliability
I would monitor pipeline execution, processing metrics, data quality expectations, and failed records to ensure reliable incremental processing.

Key Interview Point:
Lakeflow Declarative Pipelines simplifies incremental processing by managing streaming progress, dependencies, and supported incremental refresh operations automatically. This avoids unnecessary full-table processing and improves pipeline efficiency.

Follow-up Interview Questions that you may research about- will be answering in coming days.

1. What is the difference between a streaming table and a materialized view in Lakeflow Declarative Pipelines?

Streaming Table = For continuous real-time ingestion.

Materialized View = For storing precomputed aggregated results for fast querying and reporting.

A simple way to remember:

👉 Streaming Table = Incremental Data Movement
 👉 Materialized View = Incremental Data Aggregation


---

🚀 Databricks Interview Preparation Series- Day 5

CDC vs. CDF vs. SCD vs. Time Travel
These four concepts are commonly discussed in Databricks and Delta Lake interviews. Although they are related to data changes and history, each serves a different purpose.
Let's understand them with simple examples.

1️⃣ CDC – Change Data Capture
What is it?
CDC is a technique used to capture changes happening in a source database, such as INSERT, UPDATE, and DELETE operations.

Example:
Suppose a customer changes their city from Dallas to Austin.
Instead of loading the entire customer table again, CDC captures only the changed customer record.

Use cases:
Incremental data ingestion
Database replication
Near-real-time data synchronization
Processing inserts, updates, and deletes

2️⃣ CDF – Change Data Feed
What is it?
CDF is a Delta Lake feature that tracks row-level changes made to a Delta table between table versions.
It provides change information such as:
INSERT
UPDATE_PREIMAGE
UPDATE_POSTIMAGE
DELETE

Example:
ALTER TABLE customers SET TBLPROPERTIES (   delta.enableChangeDataFeed = true );

Read changes:
df = ( spark.read .format("delta")     .option("readChangeFeed", "true")     .option("startingVersion", 10)     .table("customers") )

Use cases:
Incremental ETL
Downstream data synchronization
Auditing row-level changes
Building CDC pipelines from Delta tables

3️⃣ SCD – Slowly Changing Dimensions
What is it?
SCD is a data warehousing technique used to manage changes in dimension attributes over time.

The two commonly used approaches are:
SCD Type 1 – Overwrite existing data
Only the latest value is maintained.
Example:
Customer ID    City
101                  Austin

The previous value, Dallas, is overwritten.

SCD Type 2 – Maintain historical records
Every important change creates a new record version.
Customer ID   City      StartDate   EndDate Current
101               Dallas     Jan 1          Jan 15     No
101               Austin     Jan 15         NULL      Yes

Use cases:
Customer history tracking
Financial reporting
Audit requirements
Historical analytics
In Databricks Lakeflow Declarative Pipelines, AUTO CDC supports implementing SCD Type 1 and Type 2.

4️⃣ Time Travel – Delta Lake
What is it?
Time Travel allows us to query an earlier version of a Delta table using its transaction history.
It is useful when we need to investigate previous data states or recover from accidental changes.
Example:
View table history:
DESCRIBE HISTORY customers;
Query an earlier version:
SELECT * FROM customers VERSION AS OF 10;
Query using a timestamp:
SELECT * FROM customers TIMESTAMP AS OF '2026-01-15 10:00:00';
Restore an earlier version:
RESTORE TABLE customers TO VERSION AS OF 10;

Use cases:
Data recovery
Auditing
Debugging
Historical reporting
Reproducing previous results
Time Travel depends on the availability of the required transaction-log history and data files.

🔥 Quick Comparison
ConceptPurposeExampleCDCCaptures source data changesINSERT, UPDATE, DELETECDFTracks row-level changes in Delta tablesReads changes between versionsSCDManages historical dimension changesType 1 and Type 2Time TravelAccesses previous table versionsVERSION AS OF

💡 Remember This in Interviews
CDC: What changed in the source system?
CDF: What changed in the Delta table?
SCD: How should we maintain changed records in the target?
Time Travel: What did the table look like in the past?


----


🚀Databricks Interview preparation series : Day 6

☀️Bringing CI/CD and Deployment Automation to Databricks with Asset Bundles!

Managing Databricks projects across Dev, QA, and Production environments can become challenging without a standardized deployment strategy.

That’s where Databricks Asset Bundles (DABs) come in!

DABs help bring software engineering best practices into Databricks by enabling us to define, validate, deploy, and manage data and AI projects as code.

🔹 Key Benefits of Databricks Asset Bundles:

✅ Infrastructure and configurations defined as code
✅ Consistent deployments across multiple environments
✅ Seamless CI/CD integration with GitHub Actions, Azure DevOps, and other CI/CD platforms
✅ Version control and collaboration through Git
✅ Parameterized configurations for Dev, QA, and Prod
✅ Automated deployment of jobs, pipelines, notebooks, and other Databricks resources

💡 Why does this matter?

Instead of manually creating and updating Databricks resources in each environment, DABs provide a repeatable, reliable, and automated deployment process.

The bigger picture: DABs help transform Databricks development from manual workspace management into a structured, scalable, and production-ready DevOps approach.


---

Databricks Interview preparation series - Day 7

Performance Optimization Q&A

Interviewers often ask questions about OPTIMIZE, ZORDER, VACUUM, partitioning, file compaction, and Spark performance tuning.

Let’s understand few of them with practical examples. 👇

🔹 1. What is OPTIMIZE in Databricks?

OPTIMIZE is used to improve Delta table performance by reorganizing data files and combining smaller files into larger files.

It primarily helps solve the small file problem.

Benefits:

* Reduces the number of small files.
* Improves query performance.
* Reduces file scanning overhead.
* Improves data organization.

🔹 2. What is ZORDER BY?

Z-Ordering is a data optimization technique that colocates related data in the same set of files.

It improves data skipping when queries frequently filter on specific columns.

Benefits:

* Improves filtering performance.
* Reduces unnecessary file scanning.
* Particularly useful for high-cardinality filter columns.

Interview question: Can we ZORDER by multiple columns?

Yes, but effectiveness generally decreases as more columns are added.

🔹 3. What is the difference between OPTIMIZE and ZORDER?

OPTIMIZE:

* Combines small files.
* Performs bin-packing.
* Improves file sizes and layout.

ZORDER:

* Improves data locality.
* Organizes related column values together.
* Helps data skipping.

Remember: ZORDER is an additional optimization used with OPTIMIZE, not a replacement for it.

🔹 4. What is VACUUM in Databricks?

VACUUM removes obsolete data files that are no longer referenced by the Delta transaction log and have exceeded the retention period.

Important points:

* Default retention period is 7 days.
* Helps reduce cloud storage costs.
* Permanently removes eligible obsolete files.
* Can affect Delta Time Travel.

⚠️ Never blindly reduce the VACUUM retention period. Databricks recommends retaining at least 7 days.

🔹 5. Difference between OPTIMIZE and VACUUM?

OPTIMIZE:

* Rewrites and reorganizes data files.
* Improves query performance.
* Does not immediately delete the old files.

VACUUM:

* Removes eligible obsolete files.
* Reduces storage consumption.
* Does not optimize the current data layout.

🔹 6. What is Liquid Clustering?

Liquid Clustering is a modern Databricks optimization technique that improves data organization based on selected clustering columns.

Benefits:

* Reduces data scanning.
* Supports changing clustering keys.
* Handles high-cardinality columns.
* Reduces the need for traditional partitioning and ZORDER.

🔥 Important: Databricks recommends Liquid Clustering for new tables instead of traditional partitioning and ZORDER in applicable Delta Lake workloads.


----

Databricks interview Preparation Series - Day 8

Moving on to advanced scenario based questions 

Q> A pipeline processes 100 million records daily, but processing time has increased from 30 minutes to 3 hours. How would you troubleshoot it?

Interview Answer

I would follow a systematic troubleshooting approach rather than immediately increasing cluster size.

First, I would compare the current pipeline execution with previous successful runs to identify where the performance degradation started.

I would investigate the following areas:

Step 1: Analyze Spark UI

I would check the Spark UI to identify:

* Which stage is taking the most time.
* Whether there are excessive shuffle operations.
* Whether tasks are running slowly.
* Whether there is data skew.
* Whether executors are experiencing memory pressure or failures.

Step 2: Check Data Skew

Sometimes, a few partitions contain significantly more records than others, causing certain tasks to take much longer.

For example, if one customer ID contains 50% of the records, joins or aggregations involving that customer may become expensive.

Possible solutions:

* Use Adaptive Query Execution (AQE).
* Apply salting for heavily skewed keys where appropriate.
* Optimize join strategies.
* Repartition based on appropriate keys.

Step 3: Analyze Shuffle Operations

Large joins, aggregations, and repartitioning operations can create expensive network shuffles.

I would:

* Reduce unnecessary repartitioning.
* Use broadcast joins for sufficiently small tables.
* Filter data before joins.
* Avoid unnecessary wide transformations.

Step 4: Check Delta Table Performance

I would investigate whether the source or target Delta tables contain too many small files and fix this using OPTIMIZE.

I would also evaluate liquid clustering or appropriate partitioning based on query patterns.

Step 5: Review Cluster Configuration

I would check:

* Executor memory.
* Number of executors.
* CPU utilization.
* Autoscaling behavior.
* Spill to disk.
* Photon availability for supported workloads.

Step 6: Review Data Volume and Pipeline Changes

I would compare:

* Daily record counts.
* Average record size.
* Source file sizes.
* Transformation logic changes.
* Number of columns being processed.
* Join conditions.


Short answer in Real world scenario-

After analyzing Spark UI, I discover that a join between customer and transaction tables is causing significant data skew.

I would investigate the skewed keys, consider AQE skew-join optimization, and review the join strategy.

I would then compare execution time and resource utilization before and after the changes.

Key Interview Point:

I would identify whether the bottleneck is caused by data skew, shuffle operations, small files, inefficient joins, or insufficient compute resources before making changes.

-----


Databricks Interview preparation series : Day 9

 Scenario based question
Q> Your source sends duplicate and out-of-order CDC records. How would you ensure the target table contains the correct latest record?

Interview Answer

When implementing CDC, handling duplicate and out-of-order records is very important because the target table must always reflect the correct latest state.

I would implement CDC using a business key, source sequence number, and appropriate deduplication logic.

Step 1: Identify the Business Key
First, I would identify the unique business key.

This identifies which customer record needs to be inserted, updated, or deleted.

Step 2: Use a Sequence Number
I would use a source-generated sequence number, commit version, or reliable event timestamp to determine the correct order of changes.

Step 3: Deduplicate Records
Before applying CDC, I would identify duplicate events using the business key and sequence number

This removes duplicate events with the same business key and sequence number, assuming the source provides a reliable event identity and ordering field.

Step 4: Apply CDC Using AUTO CDC
In Lakeflow Declarative Pipelines, I would use AUTO CDC to manage inserts, updates, deletes, and SCD Type 1 or Type 2 processing.

The sequence field ensures that changes are applied in the correct logical order.

Step 5: Ensure Idempotency
I would make sure that replaying the same CDC events does not create duplicate records or incorrect updates.

I would also monitor:
* Duplicate events.
* Missing sequence numbers.
* Late-arriving records.
* Invalid business keys.
* Delete events.
* Source-to-target reconciliation.

Key Interview Point:

I would use business keys, reliable source sequence numbers, deduplication, and AUTO CDC to ensure the target reflects the correct latest state. I would also make the processing idempotent to handle retries and duplicate events safely.


-----

Databricks Interview preparation Series: Day 10

Q>  Multiple teams are accessing the same Delta tables. How would you implement security, governance, and workload isolation?

Interview Answer

In an enterprise Databricks environment, I would implement centralized governance using Unity Catalog, role-based access control, and workload isolation.
My goal would be to ensure that teams access only the data they are authorized to use while maintaining security, performance, and operational stability.

I would approach this in six steps.

Step 1: Implement Unity Catalog
I would use Unity Catalog as the centralized governance layer. It provides:

Centralized permissions.
Data discovery.
Audit capabilities.
Data lineage.
Fine-grained access control.
I would organize data using catalogs, schemas, and tables.

Step 2: Implement Role-Based Access Control
I would create groups based on business responsibilities.
I would follow the principle of least privilege.

Step 3: Implement Row-Level and Column-Level Security
Sometimes different users should access different rows or columns in the same table.
For example:
Finance users can see all financial transactions.
Regional managers can see only their region's transactions.
Sensitive customer information should be restricted.
I would use supported Unity Catalog features such as row filters and column masks to enforce these requirements.

Step 4: Implement Data Governance
I would establish:
Data ownership.
Data classification.
Sensitive data identification.
Data lineage.
Audit logging.
Data quality standards.
I would also consider Microsoft Purview integration where enterprise-wide governance requirements call for it.

Step 5: Implement Workload Isolation
Security alone is not enough. One team's heavy workload should not affect another team's production jobs.
I would separate workloads using:
Dedicated job compute.
Separate job clusters or appropriately configured compute.
Separate SQL warehouses for different workloads.
Workload-specific resource configurations.
Cluster policies and compute permissions.
I would also monitor resource consumption and configure appropriate concurrency and scaling settings.

Step 6: Monitor and Audit Access
I would use Unity Catalog audit capabilities and system tables where available to monitor:
Who accessed the data.
Which tables were queried.
Permission changes.
Data access patterns.
Unusual activity.

Real-World Example
Suppose Finance, Sales, and HR teams access the same enterprise Databricks platform.
I would create separate catalogs or schemas based on business domains, assign group-based permissions, and apply row-level or column-level restrictions for sensitive data.
I would also isolate production ETL workloads from interactive analytics workloads to prevent resource contention.

Key Interview Point:
I would combine Unity Catalog for centralized governance, role-based permissions for secure access, row and column-level controls for sensitive data, and separate compute resources for workload isolation.


---------


