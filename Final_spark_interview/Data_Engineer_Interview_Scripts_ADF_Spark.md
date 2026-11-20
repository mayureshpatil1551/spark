# Data Engineer Interview Preparation — Real-Project Answer Scripts

**Prepared around your resume:** FinOps Dataverse migration using Azure Data Factory (ADF), Azure Databricks, PySpark, Delta Lake and ADLS Gen2; and the e-Clinical platform using PySpark, Apache Iceberg and AWS S3.

## How to use these answers

- Speak in the first person, but do not claim a tool or trigger was used in production unless you actually used it.
- Your strongest real examples are the FinOps pipeline handling **16+ TB**, config-driven ADF/Databricks ingestion from Oracle and flat files, and optimization work that improved report query latency by **about 40%**.
- For the e-Clinical project, use the **20+ TB**, multi-source ingestion and PySpark/Iceberg examples.
- Where an answer says “I would design”, it is an architecture answer—not a claim that you implemented that exact feature.

---

# Part 1 — Architecture, Spark and data engineering

## 1. How do you optimize a PySpark job that is taking too long to complete?

**Interview script:**

“First, I identify where the time is being spent instead of randomly changing Spark configurations. I check the Spark UI for slow stages, task imbalance, shuffle read and write, spills, and executor utilization. I also review the execution plan using `explain()`.

Then I optimize the data flow: filter early, select only required columns, avoid unnecessary `collect()` and repeated actions, and use broadcast joins when one side is small. For skewed joins, I consider skew handling or salting. I also check partition counts and file sizes, and use partition pruning and AQE where appropriate.

In my FinOps project, we worked with more than 16 TB of data. We improved the storage layout and query performance using techniques such as Z-Ordering, clustering and compaction. This reduced report query latency by around 40% across more than 300 reports.”

**If asked what you check in Spark UI:** jobs → stages → tasks; longest stage; task-duration imbalance; shuffle read/write; spill to memory/disk; failed or straggler tasks; executor utilization.

## 2. Explain the difference between Delta Lake, Data Lake and Data Warehouse.

**Interview script:**

“A Data Lake is the storage layer where we keep large volumes of raw and processed data in formats such as Parquet, CSV and JSON, usually on low-cost object storage like ADLS or S3.

Delta Lake is a table format and storage layer built on data lake storage. It adds a transaction log and capabilities such as ACID transactions, schema enforcement, schema evolution, time travel and reliable `MERGE` operations.

A Data Warehouse is designed for curated, structured, analytics-ready data and SQL reporting.

In my FinOps project, ADLS Gen2 was the storage foundation and Delta Lake was used for managed data tables processed through Databricks. The curated data supported downstream reporting. So, the lake is the storage foundation, Delta adds reliable table management, and the warehouse is the analytics-serving concept.”

**Quick distinction:**

| Concept | Main purpose | Example |
|---|---|---|
| Data Lake | Store raw and varied data at scale | ADLS Gen2 / S3 |
| Delta Lake | Reliable transactional tables on a data lake | Delta tables |
| Data Warehouse | Curated analytics and BI workloads | Azure Synapse dedicated SQL pool / Snowflake |

## 3. How would you design an end-to-end pipeline using ADF and Databricks?

**Interview script:**

“In my FinOps project, ADF was used for orchestration and Databricks for large-scale data processing.

The source data came from systems such as Oracle and flat files. ADF handled the pipeline orchestration, source connectivity and parameter passing. It invoked Databricks notebooks and passed configuration values such as source, target and load details.

In Databricks, I used PySpark to read the data, apply transformations and validations, and write it into Delta tables in ADLS Gen2. We followed a layered approach so that raw ingestion was separated from transformed and reporting-ready data.

I would also include audit logging, row-count checks, error handling, retries, alerts and incremental processing. After successful processing, the pipeline status and metrics should be recorded for monitoring and support.”

**Architecture flow:**

`Source systems → ADF pipeline → Databricks notebook (PySpark) → ADLS Gen2 / Delta tables → curated reporting layer`

**Useful follow-up points:**
- ADF parameters make pipelines reusable.
- Key Vault or managed identity should be used for secrets and authentication.
- Use incremental loads where possible rather than reprocessing all historical data.
- Add validation and reconciliation before marking a batch successful.

## 4. What happens internally when a Spark job is executed?

**Interview script:**

“When an application is submitted, the Driver creates the Spark session and coordinates execution. When an action such as `count()`, `collect()` or a write operation is called, Spark creates a job.

The Driver converts the logical plan into an optimized physical plan and builds a DAG. The DAG Scheduler divides the job into stages, usually separated by shuffle boundaries. The Task Scheduler then sends tasks to executors.

Executors process partitions in parallel and perform operations such as reading, filtering, joining and aggregating. If a shuffle is required, data is redistributed between executors. Finally, task results are returned or written to the target.

In simple terms: Driver coordinates, DAG Scheduler creates stages, Task Scheduler schedules tasks, and Executors process the data.”

**Remember:**
- **Driver:** coordinates the Spark application.
- **Executor:** runs tasks and can cache data.
- **Job:** created by an action.
- **Stage:** group of tasks that can run without crossing a shuffle boundary.
- **Task:** unit of work for one partition.
- **Shuffle:** redistribution of data, commonly required for joins, `groupBy` and aggregations.

## 5. How do you handle schema changes in production data pipelines?

**Interview script:**

“I handle schema changes through validation and controlled schema evolution. First, I compare the incoming schema with the expected schema and classify the change.

If a nullable column is added, it may be backward-compatible and can be accepted after validation. If a column is removed, renamed or its data type changes, I treat it as a breaking change and assess the downstream impact before allowing the pipeline to continue.

In a Delta-based pipeline, schema evolution can be enabled for approved changes, but I would not enable it blindly for every incoming file. I validate required columns, data types and key fields, capture rejected records, log the schema version and alert the support team.

In my project, because data came from multiple source systems, keeping ingestion configuration and validation controlled was important for reliable downstream reporting.”

**Good production practices:**
- Maintain an expected schema or contract.
- Allow only approved additive changes.
- Validate mandatory columns, types and business keys.
- Quarantine incompatible records or files.
- Alert on breaking changes and track schema versions.
- Test downstream tables and reports before deploying a breaking change.

---

# Part 2 — Azure Data Factory triggers

## 1. What is a Trigger in Azure Data Factory, and why is it used?

**Interview script:**

“A trigger is the mechanism that starts an ADF pipeline automatically based on a defined condition. It removes the need to start the pipeline manually.

For example, in a data ingestion solution, we may need to run a pipeline every morning, every 30 minutes, or whenever a new file arrives in ADLS Gen2. We select the trigger based on the business requirement.”

## 2. What are the different types of triggers in ADF?

**Interview script:**

“ADF mainly supports three commonly used trigger types: Schedule, Tumbling Window and Event-based triggers. Event-based triggers include Storage Event and Custom Event triggers.

A Schedule Trigger runs at defined calendar times. A Tumbling Window Trigger runs at fixed, continuous time windows and supports dependencies and retries. A Storage Event Trigger starts a pipeline when a storage event, such as a new blob, occurs. A Custom Event Trigger starts a pipeline when a matching custom event is published through Event Grid.”

## 3. Files are generated every day at 6 AM. Which trigger would you use and why?

**Interview script:**

“I would use a Schedule Trigger because the requirement is time-based and the file is expected at a fixed time every day.

I would configure it to run daily at 6 AM in the source system’s relevant time zone. I would also add a file-existence or readiness check before processing, because a scheduled pipeline may start before the file is fully available.”

## 4. How would you execute an ADF pipeline every 30 minutes?

**Interview script:**

“I would create a Schedule Trigger with a 30-minute recurrence. I would configure the start time, time zone and recurrence interval, attach it to the pipeline, publish the changes and start the trigger.

I would also check the pipeline’s concurrency and make sure the previous run cannot create duplicate or conflicting writes.”

## 5. What is a Schedule Trigger? Give a real-time scenario.

**Interview script:**

“A Schedule Trigger starts a pipeline at a specified time or recurring schedule. For example, if a business wants to load the previous day’s financial data every morning at 6 AM, I can configure a daily Schedule Trigger.

The pipeline can then extract the data, invoke Databricks for transformations and load the curated output for reporting.”

## 6. What is a Tumbling Window Trigger, and how is it different from a Schedule Trigger?

**Interview script:**

“A Tumbling Window Trigger runs pipelines for fixed, contiguous time windows, such as 10 AM to 11 AM and 11 AM to 12 PM. Each window is tracked independently and it supports retries, dependencies and backfilling.

A Schedule Trigger is mainly calendar-based. It starts at configured times, but it does not provide the same window-state and dependency management.

For hourly data processing where every hour must be accounted for, I would prefer a Tumbling Window Trigger.”

## 7. Business requires continuous processing in hourly windows. Which trigger would you use?

**Interview script:**

“I would use a Tumbling Window Trigger with a one-hour window and a one-hour frequency. Each run receives its own window start and end time.

I would pass those values to the pipeline and use them as filters in the source query or Databricks notebook. This makes each hourly batch traceable and supports recovery of failed windows.”

## 8. How do you configure dependencies between Tumbling Window Triggers?

**Interview script:**

“I configure a dependency from the downstream Tumbling Window Trigger to the upstream trigger. I select the upstream trigger and define the dependency offset if required.

For example, if the reporting pipeline should run only after the hourly ingestion pipeline completes, I make the reporting trigger dependent on the ingestion trigger. I also validate the window alignment and dependency conditions before publishing.”

## 9. What is a Storage Event Trigger in ADF?

**Interview script:**

“A Storage Event Trigger starts an ADF pipeline when a supported storage event occurs, usually when a blob is created or deleted in Azure Storage.

It uses Azure Event Grid for event delivery. For example, when a new source file arrives in an ADLS Gen2 landing folder, the trigger can start the ingestion pipeline automatically.”

## 10. Whenever a new file arrives in ADLS Gen2, how would you implement this?

**Interview script:**

“I would create an Event-based Storage Trigger in ADF and connect it to the storage account. I would configure the required container and folder filters, then attach the trigger to the ingestion pipeline.

I would pass the event’s file name and folder path to the pipeline. The pipeline would validate the file, process it through Databricks and record the processed file details to prevent duplicate processing.”

## 11. How do you configure an event trigger for a specific folder and file extension?

**Interview script:**

“I configure the Storage Event Trigger with the required container, folder path and file-name filters. For example, I can restrict the folder to `/landing/finance/` and use a file-name suffix filter for `.csv`.

I would test the filter with both matching and non-matching files to make sure only the required files start the pipeline.”

## 12. Pipeline should execute only when a CSV arrives, not JSON. How would you handle this?

**Interview script:**

“I would configure the Storage Event Trigger’s file-name filter to match the `.csv` suffix. JSON files would not match that filter, so they would not start the pipeline.

I would still validate the file extension and content inside the pipeline as a second layer of protection.”

## 13. What is a Custom Event Trigger in ADF?

**Interview script:**

“A Custom Event Trigger starts an ADF pipeline when a custom event matching the configured subscription is published through Azure Event Grid.

For example, an upstream application may publish an event named `FinanceBatchReady` after it completes file generation. ADF can subscribe to that event and start the processing pipeline.”

## 14. How would you trigger an ADF pipeline when an event is published through Azure Event Grid?

**Interview script:**

“I would create a Custom Event Trigger in ADF and configure the Event Grid subscription details and event filters. I would select the event type or subject filter required by the application and attach the trigger to the pipeline.

When the publisher sends a matching event, Event Grid delivers it and ADF starts the pipeline. I would pass relevant event details as pipeline parameters and make the processing idempotent.”

## 15. Difference between Schedule Trigger and Tumbling Window Trigger?

**Interview script:**

“Schedule Trigger is suitable when a pipeline must run at particular calendar times, such as every day at 6 AM. Tumbling Window Trigger is suitable when data must be processed in fixed time windows, such as every hour, with window tracking, retries, dependencies and backfill.

So, for a simple daily batch I would use Schedule Trigger. For hourly window-based processing where every interval must be accounted for, I would use Tumbling Window Trigger.”

## 16. Can multiple triggers execute the same ADF pipeline?

**Interview script:**

“Yes. Multiple triggers can be attached to the same pipeline. For example, a daily Schedule Trigger can run the regular batch, while a Storage Event Trigger can run the same pipeline when a late-arriving file is received.

I would use pipeline parameters to identify the execution source and input details. I would also implement idempotency and concurrency controls so that overlapping runs do not process the same data incorrectly.”

## 17. How do you start, stop and monitor triggers in ADF?

**Interview script:**

“In ADF Studio, I go to the Manage section and open the Triggers page. I can start or stop a trigger from there. If I make changes to a trigger, I publish them before starting it again.

For monitoring, I check the Monitor section for trigger runs and pipeline runs. I review the run status, start time, errors and the linked pipeline execution.”

## 18. A scheduled pipeline failed yesterday. How would you rerun the missed execution?

**Interview script:**

“I would first open the Monitor page and identify the failed pipeline run. I would review the error, fix the root cause and then rerun the failed pipeline with the same parameters.

If it is a missed Schedule Trigger execution, I would manually trigger the pipeline for the missed business date or use a backfill mechanism in the design. I would make sure the load is idempotent so rerunning does not create duplicate records.”

## 19. What happens when you modify a trigger that is already running?

**Interview script:**

“In ADF, trigger changes need to be published. For a running trigger, I generally stop it, make the required changes, publish them and start it again.

I would also check existing pipeline runs separately. Updating a trigger configuration does not mean an already-started pipeline run is automatically changed or cancelled.”

## 20. How do you pass dynamic parameters from a trigger to an ADF pipeline?

**Interview script:**

“I define parameters in the pipeline and map trigger values to those parameters in the trigger configuration.

For example, for a Tumbling Window Trigger I can pass `windowStartTime` and `windowEndTime`. For a Storage Event Trigger, I can pass the folder path and file name from the event. Inside the pipeline, I access them using pipeline parameters and pass them to the Databricks notebook.”

**Example expressions:**

```text
@trigger().outputs.windowStartTime
@trigger().outputs.windowEndTime
@triggerBody().folderPath
@triggerBody().fileName
```

“The exact event properties depend on the trigger type and its output schema, so I verify them in the trigger’s debug output.”

## 21. Pipeline should execute only on weekdays. How would you configure it?

**Interview script:**

“I would configure a Schedule Trigger with a weekly recurrence and select Monday through Friday. I would set the required execution time and time zone.

I would test the next scheduled run times and make sure the pipeline handles holidays separately if the business calendar requires it.”

## 22. How would you prevent duplicate pipeline executions when multiple files arrive simultaneously?

**Interview script:**

“I would not rely only on the trigger to prevent duplicates. I would make the pipeline idempotent.

For each incoming file, I would maintain a processing-control table or metadata record with a unique file identifier, path, status and processed timestamp. Before processing, I would check whether the file has already completed successfully. I would also use a suitable concurrency setting and avoid writing the same business data twice.

For parallel file arrivals, I can process independent files in parallel, but I would control concurrent writes to the same target where necessary.”

## 23. Difference between a Trigger Run and a Pipeline Run?

**Interview script:**

“A Trigger Run represents the trigger’s evaluation or firing for its configured schedule or event. A Pipeline Run represents the actual execution of the pipeline.

A trigger can initiate a pipeline run, but they are separate monitoring records. I check the trigger run to understand whether the trigger fired and the pipeline run to understand whether the data processing succeeded.”

## 24. Tumbling Window Trigger failed for multiple consecutive windows. How would you troubleshoot and recover?

**Interview script:**

“First, I would check the failed windows in ADF Monitor and identify whether the failure is caused by the source, connectivity, authentication, Databricks execution or target write.

I would inspect the activity-level error details and logs, fix the root cause and then rerun the failed windows. I would verify the window start and end parameters and confirm that the target write is idempotent.

After recovery, I would validate row counts and data completeness for all affected windows before allowing downstream reporting to continue.”

## 25. How would you select triggers for daily batch, hourly processing and event-based processing?

**Interview script:**

“For daily batch processing at a fixed time, I would use a Schedule Trigger. For hourly processing where every time window must be tracked and recoverable, I would use a Tumbling Window Trigger. For event-based processing where a pipeline should start as soon as a file arrives, I would use a Storage Event Trigger.

I select the trigger based on the business timing requirement, recovery needs and whether the pipeline is time-driven or event-driven.”

---

# Part 3 — Practical follow-up questions you may get

## A. How do you make an incremental pipeline reliable?

**Interview script:**

“I use a watermark or another change-tracking mechanism to identify new or changed data. The pipeline reads the last successful watermark, processes only the required data and writes the target using an upsert or merge strategy where appropriate.

I update the watermark only after the target write and validation succeed. I also maintain audit details such as batch ID, source count, target count, start time, end time and status. This helps with restart and troubleshooting.”

**Project connection:** You can relate this to your multi-source ingestion and incremental processing work. Be precise about which project used which watermark implementation.

## B. What happens if Databricks succeeds but the ADF pipeline fails?

**Interview script:**

“I would check the activity status and the Databricks job output before rerunning. ADF may show a failed pipeline even if the notebook completed its data write but a later activity failed.

To handle this safely, I design the target write to be idempotent and maintain a batch or run identifier. On retry, the pipeline checks whether that batch was already committed, rather than blindly appending the same data again.”

## C. How do you handle pipeline failures?

**Interview script:**

“I use activity-level retries for transient failures, timeouts and failure paths for logging and notifications. I capture the pipeline run ID, batch ID, source details and error message.

For a data-quality or schema failure, I would quarantine the invalid input instead of repeatedly retrying it. Once the root cause is fixed, I rerun the failed batch and validate the output.”

## D. How do you secure ADF and Databricks pipelines?

**Interview script:**

“I prefer managed identity or service principals for authentication and Azure Key Vault for secrets that are still required. I follow least-privilege access for storage, databases and workspaces. I avoid hardcoding credentials in notebooks or pipeline JSON, and I make sure sensitive values are not printed in logs.”

## E. How do you monitor a production pipeline?

**Interview script:**

“I monitor pipeline and activity status in ADF, and notebook execution details in Databricks. I also capture operational metrics such as rows read, rows written, rejected rows, processing duration and batch status.

For production support, I use alerts for failures and delays, and I keep enough audit information to identify which source file or batch caused the issue.”

---

# Part 4 — Short technical revision

| Topic | Interview-ready point |
|---|---|
| Broadcast join | Use when one side of the join is small enough to fit in executor memory; reduces shuffle of the large side. |
| Shuffle | Data redistribution across partitions/executors; often expensive because it involves network and disk I/O. |
| Data skew | Some partitions contain much more data than others; inspect task duration and shuffle size, then consider AQE skew handling or salting. |
| Partition pruning | Reads only relevant partitions when filters match the partition columns. |
| Z-Ordering | Co-locates related data values to improve data skipping for common filter columns; choose columns based on query patterns. |
| Compaction | Combines many small files into fewer larger files to reduce file listing and task overhead. |
| AQE | Adaptive Query Execution can revise the physical plan at runtime, including shuffle partition coalescing and skew-join handling. |
| `repartition()` | Increases or redistributes partitions and usually causes a shuffle. |
| `coalesce()` | Reduces partitions, often with less data movement; it may not evenly redistribute data. |
| `cache()` / `persist()` | Reuse an expensive DataFrame across multiple actions; only persist when reuse justifies the memory/storage cost. |
| ADF Integration Runtime | Compute/infrastructure used by ADF for data movement, activity dispatch and connectivity. |
| ADF parameters | Values supplied to a pipeline, dataset or linked service at runtime to make it reusable. |
| ADF variables | Mutable values used inside a pipeline run. |
| Pipeline concurrency | Limits the number of concurrent runs of a pipeline; it does not replace data-level idempotency. |
| Idempotency | Re-running the same batch produces the same correct target state without duplicate effects. |

---

# Part 5 — A strong project introduction for these questions

**Interview script (about 45–60 seconds):**

“I have around three years of experience in data engineering, mainly working with PySpark, SQL, Azure Data Factory and Databricks.

In my current FinOps Dataverse project, I work on data ingestion and transformation pipelines using ADF and Azure Databricks. We process large volumes of financial data from sources such as Oracle and flat files, store it in ADLS Gen2 using Delta Lake, and prepare curated data for reporting. The platform handles more than 16 TB, and our storage and query optimization work improved report query latency by around 40%.

Earlier, I worked on an e-Clinical data platform using PySpark and Apache Iceberg on AWS S3, integrating data from Oracle, Snowflake and APIs.

My focus is on building reliable incremental pipelines, handling large datasets, optimizing Spark jobs and supporting production data flows.”

---

# Final interview reminders

1. **Do not overstate trigger experience.** If you configured a particular trigger in your project, describe the exact implementation. Otherwise say, “For this requirement, I would use…”
2. **Use one real example in every major answer.** FinOps is your best ADF + Databricks + Delta example; e-Clinical is your best PySpark + Iceberg + multi-source example.
3. **Explain the reason behind the design.** For example: Schedule = fixed time; Tumbling Window = tracked time intervals; Storage Event = file/event arrival.
4. **For optimization questions, start with measurement.** Mention Spark UI and execution plan before listing tuning techniques.
5. **Do not claim exactly-once processing from an event trigger.** Explain idempotency, deduplication and recovery instead.
6. **Keep answers conversational.** Give the direct answer first, then the project example, then one or two implementation details.
