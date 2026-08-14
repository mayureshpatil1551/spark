# Data Engineering Interview Questions & Answers

## Q1. How do you design a cost-optimized data pipeline in Azure for large-scale batch processing?

In our project, I would use ADF for orchestration and Azure Databricks for heavy transformations.

1. **ADF for orchestration** - Use Copy Activity for ingestion and parameterized, metadata-driven pipelines instead of creating separate pipelines for every table.

2. **Incremental loading** - Avoid processing the entire dataset every time. Use a watermark column such as `LAST_UPDATED_TS` to process only new or changed records.

3. **Databricks Job Clusters** - Use job clusters instead of all-purpose clusters so compute starts only when the batch job runs and terminates after completion.

4. **Autoscaling** - Configure minimum and maximum workers so the cluster scales based on workload.

5. **Optimize Spark processing** - Use partition pruning, appropriate partitioning, broadcast joins for small lookup tables, and avoid unnecessary shuffles and `collect()` operations.

6. **Use efficient storage formats** - Store data in Delta/Iceberg/Parquet rather than CSV because they provide better compression and query performance.

7. **Control small files** - Optimize the number and size of output files to avoid the small-file problem.

8. **Monitoring and cost control** - Monitor ADF pipeline duration and Databricks job/cluster utilization, and set appropriate cluster policies, auto-termination, and job timeouts.

---

## Q2. What is the role of Managed Identity in Azure and How do you use it in ADF or Databricks?

In our Azure environment, we used Managed Identity to avoid hardcoding credentials in ADF pipelines and Databricks notebooks.

We assigned the required RBAC permissions to the identity on Azure resources such as ADLS Gen2 and Key Vault. ADF could then authenticate to those services through the managed identity, which made the pipeline more secure and easier to maintain.

### In ADF

> "In ADF, I can enable the System-assigned Managed Identity for the Data Factory and then grant that identity the required RBAC permissions on resources such as ADLS Gen2 or Key Vault. Then I use Managed Identity authentication in the corresponding Linked Service."

### In Databricks

> "For Databricks, we can use Azure Managed Identity or workload identity-based authentication to access Azure resources securely. For example, the identity can be granted permissions on ADLS Gen2, allowing Databricks jobs to access storage without putting storage account keys or passwords in notebooks."

---

## Q3. How would you implement data masking in Azure SQL for sensitive columns?

Data masking hides sensitive data such as customer names, phone numbers, or email IDs from unauthorized users while keeping the actual data unchanged.

> "In Azure SQL, I would implement Dynamic Data Masking on sensitive columns such as email, phone number, or customer ID. I would define a masking rule on those columns and control which users can view the unmasked data using permissions. This allows us to protect sensitive data without changing the underlying values."

```sql
ALTER TABLE Customer
ALTER COLUMN email
ADD MASKED WITH (FUNCTION = 'email()');
```

---

## Q4. Write a SQL approach to detect duplicate records based on multiple columns.

```sql
SELECT id, name, COUNT(*)
FROM table_name
GROUP BY id, name
HAVING COUNT(*) > 1;
```

---

## Q5. How do you handle schema inference vs predefined schema in PySpark? Which is better in production?

I prefer a predefined schema, especially for large or frequently arriving files. It gives us better performance because Spark does not need to infer the schema, and it also prevents unexpected data-type changes from causing downstream issues.

```python
from pyspark.sql.types import *

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("amount", DoubleType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(path)
```

---

## Q6. What is checkpointing in Spark and when should you use it?

I have not worked on streaming data in my project. My experience is mainly with batch processing.

I know Spark checkpointing is used to truncate long lineage and provide recovery, but I have not implemented streaming checkpointing in production.

---

## Q7. How do you implement parallel processing in Python ETL workloads?

In my project, we achieve parallel processing mainly through PySpark.

Spark divides the data into partitions and processes those partitions in parallel across executors.

At the pipeline level, we use ADF ForEach with controlled parallelism when multiple independent tables need to be processed.

---

## Q8. How would you structure a reusable Python ETL framework?

In our project, we developed a reusable PySpark-based ETL framework where the same generic code could handle multiple source systems like Oracle, CSV, XML, and REST APIs.

We made the framework metadata-driven, so source type, connection details, source object, target, and load type were passed as parameters.

Based on the source type, the framework selected the appropriate reader, applied common transformations, and loaded the data into the target.

This reduced code duplication and made onboarding new sources much easier.

---

## Q9. Your Databricks job is running out of memory. How would you troubleshoot and fix it?

In one of our Databricks batch jobs, we were processing a large dataset and the job failed with an Out Of Memory error. I first checked the Spark UI and found that the failure was happening during a join stage, where one partition was much larger than the others. This indicated data skew.

I checked the join keys and found that some keys had a very high number of records. Instead of immediately increasing the cluster size, I optimized the code.

For a small lookup table, I used a broadcast join so that Spark did not need to perform a large shuffle. For the skewed data, I adjusted the partitioning and, where required, used salting. I also checked that we were not unnecessarily caching large DataFrames.

After the changes, I reran the job and monitored the Spark UI again to verify that the partitions were more evenly distributed and the memory consumption was reduced.

If the workload was still genuinely too large after code optimization, then I would increase the worker size or number of workers.

### What exactly did you check in Spark UI?

> "I checked the failed stage, executor memory usage, task duration, input size, shuffle read/write, and partition sizes. If one task was processing significantly more data than other tasks, I would investigate data skew."

---

## Q10. You need to process incremental data without duplicates. How would you design the logic?

For example, if the source has 1 million records and only 10,000 records were newly inserted or updated, we do not process all 1 million records.

We use the previous watermark from Azure SQL and extract only the incremental records.

Before loading, we deduplicate them using the business key, and then perform a MERGE into the target.

This makes the process idempotent, so if the pipeline is rerun, the same records would not be inserted again.

After successful processing, we update the watermark.

```sql
WITH src AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY dv_composite_key
               ORDER BY update_ts DESC
           ) AS rn
    FROM source_table
)
MERGE INTO target_table t
USING (
    SELECT *
    FROM src
    WHERE rn = 1
) s
ON t.dv_composite_key = s.dv_composite_key
WHEN MATCHED AND (
       NOT (t.name <=> s.name)
    OR NOT (t.add <=> s.add)
)
THEN UPDATE SET
    t.name = s.name,
    t.add = s.add
WHEN NOT MATCHED
THEN INSERT (id, name, add)
VALUES (
    s.id,
    s.name,
    s.add
);
```

---

## Q11. A pipeline runs successfully but produces incorrect data. How will you debug it?

If my ADF pipeline succeeds but produces incorrect data, I would first compare the source and target data.

I would check the ADF Monitor and activity outputs, especially the source query, parameters, row counts, and execution details.

Then I would check the Databricks notebook. I would validate the input parameters, watermark value, filters, joins, deduplication logic, and business transformations.

I would compare record counts and sample records after each layer—Raw, Silver, and Gold—to identify exactly where the incorrect data was introduced.

If it is an incremental load, I would also verify the watermark table in Azure SQL and make sure the correct date range was processed.

Finally, I would check the target MERGE condition to ensure records were not incorrectly updated or inserted.

---

## Q12. You need to migrate data from on-premise SQL Server to ADLS Gen2 securely. What approach would you follow?

I would create an ADF pipeline with a Copy Activity.

Since SQL Server is on-premise, I would install a Self-hosted Integration Runtime inside the client's network.

The SQL Server Linked Service would use this IR to securely access the source.

For ADLS Gen2, I would use Managed Identity authentication and grant the required RBAC permissions to ADF.

For the initial migration, I would perform a full load.

For subsequent runs, I would implement incremental loading using a watermark column such as `LastModifiedDate`, storing the watermark in Azure SQL.

I would also parameterize the pipeline so the same framework can be reused for multiple tables.

Finally, I would validate source and target record counts and configure monitoring and retry handling in ADF.
