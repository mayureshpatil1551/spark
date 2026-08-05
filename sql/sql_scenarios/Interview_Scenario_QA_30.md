# 30 Scenario-Based Interview Questions ("What problem did you face, how did you fix it?")

Each answer follows the same simple structure: **Problem → Why it happened → What I did → Result**. These are prep templates built from your resume — practice them in your own words, and adjust any number/detail that doesn't exactly match what you did.

---

**1. Q: Your pipeline runtime dropped from 4 hours to 58 minutes. What was actually going wrong before, and how did you fix it?**
A: The pipeline was slow because the data wasn't partitioned well — Spark was creating too many small tasks and shuffling a lot of data between nodes. I looked at the Spark UI, found the stage that was taking the longest, and re-partitioned the data on the right key (a column used often in filters/joins). This cut shuffle time a lot. I also increased the file size per partition to avoid the "small file problem." Result: runtime dropped from 4 hours to under an hour.

**2. Q: You mention cutting query latency by 40%. What was slow, and what did you do?**
A: Reports built on top of our Gold tables were slow because Delta Lake had too many small files after repeated writes, and queries were scanning more data than needed. I ran Z-Ordering on the columns that reports filtered on most, and set up regular file compaction (OPTIMIZE) so small files got merged. Result: query latency dropped by about 40% across 300+ reports.

**3. Q: Tell me about a time incremental loading broke or gave wrong data.**
A: Early on, our watermark logic wasn't handling records that arrived late (updated after the watermark had already moved past their timestamp). That meant some updates got skipped. I fixed it by adding a small buffer/lookback window to the watermark query, so we always re-checked a bit of overlap. Result: no more missed updates, and duplicates were handled by the MERGE logic so it stayed safe.

**4. Q: Describe a time you found duplicate or conflicting records across systems.**
A: In the MDM project, the same entity (e.g., a site or product) existed in Oracle, Snowflake, and flat files with slightly different values. I built an XREF (cross-reference) table that mapped each source record to one Golden Record ID, and wrote rule-based logic to decide which source's value should "win" when there was a conflict. Result: fewer duplicate/conflicting records reaching downstream reports.

**5. Q: What happened when a source system was down during a scheduled pipeline run?**
A: One of our five source systems (like Snowflake) was occasionally unavailable during a run. Instead of failing the whole pipeline, I designed it so that source's step could fail independently, log the error, and the rest of the pipeline continued. Then we had a retry/backfill job pick up the missed source later. Result: pipeline availability stayed high (around 99.9%) instead of one bad source blocking everything.

**6. Q: Tell me about a data quality issue a business user reported that wasn't a pipeline failure.**
A: A stakeholder said numbers in a report looked wrong, but there was no error in the pipeline logs — it was a "silent" issue. I traced the number back layer by layer: Gold table → Silver table → Bronze table → source system, comparing row counts and values at each stage. Found the issue was in a transformation rule that wasn't handling a specific edge case (like nulls). I fixed the rule and re-ran the affected batch. Result: correct numbers, and we added a validation check so it wouldn't happen silently again.

**7. Q: Describe a time you had to onboard a new data source quickly.**
A: Normally, adding a new source meant building a new pipeline from scratch, which took time. I built a configuration-driven framework — a metadata/config table that stores connection details, schema, and load type for each source. Onboarding a new source became "add a row to the config table" instead of writing new code. Result: onboarding time dropped by about 50%.

**8. Q: What problem came up when doing the 20+ TB migration to Iceberg with zero data loss?**
A: The biggest risk was missing or dropping records during the cutover from the old system to the new Iceberg tables. I ran the old and new systems in parallel for a period, and wrote a reconciliation script that compared row counts and checksums between both. When I found small gaps, I traced them to a filter condition in the migration script that was excluding some edge-case records, fixed the filter, and re-ran just those records. Result: verified zero data loss before decommissioning the old system.

**9. Q: Tell me about a time your MERGE/upsert logic caused duplicate or wrong data.**
A: When first implementing incremental upserts into Iceberg, I used a merge key that wasn't fully unique — two different source records could match to the same target row. This caused overwrites of good data with stale data. I fixed it by adding a proper composite key (like source ID + record ID) and adding a check that alerts if the merge key isn't unique before running. Result: safe, accurate upserts going forward.

**10. Q: Describe a time a small-file problem slowed down your Delta/Iceberg tables.**
A: After months of frequent incremental writes, our tables had thousands of tiny files, which made queries slow because Spark had to open each file separately. I set up a scheduled compaction job (OPTIMIZE for Delta) to merge small files into larger ones on a regular basis. Result: noticeably faster queries and lower job overhead.

**11. Q: What happened when the on-prem-to-cloud Oracle connection failed mid-load?**
A: The Integration Runtime connecting to on-prem Oracle occasionally dropped mid-copy, leaving a partial load. I made sure the Copy Activity used a staged/atomic write pattern (load to a staging area first, then move/merge into the final table) so a failed run wouldn't leave the destination table half-updated. I also added retry policies in ADF. Result: no more partial/corrupt loads even when the connection was unstable.

**12. Q: Tell me about a schema change from a source system that broke your pipeline.**
A: A source added a new column (or changed a data type) without notice, and the pipeline failed on schema mismatch. I added schema validation/evolution handling — for Delta, enabling controlled schema merge; for stricter tables, catching the mismatch early and alerting instead of failing silently. Result: new columns could be handled gracefully or flagged for review instead of crashing the whole pipeline.

**13. Q: Describe a time a Spark job failed midway through processing a huge dataset (like the 26TB or 20TB migrations).**
A: A large write job failed partway through due to a transient cluster/network issue, after processing most of the data. Because I was using Delta/Iceberg (which support ACID transactions), the partial write didn't corrupt the table — it just didn't commit. I re-ran the job, and because of idempotent/incremental design, it safely picked up where needed without duplicating already-written data. Result: no manual cleanup needed, just a re-run.

**14. Q: Tell me about a time REST API ingestion caused a problem (rate limits, pagination, etc.).**
A: An API source had rate limits, and pulling a lot of records too fast caused failures/throttling. I added pagination handling (pulling data page by page) and a retry-with-backoff mechanism so if a call got throttled, it waited and retried instead of failing the whole job. Result: reliable daily ingestion without hitting rate limit errors.

**15. Q: What issue came up with a manually-edited source like Smartsheet?**
A: Because Smartsheet data was edited directly by business users, it sometimes had inconsistent formatting or missing required fields — unlike our system-generated sources. I added extra validation checks specifically for that source (null checks, format checks) before it entered the pipeline, and flagged bad rows instead of letting them fail the whole batch. Result: bad manual entries got caught early instead of corrupting downstream data.

**16. Q: Describe a time your automated DDL/config generator didn't handle something correctly.**
A: My Python script that auto-generated table DDL and pipeline configs worked fine for simple flat schemas, but broke on a source with nested/complex fields (like arrays). I extended the generator's logic to detect nested types and generate the correct DDL syntax for them, instead of a full rewrite. Result: the tool kept saving ~50 hours/week of manual work, now covering more source types too.

**17. Q: Tell me about a time two systems (Iceberg and Snowflake) got out of sync during dual-target writes.**
A: In the dual-target pipeline, a write to one target occasionally succeeded while the other failed (e.g., a transient Snowflake connection issue), causing the two platforms to drift. I added a reconciliation check that compared row counts/checksums between both targets after each run, and a retry step for whichever target failed. Result: consistency between Iceberg and Snowflake was restored and caught automatically going forward.

**18. Q: Describe a performance issue caused by data skew.**
A: Some partitions had way more data than others (data skew) — for example, one financial category had far more records than the rest — which made a few Spark tasks run much longer than others and slow the whole job. I identified the skewed key using the Spark UI, and either salted the key or used a broadcast join for the smaller side of the join. Result: task times became more balanced and the job finished faster.

**19. Q: Tell me about a time a scheduled pipeline failed at an inconvenient time and how you responded.**
A: A pipeline failed overnight due to a source connection timeout. I checked the ADF/Databricks run logs first to identify the failed activity, confirmed it was a transient network issue (not a data problem), and re-triggered just that failed activity instead of the whole pipeline. Result: minimal delay, and I added a retry policy so future transient failures wouldn't need manual intervention.

**20. Q: Describe a situation where your business rule logic gave the wrong result on an edge case.**
A: A deduplication rule that used "most recent updated_at wins" gave the wrong result when two records had the exact same timestamp (a tie). I added a secondary tiebreaker rule (like preferring the more "trusted" source system) to resolve ties deterministically. Result: no more ambiguous/duplicate Golden Records from timestamp ties.

**21. Q: Tell me about a time you had to debug why a report's numbers didn't match the source system.**
A: I compared the Gold table output against a direct query on the SAP source for the same period. The mismatch came from a transformation step that was excluding certain rows due to an incorrect filter condition. I fixed the filter, validated against source again, and re-ran the affected load. Result: numbers matched, and I added an automated row-count/value check between source and Gold as an early warning for future mismatches.

**22. Q: Describe a time your watermark/control table got corrupted or reset.**
A: The watermark control table had a bad value written to it (from a failed run), which risked either re-processing old data or skipping new data on the next run. I manually corrected the watermark value after validating the last successfully processed timestamp from logs, then re-ran the pipeline. Result: no data loss or duplication, and I added a safeguard so watermark updates only commit after a successful run.

**23. Q: Tell me about a time Z-Ordering or clustering didn't actually help performance.**
A: I Z-Ordered on a column that wasn't actually used in most report filters, so latency didn't improve much. I went back and checked the actual query patterns from the 300+ reports, picked the columns that were filtered/joined on most often, and re-applied Z-Ordering on those. Result: the latency improvement (the 40% figure) came after correcting the column choice.

**24. Q: Describe a time you had to reduce manual/repetitive work for your team.**
A: Every new source needed manually written DDL and pipeline configs, which took a lot of time and was error-prone (typos, inconsistent naming). I built a Python-based generator that created DDL and config files automatically from a schema definition. Result: eliminated roughly 50 hours/week of manual work and reduced config-related errors.

**25. Q: Tell me about a time you had to handle a "late merge" — two Golden Records that turned out to be the same real entity.**
A: We found two Golden Records that were actually duplicates of the same entity created from different source combinations. Using the XREF table, I mapped both sets of source records to a single surviving Golden Record ID and updated the mapping, instead of deleting history. Result: downstream reports could still trace back correctly, and duplicate Golden Records were resolved without breaking existing references.

**26. Q: Describe a time cost or resource usage became a problem during a large migration.**
A: During the 20TB+ migration, running everything as one huge job was expensive and slow because of resource contention on the cluster. I broke the migration into smaller, parallel batches by logical partition (e.g., by date range or source category) and tuned cluster size per batch. Result: better resource usage and a more predictable, faster overall migration.

**27. Q: Tell me about a time you had to validate data after a migration to prove correctness.**
A: After migrating to Iceberg, I wrote SQL-based validation scripts that compared row counts, key values, and aggregate sums (like total transaction amounts) between the old and new systems. When I found a mismatch, I drilled into the specific table/partition to isolate it. Result: gave the business confidence to fully cut over, since the numbers were proven to match.

**28. Q: Describe a time you had to balance "full load" vs "incremental load" decisions.**
A: For most tables, incremental was efficient, but one table had frequent hard deletes at the source that incremental logic (which only looks at inserts/updates) couldn't detect. I switched that specific table to a periodic full reload (or added a delete-detection step comparing key sets) instead of forcing incremental everywhere. Result: accurate data even for tables with deletes, without over-engineering every table the same way.

**29. Q: Tell me about a time monitoring/alerting helped you catch an issue early.**
A: Before proper alerting was in place, pipeline failures were sometimes noticed late by business users instead of by us. I set up monitoring/alerts on pipeline failures and key data quality checks (like row count thresholds), so the team got notified immediately instead of finding out from a stakeholder complaint. Result: faster response time and fewer "surprise" data issues reaching business users.

**30. Q: Describe a time you had to explain a technical trade-off to a non-technical stakeholder when something went wrong.**
A: When a report was delayed because I had to fix a data quality issue rather than just push the (wrong) numbers on time, I explained to the finance stakeholder in simple terms: releasing wrong numbers now would cost more time later to correct and would hurt trust in the reports, so a short delay for a fix was the safer choice. Result: stakeholder agreed with the delay, and it built trust that we prioritized accuracy over just hitting a deadline.

---

### Note
Read through these and mark any where the specific detail (a technology, a root cause, a fix) doesn't match what you actually did — swap it for your real detail before the interview. Interviewers often ask one sharp follow-up ("what exactly did the Spark UI show you?" / "what was the actual config value?"), so the more these match your real memory of the work, the safer you are under follow-up questions.
