02_Master Reading spark DAGs
----------------------------------


catalog = maintance info about table info, schema

logical plan - 

filter push down  optimized logical plan - u r doing filter in middle on query. its retuurn smaller no. of records.
pushdown projection optimized logical plan - select * but spark know u r using 5 col only.

and generate optimized plan for use.
yhen converted into serval phisical plan. that plan r going run actual in cluster. 
. its goes thruugh cost model. picks up the one , which is most efficient one.


=====================================

Narrow Transformations - 
Wide Transformations - 
* Repartition (200)-
    it can be used to both increase and decrease number of partitions.
    It shuffle the data.f
df.rdd.getNumPartitions()
df.repartition(24).explain(True)

AQE - adaptiveSparkPlan (part of AQE)
AQE refer to run time static stics.
using them seelct most efficient exiqution plan. chice the best plan


* Coalesce -
    It reduce the no. of partitions.
    Its tried to avoid the shuffle.
df.rdd.getNumPartitions()
df.coalesce(10).explain(True)

Q. Why doesnt .coalesce() explicity show the partitioning scheme?
--> .coalesce doesn't show the partitioning scheme e.g. RoundRobinPartitioning because:
- The operation only minimizes data movement by merging into fewer partitions on the same executor. It doesnt do any shuffling..
- because no shuffling is done. the partitioning shema remains the same as the original DF and spark doesn't include it explicity in it
s plan as the partitioning scheme is unaffected by .coalesce.

when u do repartition, partiton scheme is invole ie roundrobinpartition.
but in coalesce,there no partition scheme is invole.beacouse there is no shuffle is invole.so there is no partition scheme.

* Joins - 
check the how the query plan look like for join
>>spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
>>df_joined = (
    df.join(
        df_customers, how='inner', on = 'cust_id'
    )
)
>>df_joined.explain(True)

hashpartitioning - same keys end up in same partitioning.
it basically, hashes ur join key/grp by key. and tell u which partition the row needs to go.


* Group by - 
check the how the query plan look like for Group by

partial count - it means it count locally befoer doing shuffle. its helpfull.


* BroupBy Count Distinct -
function[] - means u are doing distinct in physical plan
=====================================================================================================

🔥 1️⃣ How does Spark execute a query internally?
Logical Plan → Optimized Plan → Physical Plan
🎯 Strong Answer:
"When we run a Spark query, it first creates a logical plan. Then Catalyst optimizer applies optimizations like filter pushdown and projection pushdown to create an optimized logical plan. After that, Spark generates multiple physical plans, evaluates them using cost-based optimization, and selects the most efficient one for execution."

🔥 2️⃣ What is Filter Pushdown? Why is it important?
Reduces data early.
🎯 Strong Answer:
"Filter pushdown moves filtering logic closer to the data source. This reduces the number of records loaded into Spark, minimizing I/O and improving performance."

🔥 3️⃣ How do you identify shuffle in a Spark physical plan?
Look for Exchange
🎯 Strong Answer:
"In the physical plan, shuffle is indicated by the Exchange operator. If I see Exchange hashpartitioning or Exchange roundrobinpartitioning, I know data is being redistributed across executors."

🔥 4️⃣ Difference between Narrow and Wide transformation?
🎯 Strong Answer:
"Narrow transformations do not require shuffle and operate within the same partition, like filter and map. Wide transformations require shuffle and create stage boundaries, like join, groupBy, and repartition."

🔥 5️⃣ Difference between Repartition and Coalesce?
🎯 Strong Answer:
"Repartition always causes shuffle and can increase or decrease partitions. Coalesce only reduces partitions and tries to avoid shuffle by merging partitions within the same executor."

Follow-up ready answer:
"Repartition creates a new partitioning scheme like RoundRobinPartitioning, while coalesce preserves the existing partitioning."

🔥 6️⃣ What is HashPartitioning in joins?
🎯 Strong Answer:
"HashPartitioning hashes the join key to ensure rows with the same key go to the same partition. This is necessary for shuffle-based joins like SortMergeJoin."

🔥 7️⃣ What is Partial Aggregation in GroupBy?
🎯 Strong Answer:
"Spark performs partial aggregation before shuffle to reduce data movement. It aggregates locally within partitions, then shuffles the reduced data, and finally performs final aggregation."

🔥 8️⃣ What is AQE? Why is it useful?
🎯 Strong Answer:
"Adaptive Query Execution uses runtime statistics to dynamically optimize the execution plan. It can change join strategies, reduce shuffle partitions, and handle skew data."

🔥 9️⃣ Why is count(distinct) expensive?
🎯 Strong Answer:
"count(distinct) is expensive because Spark must ensure global uniqueness, which requires additional shuffle and often multiple aggregation stages."

🔥 🔟 How do you analyze a slow Spark job?
🎯 Strong Answer:
"I check the physical plan using explain(true), identify Exchange operators to detect shuffle, analyze join strategy, verify partition count, check for skew, and ensure AQE is enabled. Then I optimize using broadcast joins, partition tuning, or reducing shuffle."

🧠 BONUS – Interview Trap Question
❓ Why doesn’t coalesce show partitioning scheme in physical plan?
🎯 Answer:
"Because coalesce avoids shuffle and preserves the original partitioning. Since no new partitioning strategy is introduced, Spark does not explicitly show a partitioning scheme."

