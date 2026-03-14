1️⃣ Spark Catalog
Catalog = metadata about tables
It stores:
Table name
Database name
Schema
Table location
Table type (managed / external)
Spark uses catalog during query planning.

2️⃣ Query Planning Phases in Spark
When you run a query:
df.filter(...).select(...).join(...).explain(True)
Spark goes through multiple stages:

🔹 1. Logical Plan				--> Initial high-level plan of your query.
🔹 2. Optimized Logical Plan	--> If you filter in the middle of a query, Spark pushes filter closer to data source.
🔹 3. Physical Plan				--> Logical plan is converted into multiple possible physical plans.

🔹 1. Logical Plan
	Initial high-level plan of your query.
	Example:
		Scan table
		Apply filter
		Select columns
		Join tables
This is NOT optimized yet.

🔹 2. Optimized Logical Plan
	Spark applies optimizations like:
	✅ Filter Pushdown
	If you filter in the middle of a query, Spark pushes filter closer to data source.
	Why?
	→ It reduces number of records early
	→ Less data moves in cluster
	→ Better performance

	✅ Projection Pushdown
	Even if you write:
	SELECT *
	But use only 5 columns later, Spark reads only required columns.
	→ Saves memory
	→ Reduces I/O

🔹 3. Physical Plan
	Logical plan is converted into multiple possible physical plans.
	Spark:
		Uses Cost-Based Optimizer (CBO)
		Evaluates statistics
		Chooses the most efficient execution plan
		This plan runs on cluster.

3️⃣ Transformations
🔹 Narrow Transformations
	* No shuffle
	* Each partition works independently
	* Fast
	Examples:
		map
		filter
		coalesce (without shuffle)

🔹 Wide Transformations
	* Shuffle happens
	* Data moves across partitions
	* Expensive
	Examples:
		groupBy
		join
		repartition

4️⃣ Repartition
>> df.rdd.getNumPartitions()
>> df.repartition(24).explain(True)
✔️ Key Points
	* Can increase OR decrease partitions
	* ALWAYS causes shuffle
	* Uses RoundRobinPartitioning (if no column specified)
	* Because shuffle happens:
		* New partitioning scheme is created
		* That’s why partitioning scheme is visible in physical plan

5️⃣ Coalesce
df.coalesce(10).explain(True)
✔️ Key Points
	Only decreases partitions
	Tries to avoid shuffle
	Merges partitions within same executor
❓ Why .coalesce() doesn’t explicitly(omething stated clearly, directly) show partition scheme?
Because:
	No shuffle happens
	Original partitioning is mostly preserved
	Spark doesn't create a new partitioning strategy like RoundRobin
👉 No shuffle → No new partition schema → Not explicitly shown

6️⃣ AQE – Adaptive Query Execution
AQE = AdaptiveSparkPlan
Enabled by:
>> spark.conf.set("spark.sql.adaptive.enabled", True)
Q. What it does?
	Spark:
	- Collects runtime statistics
	- Adjusts plan during execution
	- Chooses better strategy dynamically
Examples:
	- Converts sort merge join → broadcast join
	- Reduces shuffle partitions automatically
	- Handles skew joins
⚡ AQE uses runtime statistics, not static stats only.

7️⃣ Joins – Reading Physical Plan
>> spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
This disables broadcast join so you can inspect other join types.
>> df_joined.explain(True)
🔹 HashPartitioning
	During shuffle join:
		- Spark hashes join key
		- Ensures same keys go to same partition
		- Required for correct join
		- Example:
			* Exchange hashpartitioning(cust_id, 200)
Means:
	Data shuffled
	Partitioned using hash of cust_id
	200 partitions created
	
8️⃣ Group By
GroupBy is a wide transformation → causes shuffle.
In physical plan you’ll see:

HashAggregate

🔹 Partial Aggregation

You may see:
Partial HashAggregate

Meaning:
Spark performs local aggregation before shuffle
Then shuffles reduced data
Then performs final aggregation
This improves performance significantly.

9️⃣ GroupBy Count Distinct

You may see:
Aggregate functions [count(distinct col)]

In physical plan:
Spark may use special aggregation strategy
Often involves extra shuffle stage
Distinct is more expensive than normal count.

💡 Interview-Level Understanding
If interviewer asks:
Q: How do you read Spark DAG?
You should say:
	Check logical plan
	Check optimized logical plan
	Check physical plan
Look for:
	Exchange (shuffle)
	HashPartitioning
	BroadcastHashJoin
	SortMergeJoin
	Partial Aggregation
	AdaptiveSparkPlan

🎯 Final Correction Summary of Your Mistakes
Your Version					Correct 						Concept
AQE 							refer to runtime static stats	AQE uses runtime statistics
coalesce 						no partition scheme involved	It preserves existing partitioning
function[] 						means distinct					It shows aggregation function in physical plan
repartition 					always shuffle					Correct

===========================================================================================================

🔥 1️⃣ How does Spark execute a query internally?
Logical Plan → Optimized Plan → Physical Plan
-->
"When we run a Spark query, it first creates a logical plan. Then Catalyst optimizer applies optimizations like filter pushdown and projection pushdown to create an optimized logical plan. After that, Spark generates multiple physical plans, evaluates them using cost-based optimization, and selects the most efficient one for execution."


===========================================================================================================

🔥 2️⃣ What is Filter Pushdown? Why is it important?
Reduces data early.
-->
"Filter pushdown moves filtering logic closer to the data source. This reduces the number of records loaded into Spark, minimizing I/O and improving performance."

===========================================================================================================


🔥 3️⃣ How do you identify shuffle in a Spark physical plan?
✅ Expectation:
Look for Exchange
-->
"In the physical plan, shuffle is indicated by the Exchange operator. If I see Exchange hashpartitioning or Exchange roundrobinpartitioning, I know data is being redistributed across executors."


===========================================================================================================

🔥 4️⃣ Difference between Narrow and Wide transformation?
-->
"Narrow transformations do not require shuffle and operate within the same partition, like filter and map. Wide transformations require shuffle and create stage boundaries, like join, groupBy, and repartition."

===========================================================================================================

🔥 5️⃣ Difference between Repartition and Coalesce?
-->

"Repartition always causes shuffle and can increase or decrease partitions. Coalesce only reduces partitions and tries to avoid shuffle by merging partitions within the same executor."

Follow-up ready answer:

"Repartition creates a new partitioning scheme like RoundRobinPartitioning, while coalesce preserves the existing partitioning."


===========================================================================================================

🔥 6️⃣ What is HashPartitioning in joins?
-->

"HashPartitioning hashes the join key to ensure rows with the same key go to the same partition. This is necessary for shuffle-based joins like SortMergeJoin."


===========================================================================================================

🔥 7️⃣ What is Partial Aggregation in GroupBy?
-->

"Spark performs partial aggregation before shuffle to reduce data movement. It aggregates locally within partitions, then shuffles the reduced data, and finally performs final aggregation."


===========================================================================================================

🔥 8️⃣ What is AQE? Why is it useful?
-->

"Adaptive Query Execution uses runtime statistics to dynamically optimize the execution plan. It can change join strategies, reduce shuffle partitions, and handle skew data."


===========================================================================================================

🔥 9️⃣ Why is count(distinct) expensive?
-->

"count(distinct) is expensive because Spark must ensure global uniqueness, which requires additional shuffle and often multiple aggregation stages."


===========================================================================================================

🔥 🔟 How do you analyze a slow Spark job?
-->

"I check the physical plan using explain(true), identify Exchange operators to detect shuffle, analyze join strategy, verify partition count, check for skew, and ensure AQE is enabled. Then I optimize using broadcast joins, partition tuning, or reducing shuffle."

🧠 BONUS – Interview Trap Question
❓ Why doesn’t coalesce show partitioning scheme in physical plan?
🎯 Answer:

"Because coalesce avoids shuffle and preserves the original partitioning. Since no new partitioning strategy is introduced, Spark does not explicitly show a partitioning scheme."

