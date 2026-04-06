11_Broadcast Joins & AQE (Adaptive Query Execution)
===============================================================================================================

Key Takeaways:
Understanding Adaptive Query Execution (AQE) and its benefits.
How to optimize shuffle partitions and joins using AQE.
Setting up a Spark session and properties to handle data skew dynamically.
Analyzing the distribution of data and identifying skewed partitions.
Comparing the performance of sort merge join with and without AQE.
-------------------------------------------------------------------------------------------------------

00:00 Introduction 
00:35 What is AQE?
04:25 Sort-Merge-Join of Customer & Transaction Dataset
06:00 Spark UI showing Data Skew
06:41Join of Customer & Transaction Dataset (AQE enabled)
07:04 Code + Spark UI - Comparing Join Performance (with & without AQE)
10:52 Broadcast Join 
11:18 Internal Working of Sort Merge Join 
13:12 Concept of Hash Partitioning 
14:47 Sort Merge Join example
17:12 Broadcast Join example
19:44 Code for Broadcast join fixing Data Skew


---------------------------------------------------------------------------------------------------------
* Introduction 
-------------------------
We are learning techniques to solve data skew in Spark:
1. AQE (Adaptive Query Execution)
2. Broadcast Join

---------------------------------------------------------------------------------------------------------
* What is AQE?
-------------------------
AQE - Adaptive Query Execution
spark on its own run time staticstics, to basically select most efficient query plan.
runtime staticstics are like -
    Size of input DataFrames (bytes)
    No. of partitions

Below tunning AQE give ous-
1. Tunning shuffle partitions
2. Optimizing joins
3. Optimizing Skew Joins

1. Tunning shuffle partitions
-->
lets say, we hv 200 shuffle partition which is defaoult.
You hv DF which hving 15 distinct keys.
means remaining 185 partition is empty.
So, spark do the coalesc all partitions into 15 partition.partition is 15
this optimize the spark does.
--
Default shuffle partitions might be too high.
Example: 200 partitions but only 15 distinct keys → 185 partitions are empty.
AQE coalesces empty partitions into actual needed partitions.


2. Optimizing Joins
-->
SortMergeJoin into Broadcast Join

SortMergeJoin are operation heavy.
first operation is shuffling. shuffling involve data transfer all over the network
Boradcast join, dont involve shuffling.
--
Converts Sort-Merge Join (SMJ) into Broadcast Join if beneficial.
Sort-Merge Join involves heavy shuffling across the network.
Broadcast Join avoids shuffling by sending a small table to all executors.


3. Optimizing Skew Joins 
-->
AQE does, the large partiton into smaller partiton. The skewed partiton in sort merge join into smaller partion. means it break into smaller partition.
which can we can easily process the code.


Spark configurations for AQE:
>> spark.conf.set("spark.sql.adaptive.enabled", "true")
>> spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

---------------------------------------------------------------------------------------------------------
* Sort-Merge-Join of Customer & Transaction Dataset
-------------------------
# spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.set("spark.sql.adaptive.enabled", "false")

from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[4]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.set("spark.sql.adaptive.enabled", "false")

## Join Skews
transactions_file = "../../data/data_skew/transactions.parquet"
customer_file = "../../data/data_skew/customers.parquet"

df_transactions = spark.read.parquet(transactions_file)
df_customers = spark.read.parquet(customer_file)
                                                                                
df_transactions.printSchema()
df_transactions.show(5, False)
root
 |-- cust_id: string (nullable = true)
 |-- start_date: string (nullable = true)
 |-- end_date: string (nullable = true)
 |-- txn_id: string (nullable = true)
 |-- date: string (nullable = true)
 |-- year: string (nullable = true)
 |-- month: string (nullable = true)
 |-- day: string (nullable = true)
 |-- expense_type: string (nullable = true)
 |-- amt: string (nullable = true)
 |-- city: string (nullable = true)

[Stage 2:>                                                          (0 + 1) / 1]
+----------+----------+----------+---------------+----------+----+-----+---+-------------+------+-----------+
|cust_id   |start_date|end_date  |txn_id         |date      |year|month|day|expense_type |amt   |city       |
+----------+----------+----------+---------------+----------+----+-----+---+-------------+------+-----------+
|C0YDPQWPBJ|2010-07-01|2018-12-01|TZ5SMKZY9S03OQJ|2018-10-07|2018|10   |7  |Entertainment|10.42 |boston     |
|C0YDPQWPBJ|2010-07-01|2018-12-01|TYIAPPNU066CJ5R|2016-03-27|2016|3    |27 |Motor/Travel |44.34 |portland   |
|C0YDPQWPBJ|2010-07-01|2018-12-01|TETSXIK4BLXHJ6W|2011-04-11|2011|4    |11 |Entertainment|3.18  |chicago    |
|C0YDPQWPBJ|2010-07-01|2018-12-01|TQKL1QFJY3EM8LO|2018-02-22|2018|2    |22 |Groceries    |268.97|los_angeles|
|C0YDPQWPBJ|2010-07-01|2018-12-01|TYL6DFP09PPXMVB|2010-10-16|2010|10   |16 |Entertainment|2.66  |chicago    |
+----------+----------+----------+---------------+----------+----+-----+---+-------------+------+-----------+
only showing top 5 rows



df_customers.printSchema()
df_customers.show(5, False)
root
 |-- cust_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- zip: string (nullable = true)
 |-- city: string (nullable = true)

+----------+-------------+---+------+----------+-----+-----------+
|cust_id   |name         |age|gender|birthday  |zip  |city       |
+----------+-------------+---+------+----------+-----+-----------+
|C007YEYTX9|Aaron Abbott |34 |Female|7/13/1991 |97823|boston     |
|C00B971T1J|Aaron Austin |37 |Female|12/16/2004|30332|chicago    |
|C00WRSJF1Q|Aaron Barnes |29 |Female|3/11/1977 |23451|denver     |
|C01AZWQMF3|Aaron Barrett|31 |Male  |7/9/1998  |46613|los_angeles|
|C01BKUFRHA|Aaron Becker |54 |Male  |11/24/1979|40284|san_diego  |
+----------+-------------+---+------+----------+-----+-----------+



# You may see some keys with millions of records, causing skew.
(
    df_transactions
    .groupBy("cust_id")
    .agg(F.countDistinct("txn_id").alias("ct"))
    .orderBy(F.desc("ct"))
    .show(5, False)
)
+----------+--------+
|cust_id   |ct      |
+----------+--------+
|C0YDPQWPBJ|17539732|
|CBW3FMEAU7|7999    |
|C3KUDEN3KO|7999    |
|C89FCEGPJP|7999    |
|CHNFNR89ZV|7998    |
+----------+--------+
only showing top 5 rows


# Perform SMJ (without AQE):
# Check Spark UI → Event Timeline → Observe long-running skewed tasks.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) # disable autoBroadcastJoinThreshold because want to do sortmergejoin
df_txn_details = (
    df_transactions.join(
        df_customers,
        on="cust_id",
        how="inner"
    )
)
start_time = time.time()
df_txn_details.count()
print(f"time taken: {time.time() - start_time}")



---------------------------------------------------------------------------------------------------------
* Spark UI showing Data Skew
-------------------------
spark UI --> Event Timeline --> Executor compustition time.

Execution Computittion time is lot



---------------------------------------------------------------------------------------------------------
* Join of Customer & Transaction Dataset (AQE enabled)
-------------------------
# Using AQE for Join Optimization - 
# AQE automatically splits skewed partitions and reduces the number of shuffle partitions dynamically.
# Check Spark UI → DAG visualization → AQEShuffleRead will show optimized partitions.
>> spark.conf.set("spark.sql.adaptive.enabled", "true")
>> spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")
>> df_txn_details = (
>>     df_transactions.join(
>>         df_customers,
>>         on="cust_id",
>>         how="inner"
>>     )
>> )
>> start_time = time.time()
>> df_txn_details.count()
>> print(f"time taken: {time.time() - start_time}")


---------------------------------------------------------------------------------------------------------
* Code + Spark UI - Comparing Join Performance (with & without AQE)
-------------------------

spark UI --> Job --> select job_id --> open with AQE

Compair with AQE and without AQE
stage ---> DAG Visualization 
stage ---> completed stages 
stage ---> Event time

check qury plan
spark UI --> SQL 
analyse sql query plan for 
    without AQE
    qith AQE - added one more step ie.AQEShuffleRead(reduce no. of partitions), check min,max time to process task



---------------------------------------------------------------------------------------------------------
* Broadcast Join 
-------------------------
- Efficient when one table is small enough to fit in executor memory.
- Avoids shuffling.
- Ideal for skewed datasets because partitioning is flexible.

Broadcast Join  - how it work for more than 2 tbl. like 4,5

---------------------------------------------------------------------------------------------------------
* Internal Working of Sort Merge Join 
-------------------------


steps - 
shuffling 
sorting
merging


---------------------------------------------------------------------------------------------------------
* Concept of Hash Partitioning 
-------------------------
Partition key determines partition:
    hash(key) % num_partitions
    hash(k1,k2,...) % num_partitions

Helps in distributing data evenly across partitions.


---------------------------------------------------------------------------------------------------------
* Sort Merge Join example
-------------------------

2 datasets =
    cusotmer - 50 records
    Transaction - 3 Milion records

3 executor - each executor hving one partiton with 1 Milion records(Transaction)

Sort Merge Join steps:
    shuffle - costly op.
    sort
    merge -- in merging, here pointer are there in 2 dataset. when pointer is match  then that row will be join

sort merge join, data is shuffled, sorte then merge
Shuffle is network-intensive → performance bottleneck.

---------------------------------------------------------------------------------------------------------
* Broadcast Join example
-------------------------

2 datasets =
    cusotmer - 50 records
    Transaction - 3 Milion records

3 executor - each executor hving one partiton with 1 Milion records(Transaction)

Here, we are going to sent one copy of cusotmer dataset to each partition(executor)
then join betwen 2 dataset

Boradcast join are immune to skewed input data set  
beacuse u hv complete flexibility to partiton it the way u want.
there is no compulsion  that you want that you hv to partiton it bye join keys.
 



---------------------------------------------------------------------------------------------------------
* Code for Broadcast join fixing Data Skew
-------------------------
# 10MB = 10485760 Bytes
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
df_txn_details = (
    df_transactions.join(
        F.broadcast(df_customers),
        on="cust_id",
        how="inner"
    )
)
start_time = time.time()
df_txn_details.count()
print(f"time taken: {time.time() - start_time}")
                                                                                
39790092
time taken: 2.7766311168670654
# spark.stop()


-----------------------------------------------
Performance Comparison - 
    - SMJ without AQE: Long tasks due to skewed partitions.
    - SMJ with AQE: Skewed partitions split → reduced task duration.
    - Broadcast join: Small table broadcast → fastest, avoids shuffle, immune to skew.


=============================================================================================
Q. How does Broadcast Join work for more than 2 tables, like 4 or 5 tables?
-->
Broadcast Join in Spark works by sending a small table to all executor nodes so that the join can happen locally without shuffling the larger table. When we have more than 2 tables, Spark can still use broadcast joins in a chained or multi-way manner, depending on table sizes and join order.

Let me explain with an example:
Suppose we have 4 tables: customers, transactions, products, and regions.
    customers table → 50 records
    transactions → 3 million records
    products → 100 records
    regions → 20 records

The idea is: small tables are broadcasted to the executor nodes where the large table partitions reside. Spark automatically decides which tables to broadcast based on the spark.sql.autoBroadcastJoinThreshold


Step-by-step:
    1. Join transactions (large) with customers (small) → broadcast customers.
    2. Join the result with products (small) → broadcast products.
    3. Join the intermediate result with regions (small) → broadcast regions.
Each broadcasted table is copied to all partitions of the large dataset in memory. This avoids shuffling for all small tables and ensures efficient joins.

Code example 
-----------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.master("local[4]").getOrCreate()

# Sample tables
customers = spark.read.parquet("customers.parquet")      # 50 rows
transactions = spark.read.parquet("transactions.parquet") # 3 million rows
products = spark.read.parquet("products.parquet")         # 100 rows
regions = spark.read.parquet("regions.parquet")           # 20 rows

# Multi-way broadcast join
df_joined = (
    transactions
    .join(broadcast(customers), on="cust_id", how="inner")
    .join(broadcast(products), on="product_id", how="inner")
    .join(broadcast(regions), on="region_id", how="inner")
)

df_joined.show(5)

-------------------------------------------------------
Key Points to Mention in Interview:
    - Broadcast join is ideal when one table is significantly smaller than the other.
    - Spark can chain multiple broadcast joins for multi-table joins.
    - Each broadcasted table is copied to all executor nodes, avoiding shuffles.
    - For 4–5 small tables joining a large table, Spark can handle this efficiently in memory.
    - If more than one table is large, Spark may revert to Sort-Merge Join for those tables.


✅ Pro Tip:- You can also mention join order matters in multi-table broadcast joins. Always broadcast the smallest tables first to avoid memory issues.
=================================================================================
1. What is Adaptive Query Execution (AQE) in Spark?
-->
"AQE is a Spark optimization technique that adjusts the physical query plan at runtime using statistics collected during execution. It can optimize shuffle partitions, convert sort-merge joins into broadcast joins, and handle skewed joins automatically.
Example configurations:

>> spark.conf.set("spark.sql.adaptive.enabled", "true")
>> spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

AQE improves performance without manual tuning."


-------------------------------------------------------
2. How does Spark handle skewed data with AQE?
-->
When a partition has much more data than others (skew), Spark splits that partition into smaller ones during runtime. This reduces task duration and prevents some executors from becoming bottlenecks.
AQE dynamically handles the skewed join partitions


-------------------------------------------------------
3. What is a Broadcast Join, and when would you use it?
-->
Broadcast Join is used when one table is small enough to fit in executor memory. The small table is copied to all partitions of the larger table, avoiding expensive shuffles.
Example:
>> from pyspark.sql.functions import broadcast
>> df_joined = large_df.join(broadcast(small_df), on="id", how="inner")

## 4. How does Broadcast Join work for multiple tables (3–5 tables)?
-->
# Spark can chain broadcast joins. For example, joining a large table with 3 small tables:
```python
>>df_joined = (
    large_df
    .join(broadcast(small1), on="id1")
    .join(broadcast(small2), on="id2")
    .join(broadcast(small3), on="id3")
    )

Each small table is copied to all partitions of the large table, avoiding shuffle.


-------------------------------------------------------
5. What is the difference between Sort-Merge Join and Broadcast Join?
-->
- Sort-Merge Join (SMJ): Works for large tables, involves shuffle → sort → merge.
    Broadcast Join: Copies small table to all partitions, avoids shuffle, faster for small+large table join.
    Example:

# SMJ is default if tables are large
df_large.join(df_large2, "id")
# Broadcast join
df_large.join(broadcast(df_small), "id")


-------------------------------------------------------

6. How do you control which table Spark broadcasts?
--->
>> "Spark uses the configuration `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). If a table size ≤ threshold, it is broadcasted.  
>> Example:

# Set threshold to 20MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 20 * 1024 * 1024)

-------------------------------------------------------

## **7. What are shuffle partitions, and how does AQE optimize them?**
**Answer:**
> "Shuffle partitions determine how data is distributed after a shuffle. Too many partitions → overhead, too few → data imbalance. AQE can **coalesce empty partitions** at runtime:
spark.conf.set("spark.sql.adaptive.enabled", "true")


-------------------------------------------------------
8. Can Broadcast Join handle skewed datasets?
-->
Yes, because the small table is copied entirely to all executor partitions, you don’t need to partition it by join key. Skew in the large table does not impact broadcast join.

-------------------------------------------------------
9. How do you compare performance of joins with and without AQE?
-->
You can use Spark UI to monitor execution time, shuffle read/write, and skewed partitions:

# AQE enabled
spark.conf.set("spark.sql.adaptive.enabled", "true")
df_joined.count()

Compare DAG visualization, task time, and partition size with AQE disabled.

-------------------------------------------------------
10. How does Spark decide between Sort-Merge Join and Broadcast Join?
-->
Spark evaluates table sizes at runtime.

    If one table is smaller than autoBroadcastJoinThreshold, Spark uses broadcast join.
    Otherwise, Sort-Merge Join is used.

AQE can upgrade a sort-merge join to broadcast join if runtime statistics show a table is smaller than threshold.

------------------------------------------------------- 