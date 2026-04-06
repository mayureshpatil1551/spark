10_How Salting Can Reduce Data Skew By 99%
=====================================================================================

00:00 Salting Concept 
07:06 Applying Salting In Joins 
12:53 Code Examples For Salting In Joins
16:56 Applying Salting In Aggregations
27:57 Code Examples For Salting In Aggregations


--------------------------------------------------------
Salting Concept 
-->
What is Salting - Adding randomness to key in order to be able to evenly distributed.
Data Skew happens when one or few keys contain a very large number of records compared to other keys.
eg.
Suppost we hv 1 dataset . in that partition value are
1 --> 1 Milion records
2 --> 5
3 --> 6
and partiton is 3.
formula --> hash(value)%shaffle.partitons
- So, one partition is very large hence its skew. It will take time to process.
To resolve this, we are using salting

salt number- its basically indicates howmuch we want to distribute the data. 
no. is we are giving 3. 
salt_number = 3
it will random give num between [0,3] --> 0,1,2
formula -->hash(value,salt)%shaffe.partitions.
before hash(1)%sp = 1%3 =1. hash(1) is same for all
now,hash(1,0)%3 = 0 partiton
    hash(1,1)%3 = 1 partiton
    hash(1,2)%3 = 2 partiton

Salting = Adding randomness to the join/aggregation key to distribute skewed data across multiple partitions.
Salt Number = Salt number determines how many buckets we divide skewed data into.



--------------------------------------------------------
Applying Salting In Joins ( How to apply salting inn skewed data base)
-->
dataset 1 --> it is skewed. value 1 is in milion
dataset 2 --> uniformly distributed dataset. partiton, values | 0 - 3 , 1 - 4, 2- 5 

problem --> All rows with value = 1 go to same partition → slow join
solution -->
Step 1 — Add salt to skewed dataset
    salt = random(0, salt_number-1)
Step 2 — Duplicate small dataset using explode
    Create salt array:
        [0,1,2]
    ⚠️ Important rule: Always explode the smaller dataset, because explode multiplies row

Step 3 — Join on value + salt
    Old join:
        join on value

    New join:
        join on (value, salt)

    Now records distribute across partitions:
        hash(value,salt) % partitions

    Result: balanced join workload

steps --
1. for dataset 1, choise is salt_number and assigning num between 0 to salt_number. we call that col ie salt
2. we create a array which contain all values form 0 until salt_number-1.
    [0,1,2] for dataset 2. 
    salt_values = [0,1,2]
    here are explode the dataset. explode is very costly operation.
    reccoamnd to do on smaller dataset
    ie -    1   0
            1   1
            1   2
            2   0   
            2   1
            2   2

3. We are going to do join on value and salt.
    early join only on value. hash(value)%shaffle.partitons
    Now, hash(1,0)%3 = 0
         hash(1,1)%3 = 1
         hash(1,2)%3 = 2


--------------------------------------------------------
Code Examples For Salting In Joins
-->

spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.adaptive.enabled", "false")


df_uniform = spark.createDataFrame([i for i in range(1000000)], IntegerType())
df_uniform.show(5, False)
[Stage 0:>                                                          (0 + 1) / 1]
+-----+
|value|
+-----+
|0    |
|1    |
|2    |
|3    |
|4    |
+-----+

# Check partition distribution:
(
    df_uniform
    .withColumn("partition", F.spark_partition_id()) # finding spark partiton id per row
    .groupBy("partition")
    .count()
    .orderBy("partition")
    .show(15, False)
)
[Stage 1:====>                                                    (1 + 11) / 12]
+---------+-----+
|partition|count|
+---------+-----+
|0        |82944|
|1        |82944|
|2        |83968|
|3        |82944|
|4        |83968|
|5        |82944|
|6        |82944|
|7        |83968|
|8        |82944|
|9        |83968|
|10       |82944|
|11       |83520|
+---------+-----+

                                  
# below df are not uniformly distributed.
# Create Skewed Dataset
df0 = spark.createDataFrame([0] * 999990, IntegerType()).repartition(1)
df1 = spark.createDataFrame([1] * 15, IntegerType()).repartition(1)
df2 = spark.createDataFrame([2] * 10, IntegerType()).repartition(1)
df3 = spark.createDataFrame([3] * 5, IntegerType()).repartition(1)
# Join Without Salting
df_skew = df0.union(df1).union(df2).union(df3)
df_skew.show(5, False)

+-----+
|value|
+-----+
|0    |
|0    |
|0    |
|0    |
|0    |
+-----+
only showing top 5 rows

                                                                                
(
    df_skew
    .withColumn("partition", F.spark_partition_id())
    .groupBy("partition")
    .count()
    .orderBy("partition")
    .show()
)
+---------+------+
|partition| count|
+---------+------+
|        0|999990|
|        1|    15|
|        2|    10|
|        3|     5|
+---------+------+

df_joined_c1 = df_skew.join(df_uniform, "value", 'inner')
(
    df_joined_c1
    .withColumn("partition", F.spark_partition_id())  # finding spark partiton id per row
    .groupBy("partition")
    .count()
    .show(5, False)
)
+---------+-------+
|partition|count  |
+---------+-------+
|0        |1000005|
|1        |15     |
+---------+-------+

# apply salting                                                                                
# Simulating Uniform Distribution Through Salting 
# Usually equal to shuffle partitions.
SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))
SALT_NUMBER
-->
3  # becouse spark.conf.set("spark.sql.shuffle.partitions", "3")

# Add Salt to Skewed Dataset
df_skew = df_skew.withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))
df_skew.show(10, truncate=False)
+-----+----+
|value|salt|
+-----+----+
|0    |2   |
|0    |0   |
|0    |0   |
|0    |1   |
|0    |2   |
|0    |2   |
|0    |0   |
|0    |2   |
|0    |2   |
|0    |1   |
+-----+----+
only showing top 10 rows


df_uniform = (
    df_uniform
    .withColumn("salt_values", F.array([F.lit(i) for i in range(SALT_NUMBER)]))
    .withColumn("salt", F.explode(F.col("salt_values")))
)
df_uniform.show(10, truncate=False)
+-----+-----------+----+
|value|salt_values|salt|
+-----+-----------+----+
|0    |[0, 1, 2]  |0   |
|0    |[0, 1, 2]  |1   |
|0    |[0, 1, 2]  |2   |
|1    |[0, 1, 2]  |0   |
|1    |[0, 1, 2]  |1   |
|1    |[0, 1, 2]  |2   |
|2    |[0, 1, 2]  |0   |
|2    |[0, 1, 2]  |1   |
|2    |[0, 1, 2]  |2   |
|3    |[0, 1, 2]  |0   |
+-----+-----------+----+
only showing top 10 rows



df_joined = df_skew.join(df_uniform, ["value", "salt"], 'inner')
(
    df_joined
    .withColumn("partition", F.spark_partition_id())
    .groupBy("value", "partition")
    .count()
    .orderBy("value", "partition")
    .show()
)
[Stage 42:>                                                         (0 + 3) / 3]
-->
value 0 is uniformly distributed on 3 partitons and other are moraless the same
+-----+---------+------+
|value|partition| count|
+-----+---------+------+
|    0|        0|332774|
|    0|        1|333601|
|    0|        2|333615|
|    1|        0|     6|
|    1|        1|     9|
|    2|        0|     2|
|    2|        1|     2|
|    2|        2|     6|
|    3|        0|     3|
|    3|        1|     2|
+-----+---------+------+


--------------------------------------------------------
Applying Salting In Aggregations
-->
count the no of values.



# Salting In Aggregations
df_skew.groupBy("value").count().show()
+-----+------+
|value| count|
+-----+------+
|    0|999990|
|    2|    10|
|    3|     5|
|    1|    15|
+-----+------+


Partitions  values  no_of records
0           0       1 Milion
1           1       15
2           2       30 
3           3       35 

--------------------------------------------------------
Code Examples For Salting In Aggregations


This performs:
1️⃣ Distributed partial aggregation
2️⃣ Final merge aggregation

(
    df_skew
    .withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))
    .groupBy("value", "salt")   # hash(value,salt)%shuffle_partition
    .agg(F.count("value").alias("count"))
    .groupBy("value")
    .agg(F.sum("count").alias("count"))
    .show()
)
spark.stop()
 
===============================================================================================
When Should You Use Salting?
-->
Use salting when:
• Join key is highly skewed
• Aggregation key is skewed
• One partition processes majority of data
• Spark UI shows long-running tasks

When NOT to Use Salting
-->
Avoid salting if:
• Data is already balanced
• Dataset explosion becomes too large
• Broadcast join is possible
Better alternatives:
• Broadcast join
• Adaptive Query Execution (AQE) skew join handling
• Repartitioning
• Data pre-aggregation


Spark 3 Feature (Important)
-->
Spark 3 introduced Automatic Skew Join Handling.
Enable:
    spark.sql.adaptive.enabled=true
    spark.sql.adaptive.skewJoin.enabled=true
Spark automatically splits skewed partitions.
But manual salting still gives better control in extreme skew cases.


=====================================================================================================================

1️⃣ What is Data Skew in Spark?
-->
Data skew occurs when the data is unevenly distributed across partitions in a Spark cluster.

Spark distributes data using a hash function:
    partition = hash(key) % number_of_partitions

If one key has a very large number of records compared to other keys, all those records go to the same partition.
For example, if key A has 1 million records and other keys have only a few records, the partition containing key A will take significantly longer to process.
As a result, most tasks finish quickly while one task runs for a long time, which slows down the entire Spark job.
This situation is called data skew.


----------------------------------------------------------------------------------------------

2️⃣ How do you identify Data Skew in Spark?
-->
There are several ways to identify data skew.
First, I check the Spark UI. If I see one task taking significantly longer than others or processing much more data, it usually indicates skew.
Second, I analyze the distribution of keys using a groupBy operation.
For example:
    df.groupBy("key").count().orderBy(desc("count"))

If some keys have extremely large counts compared to others, it indicates skew.
Third, I sometimes check partition distribution using spark_partition_id() to see how records are distributed across partitions.


----------------------------------------------------------------------------------------------
3️⃣ What is Salting in Spark?
-->
Salting is a technique used to reduce data skew by adding randomness to the join or aggregation key.
Instead of joining on a single key, we add a salt column, which is a random number.
So instead of joining on:
    key
we join on:
    (key, salt)
The salt distributes records with the same key across multiple partitions.
This helps Spark process the data more evenly and improves overall performance.
----------------------------------------------------------------------------------------------
4️⃣ How do you apply Salting in Spark Join?
-->
To apply salting in a join, I follow three steps.
First, I add a random salt column to the skewed dataset.
Example:
    df_skew = df_skew.withColumn("salt", (rand() * N).cast("int"))
Second, I create a salt array in the smaller dataset and explode it so that each row is duplicated with different salt values.
Example:
    df_small = df_small
    .withColumn("salt_values", array(lit(0), lit(1), lit(2)))
    .withColumn("salt", explode("salt_values"))
Third, I perform the join using both key and salt.
    df_skew.join(df_small, ["key", "salt"])
This distributes skewed keys across multiple partitions and improves join performance.

----------------------------------------------------------------------------------------------
5️⃣ Why should explode be applied only on the smaller dataset?
-->
Explode multiplies the number of rows in a dataset.
For example, if the salt number is 5 and a dataset has 1 million rows, exploding it will create 5 million rows.
If we explode a very large dataset, it can significantly increase memory usage and shuffle size.
Therefore, the best practice is to apply explode only on the smaller dataset, so the increase in data size remains manageable.
----------------------------------------------------------------------------------------------
6️⃣ How does Salting work for Aggregations?
-->
Salting can also be used to handle skew during groupBy aggregations.
Normally, when we perform:
    groupBy(key)
all records with the same key go to a single partition.
To avoid this, we perform two-stage aggregation.
First, we add a salt column and perform a partial aggregation:
    groupBy(key, salt)
Then we perform a final aggregation:
    groupBy(key).sum()
This distributes the aggregation workload across multiple partitions and reduces skew.
----------------------------------------------------------------------------------------------
7️⃣ How do you choose the Salt Number?
-->
The salt number represents how many buckets we want to distribute the skewed data into.
In most cases, the salt number is chosen based on the number of shuffle partitions.
For example, if:
    spark.sql.shuffle.partitions = 200
we might choose a salt number between 10 and 50, depending on the severity of skew.
If the skew is extreme, we increase the salt number. However, if the salt number is too large, it can increase shuffle and data duplication.
So it should be chosen carefully based on the dataset size.
----------------------------------------------------------------------------------------------
8️⃣ What are alternative ways to handle Data Skew in Spark?
-->
There are several alternatives to salting.
First is broadcast join, where the smaller dataset is broadcast to all executors. This avoids shuffle completely.
Second is Adaptive Query Execution, which automatically detects skewed partitions and splits them.
Third is repartitioning the dataset based on a key to distribute data more evenly.
Fourth is pre-aggregating data before performing joins, which reduces the data size and skew.
Usually, I try these approaches before implementing salting.
----------------------------------------------------------------------------------------------
9️⃣ What are the disadvantages of Salting?
-->
Salting has some disadvantages.
First, it duplicates data in the smaller dataset, which increases the size of the dataset.
Second, it introduces additional shuffle operations, which may increase computation overhead.
Third, the logic becomes more complex and harder to maintain compared to a simple join.
Therefore, salting should be used only when other solutions like broadcast joins or AQE cannot resolve the skew problem.
----------------------------------------------------------------------------------------------
🔟 What is the difference between Salting and Adaptive Query Execution?
-->
Salting is a manual technique used to handle skew by adding random salt values to the join key.
Adaptive Query Execution, or AQE, is a Spark optimization feature introduced in Spark 3 that automatically detects skewed partitions and splits them during execution.
Salting provides more control but requires manual implementation.
AQE is simpler to use because Spark handles skew automatically.
In practice, we usually enable AQE first and apply salting only when skew is still not resolved.

----------------------------------------------------------------------------------------------
🔟 Difference Between Salting and Spark AQE Skew Join
-->

| Feature        | Salting               | AQE Skew Join          |
| -------------- | --------------------- | ---------------------- |
| Implementation | Manual                | Automatic              |
| Control        | High                  | Low                    |
| Complexity     | Higher                | Simple                 |
| Performance    | Best for extreme skew | Good for moderate skew |
| Spark Version  | Works in all versions | Spark 3+               |
