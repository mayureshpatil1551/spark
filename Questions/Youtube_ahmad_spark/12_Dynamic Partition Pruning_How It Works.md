12_Dynamic Partition Pruning: How It Works (And When It Doesn’t)
===============================================================================================================

00:00 Introduction
00:23 What is static pruning? 
02:47 Dynamic partition pruning
12:07 Caveats when using dynamic partition pruning
14:29 Code to understand dynamic partition pruning


-------------------------------------------------------------------------------------------------------
* Introduction
-->
Dynamic Partition Pruning (DPP) is a performance optimization in Spark.
It reduces the number of partitions scanned during a query, especially when filter values are not known until runtime.
Helps avoid full scans on large partitioned datasets.

-------------------------------------------------------------------------------------------------------
* What is static pruning? 
-->
df_listening_activity.filter(F.col("listening_date")='2023-06-04')

Here, listening_activity is file. we partition by listening_date col.
in filter condition, we are filtering based on partition col ie listening_activity.
so, it will scan only 2023-06-04 partiton and other will not scan.
hence, it reduce scanning of entire dataframe.
this is satic prunning.

-->
Occurs when filter values are known at compile time.
Example:
    df_listening_activity.filter(F.col("listen_date") == '2023-06-04')
Here, listen_date is a partition column. Spark scans only the partition for '2023-06-04' instead of the entire dataset.
✅ Reduces I/O and improves query performance.
-------------------------------------------------------------------------------------------------------
* Dynamic partition pruning
-->
problem statement
analyzse the listening activity of users :
1. release songs After 2019-04-03
2. On the release date of a song ( listening_date == release_date)

we hv 2 tbls:
    1.listening_activity
    2.songs
    join confition --> song_id

So, when we are going to join, 
listening_activity.join(song,on="song_id" & "listening_date"= "release_date",how='inner')
# full scan will happen
then it will filter  listening_date == release_date 
then give the output
but here problem is ,❌ Problem: This triggers a full scan on the listening_activity dataset.
in join by listening_date and release_date.

Sol-->
listening_activity.join(song,on="song_id",how='inner')
question - U want to analyzse the listening users on release date.
means listening_date == release_date

if some how, I pass the release_date to listening_activity. 
tell spark, give me activity only on release_date.
spark will only read those partition which u given
so, bacially we did, selected scan. we only read partition that we needed and process.

then we use the df and apply join 
ie. Dynamic Partition prunning (DPP)

spark enables Dynamic partition pruning by Default.


-------------------------------------------------------------------------------------------------------
* Caveats when using dynamic partition pruning
-->

spark enables Dynamic partition pruning by Default. but ay be work all aways.
IMP - one dataframe should be partitioned for DPP to work.

here, listening_date col is partition col. we are filtering records on listening_date.
so, here DPP can use.
but if we filter the records based on song_id or other than partition col, 
then it will scan full df and DPP not going use.


-->
- Only works on partitioned tables.
- The filter must involve a partition column.
- If the filter is on a non-partition column, Spark cannot prune partitions dynamically.
- DPP is useful in joins and subqueries where the filter value is not known until runtime.

-------------------------------------------------------------------------------------------------------
* Code to understand dynamic partition pruning
-->
# Pruning partitions at runtime
# Problem Statement: Analyse the listening activity of users on the release date of a song on/after 2020-01-01

from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .config("spark.driver.memory", "10g")
    .master("local[*]")
    .appName("6_1_dynamic_partition_pruning")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")
#--------------------------------------------

df_listening_actv = spark.read.csv("../data/partitioning/raw/Spotify_Listening_Activity.csv", header=True, inferSchema=True)
df_listening_actv = (
    df_listening_actv
    .withColumnRenamed("listen_date", "listen_time")
    .withColumn("listen_date", F.to_date("listen_time", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
)

# Partitioning listening activity by the listen date
(
    df_listening_actv
    .write
    .partitionBy("listen_date")
    .mode("overwrite")
    .parquet("../data/partitioning/partitioned/listening_activity_pt")
)

>> df_listening_actv_pt = spark.read.parquet("../data/partitioning/partitioned/listening_activity_pt")
>> df_listening_actv_pt.show(5, False)
+-----------+-------+--------------------------+---------------+-----------+
|activity_id|song_id|listen_time               |listen_duration|listen_date|
+-----------+-------+--------------------------+---------------+-----------+
|4456       |16     |2023-07-18 10:15:47.023264|151            |2023-07-18 |
|4457       |65     |2023-07-18 10:15:47.023264|181            |2023-07-18 |
|4458       |60     |2023-07-18 10:15:47.023264|280            |2023-07-18 |
|4459       |3      |2023-07-18 10:15:47.023264|249            |2023-07-18 |
|4460       |45     |2023-07-18 10:15:47.023264|130            |2023-07-18 |
+-----------+-------+--------------------------+---------------+-----------+

>> df_songs = spark.read.csv("../data/partitioning/raw/Spotify_Songs.csv", header=True, inferSchema=True)
>> df_songs.printSchema()
root
 |-- song_id: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: integer (nullable = true)
 |-- release_date: string (nullable = true)

 

df_songs = (
    df_songs
    .withColumnRenamed("release_date", "release_datetime")
    .withColumn("release_date", F.to_date("release_datetime", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
)
df_songs.show(5, False)
df_songs.printSchema()
+-------+------+---------+--------------------------+------------+
|song_id|title |artist_id|release_datetime          |release_date|
+-------+------+---------+--------------------------+------------+
|1      |Song_1|2        |2021-10-15 10:15:47.006571|2021-10-15  |
|2      |Song_2|45       |2020-12-07 10:15:47.006588|2020-12-07  |
|3      |Song_3|25       |2022-07-11 10:15:47.006591|2022-07-11  |
|4      |Song_4|25       |2019-03-09 10:15:47.006593|2019-03-09  |
|5      |Song_5|26       |2019-09-07 10:15:47.006596|2019-09-07  |
+-------+------+---------+--------------------------+------------+
only showing top 5 rows
root
 |-- song_id: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: integer (nullable = true)
 |-- release_datetime: string (nullable = true)
 |-- release_date: date (nullable = true)


# Pick songs released in 2020
df_selected_songs = df_songs.filter(F.col("release_date") > F.lit("2019-12-31"))
df_listening_actv_of_selected_songs = df_listening_actv_pt.join(
    df_selected_songs, 
    on=(df_songs.release_date == df_listening_actv_pt.listen_date) & (df_songs.song_id == df_listening_actv_pt.song_id), 
    how="inner"
)


df_listening_actv_of_selected_songs.explain(True)
# -->
== Parsed Logical Plan ==
== Analyzed Logical Plan ==
== Optimized Logical Plan ==

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- BroadcastHashJoin [listen_date#48, song_id#45], [release_date#104, song_id#91], Inner, BuildRight, false
   :- Filter isnotnull(song_id#45)
   :  +- FileScan parquet [activity_id#44,song_id#45,listen_time#46,listen_duration#47,listen_date#48] Batched: true, DataFilters: [isnotnull(song_id#45)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/afaqueahmad/Documents/YouTube/spark-experiments/data/parti..., PartitionFilters: [(listen_date#48 > 2019-12-31), isnotnull(listen_date#48), dynamicpruningexpression(listen_date#4..., PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<activity_id:int,song_id:int,listen_time:string,listen_duration:int>
   :        +- SubqueryAdaptiveBroadcast dynamicpruning#155, 0, true, Project [song_id#91, title#92, artist_id#93, release_date#94 AS release_datetime#99, cast(gettimestamp(release_date#94, yyyy-MM-dd HH:mm:ss.SSSSSS, TimestampType, Some(Asia/Kolkata), false) as date) AS release_date#104], [release_date#104, song_id#91]
   :           +- AdaptiveSparkPlan isFinalPlan=false
   :              +- Project [song_id#91, title#92, artist_id#93, release_date#94 AS release_datetime#99, cast(gettimestamp(release_date#94, yyyy-MM-dd HH:mm:ss.SSSSSS, TimestampType, Some(Asia/Kolkata), false) as date) AS release_date#104]
   :                 +- Filter ((cast(gettimestamp(release_date#94, yyyy-MM-dd HH:mm:ss.SSSSSS, TimestampType, Some(Asia/Kolkata), false) as date) > 2019-12-31) AND isnotnull(song_id#91))
   :                    +- FileScan csv [song_id#91,title#92,artist_id#93,release_date#94] Batched: false, DataFilters: [(cast(gettimestamp(release_date#94, yyyy-MM-dd HH:mm:ss.SSSSSS, TimestampType, Some(Asia/Kolkata..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/afaqueahmad/Documents/YouTube/spark-experiments/data/parti..., PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<song_id:int,title:string,artist_id:int,release_date:string>
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[4, date, true], input[0, int, true]),false), [id=#112]
      +- Project [song_id#91, title#92, artist_id#93, release_date#94 AS release_datetime#99, cast(gettimestamp(release_date#94, yyyy-MM-dd HH:mm:ss.SSSSSS, TimestampType, Some(Asia/Kolkata), false) as date) AS release_date#104]
         +- Filter ((cast(gettimestamp(release_date#94, yyyy-MM-dd HH:mm:ss.SSSSSS, TimestampType, Some(Asia/Kolkata), false) as date) > 2019-12-31) AND isnotnull(song_id#91))
            +- FileScan csv [song_id#91,title#92,artist_id#93,release_date#94] Batched: false, DataFilters: [(cast(gettimestamp(release_date#94, yyyy-MM-dd HH:mm:ss.SSSSSS, TimestampType, Some(Asia/Kolkata..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/afaqueahmad/Documents/YouTube/spark-experiments/data/parti..., PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<song_id:int,title:string,artist_id:int,release_date:string>

# FileScan parquet --> dynamicpruningexpression [DPP used]
# Check the physical plan: dynamicpruningexpression indicates DPP was applied.

df_listening_actv_of_selected_songs.show()
+-----------+-------+--------------------+---------------+-----------+-------+-------+---------+--------------------+------------+
|activity_id|song_id|         listen_time|listen_duration|listen_date|song_id|  title|artist_id|    release_datetime|release_date|
+-----------+-------+--------------------+---------------+-----------+-------+-------+---------+--------------------+------------+
|       9760|     89|2023-07-24 10:15:...|             81| 2023-07-24|     89|Song_89|       33|2023-07-24 10:15:...|  2023-07-24|
|       9768|     89|2023-07-24 10:15:...|            295| 2023-07-24|     89|Song_89|       33|2023-07-24 10:15:...|  2023-07-24|
|       9799|     89|2023-07-24 10:15:...|            272| 2023-07-24|     89|Song_89|       33|2023-07-24 10:15:...|  2023-07-24|
|       7322|     64|2023-10-25 10:15:...|             95| 2023-10-25|     64|Song_64|       32|2023-10-25 10:15:...|  2023-10-25|
+-----------+-------+--------------------+---------------+-----------+-------+-------+---------+--------------------+------------+



------------------------------------------------------------------------------------------------
5. Key Points for Interviews - 
    - DPP improves performance by reducing partitions scanned.
    - Works only with partitioned tables and filters on partition columns.
    - Enabled by default in Spark 3.x.
    - Often used in joins where one side is partitioned and filter values are known only at runtime.
    - Explain plan is the best way to verify if DPP is applied.
    - Fallback: If filter is not on a partition column, Spark does full scan.

6. Extra Tips - 
    - Combine DPP with broadcast joins for small tables to further improve performance.
    - Use spark.sql.dynamicPartitionPruning.enabled = true to ensure DPP is active.
    - Adaptive Query Execution (AQE) in Spark 3.x automatically optimizes DPP joins.
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

1. What is Dynamic Partition Pruning in Spark?
-->
Dynamic Partition Pruning (DPP) is a performance optimization in Spark that reduces the number of partitions read during a query when filter values are only known at runtime. It is commonly used in joins where one table is partitioned, and the filter condition comes from another table. Spark scans only relevant partitions instead of reading the full dataset.


-------------------------------------------------------------------------------------------------------
2. How is DPP different from static partition pruning?
-->
Static pruning happens when the filter value is known at compile time; Spark reads only relevant partitions.
Dynamic pruning happens when the filter value is determined at runtime, such as in a join or subquery.
DPP improves query performance for large partitioned tables with runtime filters.


-------------------------------------------------------------------------------------------------------
3. When does Dynamic Partition Pruning work?
-->
The table must be partitioned.
The filter must involve a partition column.
Typically used in joins or subqueries where the filter value is not known until query execution.
Spark 3.x enables DPP by default.



-------------------------------------------------------------------------------------------------------
4. Can DPP work if the filter is on a non-partition column?
-->
No. DPP only works for partition columns. If the filter is on a non-partition column, Spark performs a full scan of the dataset.

-------------------------------------------------------------------------------------------------------
5. How do you enable or disable Dynamic Partition Pruning in Spark?
-->
By default, DPP is enabled in Spark 3.x. You can explicitly control it using:
>> spark.conf.set("spark.sql.dynamicPartitionPruning.enabled", "true")  # Enable
>> spark.conf.set("spark.sql.dynamicPartitionPruning.enabled", "false") # Disable

-------------------------------------------------------------------------------------------------------
6. How does Dynamic Partition Pruning improve query performance?
-->
DPP reduces the I/O overhead by reading only the relevant partitions.
For example, if a dataset is partitioned by date and we need data for a runtime-calculated date, DPP ensures Spark scans only that date’s partition instead of the entire table, reducing execution time significantly.

-------------------------------------------------------------------------------------------------------
7. Give an example scenario where DPP is useful.
-->
Example:
Table listening_activity partitioned by listen_date.
Table songs with release_date.
Query: Find listening activity on the release date of songs released after 2020.
Here, the release_date from songs is only known at runtime, and DPP ensures Spark scans only partitions for those dates instead of the full listening_activity table.

-------------------------------------------------------------------------------------------------------
8. How can you check if DPP is applied in Spark?
-->
Use the physical plan to verify:
>> df.join(...).explain(True)
Look for dynamicpruningexpression in the optimized or physical plan. This confirms DPP is being used.



-------------------------------------------------------------------------------------------------------
9. What are the limitations of DPP?
-->
Only works on partitioned tables.
The filter must involve partition columns.
May not be effective if partition column cardinality is too high or AQE is disabled.
Can increase query planning time slightly for very large datasets due to runtime computation of partition filters.



-------------------------------------------------------------------------------------------------------
10. How does DPP interact with Adaptive Query Execution (AQE)?
-->
AQE in Spark 3.x automatically optimizes joins, shuffle partitions, and applies dynamic partition pruning.
With AQE enabled (spark.sql.adaptive.enabled = true), Spark dynamically decides the best execution plan at runtime, enhancing DPP efficiency.
Recommended to keep AQE enabled for partitioned table joins with runtime filters.

==================================================================================================================================

1. You have a large partitioned table with millions of partitions. How would DPP perform? Any concerns?
-->
DPP is effective in reducing I/O, but with millions of partitions, runtime pruning may cause query planning overhead because Spark needs to compute the list of partitions dynamically.
Solutions:
Use coarser partitioning if possible.
Enable Adaptive Query Execution (AQE) to optimize partition scans.


2. Can DPP be applied in a broadcast join?
Yes, but the small table side is usually broadcasted, and the large partitioned table benefits from DPP.

3. What happens if the filter column is derived via transformation instead of directly using the partition column?
-->
If the filter is not on the raw partition column, DPP may not work.
Example: filter(F.date_format("listen_time","yyyy-MM-dd") == release_date) – here Spark might scan all partitions because the filter is computed at runtime.
✅ Solution: Always filter on the exact partition column (listen_date) to enable DPP.


4. How does DPP differ from partition pruning in Hive or Presto?
Hive/Presto mostly use static pruning at query compile time.
Spark DPP allows runtime evaluation of filters (dynamic), which is crucial for joins/subqueries.
DPP is more adaptive and can be combined with AQE for runtime optimization.


7. How does DPP affect shuffle operations?
DPP reduces shuffle on the partitioned dataset because fewer partitions are read.
If the join requires shuffling the smaller table (non-partitioned), Spark may still shuffle it, but the data volume is reduced, improving performance.


8. Scenario: Large table events partitioned by event_date. Small table users. You want events for active users in last month. How do you apply DPP?
Filter users to get active users: active_users = users.filter(last_month_activity).
Use join with events on user_id and event_date filter from active_users.
Spark will prune only the partitions of events corresponding to event_date of active users.
Benefits: Only relevant partitions are scanned, avoiding full scan of events.

10. How does AQE enhance DPP in Spark 3.x?
AQE allows runtime optimization of join plans and shuffles.
With AQE, DPP filters can be evaluated during query execution, reducing unnecessary scans and shuffles.
This is particularly useful for skewed or large datasets.




 