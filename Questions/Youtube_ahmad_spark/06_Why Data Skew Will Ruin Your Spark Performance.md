06_Why Data Skew Will Ruin Your Spark Performance
-----------------------------------------------
* Introduction
* How to identify a Data Skew? 
* When does Data Skew happen?
* Operations that cause Data Skew
* Why is Data Skew bad? Why does it matter?
* Code example to simulate a skewed dataset



* Introduction
Data skew is refers to ur data being unevenly partitioned.
means some of the partition are going to lot of data then others.


* How to identify a Data Skew? 
-->
eg. spark job is stuck at 199/200 tasks infinitely long to complete.
In spark UI -
1. u can identify in *even time line*.
one task takes bery long to complete.
2. one more, at Summary Metrics for ur task.
u can see, min time and max time. there u will the huge gap.



* When does Data Skew happen?
-->
ur data divided into 5 partition.
p1 to p5
p2 is smallest partition & p3 is large partition.

there is un-even utilization of resources & u r paying for resurce tht u r not using.they are idle.
that os skwed partition.


uniform partition(idle scenario)
-- it is ur data evenly distributed by partition and take same time of time to complete.



* Operations that cause Data Skew
-->
1. Aggregation operation( GroupBy)-
eg. groupBy(country)
IND having more records as compair to USA & China.
so.IND partition is big.


2. Join Operation - 

order line join with Product on prod_id.
we get oder line of prod details.
we try to find of out no. of row.
we see P2 most no. to others.
P2 take more time to compair other


* Why is Data Skew bad? Why does it matter?
-->
1. Job is taking time -
    cost u devloper time.
    time the resource are idle
2. Uneven Utilizationof Resources - Compute & Memory
3. OOO Error / Data spills- 
    Data spills are very costlu operation beacouse  spark will write dataset into disk then it would read form that disk. this back and fore is very costly.
that's why dataskew is bad.


* Code example to simulate a skewed dataset
-->
Skewed Vs Uniform Dataset

to check spark partiton run below code--

import pyspark.sql import functions as F
(
    df_uniform
    .withColumn("partition", F.spark_partition_id())
    .groupBy("partition")
    .count()
    .orderBy("partition")
    .show()
)

 