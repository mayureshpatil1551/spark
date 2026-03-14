Apache Spark Execution Tunning
-------------------------------------
* Introduction to Executor Tuning in Apache Spark
* Understanding Executors in a Spark Cluster
* Example: Sizing Executors in a Cluster
* Example: Sizing a Fat Executor
* Example: Sizing a Thin Executor
* Advantages and Disadvantages of Fat Executor
* Advantages and Disadvantages of Thin Executor
* Example 1: Sizing an Optimal Executor
* Example 2: Sizing an Optimal Executor
* Key Takeaway

Node- one machine in cluster. having core-17  RAM-20GB
Which hving many nodes in cluster.

* Understanding Executors in a Spark Cluster
-->
let suppose we hv 3 executor having core 5 and memory 6Gb
Executor 1 - core 5 & RAM 6GB 
Executor 2 - core 5 & RAM 6GB 
Executor 3 - core 5 & RAM 6GB 
Total is --> core 15 and RAM 18Gb in node.

* Example: Sizing Executors in a Cluster
Approaches to size an executor

Q. How did u deside the no. of executors when running the spark job. core & memory of executor.

3 option
thin executor
optimally size executor
fat executor

suppose we hv 
5 node
12 core 
48 Gb RAM

Leave out 1gb ,1 core for Hadoop/YARN/OS Daemons(per Node)

* Example: Sizing a Fat Executor
-->
Fat executor are which occupie large portion of resources.
fact executor scenario look ike below-
one executor having 
11 core | 47 GB RAM
so we hv 5 node means 5 executor.
each hving 11 core | 47 GB RAM
SO, this are so powerfull executors.

spark-submit looks like -
--num-executpr 5
--executor-cores 11
--executor-memory 47g



* Example: Sizing a Thin Executor
-->
It occupie minimal resource from node.

11 core
47GB RAM

1 executor = 1 core
1 node = 11 executor

memory per executor = 47/11 ~= 4Gb

1 executor --> 1 core | 4Gb RAM

5 nodes --> 55 executor (11x5)

--num-executors 55
--executor-core 1
--executor-memory 4g


* Advantages and Disadvantages of Fat Executor
-->
advantages-
increased parallelism[task level ] - they hv more core & memory. more tasks run in ller. so performance will improve.
Enhanced data locality - this reduces network traffic improving overall application speed.

disAdvantages-
under utilidation - can lead to inefficient use of resources id all cores or memory are not utilised.
fault tolerance - each executor is handling large amount of data. if executor fails, recovering from failures can be costly in terms of time and computation reducing overall appln reliability.
Hdfs throughput [means the rate at which write data to hdfs and read data from hdfs] - using high num of executors if we are using HDFS. problem will facing during GC. recommendation is to hv 3-5 cores per executors.

 
* Advantages and Disadvantages of Thin Executor
-->
Advantages - 
- Increased Parallelism[Executor level ] -
increase ller as there are more executors handling smaller tasks. Thisis beneficial when tasks are lightweight.
- Fault Tolerance -
one executor going down amounts to losing a small unit of work done, which is easier to recover.

DisAdvantages-
- High Network Traffic-
execuotor may increase the traffic due to each executor has a small memory.
- Reduced data locality-
memory small, then amount of partition is small.    



* Rules for sizing an Optimal Executor
-->
rules
1. leave out 1core and 1gb n 1Ram for hadoop/yarn/OS
2. Yarn appl master-
YARN AM is responsiable for
negoshiating the resource to RM.
so leaveout som memory for AM.
1 executor[not suitable for fat executor ]
or
1 core 1gb ram
3. 3-5 core per executor
4. when u define ur executor memory. so this executor memory should exclude the memory overhead.
basically, actual executor memory should alway exclude the overhead memory.



* Example 1: Sizing an Optimal Executor
-->
5 node
12 core
48GB RAM
===
1 core & 1gb ran for hadoop
===
11 core
47gb ram

cluster level
total memory = 47 x 5 = 235-1 = 234gb
total cores = 11 x 5 = 55-1 = 54 cores

executor
cores - 5
memory -  ?

total executor = 54/5 ~= 10 executors

memory per executor = 23 - m(384MB, 2.3gb)
                    = 23 - 2.3gb
                    ~ 20Gb

conclusion- 
--num-executor 10
--executor-cores 5
--executor-memory 20gb 


Q. why we are not talking about size of data?
-->
we hv to fous on memory per core.

5 core taking 20 gb
1 core = 4gb

1 core= 1partiton
as long as my partition is <=4gb. the processing happen simenteniously

defoult partiton size is 128 mg
if file is 10 gb.
1024*10/128 = 80 partition





* Example 2: Sizing an Optimal Executor
-->
configuration of cluster
3 node
16 core
48GB RAM

=====
per node
15 core
47 Gb 

total cores = 15x3 = 45
total memory = 47x3 = 141gb

1 core & 1gb ram for AM

total cores = 15x3 = 45 - 1 =44
total memory = 47x3 = 141 - 1 = 140gb

excutors 
core - ?
memory - ?

if we give 4 core.
then
executor = 44/4 = 11 executors
each of them having 4 core

memory -
140/11 ~= 12gb


overhead = m(384mb, 10% of 12gb)= 1gb
memory - 1 = 12-1= 11gb(memory)

final calculation
--num-executor 11
--executor-core 4
--executor-memory 11gb
 