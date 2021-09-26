# Hadoop
## 1.Using Java to operate hadoop
- [Put/Get files into/from HDFS clusters by using Java](https://github.com/ScytheCarl/Hadoop/tree/master/src/main/java/com/imooc/hdfs)

## 2.[Rpc](https://github.com/ScytheCarl/Hadoop/tree/master/src/main/java/com/imooc/rpc) (Client/Server)
- Client side
- Server side
- Protocol 

## 3. MapReduce program
- [Wordcount job & SecondarySort & TopN](https://github.com/ScytheCarl/Hadoop/tree/master/src/main/java/com/imooc/mr)
- Hadoop Serialization (compare to Java Serialization): Efficient use of storage space; The cost of reading data is relatively small
- InputFormat Hierarchical analysis([Source code](https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/)):
- <img width="1194" alt="截屏2021-09-22 下午11 14 41" src="https://user-images.githubusercontent.com/42943349/134461863-3de39893-14b8-448e-898d-39a2bd61fdf8.png">
- Deal with many small files by using [SequenceFile](https://github.com/ScytheCarl/Hadoop/blob/master/src/main/java/com/imooc/mr/SmallFileSeq.java) and [MapFile](https://github.com/ScytheCarl/Hadoop/blob/master/src/main/java/com/imooc/mr/SmallFileMap.java)
### Addtional thinking:
- Q: Can i use zip or rar files to solve the problem of small files in HDFS, why? If not, why?

  A: No, the binary files of compressed files can only be used for storage.

- Q: What is the difference between using zip or rar files and the small file solutions provided by Hadoop (SquenceFile and MapFile)?

  A: The zip or rar files can only be used for storage. There is only one InputSplit for MapReduce tasks, which may cause MR loses its meaning; SquenceFile and    MapFile can be divided as Blocks and divided into InpuSplit normally for MapReduce tasks.
### Data Skew problem
- Senario: If we have a file which has ten millios data with key from 1 to 9. And we got 9 millios data which have same key, lets assume it is 5, and all others keys just have 1 million in total. If we run it in MR with 10 reduce tasks, one reduce task will be executed much more time than other reduce tasks. How can we improve it?

  Sometioms if our data skew is not serious, increase number of reduce tasks may help. But in this senario, result wont change much if we only increase number of    reduce tasks. Therefore, what we gonna do is to scatter the key 5 into differrent keys randomly like (5_0, 5_1, ......, 5_9), then we will find that all our reduce tasks will take similar time and total execution time will improve a lot. However, we need another simple MR program which similar to wordcount to combine all the keys which are 5_0, 5_1,....5_10 into 5 to get the final result as what we expected at the begining. 
