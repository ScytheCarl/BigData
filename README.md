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
###### addtional thinking:
Q: Can i use zip or rar files to solve the problem of small files in HDFS, why? If not, why?
A: No, the binary files of compressed files can only be used for storage.

Q: What is the difference between using zip or rar files and the small file solutions provided by Hadoop (SquenceFile and MapFile)?
A: The zip or rar files can only be used for storage. There is only one InputSplit for MapReduce tasks, which may cause MR loses its meaning; SquenceFile and MapFile can be divided as Blocks and divided into InpuSplit normally for MapReduce tasks.
