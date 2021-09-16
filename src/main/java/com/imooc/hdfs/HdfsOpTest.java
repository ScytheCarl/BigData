package com.imooc.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsOpTest {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //set Hdfs address
        conf.set("fs.defaultFS", "hdfs://192.168.34.100:9000");
        FileSystem fileSystem = FileSystem.get(conf);

        FSDataOutputStream fos = fileSystem.create(new Path("/user.txt"));
        fos.write("Hello, world".getBytes());
        fos.close();
    }
}
