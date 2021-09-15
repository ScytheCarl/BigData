package com.imooc.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Use JAVA code to operate HDFS
 *
 * File Operation: upload, download, delete
 *
 * Created by phm
 */
public class HdfsOp {
    public static void main(String[] args) throws Exception{
        //create a configuration object
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.34.100:9000");
        //get the HDFS object
        FileSystem fileSystem = FileSystem.get(conf);

        //upload files
        //put(fileSystem);

        //download files
        //get(fileSystem);

        delete(fileSystem);
    }


    /**
     * Delete files or directory
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void delete(FileSystem fileSystem) throws IOException {
        //Delete files or directory
        boolean flag = fileSystem.delete(new Path("/user.txt"), true);
        if (flag){
            System.out.println("success");
        }else{
            System.out.println("failed");
        }
    }


    /**
     * Download files
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void get(FileSystem fileSystem) throws IOException {
        //Get the input stream of HDFS file system
        FSDataInputStream fis = fileSystem.open(new Path("/README.txt"));
        //Get the output stream of a local file
        FileOutputStream fos = new FileOutputStream("/Users/panhaoming/Desktop/readme.txt");
        //download files
        IOUtils.copyBytes(fis, fos, 1024,true);
    }


    /**
     * Upload files
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void put(FileSystem fileSystem) throws IOException {
        //Get the input stream of a local file
        FileInputStream fis = new FileInputStream("/Users/panhaoming/Desktop/4414/readme.txt");
        //Get the output stream of HDFS file system
        FSDataOutputStream fos = fileSystem.create(new Path("/user.txt"));
        //upload
        IOUtils.copyBytes(fis, fos, 1024, true);
    }
}
