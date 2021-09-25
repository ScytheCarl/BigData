package com.imooc.mr;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;

/**
 * Small files solution2: MapFile
 */
public class SmallFileMap {

    public static void main(String[]args) throws Exception{
        write("/Users/panhaoming/Desktop/SmallFiles", "/MapFile");
        read("/MapFile");
    }

    private static void write(String inputDir, String outputFile) throws Exception{
        //set Configuration & hdfs address
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");

        //delete output fils on HDFS
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputFile), true);

        //create opts array with 2 elements
        /**
         * type of key
         * type of value
         */
        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(Text.class)
        };

        MapFile.Writer writer = new MapFile.Writer(conf, new Path(outputFile), opts);

        File inputDirPath = new File(inputDir);

        if (inputDirPath.isDirectory()) {
            File[] files = inputDirPath.listFiles();
            for (File file: files) {
                String content = FileUtils.readFileToString(file, "UTF-8");
                String fileName = file.getName();
                Text key = new Text(fileName);
                Text value = new Text(content);
                writer.append(key, value);
            }
        }

        writer.close();
    }

    /**
     * Read SequenceFile
     * @param inputFile
     * @throws Exception
     */
    private static void read(String inputFile) throws Exception{
        //set Configuration & hdfs address
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");

        MapFile.Reader reader = new MapFile.Reader(new Path(inputFile), conf);
        Text key = new Text();
        Text value = new Text();

        while (reader.next(key, value)) {
            System.out.println("output filename: " + key.toString() + ",");
            System.out.println("file content: " + value + "");
        }
        reader.close();
    }
}
