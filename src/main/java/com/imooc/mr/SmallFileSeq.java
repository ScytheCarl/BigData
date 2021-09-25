package com.imooc.mr;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;

/**
 * Small files solution By SequenceFile
 * Created By Chris Pan
 */

public class SmallFileSeq {

    public static void main(String[]args) throws Exception{
        write("/Users/panhaoming/Desktop/SmallFiles", "/SequenceFile");
        read("/SequenceFile");
    }
    /**
     * Generate SequenceFile
     * @param inputDir -local address
     * @param outputFile -hdfs address
     * @throws Exception
     */
    private static void write(String inputDir, String outputFile) throws Exception{
        //set Configuration & hdfs address
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");

        //delete output fils on HDFS
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputFile), true);

        //create opts array with 3 elements
        /**
         * output address[file]
         * type of key
         * type of value
         */
        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                SequenceFile.Writer.file(new Path("outputFile")),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class)
        };

        SequenceFile.Writer writer = SequenceFile.createWriter(conf, opts);
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

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(inputFile)));
        Text key = new Text();
        Text value = new Text();

        while (reader.next(key, value)) {
            System.out.println("output filename: " + key.toString() + ",");
            System.out.println("file content: " + value + "");
        }
    }
}
