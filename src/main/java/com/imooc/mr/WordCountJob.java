package com.imooc.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Basic MR Job
 */

public class WordCountJob {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        Logger logger = LoggerFactory.getLogger(MyMapper.class);
        /**
         * Mapper function with <k1, v1> inputs and <k2, v2> outputs
         * @param k1
         * @param v1
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1, Text v1, Context context)
                throws IOException, InterruptedException {

            logger.info("<k1,v1>=<" + k1.get() + "," + v1.toString() + ">");

            String[] words = v1.toString().split(" ");
            for (String word : words){
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                context.write(k2, v2);
            }
        }
    }


    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        Logger logger = LoggerFactory.getLogger(MyReducer.class);
        /**
         *
         * @param k2
         * @param v2s
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context)
                throws IOException, InterruptedException {

            long sum = 0L;
            for (LongWritable v2: v2s){
                logger.info("<k2,v2s>=<" + k2.toString() + "," + v2.get() + ">");
                sum += v2.get();
            }

            context.write(k2, new LongWritable(sum));
        }
    }

    public static void main(String[] args) {
        try{
            if (args.length !=2){
                System.exit(100);
            }

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(WordCountJob.class);

            //Define input path
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            //Output path must not exist
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            //Map job setting
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            //Reduce job setting
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.waitForCompletion(true);

        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
