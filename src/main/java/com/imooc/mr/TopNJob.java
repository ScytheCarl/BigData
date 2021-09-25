package com.imooc.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Author Chris Pan
 **/

public class TopNJob {

    public static class topnMapper extends Mapper<LongWritable, Text,LongWritable, NullWritable>{
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws
                IOException, InterruptedException {
            LongWritable new_key = new LongWritable(Long.parseLong(v1.toString()));
            context.write(new_key, NullWritable.get());
        }
    }

    public static class topnReducer extends Reducer<LongWritable,NullWritable,LongWritable,NullWritable>{
        List<Long> arr = new ArrayList<>();
        //set TopN
        final int N = 5;
        @Override
        protected void reduce(LongWritable k2, Iterable<NullWritable> v2s, Context context)
                throws IOException, InterruptedException {
            arr.add(k2.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(arr);
            Collections.reverse(arr);
            for(int i = 0;i<= N;i++){
                context.write(new LongWritable(arr.get(i)), NullWritable.get());
            }
        }
    }


    public static void main(String[] args) {

        if(args.length != 2){
            System.exit(100);
        }

        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(TopNJob.class);

            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            job.setMapperClass(topnMapper.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(NullWritable.class);

            job.setReducerClass(topnReducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(NullWritable.class);

            job.waitForCompletion(true);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
