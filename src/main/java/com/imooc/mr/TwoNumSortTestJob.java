package com.imooc.mr;


import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author Chris Pan
 **/

public class TwoNumSortTestJob {

    public static class sortMapper extends Mapper<LongWritable, Text,TwoIntWritableA, NullWritable>{
        /**
         * Map阶段
         * @param k1
         * @param v1
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1, Text v1, Context context)
                throws IOException, InterruptedException {
            String[] nums = v1.toString().split("\t");
            TwoIntWritableA tiw = new TwoIntWritableA();

            tiw.setNum1(Integer.valueOf(nums[0]));
            tiw.setNum2(Integer.valueOf(nums[1]));

            context.write(tiw, NullWritable.get());
        }
    }

    public static class sortReduce extends Reducer<TwoIntWritableA,NullWritable,Text,NullWritable>{
        /**
         * Reduce阶段
         * @param k2
         * @param v2s
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(TwoIntWritableA k2, Iterable<NullWritable> v2s, Context context)
                throws IOException, InterruptedException {
            context.write(new Text(k2.getNum1()+"->"+k2.getNum2()), NullWritable.get());
        }
    }

    public static void main(String[] args) {

        if(args.length != 2){
            System.exit(100);
        }

        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(TwoNumSortTestJob.class);

            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            job.setMapperClass(sortMapper.class);
            job.setMapOutputKeyClass(TwoIntWritableA.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setReducerClass(sortReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.waitForCompletion(true);


        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

class TwoIntWritableA implements WritableComparable<TwoIntWritableA>{

    private int num1;
    private int num2;

    public int getNum1() {
        return num1;
    }

    public void setNum1(int num1) {
        this.num1 = num1;
    }

    public int getNum2() {
        return num2;
    }

    public void setNum2(int num2) {
        this.num2 = num2;
    }

    @Override
    public int compareTo(TwoIntWritableA tiw) {
        return tiw.num1 == this.num1 ? tiw.num2 - this.num2 : this.num1 - tiw.num1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(num1);
        out.writeInt(num2);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.num1 = in.readInt();
        this.num2 = in.readInt();
    }
}
