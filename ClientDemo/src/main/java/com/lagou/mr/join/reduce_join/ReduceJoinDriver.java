package com.lagou.mr.join.reduce_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries,"ReduceJoinDriver");

        job.setJarByClass(ReduceJoinDriver.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(DeliverBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DeliverBean.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\新大数据\\3-1\\大数据正式班第一阶段模块一\\资料\\MRjoin\\reduce_join\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\新大数据\\3-1\\大数据正式班第一阶段模块一\\资料\\MRjoin\\reduce_join\\input\\o"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
