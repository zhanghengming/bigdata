package com.lagou.mr.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PartitionDriver.class);

        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PartitionBean.class);

        //job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job,new Path("D:\\新大数据\\3-1\\大数据正式班第一阶段模块一\\资料\\data\\mr-writable案例\\speak.data"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\新大数据\\3-1\\大数据正式班第一阶段模块一\\资料\\data\\mr-writable案例\\output2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0: 1);
    }
}
