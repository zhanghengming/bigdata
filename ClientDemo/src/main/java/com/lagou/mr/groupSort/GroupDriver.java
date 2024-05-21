package com.lagou.mr.groupSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GroupDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(GroupDriver.class);
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);

        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(OrderBean.class);

        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\新大数据\\3-1\\大数据正式班第一阶段模块一\\资料\\data\\GroupingComparator\\groupingComparator.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\新大数据\\3-1\\大数据正式班第一阶段模块一\\资料\\data\\GroupingComparator\\output"));
        // 指定分区器
        job.setPartitionerClass(CustomPartitioner.class);
        job.setGroupingComparatorClass(CustomGroupingComparator.class);
        job.setNumReduceTasks(2);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
