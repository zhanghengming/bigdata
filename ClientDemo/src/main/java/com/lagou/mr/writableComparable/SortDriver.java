package com.lagou.mr.writableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，job实例对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(SortDriver.class);

        // 3 指定本业务job要使用的mapper、reducer类
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);

        // 4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(SpeakBeanSort.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SpeakBeanSort.class);

        // 6 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0: 1);
    }
}
