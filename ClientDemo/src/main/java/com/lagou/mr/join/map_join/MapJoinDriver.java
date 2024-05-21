package com.lagou.mr.join.map_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries, "MapJoinDriver");

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path("D:\\新大数据\\3-1\\大数据正式班第一阶段模块一\\资料\\MRjoin\\reduce_join\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\新大数据\\3-1\\大数据正式班第一阶段模块一\\资料\\MRjoin\\reduce_join\\input\\o1"));

        // 7.加载缓存文件
        job.addCacheFile(new URI("file:///D:/新大数据/3-1/大数据正式班第一阶段模块一/资料/MRjoin/reduce_join/input/position.txt"));
        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
