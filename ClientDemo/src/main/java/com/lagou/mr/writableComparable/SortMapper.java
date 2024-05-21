package com.lagou.mr.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SpeakBeanSort, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split("\t");
        SpeakBeanSort beanSort = new SpeakBeanSort(Long.parseLong(arr[1]), Long.parseLong(arr[2]), arr[0]);
        context.write(beanSort,NullWritable.get());
    }
}
