package com.lagou.mr.practice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class practiceMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    IntWritable k = new IntWritable();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        k.set(Integer.parseInt(value.toString()));
        context.write(k,v);
    }
}
