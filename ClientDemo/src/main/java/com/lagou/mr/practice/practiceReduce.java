package com.lagou.mr.practice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class practiceReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    int sum = 0;
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            int i = value.get();
            sum += i;
            v.set(sum);
            context.write(v,key);
        }
    }
}
