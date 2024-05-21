package com.lagou.mr.groupSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        OrderBean orderBean = new OrderBean(split[0], Double.parseDouble(split[2]));
        context.write(orderBean,NullWritable.get());
    }
}
