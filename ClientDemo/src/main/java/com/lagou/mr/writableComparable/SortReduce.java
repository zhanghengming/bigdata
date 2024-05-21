package com.lagou.mr.writableComparable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<SpeakBeanSort, NullWritable,NullWritable,SpeakBeanSort> {

    @Override
    protected void reduce(SpeakBeanSort key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(value,key);
        }
    }
}
