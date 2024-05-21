package com.lagou.mr.groupSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //每个相同的key调一次，所以取出第一个key即可
        context.write(key,NullWritable.get());
    }
}
