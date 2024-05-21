package com.lagou.mr.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PartitionReducer extends Reducer<Text,PartitionBean, NullWritable,PartitionBean> {

    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Context context) throws IOException, InterruptedException {
        for (PartitionBean bean : values) {
            context.write(NullWritable.get(),bean);
        }
    }
}
