package com.lagou.mr.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionMapper extends Mapper<LongWritable, Text,Text,PartitionBean> {

    PartitionBean bean = new PartitionBean();
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String appkey = split[2];
        bean.setId(split[0]);
        bean.setDeviceId(split[1]);
        bean.setAppkey(appkey);
        bean.setIp(split[3]);
        bean.setSelfDuration(Long.parseLong( split[4]));
        bean.setThirdPartDuration(Long.parseLong(split[5]));
        bean.setStatus(split[6]);

        text.set(appkey);
        context.write(text,bean);
    }
}
