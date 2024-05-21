package com.lagou.mr.speak;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//第一对kv：map输入参数的kv类型，k -》一行文本偏移量，v --》一行文本内容
//第二对kv：map输出参数
public class SpeakMapper extends Mapper<LongWritable, Text, Text, SpeakBean> {

    // 每一个key调一次改方法
    SpeakBean v = new SpeakBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // 获取一行
        String line = value.toString();
        // 切割获取每个字段
        String[] fields = line.split("\t");
        // 封装对象，将字段封装到对象中
        // 取出设备id
        String deviceId = fields[1];

        // 取出自有和第三方时长数据
        long selfDuration = Long.parseLong(fields[fields.length - 3]);
        long thirdPartDuration = Long.parseLong(fields[fields.length - 2]);

        k.set(deviceId);
        v.set(selfDuration,thirdPartDuration);

        // 写出
        context.write(k,v);


    }
}
