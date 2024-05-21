package com.lagou.mr.join.reduce_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable, Text,Text,DeliverBean> {

    String name;
    DeliverBean bean = new  DeliverBean();
    Text k = new Text();

    // 预处理工作
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1 获取输入文件切片
        FileSplit split = (FileSplit)context.getInputSplit();
        // 2 获取输入文件名称
        name = split.getPath().getName();
    }
    // 读取两个文件，将不同文件中的相同的k输出
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (name.startsWith("deliver_info")){
            String[] split = line.split("\t");
            bean.setUserId(split[0]);
            bean.setPositionId(split[1]);
            bean.setDate(split[2]);
            bean.setPositionName("");
            bean.setFlag("deliver");
            k.set(split[1]);
        }else {
            // 2.3 切割
            String[] fields = line.split("\t");
            // 2.4 封装bean对象
            bean.setPositionId(fields[0]);
            bean.setPositionName(fields[1]);
            bean.setUserId("");
            bean.setDate("");
            bean.setFlag("position");
            k.set(fields[0]);
        }
        context.write(k,bean);
    }
}
