package com.lagou.mr.join.map_join;

import com.lagou.mr.join.reduce_join.DeliverBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    String name;
    DeliverBean bean = new DeliverBean();
    Text k = new Text();
    Map<String,String> map = new HashMap<>();

    //读取文件
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1 获取缓存的文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("position.txt"), "UTF-8"));
        String line;
        while (StringUtils.isNoneEmpty(line = reader.readLine())){
            // 2 切割
            String[] split = line.split("\t");
            // 3 缓存数据到集合
            map.put(split[0],split[1]);
        }
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String s = value.toString();
        // 2 截取
        String[] split = s.split("\t");
        // 3 获取职位id
        String pid = split[1];
        // 4 获取职位名称
        String pname = map.get(pid);
        // 5 拼接
        k.set(s + "\t" +pname);
        // 写出
        context.write(k,NullWritable.get());
    }
}
