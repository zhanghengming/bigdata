package com.lagou.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text,IntWritable> {

    IntWritable total = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 遍历key对应的values，然后累加结果
        int sum = 0;
        for (IntWritable value : values){
            int i = value.get();
            sum += 1;
        }
        //直接输出当前key对应的sum值，结果就是单词出现的总次数
        total.set(sum);
        context.write(key,total);
    }
}
