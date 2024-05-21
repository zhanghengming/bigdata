package com.lagou.mr.speak;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SpeakerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

       // args = new String[]{"D:/新大数据/3-1/大数据正式班第一阶段模块一/资料/data/mr-writable案例/", "D:/新大数据/3-1/大数据正式班第一阶段模块一/资料/data/mr-writable案例/test"};

        // 1 获取配置信息，或者job实例对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"speakDriver");

        // 2 指定jar包的本地路径
        job.setJarByClass(SpeakBean.class);
        // 3 指定本业务job要使用的mapper/Reducer业务类
         job.setMapperClass(SpeakMapper.class);
         job.setReducerClass(SpeakReducer.class);
         //4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);
        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);
        // 6 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath (job, new Path(args[1]));
        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0: 1);
    }
}
