package com.demo.flink.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 流批一体的WordCount
public class StreamBatchWordCount {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 按批计算模式去执行
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // 显式声明为本地运行环境，且带webUI
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 按流计算模式去执行
        // env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // flink自己判断决定
        // env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 读文件 得到  dataStream
        DataStreamSource<String> source = env.readTextFile("flink/data/wc/input/wc.txt");
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\s+");
                for (String s1 : split) {
                    collector.collect(Tuple2.of(s1, 1));
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).sum(1).print();

        env.execute();

    }
}
