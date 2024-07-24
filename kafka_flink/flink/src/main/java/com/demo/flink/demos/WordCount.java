package com.demo.flink.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 通过socket数据源，请求一个socket服务，获取数据，然后进行wordcount统计
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建入口环境 ExecutionEnvironment 批处理的入口环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // 流批一体的环境
        /**
         * 本地运行模式时，程序的默认并行度为 ，你的cpu的逻辑核数
         */
        env.setParallelism(1);  // 默认并行度可以通过env人为指定
        // 通过source算子，映射成一个dataStream nc -lk 9999
        DataStreamSource<String> source = env.socketTextStream("linux121", 9999);
        // 通过算子对数据进行转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\s+");
                for (String word : split) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {

                return tuple2.f0;
            }
        });
        // 通过sink算子，将数据输出到控制台
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyed.sum("f1");
        result.print();
        // 触发程序的提交执行
        env.execute();
    }
}
