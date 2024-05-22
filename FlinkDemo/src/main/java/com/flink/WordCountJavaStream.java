package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountJavaStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = streamExecutionEnvironment.socketTextStream("linux121", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split("\\s+")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = outputStreamOperator.keyBy(0).sum(1);
        //根据 输入的参数，第二个参数为输出的参数 即keyBy的入参
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = outputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        sum.print();
        streamExecutionEnvironment.execute();
    }
}
