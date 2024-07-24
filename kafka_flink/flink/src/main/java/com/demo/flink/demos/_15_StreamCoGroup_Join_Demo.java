package com.demo.flink.demos;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class _15_StreamCoGroup_Join_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);


        // id,name
        DataStreamSource<String> stream1 = env.socketTextStream("linux123", 9998);

        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        DataStreamSource<String> stream2 = env.socketTextStream("linux123", 9999);
        // id,age,city
        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });


        CoGroupedStreams<Tuple2<String, String>, Tuple3<String, String, String>> tuple2Tuple3CoGroupedStreams = s1.coGroup(s2);
        DataStream<String> result = tuple2Tuple3CoGroupedStreams
                .where(s -> s.f0)// 左流的  f0 字段
                .equalTo(s -> s.f0)// 右流的 f0 字段
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    /**
                     *
                     * @param first  是协同组中的第一个流的数据，也就是左流中第一个key值所对应的数据
                     * @param second 是协同组中的第二个流的数据
                     * @param out 是处理结果的输出器
                     * @throws Exception
                     */
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> first, Iterable<Tuple3<String, String, String>> second, Collector<String> out) throws Exception {
                        // 在这里实现  left out  join
                        for (Tuple2<String, String> tuple2 : first) {
                            if (second.iterator().hasNext()) {
                                for (Tuple3<String, String, String> tuple3 : second) {
                                    out.collect(tuple2.f0 + ":" + tuple2.f1 + ":" + tuple3.f0 + ":" + tuple3.f1 + ":" + tuple3.f2);
                                }
                            } else {
                                // 如果左流中的数据，有对应的右流中的数据，则进行输出
                                // 如果左流中的数据，没有对应的右流中的数据，则用null进行输出
                                // 如果左流中的数据，没有对应的右流中的数据，则进行输出
                                out.collect(tuple2.f0 + ":" + tuple2.f1 + ":null:null:null");
                            }
                        }
                    }
                });
        result.print();

        /**
         * 流的 join 算子 join上的才会出现在join的方法中，join上的每条数据都会调用一次join方法，
         * 案例背景：
         *    流1数据：  id,name
         *    流2数据：  id,age,city
         *    利用join算子，来实现两个流的数据按id关联
         */
        JoinedStreams<Tuple2<String, String>, Tuple3<String, String, String>> joinedStreams = s1.join(s2);
        DataStream<String> joinedStream = joinedStreams
                .where(tp2 -> tp2.f0)
                .equalTo(tp3 -> tp3.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> t1, Tuple3<String, String, String> t2) throws Exception {
                        return t1.f0 + "," + t1.f1 + "," + t2.f0 + "," + t2.f1 + "," + t2.f2;
                    }
                });

        joinedStream.print();


        env.execute("Stream CoGroup Join Demo");
    }
}
