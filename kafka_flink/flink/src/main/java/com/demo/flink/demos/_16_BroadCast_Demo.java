package com.demo.flink.demos;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class _16_BroadCast_Demo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // id,eventId
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);

        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        // id,age,city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });


        /**
         * 案例背景：
         *    流 1：  用户行为事件流（持续不断，同一个人也会反复出现，出现次数不定
         *    流 2：  用户维度信息（年龄，城市），同一个人的数据只会来一次，来的时间也不定 （作为广播流）
         *
         *    需要加工流1，把用户的维度信息填充好，利用广播流来实现
         */
        // 将字典数据所在流： s2  ，  转成 广播流
        MapStateDescriptor<String, Tuple2<String, String>> mapStateDescriptor = new MapStateDescriptor<>("state", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        BroadcastStream<Tuple3<String, String, String>> broadcastStream = s2.broadcast(mapStateDescriptor);

        /**
         *   对 连接了广播流之后的 ”连接流“ 进行处理
         *   核心思想：
         *      在processBroadcastElement方法中，把获取到的广播流中的数据，插入到 “广播状态”中
         *      在processElement方法中，对取到的主流数据进行处理（从广播状态中获取要拼接的数据，拼接后输出）
         */
        // 哪个流处理中需要用到广播状态数据，就要 去  连接 connect  这个广播流
        SingleOutputStreamOperator<String> process = s1.connect(broadcastStream)
                .process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {

                    /**
                     * 本方法，是用来处理 主流中的数据（每来一条，调用一次）
                     * @param element  左流（主流）中的一条数据
                     * @param readOnlyContext  上下文
                     * @param collector  输出器
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<String, String> element, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                        // 获取广播流
                        ReadOnlyBroadcastState<String, Tuple2<String, String>> readOnlyBroadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        if (readOnlyBroadcastState != null) {
                            Tuple2<String, String> userInfo = readOnlyBroadcastState.get(element.f0);
                            collector.collect(element.f0 + "," + element.f1 + "," + (userInfo == null ? null : userInfo.f0) + "," + (userInfo == null ? null : userInfo.f1));
                        } else {
                            collector.collect(element.f0 + "," + element.f1 + "," + null + "," + null);
                        }
                    }

                    /**
                     *
                     * @param broadcastElement  广播流中的一条数据
                     * @param context  上下文
                     * @param collector 输出器
                     * @throws Exception
                     */
                    // 处理广播的数据，每来一个广播数据，就填充广播流状态
                    @Override
                    public void processBroadcastElement(Tuple3<String, String, String> broadcastElement, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context context, Collector<String> collector) throws Exception {
                        // 获取广播流
                        BroadcastState<String, Tuple2<String, String>> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        // 填充广播流状态
                        broadcastState.put(broadcastElement.f0, Tuple2.of(broadcastElement.f1, broadcastElement.f2));
                    }
                });

        process.print();

        env.execute("Broadcast Demo");
    }
}
