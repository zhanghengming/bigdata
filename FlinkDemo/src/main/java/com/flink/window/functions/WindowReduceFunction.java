package com.flink.window.functions;


import com.flink.entity.Event;
import com.flink.streamdatasource.CustomClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * 最基本的聚合方式就是规约（reduce），
 * 将窗口中收集到的数据两两进行归约，每来一个新的数据，就和之前的聚合状态做归约。
 *    窗口函数中也提供了 ReduceFunction：只要基于 WindowedStream 调用.reduce()方法，然
 * 后传入 ReduceFunction 作为参数，可以指定以归约两个元素的方式去对窗口中数据进行聚
 * 合了。
 *     ReduceFunction 中需要重写一个 reduce 方法，它的两个参数代表输入的两
 * 个元素，而归约最终输出结果的数据类型，与输入的数据类型必须保持一致。也就是说，中间
 * 聚合的状态和输出的结果，都和输入的数据类型是一样的。
 */

// 统计每个用户的浏览次数
public class WindowReduceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认用的是数据进入flink的时间，我们要改为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 从自定义数据源读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.map(new MapFunction<Event, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                // 将数据转换为二元组，方便计算
                System.out.println(Tuple2.of(value.user,new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(value.timestamp)));
                return Tuple2.of(value.user,1L);
            }
        })
                .keyBy(r -> r.f0)
                // 设置滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 窗口函数的处理逻辑，定义累加规则，窗口闭合时，向下游发送累加结果
//                        System.out.println(Tuple2.of(value1.f0,value1.f1)  );
//                        System.out.println("-----------");
//                        System.out.println(Tuple2.of(value1.f0, value1.f1 + value2.f1) );
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();
        env.execute();
    }
}
