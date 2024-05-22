package com.flink.window.functions;

import com.flink.entity.Event;
import com.flink.streamdatasource.CustomClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.util.HashSet;



// 人均重复访问量
public class WindowAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认用的是数据进入flink的时间，我们要改为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomClickSource())
                // 单调有序的时间戳
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 所有数据设置相同的 key，发送到同一个分区统计 PV 和 UV，再相除
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
                /**
                 * 相比于reduce 三者的类型必须一致
                 * 输入数据、中间状态、输出结果三者类
                 * 型都可以不同
                 */
                .aggregate(new AggregateFunction<Event, Tuple2<HashSet<String>,Long>, Double>() {
                    /**
                     * 创建一个累加器，这就是为聚合创建了一个初始状态，每个聚
                     * 合任务只会调用一次。
                     */
                    @Override
                    public Tuple2<HashSet<String>, Long> createAccumulator() {
                        // 创建累加器
                        return Tuple2.of(new HashSet<String>(),0L);
                    }

                    /**
                     * 将输入的元素添加到累加器中。
                     * 方法传入两个参数：当前新到的数据 value，和当前的累加器
                     * accumulator；返回一个新的累加器值，也就是对聚合状态进行更新。每条数据到来之
                     * 后都会调用这个方法。
                     */
                    @Override
                    public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
                        // 属于本窗口的数据来一条累加一次，并返回累加器
                        System.out.println(Tuple2.of(value.user,new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(value.timestamp)));
//                        System.out.println(Tuple2.of(accumulator.f0,accumulator.f1 + 1L));
                        accumulator.f0.add(value.user);
                        return Tuple2.of(accumulator.f0,accumulator.f1 + 1L);
                    }

                    /**
                     * 从累加器中提取聚合的输出结果。定义多个状态，
                     * 然后再基于这些聚合的状态计算出一个结果进行输出。
                     * 这个方法只在窗口要输出结果时调用。
                     */
                    @Override
                    public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
                        // 窗口闭合时，增量聚合结束，将计算结果发送到下游
                        System.out.println("窗口结束--------");
                        System.out.println((double) (accumulator.f1 / accumulator.f0.size()));
                        return (double) (accumulator.f1 / accumulator.f0.size());
                    }

                    /**
                     * 合并两个累加器，并将合并后的状态作为一个累加器返回。
                     * 这个方法只在需要合并窗口的场景下才会被调用；最常见的合并窗口
                     * 的场景就是会话窗口
                     */
                    @Override
                    public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
                        return null;
                    }
                })
                .print();
        System.out.println("这里");
        env.execute();
    }
}
