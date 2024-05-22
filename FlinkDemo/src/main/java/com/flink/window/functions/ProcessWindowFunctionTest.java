package com.flink.window.functions;

import com.flink.entity.Event;
import com.flink.streamdatasource.CustomClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 电商网站统计每小时 UV
 */
public class ProcessWindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认用的是数据进入flink的时间，我们要改为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        // 将数据全部发往同一分区，按窗口统计 UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 全窗口函数，将所有数据缓存下来，等到窗口触发计算时使用
                .process(new ProcessWindowFunction<Event, Object, Boolean, TimeWindow>() {

                    @Override
                    // 自定义窗口处理函数
                    public void process(Boolean aBoolean, ProcessWindowFunction<Event, Object, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<Object> out) throws Exception {
                        // 遍历所有数据，放到 Set 里去重
                        HashSet<String> userSet = new HashSet<>();
                        for(Event event : elements) {
                            userSet.add(event.user);
                        }
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        out.collect(" 窗 口 : " + new Timestamp(start) + " ~ " + new
                                Timestamp(end) + " 的独立访客数量是：" + userSet.size());
                    }
                }).print();

        env.execute();
    }
}

