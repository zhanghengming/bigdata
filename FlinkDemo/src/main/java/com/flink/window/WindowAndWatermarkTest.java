package com.flink.window;

import com.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * Alice, ./home, 1000
 * Alice, ./cart, 2000
 * Alice, ./prod?id=100, 10000
 * Alice, ./prod?id=200, 8000
 * Alice, ./prod?id=400, 15000
 * 当设置了延迟时间后，当10000的数据到来后水位线不会直接推到10s，而是推进到5s，当8000的数据来之后比较当前最大的事件时间还是10s所以水位线不会推进，
 * 当15000的数据到来之后，水位线推进到10s，窗口闭合，触发计算。实际上比较的还是事件时间和当前最大的水位线（窗口）
 * 插入一个时间戳为 15000L – 5 * 1000L – 1L = 9999 毫秒的水位线
 * 而后面再输入一条[Alice, ./prod?id=200,
 * 9000]时，将不会有任何结果；因为这是一条迟到数据，它所属于的窗口已经触发计算然后销
 * 毁了（窗口默认被销毁），所以无法再进入到窗口中
 */
public class WindowAndWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认用的是数据进入flink的时间，我们要改为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 将数据源改为 socket 文本流，并转换成 Event 类型
        env.socketTextStream("linux121",9999)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Event(split[0].trim(), split[1].trim(),Long.valueOf(split[2].trim()));
                    }
                })
                // 针对乱序流插入水位线，延迟时间设置为 5s
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }))
                // 根据 user 分组，开窗统计
                .keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 用全窗口函数，用里面的上下文信息
                .process(new WatermarkTestResult())
                .print();
        env.execute();
    }
    public static class WatermarkTestResult extends ProcessWindowFunction<Event,String,String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            long currentWatermark = context.currentWatermark();
            long start = context.window().getStart();
            long end = context.window().getEnd();
            elements.forEach(System.out::println);
            long count = elements.spliterator().getExactSizeIfKnown();
            out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素， 窗口闭合计算时，水位线处于：" + currentWatermark);
        }
    }
}
