package com.flink.window;

import com.flink.entity.Event;
import com.flink.entity.UrlViewCount;
import com.flink.window.functions.WindowAggregateTwoParseFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Alice, ./home, 1000
 * Alice, ./home, 2000
 * Alice, ./home, 10000
 * Alice, ./home, 9000
 * Alice, ./cart, 12000
 * Alice, ./prod?id=100, 15000
 * Alice, ./home, 9000
 * Alice, ./home, 8000
 * Alice, ./prod?id=200, 70000
 * Alice, ./home, 8000
 * Alice, ./prod?id=300, 72000
 * Alice, ./home, 8000
 * 10s的滚动窗口，2s的延迟，当12s的时候窗口里有3条数据，窗口触发计算，窗口又设置了允许处理迟到的数据1分钟，
 * 所以当之后的数据来的时候，每来一条更新一次之前的结果。当等待时间到之后（加上最大延迟时间），开启新的窗口，之后再来的数据就丢弃
 */
public class ProcessingLateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认用的是数据进入flink的时间，我们要改为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 将数据源改为 socket 文本流，并转换成 Event 类型
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("linux121", 9999)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                    }
                })
                // 方式一：设置 watermark 延迟时间，2 秒钟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        // 定义侧输出流标签
        OutputTag<Event> outputTag = new OutputTag<Event>("late"){};
        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 方式二：允许窗口处理迟到数据，设置 1 分钟的等待时间
                .allowedLateness(Time.minutes(1))
                // 方式三：将最后的迟到数据输出到侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new AggFunction(), new ProcessWindowFun());
        result.print();
        result.getSideOutput(outputTag).print("late");
        // 为方便观察，可以将原始数据也输出
        stream.print("input");
        env.execute();

    }
    public static class AggFunction implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }

    }


    // 窗口处理函数，只需要将窗口聚合后的信息包装即可
    // IN：输入的类型 OUT：输出的类型 KEY：key的类型 W：可应用的窗口类型
    public static class ProcessWindowFun extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        /**
         * @param rul        The key for which this window is evaluated.
         * @param context  The context in which the window is being evaluated.
         * @param elements The elements in the window being evaluated.
         * @param out      A collector for emitting elements.
         * @throws Exception
         */
        @Override
        public void process(String rul, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 迭代器中只有一个元素，就是增量聚合函数的计算结果
            out.collect(new UrlViewCount(rul,elements.iterator().next(),context.window().getStart(),context.window().getEnd()));
        }
    }
}
