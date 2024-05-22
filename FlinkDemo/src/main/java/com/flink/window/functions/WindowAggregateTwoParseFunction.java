package com.flink.window.functions;

import com.flink.entity.Event;
import com.flink.entity.UrlViewCount;
import com.flink.streamdatasource.CustomClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * AggregateFunction 与 ProcessWindowFunction 结合
 * public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
 *  AggregateFunction<T, ACC, V> aggFunction,
 *  ProcessWindowFunction<V, R, K, W> windowFunction
 *  )
 *  基于第一个参数（增量聚合函数）来处理窗口数据，每来一个数
 * 据就做一次聚合；等到窗口需要触发计算时，则调用第二个参数（全窗口函数）的处理逻辑输
 * 出结果。需要注意的是，这里的全窗口函数就不再缓存所有数据了，而是直接将增量聚合函数
 * 的结果拿来当作了 Iterable 类型的输入。一般情况下，这时的可迭代集合中就只有一个元素了。
 *
 * 统计 10 秒钟的 url 浏览量，每 5 秒钟更新一次
 */
public class WindowAggregateTwoParseFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认用的是数据进入flink的时间，我们要改为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                // 同时传入增量聚合函数和全窗口函数
                .aggregate(new AggFunction(),new ProcessWindowFun())
                .print();
        env.execute();
    }

    // 自定义增量聚合函数，来一条数据就加一
    // 静态内部类实现接口
    // IN：输入的类型 ACC：累加器的类型 OUT：输出的类型
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
