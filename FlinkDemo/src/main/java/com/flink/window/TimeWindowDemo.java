package com.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;


public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 添加数据源
        DataStreamSource<String> source = env.addSource(new SourceFunction<String>() {
            int i = 0;
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    ctx.collect(i + "号数据源");
                    i++;
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//        Date date = new Date();

        // 转化数据源，添加时间戳标识
        SingleOutputStreamOperator<Tuple2<String, String>> maped = source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                long timeMillis = System.currentTimeMillis();
                String format = simpleDateFormat.format(timeMillis);
                return new Tuple2<>(s, format);
            }
        });
        // 增加是数据的并行度
        /**
         * 如果使用的countwindow，则需要根据key的数量进行划分窗口，则相同的key会进入到同一个槽中，那么窗口也是该槽中的窗口
         */
        KeyedStream<Tuple2<String, String>, String> keyedStream = maped.keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2.f0;
            }
        });
        // 5s的时间窗口
        WindowedStream<Tuple2<String, String>, String, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(5));

        // 滚动处理时间窗口
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // 10s的会话窗口
//        WindowedStream<Tuple2<String, String>, String, TimeWindow> sessionWindow = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));


        // 操作窗口数据
        SingleOutputStreamOperator<String> applyed = timeWindow.apply(new WindowFunction<Tuple2<String, String>, String, String, TimeWindow>() {

            /**
             * @param s      The key for which this window is evaluated.
             * @param window The window that is being evaluated.
             * @param input  The elements in the window being evaluated.
             * @param out    A collector for emitting elements.
             */
//            StringBuilder stringBuilder = new StringBuilder();
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, String>> input, Collector<String> out) throws Exception {
                Iterator<Tuple2<String, String>> iterator = input.iterator();
                StringBuilder stringBuilder = new StringBuilder();
                System.out.println("------------");
                while (iterator.hasNext()) {
                    Tuple2<String, String> next = iterator.next();
                    stringBuilder.append(next.f0 + "..." + next.f1);
//                    stringBuilder.append(iterator.next().f1);
                }
                // s是数据的key，input是窗口中的数据流，out是用于发射元素的收集器
                String output = s + "||" + simpleDateFormat.format(window.getStart()) + "||" + simpleDateFormat.format(window.getEnd()) + "||" + stringBuilder;
                out.collect(output);
            }
        });
        // 输出窗口数据
        applyed.print();
        env.execute();
    }
}
