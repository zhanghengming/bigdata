package com.flink.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

import java.util.Collections;
import java.util.Iterator;

public class WaterMarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认用的是数据进入flink的时间，我们要改为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置自动生成水印的间隔，周期性向外发送水印的时间是1s
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("linux121", 7777);

        SingleOutputStreamOperator<Tuple2<String, Long>> maped = dataStreamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<String, Long>(split[0], Long.valueOf(split[1]));
            }
        });

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        // 设置水印
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarks = maped.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
            @Override
            // 水印生成器
            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Tuple2<String, Long>>() {
                    private long maxTimeStamp = Long.MIN_VALUE;

                    @Override
                    // 每来一条数据执行一次，每个数据称为一个事件，来计算当前最大的事件时间，用来生成最新的水印
                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput watermarkOutput) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.f1);
                        System.out.println("maxTimeStamp:" + maxTimeStamp + "...format:" + sdf.format(maxTimeStamp));
                    }

                    @Override
                    /**
                     * 周期性发射器，将水印周期性发送出去
                     * 通过上面设置的周期时间来周期性执行该代码发射水印
                     */
                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                        System.out.println(".....onPeriodicEmit....");
                        // 允许延迟时间
                        long maxOutOfOrderness = 3000L;
                        // 水印为：当前事件的最大时间 - 允许延迟的时间 当水印时间大于窗口结束时间，并且窗口中有数据，则触发计算
                        watermarkOutput.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                    }
                };
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>(){
            /**
             * @param stringLongTuple2 事件
             * @param l
             * @return 用哪个字段生成水印
             */
            @Override
            // 水印的生成策略包装，用哪个字段生成水印
            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                return stringLongTuple2.f1;
            }
        }));
        KeyedStream<Tuple2<String, Long>, String> keyedStream = watermarks.keyBy(value -> value.f0);

        // 调用window操作
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(4)));

        /**
         * Tuple2<String, Long> 传入的数据
         * String 传递出的数据
         * String key的数据类型
         * TimeWindow 窗口类型
         */
        SingleOutputStreamOperator<String> applyed = window.apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                /**
                 * 窗口的开始时间是系统自动划分的，不是根据事件时间，所以只有当水印时间大于窗口的结束时间才会触发
                 */
                System.out.println("..." + sdf.format(window.getStart()));
                String key = s;
                ArrayList<Long> list = new ArrayList<>();
//                for (Tuple2<String, Long> tuple2 : input) {
//                    list.add(tuple2.f1);
//                }
                Iterator<Tuple2<String, Long>> iterator = input.iterator();
                while (iterator.hasNext()) {
                    Tuple2<String, Long> next = iterator.next();
                    list.add(next.f1);
                }
                Collections.sort(list);
                // 将窗口中的第一个和最后一个数据以及窗口的开始和结束时间打印出来
                String result = "key:" + key + "..." + "list.size:" + list.size() + "...list.first:" + sdf.format(list.get(0)) + "...list.last:" + sdf.format(list.get(list.size() - 1)) + "...window.start:" + sdf.format(window.getStart()) + "..window.end:" + sdf.format(window.getEnd());
                out.collect(result);
            }
        });
        applyed.print();
        env.execute();

    }
}
