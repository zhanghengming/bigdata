package com.flink.cep;

import com.flink.cep.entity.CEPLoginBean;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;


// 找出5秒内，连续登录失败的账号
public class CepDemo {
    public static void main(String[] args) throws Exception {
        /**
         * 1、数据源
         * 2、在数据源上做出watermark
         * 3、在watermark上根据id分组keyby
         * 4、做出模式pattern
         * 5、在数据流上进行模式匹配
         * 6、提取匹配成功的数据
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置流的时间特征为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<CEPLoginBean> data = env.fromElements(
                new CEPLoginBean(1L, "fail", 1597905234000L),
                new CEPLoginBean(1L, "success", 1597905235000L),
                new CEPLoginBean(2L, "fail", 1597905236000L),
                new CEPLoginBean(2L, "fail", 1597905237000L),
                new CEPLoginBean(2L, "fail", 1597905238000L),
                new CEPLoginBean(3L, "fail", 1597905239000L),
                new CEPLoginBean(3L, "success", 1597905240000L)
        );

        // 2、在数据源上做出watermark  分配时间戳和水印，构造水印策略
        SingleOutputStreamOperator<CEPLoginBean> watermarks = data.assignTimestampsAndWatermarks(new WatermarkStrategy<CEPLoginBean>() {
            @Override
            public WatermarkGenerator<CEPLoginBean> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<CEPLoginBean>() {
                    long maxTimeStamp = Long.MIN_VALUE;

                    @Override
                    public void onEvent(CEPLoginBean event, long eventTimestamp, WatermarkOutput output) {
                        // 计算当前事件的最新事件来生成最新的水印
                        maxTimeStamp = Math.max(maxTimeStamp, event.getTs());
                    }

                    long maxDelayTime = 500L;

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimeStamp - maxDelayTime));

                    }
                };
            }
            // 使用新的水印生成策略来包装这个策略
        }.withTimestampAssigner(new SerializableTimestampAssigner<CEPLoginBean>() {
            @Override
            public long extractTimestamp(CEPLoginBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        // 3、在watermark上根据id分组keyby
        KeyedStream<CEPLoginBean, Long> keyedStream = watermarks.keyBy(value -> value.getId());
        // 4、做出模式pattern
        Pattern<CEPLoginBean, CEPLoginBean> pattern = Pattern.<CEPLoginBean>begin("start").where(new IterativeCondition<CEPLoginBean>() {
            @Override
            // 以什么作为开始的标志
            public boolean filter(CEPLoginBean cepLoginBean, Context<CEPLoginBean> context) throws Exception {
                return cepLoginBean.getState().equals("fail");
            }
        }).next("next").where(new IterativeCondition<CEPLoginBean>() {
            @Override
            public boolean filter(CEPLoginBean cepLoginBean, Context<CEPLoginBean> context) throws Exception {
                return cepLoginBean.getState().equals("fail");
            }
        }).within(Time.seconds(5L));
        // 5、在数据流上进行模式匹配
        PatternStream<CEPLoginBean> patternStream = CEP.pattern(keyedStream, pattern);
        // 6、提取匹配成功的数据
        SingleOutputStreamOperator<Long> process = patternStream.process(new PatternProcessFunction<CEPLoginBean, Long>() {
            /**
             * @param map 存的是匹配的标志和匹配上的数据组成的kv对
             * @param context
             * @param collector 向下游发送的数据
             * @throws Exception
             */
            @Override
            public void processMatch(Map<String, List<CEPLoginBean>> map, Context context, Collector<Long> collector) throws Exception {
//                System.out.println(map);
                collector.collect(map.get("start").get(0).getId());
            }
        });

        process.print();
        env.execute();

    }
}
