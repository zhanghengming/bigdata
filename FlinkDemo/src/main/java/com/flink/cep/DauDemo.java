package com.flink.cep;

import com.flink.cep.entity.UserBean;
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

// 找出24小时内，至少5次有效交易的用户
public class DauDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<UserBean> source = env.fromElements(
                new UserBean("100XX", 0.0D, 1597905234000L),
                new UserBean("100XX", 100.0D, 1597905235000L),
                new UserBean("100XX", 200.0D, 1597905236000L),
                new UserBean("100XX", 300.0D, 1597905237000L),
                new UserBean("100XX", 400.0D, 1597905238000L),
                new UserBean("100XX", 500.0D, 1597905239000L),
                new UserBean("101XX", 0.0D, 1597905240000L),
                new UserBean("101XX", 100.0D, 1597905241000L)
        );

        SingleOutputStreamOperator<UserBean> watermarks = source.assignTimestampsAndWatermarks(new WatermarkStrategy<UserBean>() {
            @Override
            public WatermarkGenerator<UserBean> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<UserBean>() {
                    long maxTimeStamp = Long.MIN_VALUE;
                    long maxDelayTime = 500L;

                    @Override
                    public void onEvent(UserBean event, long eventTimestamp, WatermarkOutput output) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.getTs());
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimeStamp - maxDelayTime));
                    }
                };
            }
        }.withTimestampAssigner((element, recordTimestamp) -> element.getTs()));
        KeyedStream<UserBean, String> keyedStream = watermarks.keyBy(value -> value.getUid());
        // 创建模式
        Pattern<UserBean, UserBean> pattern = Pattern.<UserBean>begin("start").where(new IterativeCondition<UserBean>() {
            @Override
            public boolean filter(UserBean userBean, Context<UserBean> context) throws Exception {
                return userBean.getMoney() > 0;
            }
        }).timesOrMore(5).within(Time.hours(24L));

        PatternStream<UserBean> patternStream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<String> process = patternStream.process(new PatternProcessFunction<UserBean, String>() {
            @Override
            public void processMatch(Map<String, List<UserBean>> map, Context context, Collector<String> collector) throws Exception {
                collector.collect(map.get("start").get(0).getUid());
            }
        });

        process.print();
        env.execute();
    }
}
