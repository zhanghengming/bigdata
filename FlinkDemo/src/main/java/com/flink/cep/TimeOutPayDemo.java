package com.flink.cep;

import com.flink.cep.entity.PayBean;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 找出下单后10分钟没有支付的订单
 */
public class TimeOutPayDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<PayBean> source = env.fromElements(
                new PayBean(1L, "create", 1597905234000L),
                new PayBean(1L, "pay", 1597905235000L),
                new PayBean(2L, "create", 1597905236000L),
                new PayBean(2L, "pay", 1597905237000L),
                new PayBean(3L, "create", 1597905239000L)
//                new PayBean(3L, "pay", 1597905839000L),
//                new PayBean(4L, "create", 1597905840000L),
//                new PayBean(4L, "pay", 1597905841000L)
        );

        SingleOutputStreamOperator<PayBean> watermarks = source.assignTimestampsAndWatermarks(new WatermarkStrategy<PayBean>() {
            @Override
            public WatermarkGenerator<PayBean> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<PayBean>() {
                    long maxTimeStamp = Long.MIN_VALUE;
                    final long maxDelayTime = 500L;

                    @Override
                    public void onEvent(PayBean event, long eventTimestamp, WatermarkOutput output) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.getTs());
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimeStamp - maxDelayTime));
                    }
                };
            }
        }.withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        KeyedStream<PayBean, Long> keyedStream = watermarks.keyBy(PayBean::getId);

        Pattern<PayBean, PayBean> pattern = Pattern.<PayBean>begin("start").where(new IterativeCondition<PayBean>() {
                    @Override
                    public boolean filter(PayBean payBean, Context<PayBean> context) throws Exception {
                        return payBean.getState().equals("create");
                    }
                    // 不是严格临近，之间可以有其它操作
                }).followedBy("next")
                .where(new IterativeCondition<PayBean>() {
                    @Override
                    public boolean filter(PayBean payBean, Context<PayBean> context) throws Exception {
                        return payBean.getState().equals("pay");
                    }
                }).within(Time.seconds(600));

        PatternStream<PayBean> patternStream = CEP.pattern(keyedStream, pattern);
        // 输出标签，用于标记匹配超时的数据，也就是与模式相反的数据
        OutputTag<PayBean> outputTag = new OutputTag<PayBean>("outputStream") {
        };

        SingleOutputStreamOperator<PayBean> select = patternStream.select(outputTag, new PatternTimeoutFunction<PayBean, PayBean>() {
            /**
             * @param map 所有匹配的数据组成的集合
             * @return 返回超时的数据
             */
            @Override
            public PayBean timeout(Map<String, List<PayBean>> map, long l) throws Exception {
                System.out.println("超时");
                return map.get("start").get(0);
            }

        }, new PatternSelectFunction<PayBean, PayBean>() {

            /**
             * @return 返回匹配上的数据
             */
            @Override
            public PayBean select(Map<String, List<PayBean>> map) throws Exception {
                System.out.println("正常");
                return map.get("start").get(0);
            }
        });
        // 侧输出
        DataStream<PayBean> sideOutput = select.getSideOutput(outputTag);
        sideOutput.print();
        System.out.println("--------");
        select.print();
        env.execute();
    }
}
