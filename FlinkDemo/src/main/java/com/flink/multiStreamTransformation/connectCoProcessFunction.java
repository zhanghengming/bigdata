package com.flink.multiStreamTransformation;

import com.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * CoProcessFunction 通用的处理函数
 * 调用.process()时，传入的则是一个 CoProcessFunction
 * CoProcessFunction 同样可以通过上下文 ctx 来
 * 访问 timestamp、水位线，并通过 TimerService 注册定时器；另外也提供了.onTimer()方法，用
 * 于定义定时触发的处理操作。
 */
public class connectCoProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 来自 app 的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                    Tuple3.of("order-1", "app", 1000L),
                    Tuple3.of("order-2", "app", 2000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));
        // 来自第三方支付平台的支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartStream = env.fromElements(
                        Tuple4.of("order-1", "third-party", "success", 3000L),
                        Tuple4.of("order-3", "third-party", "success", 4000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                return element.f3;
                            }
                        }));

        // 检测同一支付单在两条流中是否匹配，不匹配就报警
        appStream.connect(thirdpartStream)
                // ConnectedStreams 进行 keyBy 操作，其实就是把两条流中 key 相同的数据放到了一起，
                //然后针对来源的流再做各自处理也可以在合并之前就将两条流分别进行 keyBy,得到的 KeyedStream 再进行连接（connect）操作,两条流定义的键的类型必须相同
                .keyBy(data -> data.f0, data1 -> data1.f0)
                .process(new OrderMatchResult())
                .print();
        env.execute();
    }

    // 自定义实现 CoProcessFunction
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String,
            String, Long>, Tuple4<String, String, String, Long>, String> {
        // 定义状态变量，用来保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        @Override
        // 初始化每个流的状态
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>(
                            "app-event"
                            , Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>(
                            "thirdparty-event",
                            Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)
                    )
            );
        }

        @Override
        /**
         * 第一个流来的处理逻辑，
         * 判断第二个流的状态是否为空，
         * 如果不为空则说明第二个流也来了，匹配成功，然后将第二个流的状态清空
         * 为空说明没有到来，更新第一个流的状态；
         * 然后注册定时器，当到达指定时间后，触发器执行，判断状态是否为空，如果不为空
         * 则说明没有匹配成功，状态没有清空。已经到来的流的状态更新了，另一个流的状态为空，另外一个流没有到来。
         */
        public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            // 看另一条流中事件是否来过
            if (thirdPartyEventState.value() != null) {
                out.collect(" 对 账 成 功 ： " + value + " " + thirdPartyEventState.value());
                // 清空状态
                thirdPartyEventState.clear();
            } else {
                // 更新状态
                appEventState.update(value);
                // 注册一个 5 秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }


        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            if (appEventState.value() != null) {
                out.collect(" 对 账 成 功 ： " + value + " " + appEventState.value());
                appEventState.clear();
            } else {
                thirdPartyEventState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
            }
        }


        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
            if (appEventState.value() != null) {
                out.collect("对账失败：" + appEventState.value() + " " + "第三方支付平台信息未到");
            }
            if  (thirdPartyEventState.value() != null) {
                out.collect("对账失败：" + thirdPartyEventState.value() + " " + "app信息未到");
            }
            appEventState.clear();
            thirdPartyEventState.clear();
        }
    }
}
