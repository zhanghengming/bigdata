package com.flink.state.keyedState;

import com.flink.entity.Event;
import com.flink.streamdatasource.CustomClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 统计每个用户的pv，不希望每次pv就将统计结果发送到下游去，
 * 所以注册一个定时器，每隔一段时间来发送pv的统计结果
 * 实现：
 * 定义一个用来保存定时器时间戳的值状态变量，当定时器出发并向下游发送数据以后
 * 便清空状态变量，当新数据到来时，可以注册新的定时器了，注册完之后将定时器时间戳继续保存在状态变量中
 */
public class ValueStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        stream.print("input");
        // 统计每个用户的 pv，隔一段时间（10s）输出一次结果,是根据每个分区单独的事件时间
        stream.keyBy(data -> data.user)
                .process(new PeriodicPvResult())
                .print();
        env.execute();
    }

    // 为了使用定时器，用process最基础的api
    // 注册定时器，周期性输出 pv
    private static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {
        // 定义两个状态，保存当前 pv 值，以及定时器时间戳
        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        // 初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 更新 count 值
            Long count = countState.value();
            if (count == null) {
                // 赋初值
                countState.update(1L);
            } else {
                countState.update(count + 1);
            }
            // 注册定时器
            if (timerTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timerTsState.update(value.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() +  " pv: " + countState.value());
            // 清空状态
            timerTsState.clear();
        }
    }
}
