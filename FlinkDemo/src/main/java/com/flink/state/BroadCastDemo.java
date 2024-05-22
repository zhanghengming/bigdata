package com.flink.state;

import com.flink.entity.MyPattern;
import com.flink.entity.UserAction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadCastDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //两套数据流，1：用户行为 2 ： 模式
        UserAction ac1 = new UserAction(1001L, "login");
        UserAction ac2 = new UserAction(1003L, "pay");
        UserAction ac3 = new UserAction(1002L, "car");
        UserAction ac4 = new UserAction(1001L, "logout");
        UserAction ac5 = new UserAction(1003L, "car");
        UserAction ac6 = new UserAction(1002L, "logout");

        DataStreamSource<UserAction> actions = env.fromElements(ac1, ac2, ac3, ac4, ac5, ac6);
        MyPattern myPattern1 = new MyPattern("login", "logout");
        MyPattern myPattern2 = new MyPattern("car", "logout");

        DataStreamSource<MyPattern> patterns = env.fromElements(myPattern1);
        // 每个用户的行为分到一个区
        KeyedStream<UserAction, Long> keyed = actions.keyBy(UserAction::getUserId);

        // 将模式流广播到下游的所有算子
        /**
         * MapState通常比在ValueState中手动维护映射更高效
         * param:name:状态描述器的名字，
         * @param keyTypeInfo 状态中key的类型.
         * @param valueTypeInfo 状态中value的类型.
         */
        // 创建广播流的状态描述器
        MapStateDescriptor<Void, MyPattern> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(MyPattern.class));
        // 广播流
        BroadcastStream<MyPattern> broadcastPatterns = patterns.broadcast(bcStateDescriptor);


        /**
         * 该方法用于keyed 流和广播流连起来的元素处理
         * <KS> – The key type of the input keyed stream  keyed流key的数据类型.
         * <IN1> – The input type of the keyed (non-broadcast) side value的数据类型.
         * <IN2> – The input type of the broadcast side 广播的数据类型.
         * <OUT> – The output type of the operator 最终输出类型.
         */
        KeyedBroadcastProcessFunction<Long, UserAction, MyPattern, Tuple2<Long, String>> keyedBroadcastProcessFunction = new KeyedBroadcastProcessFunction<Long,UserAction,MyPattern, Tuple2<Long,String>>() {
            ValueState<String> keyedState ;
            @Override
            // 初始化keyedStream的一个状态
            public void open(Configuration parameters) throws Exception {
                keyedState = getRuntimeContext().getState(new ValueStateDescriptor<String>("keyedState", Types.STRING));
            }

            /**
             * 处理keyd流中的每一个元素
             * @param value The stream element.
             * @param ctx   一个可以迭代只读的状态的上下文，也就是说不能对它所在的状态进行更改.
             * @param out   The collector to emit resulting elements to
             */
            @Override
            public void processElement(UserAction value, KeyedBroadcastProcessFunction<Long, UserAction, MyPattern, Tuple2<Long, String>>.ReadOnlyContext ctx, Collector<Tuple2<Long, String>> out) throws Exception {
                // 获取广播流的状态来和keyed流中的元素做操作
                ReadOnlyBroadcastState<Void, MyPattern> broadcastState = ctx.getBroadcastState(bcStateDescriptor);
                // 获取广播流中的元素
                MyPattern myPattern = broadcastState.get(null);
                // 把用户行为流和模式流中的模式进行匹配，也就是用状态来进行匹配
                String keyedValue = keyedState.value();
                // 当俩流的状态都不为空时，即之前流中已经有元素时
                if (myPattern != null && keyedValue != null) {
                    if (myPattern.getFirstAction().equals(keyedValue) && myPattern.getSecondAction().equals(value.getAction())) {
                        out.collect(Tuple2.of(value.getUserId(), keyedValue + value.getAction()));
                    } else {

                    }
                }
                // 更新keyed流的状态
                keyedState.update(value.getAction());

            }

            /**
             * 处理广播流中的每一个元素
             * @param value The stream element.
             * @param ctx   可以更新状态的上下文.
             * @param out   The collector to emit resulting elements to
             */
            @Override
            public void processBroadcastElement(MyPattern value, KeyedBroadcastProcessFunction<Long, UserAction, MyPattern, Tuple2<Long, String>>.Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
                BroadcastState<Void, MyPattern> broadcastState = ctx.getBroadcastState(bcStateDescriptor);
                // 更新广播状态
                broadcastState.put(null,value);
            }

        };
        // 将流连接起来,process来处理连接流的元素
        SingleOutputStreamOperator<Tuple2<Long, String>> streamOperator = keyed.connect(broadcastPatterns).process(keyedBroadcastProcessFunction);
        streamOperator.print();
        env.execute();

    }
}
