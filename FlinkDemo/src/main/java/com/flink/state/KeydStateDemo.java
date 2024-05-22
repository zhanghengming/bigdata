package com.flink.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeydStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> source = env.fromElements(new Tuple2<Long, Long>(1L, 3L), new Tuple2<Long, Long>(1L, 5L), new Tuple2<Long, Long>(1L, 7L), new Tuple2<Long, Long>(1L, 9L));

        KeyedStream<Tuple2<Long, Long>, Long> keyedStream = source.keyBy(value -> value.f0);

        // 按照key分组策略，对流式数据调用状态化处理
        SingleOutputStreamOperator<Tuple2<Long, Long>> maped = keyedStream.map(new RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            ValueState<Tuple2<Long, Long>> sumState;

            @Override
            // 初始化方法，做出状态state
            public void open(Configuration parameters) throws Exception {
                // 状态的描述
                ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                        // 状态的名字
                        "average",
                        // 类型信息
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        }),
                        // 初始值
                        Tuple2.of(0L, 0L)
                );
                /**
                 * 获取当前的state,获取运行时上下文，得益于richFunction，
                 * getState是RuntimeContext类中的方法，可以获取操作当前状态的一个句柄，
                 * 只有在keyedStream才能访问，在每次访问时，都会根据当前的key访问当前的值
                 */

                sumState = getRuntimeContext().getState(descriptor);
                super.open(parameters);
            }

            @Override
            // 每调用一次更新一次状态
            public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
                Tuple2<Long, Long> currentStateValue = sumState.value();
                currentStateValue.f0 += 1;
                currentStateValue.f1 += value.f1;
                sumState.update(currentStateValue);

                if (currentStateValue.f0 == 4) {
                    long average = currentStateValue.f1 / currentStateValue.f0;
                    return Tuple2.of(currentStateValue.f0, average);
                }

                return currentStateValue;
            }
        });

        maped.print();
        env.execute();
    }
}
