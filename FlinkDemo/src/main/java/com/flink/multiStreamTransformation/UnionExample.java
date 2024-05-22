package com.flink.multiStreamTransformation;

import com.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * union可以同时进行多条流的合并
 * 将多条流合在一起,联合操作要求必须流中的数据类型必须相同,
 * 合并之后的新流会包括所有流中的元素，数据类型不变
 * 基于 DataStream 直接调用.union()方法，传入其他 DataStream 作为参数，就可以实现流的联合了；得到的依然是一个 DataStream
 * 水位线：
 * 多流合并时处理的时效性是以最慢的那个流为准的
 * Flink流在开始处，插入一个负无穷大（Long.MIN_VALUE）的水位线，合流后的ProcessFunction 对应的处理任务，会为合并的每条流保存一个“分区水位线”，初始值
 * 都是 Long.MIN_VALUE；而此时算子任务的水位线是所有分区水位线的最小值，因此也是
 * Long.MIN_VALUE。
 * 第一条 socket 文本流输入数据[Alice, ./home, 1000] 时，水位线不会立即改变，只
 * 有到水位线生成周期的时间点（200ms 一次）才会推进到 1000 - 1 = 999 毫秒不过即使第一条水位线推进到了 999，由于另一条
 * 流没有变化，所以合流之后的 Process 任务水位线仍然是初始值
 * 这时我们在第二条 socket 文本流输入数据[Alice, ./home, 2000]，那么第二条流的水位线会随之推进到 2000 – 1 = 1999 毫秒，Process 任务所保存的第二条流分区水位线更新为 1999；
 * 这样两个分区水位线取最小值，Process 任务的水位线也就可以推进到 999 了
 */
public class UnionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("linux121", 77777)
                .map(data -> {
                    String[] split = data.split(",");
                    return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream1.print("stream1");

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("linux121", 77777)
                .map(data -> {
                    String[] split = data.split(",");
                    return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream2.print("stream2");
        // 合并两条流
        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(" 水 位 线 ： " + ctx.timerService().currentWatermark());
                    }
                }).print();
        env.execute();

    }
}
