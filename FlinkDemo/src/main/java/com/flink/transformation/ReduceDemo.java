package com.flink.transformation;

import com.flink.entity.Event;
import com.flink.streamdatasource.CustomClickSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 按照用户 id 进行分区，然后用一个 reduce 算子实现 sum 的功能，统计每个用户访问的频次；
 * 进而将所有统计结果分到一组，用另一个 reduce 算子实现 maxBy 的功能，记录所有用户中访问频次最高的那个，也就是当前访问量最大的用户是谁。
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomClickSource());
        // 将数据流转化为元组的形式，方便分组操作
        eventDataStreamSource.map(event -> Tuple2.of(event.user,1L))
                .returns(new TypeHint<Tuple2<String,Long>>(){})
                // 按照用户进行分组，不同的key分配到不同分区中
                .keyBy(e -> e.f0)
                // 将每个分区中的数据进行累加操作,每个分区都会得到一个唯一的值
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1) ;
                    }
                })
                // 为每一条数据分配同一个 key，将聚合结果发送到一条流中
                .keyBy(new KeySelector<Tuple2<String, Long>, Boolean>() {
                    @Override
                    public Boolean getKey(Tuple2<String, Long> bool) throws Exception {
                        return true;
                    }
                })
                // 将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                .reduce((t1,t2) -> t1.f1 > t2.f1 ? t1 : t2)
                .print();
        env.execute();

    }
}
