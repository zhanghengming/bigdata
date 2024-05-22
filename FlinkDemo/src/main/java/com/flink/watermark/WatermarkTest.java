package com.flink.watermark;

import com.flink.entity.Event;
import com.flink.streamdatasource.CustomClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

// 乱序流
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new CustomClickSource())
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线，延迟时间设置为 5s，这里面其实封装了水位生成器，直接调用静态方法来创建水位生成器。
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>(){
                            // 抽取时间戳的逻辑
                            /**
                             * @param element         要分配时间戳的元素.
                             * @param recordTimestamp 元素的内部时间戳.如果时间戳未被分配，则为负值
                             */
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })).print();
        env.execute();
    }
}
