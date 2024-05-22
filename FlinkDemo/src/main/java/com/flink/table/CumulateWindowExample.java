package com.flink.table;

import com.flink.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 滚动窗口 TUMBLE()、滑动窗口 HOP()和累积窗口（CUMULATE）
 * 三种表值函数（TVF），表值函数得到的是一个表，表值函数就类似于窗口函数，
 * 基于窗口后的结果进行聚合
 */
public class CumulateWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        );
        // 为方便在 SQL 中引用，在环境中注册表 EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);
        // 设置累积窗口，执行 SQL 统计查询
        Table resultTable = tableEnv.sqlQuery(
                "SELECT " +
                        "user, " +
                        "window_end AS endT, " +
                        "COUNT(url) AS cnt " +
                        "FROM TABLE( " +
                        "CUMULATE( TABLE EventTable, " + // 定义累积窗口
                        "DESCRIPTOR(ts), " +
                        "INTERVAL '30' MINUTE, " +
                        "INTERVAL '1' HOUR)) " +
                        "GROUP BY user, window_start, window_end "
        );
        tableEnv.toDataStream(resultTable).print();
        env.execute();
    }
}
