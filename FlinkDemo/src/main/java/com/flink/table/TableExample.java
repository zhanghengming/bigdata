package com.flink.table;

import com.flink.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> data = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );
        // 获取表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        // 将数据流转换成表
        Table eventTable = tableEnvironment.fromDataStream(data);
        // 用执行 SQL 的方式提取数据
        Table table = tableEnvironment.sqlQuery("select url, user from " + eventTable);
        // 将表转换成数据流，打印输出
        tableEnvironment.toDataStream(table).print();
        env.execute();

    }
}
