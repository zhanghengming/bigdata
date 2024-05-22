package com.flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;


public class TableApiDemo {
    public static void main(String[] args) throws Exception {
        // flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 用env做出table环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                // 设置blinkPlanner
                .useBlinkPlanner()
                // 默认是流模式
                .inStreamingMode()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // 获取流式数据源
        DataStreamSource<Tuple2<String, Integer>> streamSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            /**
             * @param ctx The context to emit elements to and for accessing locks.
             * @throws Exception
             */
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Tuple2<>("name", 10));
                    Thread.sleep(1000);
                }
            }

            /**
             *
             */
            @Override
            public void cancel() {

            }
        });
        // 将流式数据源做成table，然后用表达式给列命名
        Table table = tableEnv.fromDataStream(streamSource, $("name"), $("age"));
        // 查数据
        Table name = table.select($("name"));

        /**
         * sql
         */
        tableEnv.createTemporaryView("table1",streamSource,$("name"),$("age"));
        String sql = "select * from table1";
        Table query = tableEnv.sqlQuery("select * from table1");
        DataStream<Tuple2<Boolean, Row>> toRetractStream = tableEnv.toRetractStream(query, Row.class);

        // 将table数据转化为流数据，往下传递，toRetractStream是可以更新、删除的，而append只能插入，第二个参数为转化为该类型的记录
        // Boolean true表示是一个追加进来的数据，false为可撤回的
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(name, Row.class);

//        tuple2DataStream.print();
        toRetractStream.print();
        env.execute();
    }
}
