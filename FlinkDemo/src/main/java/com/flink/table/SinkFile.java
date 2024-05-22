package com.flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.api.Expressions.$;

public class SinkFile {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);

        //读数据
        DataStreamSource<Tuple2<String, Integer>> data = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                int num = 0;
                while (true) {
                    num++;
                    ctx.collect(new Tuple2<>("name" + num, num));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        Table table = tEnv.fromDataStream(data,$("name"));

        String sql = "create table sinkTable(" +
                "       name varchar" +
                "       ) with (" +
                "       'connector' = 'filesystem'," +
                "       'format' = 'csv'," +
                "       'path' = 'D:\\新大数据\\data\\out.txt'" +
                "       )";
        // 将数据形成表
        tEnv.executeSql(sql);

//        tEnv.connect(new FileSystem().path("D:\\新大数据\\data\\out.txt"))
//                .withFormat(new Csv())
//                .withSchema(
//                        new Schema().field("name", DataTypes.STRING())
//
//                ).createTemporaryTable("sinkTable");
        table.executeInsert("sinkTable");
    }
}
