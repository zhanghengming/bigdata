package com.flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class FromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);


        String sql = "create table csvTable(" +
                "       name varchar" +
                "       ) with (" +
                "       'connector' = 'filesystem'," +
                "       'format' = 'csv'," +
                "       'path' = 'D:\\新大数据\\data\\wc.txt'" +
                "       )";
        // 将数据形成表
        tEnv.executeSql(sql);

        TableResult tableResult = tEnv.executeSql("select * from csvTable");

        tableResult.print();

    }
}
