package com.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class FromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
//                .withBuiltInCatalogName("default_catalog")
//                .withBuiltInDatabaseName("default_database")
                .build();
        // 创建table环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.executeSql("CREATE TABLE KafkaTable (" +
                "`user` STRING" +
                ") with (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'topic_test_01'," +
                " 'properties.bootstrap.servers' = 'linux121:9092'," +
                " 'properties.group.id' = 'testGroup'," +
                " 'scan.startup.mode' = 'earliest-offset'," +
                " 'format' = 'csv'" +
                ")");

        tEnv.executeSql("CREATE TABLE KafkaSink (" +
                "`user` STRING" +
                ") with (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'flinkSql_topic'," +
                " 'properties.bootstrap.servers' = 'linux121:9092'," +
                " 'format' = 'csv'" +
                ")");
        Table table = tEnv.sqlQuery("select * from KafkaTable");

        // 从Kafka的一个分区写入到另一个分区
        tEnv.executeSql("insert into KafkaSink select * from KafkaTable");

        tEnv.toAppendStream(table, Row.class).print();
        env.execute();
    }
}
