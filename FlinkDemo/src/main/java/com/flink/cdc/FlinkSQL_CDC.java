package com.flink.cdc;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("CREATE TABLE users (" +
                " id INT PRIMARY key," +
                " name STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'linux123'," +
                " 'scan.startup.mode' = 'latest-offset'," + //这个只展示最新变化的数据
//                " 'scan.startup.mode' = 'initial'," + // 这个不会输出实时改变
//                " 'scan.startup.mode' = 'earliest'," + // 这个不会输出实时改变
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name' = 'cdc_test'," +
                " 'table-name' = 'user'" +
                ")");
        tableEnv.executeSql("select * from users").print();
//        Table table = tableEnv.sqlQuery("select * from users");
//        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table, Row.class);
//        tuple2DataStream.print();
//        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from users"), Row.class).print();
//        env.execute();//代码中没有流算子时，最后不需要加env.execute()。当用tEnv调用executeSql（）执行了sql时，不需要再使用execute()方法
    }
}
