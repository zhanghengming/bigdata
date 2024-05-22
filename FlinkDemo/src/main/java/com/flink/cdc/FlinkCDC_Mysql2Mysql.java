package com.flink.cdc;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;



public class FlinkCDC_Mysql2Mysql {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        //开启 Checkpoint,每隔 5 秒钟做一次 CK
        env.enableCheckpointing(5000);
        //指定 CK 的一致性语义
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置状态后端，ck保存的位置，默认是内存，指定保存到hdfs的位置
        // 开启检查点的外部持久化保存，作业取消后依然保留(默认不保留)
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //创建 Flink-MySQL-CDC 的 Source1
        tableEnv.executeSql("CREATE TABLE Source(" +
                " id INT PRIMARY key," +
                " name STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'linux123'," +
//                " 'scan.startup.mode' = 'latest-offset'," +
                " 'scan.startup.mode' = 'initial'," + //需要先做一个快照来确保全量的数据已经写入到下游后再读取增量数据
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name' = 'cdc_test'," +
                " 'table-name' = 'user_source.*'" + //正则匹配多张分表。
                ")");

        //创建MySQL的 sink
        tableEnv.executeSql("CREATE TABLE Sink(" +
                "  id int," +
                "  name STRING," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:mysql://linux123:3306/cdc_test?useSSL=false'," +
                "   'table-name' = 'user_sink'," +
                "   'username' = 'root'," +
                "   'password' = '123456'" +
                ")");


        tableEnv.executeSql("insert into Sink select id,name from Source");

    }
}

