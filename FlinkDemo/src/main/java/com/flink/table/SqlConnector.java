package com.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class SqlConnector {
    public static void main(String[] args) {
        // 对 Hive 表的读写操作只有 Blink 的
        //planner 才支持。所以一般我们需要将表环境的 planner 设置为 Blink。
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "myhive";
        String defaultDatabase = "mydatabase";
        String hiveConfDir = "/opt/hive-conf";
        // 创建一个 HiveCatalog，并在表环境中注册
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        // 使用 HiveCatalog 作为当前会话的 catalog
        tableEnv.useCatalog("myhive");

        // 配置 hive 方言 先切换到 hive 方言，然后才能使用 Hive SQL 的语法
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // 配置 default 方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String hive_table =
                "CREATE TABLE hive_table (\n" +
                        " user_id STRING,\n" +
                        " order_amount DOUBLE\n" +
                        ") PARTITIONED BY (dt STRING,hr STRING) STORED AS parquet TBLPROPERTIES (" +
                        " 'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
                        " 'sink.partition-commit.trigger'='partition-time',\n" +
                        " 'sink.partition-commit.delay'='1 h',\n" +
                        " 'sink.partition-commit.policy.kind'='metastore,success-file'" +
                        ");";
        String kafka_table =
                "CREATE TABLE kafka_table (\n" +
                        " user_id STRING,\n" +
                        " order_amount DOUBLE,\n" +
                        " log_ts TIMESTAMP(3)," +
                        " WATERMARK FOR log_ts - INTERVAL '5' SECOND" +
                        ") WITH ();";

        // -- 将 Kafka 中读取的数据经转换后写入 Hive
        String insert_hive =
                "INSERT INTO TABLE hive_table \n" +
                "SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), \n" +
                "DATE_FORMAT(log_ts, 'HH')\n" +
                "FROM kafka_table;";

        /**
         * 连接到kafka
         * ts，它的声明中用到了
         * METADATA FROM，这是表示一个“元数据列”（metadata column），它是由 Kafka 连接器的
         * 元数据“timestamp”生成的。这里的 timestamp 其实就是 Kafka 中数据自带的时间戳，我们把
         * 它直接作为元数据提取出来，转换成一个新的字段 ts
         */
        String kafkaSource =
                "create table KafkaTable (" +
                        "user string," +
                        "url string," +
                        "ts timestamp(3) metadata from 'timestamp'" +
                        ") with (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'events'," +
                        "'properties.bootstrap.servers' = 'linux121:9092'," +
                        "'properties.group.id' = 'testGroup'," +
                        "'scan.startup.mode' = 'earliest-offset'," +
                        "'format' = 'csv'" +
                        ")";

        /**
         * Upsert Kafka
         * 这个连接器支持以更新插入（UPSERT）的方式向 Kafka 的 topic 中读写数据。
         * 根据key进行更新
         */
        String upsert_kafka_sink =
                "CREATE TABLE pageviews_per_region (" +
                        " user_region STRING," +
                        " pv BIGINT," +
                        " uv BIGINT," +
                        " PRIMARY KEY (user_region) NOT ENFORCED" +
                        ") WITH (" +
                        " 'connector' = 'upsert-kafka',\n" +
                        " 'topic' = 'pageviews_per_region',\n" +
                        " 'properties.bootstrap.servers' = '...',\n" +
                        " 'key.format' = 'avro',\n" +
                        " 'value.format' = 'avro'" +
                        ")";
        String source_kafka =
                "CREATE TABLE pageviews (\n" +
                        " user_id BIGINT,\n" +
                        " page_id BIGINT,\n" +
                        " viewtime TIMESTAMP,\n" +
                        " user_region STRING," +
                        " WATERMARK FOR viewtime - INTERVAL '2' SECOND" +
                        ") WITH (" +
                        " 'connector' = 'kafka',\n" +
                        " 'topic' = 'pageviews'," +
                        " 'properties.bootstrap.servers' = '...',\n" +
                        " 'format' = 'json'" +
                        " )";
        String query_sql =
                "INSERT INTO pageviews_per_region\n" +
                "SELECT\n" +
                " user_region,\n" +
                " COUNT(*),\n" +
                " COUNT(DISTINCT user_id)\n" +
                "FROM pageviews\n" +
                "GROUP BY user_region;";

        /**
         * 文件系统
         * 这里在 WITH 前使用了 PARTITIONED BY 对数据进行了分区操作。文件系统连接器支持
         * 对分区文件的访问。
         */
        String fileSystem =
                "CREATE TABLE MyTable (\n" +
                        " column_name1 INT,\n" +
                        " column_name2 STRING,\n" +
                        " ...\n" +
                        " part_name1 INT,\n" +
                        " part_name2 STRING\n" +
                        ") PARTITIONED BY (part_name1, part_name2) WITH (\n" +
                        " 'connector' = 'filesystem', -- 连接器类型\n" +
                        " 'path' = '...', -- 文件路径\n" +
                        " 'format' = '...' -- 文件格式\n" +
                        ")";
        /**
         * JDBC
         * 如果有主键，那么 JDBC 连接器就将以更新插入（Upsert）模式运行，可以向
         * 外部数据库发送按照指定键（key）的更新（UPDATE）和删除（DELETE）操作；如果没有
         * 定义主键，那么就将在追加（Append）模式下运行，不支持更新和删除操作。
         * 写入 MySQL 中真正的表名称是users，而 MyTable 是注册在 Flink 表环境中的表。
         */
        String mysql =
                "CREATE TABLE MyTable (\n" +
                        " id BIGINT,\n" +
                        " name STRING,\n" +
                        " age INT,\n" +
                        " status BOOLEAN,\n" +
                        " PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'url' = 'jdbc:mysql://localhost:3306/mydatabase',\n" +
                        " 'table-name' = 'users'\n" +
                        ");";
        String insert_sql = "INSERT INTO MyTable\n" +
                "SELECT id, name, age, status FROM T;";

        /**
         * Hbase
         * 连接器作为 TableSink 向 HBase 写入数据时，采用的始终是更新插入
         * （Upsert）模式。也就是说，HBase 要求连接器必须通过定义的主键（primary key）来发送更
         * 新日志（changelog）。所以在创建表的 DDL 中，我们必须要定义行键（rowkey）字段，并将
         * 它声明为主键；如果没有用 PRIMARY KEY 子句声明主键，连接器会默认把 rowkey 作为主键。
         */
        String hbase_sink =
                "CREATE TABLE MyTable (\n" +
                        "rowkey INT,\n" +
                        "family1 ROW<q1 INT>,\n" +
                        "family2 ROW<q2 STRING, q3 BIGINT>,\n" +
                        "family3 ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>,\n" +
                        "PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "'connector' = 'hbase-1.4',\n" +
                        "'table-name' = 'mytable',\n" +
                        "'zookeeper.quorum' = 'localhost:2181'\n" +
                        ");";
        // -- 假设表 T 的字段结构是 [rowkey, f1q1, f2q2, f2q3, f3q4, f3q5, f3q6]
        // 将另一张 T 中的数据提取出来，并用 ROW()函数来构造出对应的 column family，最
        //终写入 HBase 中名为 mytable 的表。
        String insert_hbase_sql =
                        "INSERT INTO MyTable\n" +
                        "SELECT rowkey, ROW(f1q1), ROW(f2q2, f2q3), ROW(f3q4, f3q5, f3q6) FROM T;";

    }
}
