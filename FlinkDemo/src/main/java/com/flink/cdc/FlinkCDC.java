package com.flink.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点续传,需要从 Checkpoint 或者 Savepoint 启动程序
        //开启 Checkpoint,每隔 5 秒钟做一次 CK
//        env.enableCheckpointing(5000);
//        //指定 CK 的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //设置状态后端，ck保存的位置，默认是内存，指定保存到hdfs的位置
//        env.setStateBackend(new FsStateBackend("hdfs://linux121:9000/flinkCDC/ck"));
//        // 开启检查点的外部持久化保存，作业取消后依然保留(默认不保留)
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        // 创建 Flink-MySQL-CDC 的 Source
        /**
         * initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
         * 默认初始化操作为： 首次启动的时候执行一个初始快照，然后将先有的数据读出来，然后读取最新的binlog（
         * 这里的首次启动指的是在有checkpoint/savepoint时，并且从ck中恢复时，会从最新的（也就是从断开的地方读）binlog读取，有断点续传的功能）
         * earliest：从最开的binlog读取，注意：需要在库被创建之前就要开启binlog
         * latest: Never to perform snapshot on the monitored database tables upon first
         * startup, just read from the end of the binlog which means only have the changes since the
         * connector was started.
         * 从最后的binlog开始读，也就是从连接开始后，有改变的才会读取
         * timestamp: Never to perform snapshot on the monitored database tables upon first
         * startup, and directly read binlog from the specified timestamp. The consumer will traverse the
         * binlog from the beginning and ignore change events whose timestamp is smaller than the
         * specified timestamp.
         * 从指定的时间戳开始读取，针对的是binlog，忽略时间戳之前的数据
         * specificOffset: Never to perform snapshot on the monitored database tables upon
         * first startup, and directly read binlog from the specified offset
         * 从指定的偏移量中读取binlog
         */
        DebeziumSourceFunction<String> build = MySqlSource.<String>builder()
                .hostname("192.168.10.102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("cdc_test")
//                .tableList("gmall-flink.z_user_info") //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                .startupOptions(StartupOptions.initial())
//                .startupOptions(StartupOptions.latest())
                .deserializer(new CustomerDeserializationSchema())
                .build();

        DataStreamSource<String> streamSource = env.addSource(build);

        streamSource.print();
        env.execute();

    }
}
