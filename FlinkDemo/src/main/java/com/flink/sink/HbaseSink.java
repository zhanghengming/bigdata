package com.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

public class HbaseSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.fromElements("hello", "world");

        source.addSink(new RichSinkFunction<String>() {
            public org.apache.hadoop.conf.Configuration configuration;
            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 获取配置文件对象
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum","linux121,linux122");
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
                // 通过conf获取到hbase集群的连接
                connection = ConnectionFactory.createConnection(configuration);
            }


            @Override
            public void close() throws Exception {
                super.close();
                connection.close();
            }


            @Override
            public void invoke(String value, Context context) throws Exception {
                // 获取表对象 表名为 test
                Table table = connection.getTable(TableName.valueOf("test"));
                // 设定rowkey 准备put对象
                Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8));
                // 列族，列，value
                put.addColumn("info".getBytes(StandardCharsets.UTF_8),value.getBytes(StandardCharsets.UTF_8),"1".getBytes(StandardCharsets.UTF_8));
                // 执行 put 操作
                table.put(put);
                // 关闭table对象
                table.close();
            }
        });
        env.execute();
    }
}
