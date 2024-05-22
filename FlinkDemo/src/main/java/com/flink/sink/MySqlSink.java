package com.flink.sink;

import com.flink.entity.Event;
import com.flink.streamdatasource.CustomClickSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySqlSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new CustomClickSource());

        String url = "jdbc:mysql://localhost:3306/test";
        String driverName = "com.mysql.jdbc.Driver";
        String user = "root";
        String password = "123456";

//        source.addSink(new RichSinkFunction<Event>() {
//            /**
//             * @param parameters
//             * @throws Exception
//             * 实例化,mysql的相关信息
//             */
//            Connection connection = null;
//            PreparedStatement preparedStatement = null;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                connection = DriverManager.getConnection(url, user, password);
//                String sql = "insert into clicks (user,url) values (?,?)";
//                preparedStatement = connection.prepareStatement(sql);
//            }
//
//
//            /**
//             * @param value   The input record.
//             * @param context Additional context about the input record.
//             * @throws Exception
//             * 每来一条记录执行一次，将数据写入sink
//             */
//            @Override
//            public void invoke(Event value, Context context) throws Exception {
//                preparedStatement.setString(1, value.user);
//                preparedStatement.setString(2, value.url);
//                preparedStatement.executeUpdate();
//            }
//
//            /**
//             * @throws Exception
//             */
//            @Override
//            public void close() throws Exception {
//                if (connection != null) {
//                    connection.close();
//                }
//                if (preparedStatement != null) {
//                    preparedStatement.close();
//                }
//            }
//        });

        source.addSink(JdbcSink.sink("insert into clicks (user,url) values (?,?)", (preparedStatement, event) -> {
                    preparedStatement.setString(1, event.user);
                    preparedStatement.setString(2, event.url);
                }, JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(200)
                        .withBatchSize(1000)
                        .withMaxRetries(5)
                        .build(),
                // protected修饰的构造函数，只能再一个包下使用
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driverName)
                        .withUsername(user)
                        .withPassword(password)
                        .build()));

        env.execute();
    }
}
