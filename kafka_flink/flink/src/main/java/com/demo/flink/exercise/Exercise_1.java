package com.demo.flink.exercise;


import com.alibaba.fastjson.JSON;
import com.demo.flink.demos.EventLog;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 创建两个流
 * 流1 ：
 * “id,eventId,cnt”
 * 1,event01,3
 * 1,event02,2
 * 2,event02,4
 * 流2 ：
 * “id,gender,city”
 * 1, male, shanghai
 * 2, female, beijing
 * <p>
 * 需求：
 * 1 , 将流1的数据展开
 * 比如，一条数据： 1,event01,3
 * 需要展开成3条:
 * 1,event01,随机数1
 * 1,event01,随机数2
 * 1,event01,随机数3
 * <p>
 * 2 , 流1的数据，还需要关联上 流2 的数据  （性别，城市）
 * 并且把关联失败的流1的数据，写入一个侧流；否则输出到主流
 * 4 , 对主流数据按性别分组， 取 最大随机数所在的那一条数据 作为结果输出
 * 5 , 把侧流处理结果，写入 文件系统，并写成 parquet格式
 * 6 , 把主流处理结果，写入  mysql， 并实现幂等更新
 **/
public class Exercise_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建流1
        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 9991);

        SingleOutputStreamOperator<EventCount> s1 = ds1.map(s -> {
            String[] arr = s.split(",");
            return new EventCount(Integer.parseInt(arr[0]), arr[1], Integer.parseInt(arr[2]));
        });

        // 创建流2
        DataStreamSource<String> ds2 = env.socketTextStream("localhost", 9992);

        SingleOutputStreamOperator<UserInfo> s2 = ds2.map(s -> {
            String[] arr = s.split(",");
            return new UserInfo(Integer.parseInt(arr[0]), arr[1], arr[2]);
        });

        // 1 , 将流1的数据展开
        SingleOutputStreamOperator<EventCount> flatMap = s1.flatMap(new FlatMapFunction<EventCount, EventCount>() {
            @Override
            public void flatMap(EventCount value, Collector<EventCount> out) throws Exception {
                // 取出count值
                int cnt = value.getCnt();
                // 循环cnt次，输出结果
                for (int i = 0; i < cnt; i++) {
                    out.collect(new EventCount(value.getId(), value.getEventId(), RandomUtils.nextInt(10, 100)));
                }
            }
        });

        // 2 , 流1的数据，还需要关联上 流2 的数据  （性别，城市）
        // 准备一个广播状态描述器
        MapStateDescriptor<Integer, UserInfo> descriptor = new MapStateDescriptor<>("state", Integer.class, UserInfo.class);
        // 关联打宽需求（通过场景分析，用广播状态最合适），并将关联失败的数据写入侧流c
        // 广播流2
        BroadcastStream<UserInfo> broadcast = s2.broadcast(descriptor);
        // 准备一个测流输出标签
        OutputTag<EventCount> tag = new OutputTag<>("side", TypeInformation.of(EventCount.class));
        // 对连接流进行process处理，来实现数据的打宽  因为使用的是维表，所以使用广播
        SingleOutputStreamOperator<EventUserInfo> joinedResult = flatMap.connect(broadcast)
                .process(new BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>() {
                    @Override
                    // 并且把关联失败的流1的数据，写入一个侧流；否则输出到主流
                    public void processElement(EventCount eventCount, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.ReadOnlyContext readOnlyContext, Collector<EventUserInfo> collector) throws Exception {
                        ReadOnlyBroadcastState<Integer, UserInfo> broadcastState = readOnlyContext.getBroadcastState(descriptor);
                        UserInfo userInfo;
                        if (broadcastState != null && (userInfo = broadcastState.get(eventCount.getId())) != null) {
                            // 关联成功的，输出到主流
                            collector.collect(new EventUserInfo(eventCount.getId(), eventCount.getEventId(), eventCount.getCnt(), userInfo.getGender(), userInfo.getCity()));
                        } else {
                            // 关联失败的，输出到侧流
                            readOnlyContext.output(tag, eventCount);
                        }
                    }

                    @Override
                    public void processBroadcastElement(UserInfo userInfo, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.Context context, Collector<EventUserInfo> collector) throws Exception {
                        BroadcastState<Integer, UserInfo> broadcastState = context.getBroadcastState(descriptor);
                        broadcastState.put(userInfo.getId(), userInfo);
                    }
                });

        // 4 , 对主流数据按性别分组， 取 最大随机数所在的那一条数据 作为结果输出
        SingleOutputStreamOperator<EventUserInfo> mainResult = joinedResult.keyBy(EventUserInfo::getGender).maxBy("cnt");

        // 5 , 把侧流处理结果，写入 文件系统，并写成 parquet格式 需要开启 checkpoint 机制 才会定时刷
        DataStream<EventCount> sideOutput = joinedResult.getSideOutput(tag);

        ParquetWriterFactory<EventCount> factory = ParquetAvroWriters.forReflectRecord(EventCount.class);
        FileSink<EventCount> sink = FileSink.forBulkFormat(new Path("d:/sidesink/"), factory)
                .withBucketAssigner(new DateTimeBucketAssigner<EventCount>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doit_edu").withPartSuffix(".parquet").build())
                .build();
        sideOutput.sinkTo(sink);

        // 6 , 把主流处理结果，写入  mysql， 并实现幂等更新
        SinkFunction<EventUserInfo> jdbcSink = JdbcSink.sink(
                "insert into t_eventuser values(?,?,?,?,?) on duplicate key update eventId=? , cnt =? ,gender =? ,city = ?",
                new JdbcStatementBuilder<EventUserInfo>() {
                    @Override
                    public void accept(PreparedStatement stmt, EventUserInfo eventUserInfo) throws SQLException {
                        stmt.setInt(1, eventUserInfo.getId());
                        stmt.setString(2, eventUserInfo.getEventId());
                        stmt.setInt(3, eventUserInfo.getCnt());
                        stmt.setString(4, eventUserInfo.getGender());
                        stmt.setString(5, eventUserInfo.getCity());

                        stmt.setString(6, eventUserInfo.getEventId());
                        stmt.setInt(7, eventUserInfo.getCnt());
                        stmt.setString(8, eventUserInfo.getGender());
                        stmt.setString(9, eventUserInfo.getCity());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/abc?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );
        mainResult.addSink(jdbcSink);


        env.execute();
    }
}
