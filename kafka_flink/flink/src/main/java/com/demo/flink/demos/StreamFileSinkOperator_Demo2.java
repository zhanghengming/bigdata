package com.demo.flink.demos;

import com.demo.flink.avro.schema.AvroEventLogBean;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * 要把处理好的数据流，输出到文件系统（hdfs）
 *   使用的sink算子，是扩展包中的 StreamFileSink
 **/
public class StreamFileSinkOperator_Demo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // 将上面的数据流输出到文件系统（假装成一个经过了各种复杂计算后的结果数据流）


        /**
         * 方式二：
         * 核心逻辑：
         *   - 编写一个avsc文本文件（json），来描述数据模式
         *   - 添加 maven代码生成器插件，来针对上述的avsc生成avro特定格式的JavaBean类
         *   - 利用代码生成器生成的 JavaBean，来构造一个 parquetWriterFactory
         *   - 利用parquetWriterFactory构造一个FileSink算子
         *   - 将原始数据流 转成 特定格式JavaBean流，输出到 FileSink算子
         */

        // 1. 先定义avsc文件放在resources文件夹中，并用maven的插件，来编译一下，生成特定格式的JavaBean ： AvroEventLogBean
        // 这种根据avsc生成的JavaBean类，自身就已经带有了Schema对象
        // AvroEventLogBean avroEventLog = new AvroEventLogBean();
        // Schema schema = avroEventLog.getSchema();

        // 2. 通过自动生成 AvroEventLogBean类，来得到一个parquetWriter
        ParquetWriterFactory<AvroEventLogBean> parquetWriterFactory = ParquetAvroWriters.forSpecificRecord(AvroEventLogBean.class);

        // 3. 利用生成好的parquetWriter，来构造一个 支持列式输出parquet文件的 sink算子
        FileSink<AvroEventLogBean> bulkSink = FileSink.forBulkFormat(new Path("d:/datasink2/"), parquetWriterFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<AvroEventLogBean>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doit_edu").withPartSuffix(".parquet").build())
                .build();


        // 4. 将自定义javabean的 EventLog 流，转成 上述sink算子中parquetWriter所需要的  AvroEventLogBean 流
        SingleOutputStreamOperator<AvroEventLogBean> avroEventLogStream = streamSource.map(new MapFunction<EventLog, AvroEventLogBean>() {
            @Override
            public AvroEventLogBean map(EventLog eventLog) throws Exception {
                HashMap<CharSequence, CharSequence> eventInfo1 = new HashMap<>();

                // 进行hashmap<charsequenct,charsequence>类型的数据转移
                Map<String, String> eventInfo2 = eventLog.getEventInfo();
                Set<Map.Entry<String, String>> entries = eventInfo2.entrySet();
                for (Map.Entry<String, String> entry : entries) {
                    eventInfo1.put(entry.getKey(), entry.getValue());
                }

                return new AvroEventLogBean(eventLog.getGuid(), eventLog.getSessionId(), eventLog.getEventId(), eventLog.getTimeStamp(), eventInfo1);
            }
        });


        // 5. 输出数据
        avroEventLogStream.sinkTo(bulkSink);

        env.execute();

    }
}