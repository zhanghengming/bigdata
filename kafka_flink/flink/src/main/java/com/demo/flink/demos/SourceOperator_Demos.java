package com.demo.flink.demos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class SourceOperator_Demos {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);  // 默认并行度

        /**
         * 引入扩展包 ：  flink-connector-kafka
         * 从kafka中读取数据得到数据流
         * 创建source
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // 设置订阅的目标主题
                .setTopics("demo")
                // 设置消费者组id
                .setGroupId("test")
                // 设置kafka服务器地址
                .setBootstrapServers("localhost:9092")
                // 起始消费位移的指定： 消费起始位移选择之前所提交的偏移量（如果没有，则重置为LATEST）
                //    OffsetsInitializer.earliest()  消费起始位移直接选择为 “最早”
                //    OffsetsInitializer.latest()  消费起始位移直接选择为 “最新”
                //    OffsetsInitializer.offsets(Map<TopicPartition,Long>)  消费起始位移选择为：方法所传入的每个分区和对应的起始偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // 设置value数据的反序列化器，如果没有key的话
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 开启kafka底层消费者的自动位移提交机制
                //    它会把最新的消费位移提交到kafka的consumer_offsets中
                //    就算把自动位移提交机制开启，KafkaSource依然不依赖自动位移提交机制
                //    （宕机重启时，优先从flink自己的状态中去获取偏移量<更可靠>）
                .setProperty("auto.offset.commit", "true") //是为了让外面的监控系统监控Kafka的消费进度
                // 把本source算子设置成  BOUNDED属性（有界流）
                //     将来本source去读取数据的时候，读到指定的位置，就停止读取并退出
                //     常用于补数或者重跑某一段历史数据
                // .setBounded(OffsetsInitializer.committedOffsets())

                // 把本source算子设置成  UNBOUNDED属性（无界流）
                //     但是并不会一直读数据，而是达到指定位置就停止读取，但程序不退出
                //     主要应用场景：需要从kafka中读取某一段固定长度的数据，然后拿着这段数据去跟另外一个真正的无界流联合处理
                //.setUnbounded(OffsetsInitializer.latest())

                .build();

        // env.addSource();  //  接收的是  SourceFunction接口的 实现类
        // 添加source
        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");

        dataStreamSource.print();

        env.execute();
    }
}
