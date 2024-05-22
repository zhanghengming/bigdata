package com.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String customerTopic = "topic_1";
        String producerTopic = "topic_2";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","linux121:9092");

        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>(customerTopic, new SimpleStringSchema(), properties));

        source.addSink(new FlinkKafkaProducer<String>(producerTopic,new SimpleStringSchema(),properties));

        env.execute();
    }
}
