package ods;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从kafka获取数据的方法
 */
public class SourceKafka {

    public FlinkKafkaConsumer<String> getKafkaSource(String topicName) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "linux121:9092,linux122:9092,linux123:9092");
        props.setProperty("group.id", "consumer-group");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset","latest");

        return new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), props);
    }
}
