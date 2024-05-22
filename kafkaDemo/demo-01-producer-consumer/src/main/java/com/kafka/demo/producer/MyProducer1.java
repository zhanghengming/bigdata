package com.kafka.demo.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MyProducer1 {
    public static void main(String[] args) {

        HashMap<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"linux121:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs);

        // 设置用户自定义的消息头字段
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("biz.name","producer.demo".getBytes()));

        for (int i = 0; i < 100; i++) {
            ProducerRecord<Integer, String> record = new ProducerRecord<>(
                    "topic_1",
                    0,
                    i,
                    "hello" + 1);
            // 消息的异步确认
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("消息的主题：" + metadata.topic());
                        System.out.println("消息的分区号：" + metadata.partition());
                        System.out.println("消息的偏移量：" + metadata.offset());
                    } else {
                        System.out.println("异常消息：" + exception.getMessage());
                    }
                }
            });
        }

        producer.close();
    }
}
