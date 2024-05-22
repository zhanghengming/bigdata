package com.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"linux121:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"mygroup");
        // 如果在kafka中找不到当前消费者偏移量，则设置为最旧的
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // 配置拦截器
        // One -> Two -> Three，接收消息和发送偏移量确认都是这个顺序
        properties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "com.kafka.demo.interceptor.OneInterceptor" +
                ",com.kafka.demo.interceptor.TwoInterceptor" +
                ",com.kafka.demo.interceptor.ThreeInterceptor"
        );
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅主题
        consumer.subscribe(Collections.singleton("tp_demo_01"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(3000);
            // 遍历消息  匿名内部类 用lamda表达式实现
            records.forEach(record ->{
                System.out.println(record.topic()
                + "\t" + record.partition()
                + "\t" + record.offset()
                + "\t" + record.key()
                + "\t" + record.value());
            });

            // 手动异步提交偏移量
            consumer.commitAsync();
        }
    }
}

