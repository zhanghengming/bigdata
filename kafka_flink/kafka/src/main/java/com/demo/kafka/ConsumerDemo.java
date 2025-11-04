package com.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        // 1. 创建消费者配置对象参数
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092,linux122:9092,linux123:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // kafka消费者默认是从所属组的最新offset开始消费，如果找不到之前记录的偏移量，则从以下参数配置策略来确定消费起始偏移量
        // earliest: 自动重置到每个分区的最前一条消息/ latest: 自动重置到每个分区最末一条消息/ none: 若偏移量无效，抛出异常
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        // 设置自动提交最新的消费位移（默认就是开启的）
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        // 自动提交最新消费位移的时间间隔（默认5000ms）
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"5000");
        // 2. 构造消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        // 3. 订阅主题(可以是多个)
        kafkaConsumer.subscribe(Collections.singletonList("demo"));
        // 4. 消费消息
        boolean flag = true;
        while (flag) {
            // 拉取数据的时候，如果服务端没有数据响应，会保持连接等待响应，这个超时时长就是等待的最大时长
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            // iterable可迭代的，iterator迭代器，iterable是迭代器的再封装
            // 实现了iterable的接口，可以用增强for循环去遍历迭代，也可以从对象上取到iterator，然后用while循环迭代
//            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            // ConsumerRecord中不光有业务数据，也有kafka的元数据
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                Optional<Integer> integer = record.leaderEpoch();
                // timestamp：记录本条数据的时间戳
                // 时间戳有两种类型：本条数据的创建时间（生产者）；本条数据的追加时间（broker写入log文件的时间）
                TimestampType timestampType = record.timestampType();
                long timestamp = record.timestamp();
                Headers headers = record.headers();// 记录头信息,是在生产者发送消息的时候设置的，可以设置多个，每个header由一个key和value组成（相当于用户自定义的元数据，可以用来过滤）
                System.out.println("key:" + key + ",value:" + value + ",topic:" + topic + ",partition:" + partition + ",offset:" + offset + ",integer:" + integer + ",timestampType:" + timestampType + ",timestamp:" + timestamp + ",headers:" + headers);
            }
        }
        // 5. 关闭消费者
        kafkaConsumer.close();


    }
}
