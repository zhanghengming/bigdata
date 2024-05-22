package com.kafka.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/*
在 消费-转换-⽣产 模式，使⽤事务保证仅⼀次发送。
* */
public class MyTransactional {
    public static KafkaProducer<String, String> getProducer() {
        Map<String,Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 提供生产者client.id
        configs.put(ProducerConfig.CLIENT_ID_CONFIG,"tx_producer_01");
        // 设置事务id
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx_id_02");

        // 需要所有的ISR副本确认
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        // 启用幂等性
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        return producer;
    }

    public static KafkaConsumer<String,String> getConsumer(String consumerGroupId){
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        //设置消费组id
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,consumerGroupId);
        // 不启用消费者偏移量的自动确认，也不要手动确认
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        // 设置消费者ID
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG,"consumer_client_02");

        // 如果找不到偏移量，自动重置为最早的
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 只读取已提交的消息
        configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        return  consumer;
    }

    public static void main(String[] args) {
        // 消费者组
        String consumerGroupId = "consumer_grp_id_101";
        KafkaProducer<String, String> producer = getProducer();
        KafkaConsumer<String, String> consumer = getConsumer(consumerGroupId);
        // 事务的初始化
        producer.initTransactions();
        // 订阅主题
        consumer.subscribe(Collections.singleton("tp_tx_01"));

        // 拉取消息
        final ConsumerRecords<String, String> records = consumer.poll(1_000);

        // 开启事务
        producer.beginTransaction();

        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
                // 将消息发送出去
                producer.send(new ProducerRecord<>("tp_tx_out_01", record.key(), record.value()));
                offsets.put(
                       new TopicPartition(record.topic(),record.partition()),
                       new OffsetAndMetadata(record.offset() + 1) // 偏移量表示下一条要消费的消息
                );
            }
            // 将该消息的偏移量提交作为事务的一部分，随事务提交和
            // 回滚（不提交消费偏移量）
            producer.sendOffsetsToTransaction(offsets,consumerGroupId);
            // 发生异常时回滚，不提交偏移量
            int i = 1 / 0;
            // 提交事务
            producer.commitTransaction();
        } catch (Exception e){
            e.printStackTrace();
            // 回滚事务
            producer.abortTransaction();
        } finally {
            // 关闭资源
            producer.close();
            consumer.close();
        }
    }
}
