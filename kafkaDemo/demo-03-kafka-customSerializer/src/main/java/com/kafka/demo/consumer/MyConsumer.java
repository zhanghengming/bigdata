package com.kafka.demo.consumer;

import com.kafka.demo.entity.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class MyConsumer {
    public static void main(String[] args) {
        Map<String,Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"linux121:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, User.class);

        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"consumer1");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG,"con1");

        // 创建一个consumer对象
        KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer<String, User>(configs);
        // 订阅主题
        kafkaConsumer.subscribe(Collections.singleton("tp_user_01"));
        // 拉取消息（等待时间）
        try {

            while (true) {
                ConsumerRecords<String, User> poll = kafkaConsumer.poll(Long.MAX_VALUE);
                // 使⽤异步提交规避阻塞
                kafkaConsumer.commitAsync();
                // 遍历拉取到的消息
                poll.forEach(new Consumer<ConsumerRecord<String, User>>() {
                    @Override
                    public void accept(ConsumerRecord<String, User> stringUserConsumerRecord) {
                        System.out.println(stringUserConsumerRecord.value());
                    }
                });
            }
        } catch (Exception e){

        } finally {
            // 最后⼀次提交使⽤同步阻塞式提交 commitAsync出现问题不会⾃动重试 所以在最后一步加上
           kafkaConsumer.commitSync();
            kafkaConsumer.close();

        }

    }
}
