package com.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Consumer;

public class MyConsumer2 {
    public static void main(String[] args) {

        HashMap<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"linux121:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"consumer_demo2");
        // 如果找不到当前消费者的有效偏移量，则自动重置到最开始
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(configs);
        // 先订阅，再消费
        consumer.subscribe(Arrays.asList("topic_1"));

        while (true) {
            // 如果主题中没有可以消费的消息，则该方法可以放到while循环中，每过3秒重新拉取一次
            // 如果还没有拉取到，过3秒再次拉取，防止while循环太密集的poll调用。
            // 批量从主题的分区拉取消息
            ConsumerRecords<Integer, String> consumerRecords = consumer.poll(3000);
            // 遍历本次从主题的分区拉取的批量消息
            consumerRecords.forEach(new Consumer<ConsumerRecord<Integer, String>>() {
                @Override
                public void accept(ConsumerRecord<Integer, String> record) {
                    System.out.println(record.topic() + "\t"
                            + record.partition() + "\t"
                            + record.offset() + "\t"
                            + record.key() + "\t"
                            + record.value());
                }
            });
        }
//        consumer.close();
    }
}
