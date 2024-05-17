package com.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

// 每次都得推到重来，2.4.1版本之后就改了
public class ConsumerDemo2 {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        // 通过类加载器找到类路径下的配置文件
        props.load(ConsumerDemo1.class.getClassLoader().getResourceAsStream("consumer.properties"));
        // 只要组id相同就是属于同一个消费组
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group2");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 创建消费者对象 一个consumer就是一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费者订阅多个主题再均衡机制触发
        consumer.subscribe(Arrays.asList("tp1","tp2"), new ConsumerRebalanceListener() {
            // 再均衡过程中，消费者会被取消之前所分配的主题分区
            // 取消之后，consumer底层就会调用这个方法
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("被取消了以下主题分区" + partitions);
            }
            // 再均衡过程中，消费者会重新分配新的主题分区
            // 分配好了新的主题分区后，consumer底层调用下面的方法
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("被重新分配了以下主题分区" + partitions);
            }
        });
    }
}
