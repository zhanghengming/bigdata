package com.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.function.BiConsumer;

public class MyOffsetManager {
    public static void main(String[] args) {
        Map<String,Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // group.id很重要
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "mygrp1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        // 订阅分区 将消费者放入消费者组
        consumer.subscribe(Collections.singleton("tp_demo_01"));

//        如何手动给消费者消费分区
//        获取当前消费者可以访问和消费的主题以及他们的分区信息
        Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
        // 一个消费者可以对应多个主题，每个主题下面又有多个分区，主题作为key，分区集合作为value
        stringListMap.forEach(new BiConsumer<String, List<PartitionInfo>>() {
            @Override
            public void accept(String topicName, List<PartitionInfo> partitionInfos) {
                System.out.println("主题名称：" + topicName);
                for (PartitionInfo partitionInfo : partitionInfos) {
                    System.out.println(partitionInfo);
                }
            }
        });
//        _consumer_offsets 默认有50个分区 存的是消费组偏移量 key是主题、消费组、分区 value是偏移量


        // 手动给当前消费者分配指定主题分区
        consumer.assign(Arrays.asList(
                new TopicPartition("tp_demo_01",0),
                new TopicPartition("tp_demo_01",1),
                new TopicPartition("tp_demo_01",2)
        ));

        //      获取给当前消费者分配的分区集合
        Set<TopicPartition> assignment = consumer.assignment();
        for (TopicPartition partition : assignment) {
            System.out.println(partition);
        }
        // 查看当前消费者在指定主题分区上的消费者偏移量
        long offset0 = consumer.position(new TopicPartition("tp_demo_01", 0));
        System.out.println("当前主题在0号分区上的位移：" + offset0);

        // 将给定主题分区的消费者偏移量移动到他们的偏移起始量，调用过poll方法或position后才会执行
        consumer.seekToBeginning(Arrays.asList(
                new TopicPartition("tp_demo_01", 0),
                new TopicPartition("tp_demo_01", 2)
        ));

        consumer.seekToEnd(Arrays.asList( new TopicPartition("tp_demo_01", 2)));
//      将给定主题分区的消费偏移量移动到指定的偏移量，即当前消费者下⼀条要消费的消息偏移量。
        consumer.seek(new TopicPartition("tp_demo_01", 2),14);

        consumer.close();
    }
}
