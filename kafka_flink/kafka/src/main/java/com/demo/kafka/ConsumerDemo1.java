package com.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

/**
 * 手动指定消费起始偏移量位置
 */
public class ConsumerDemo1 {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        // 通过类加载器找到类路径下的配置文件
        props.load(ConsumerDemo1.class.getClassLoader().getResourceAsStream("consumer.properties"));
        // 只要组id相同就是属于同一个消费组
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group2");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 创建消费者对象 一个consumer就是一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
/*        // 订阅主题，是需要参与消费者组的再平衡机制才能获取到自己要消费的分区，当消费者组中的消费者发生变化，或者订阅的主题发生变化，或者订阅的分区数改变，都会触发再平衡机制
        consumer.subscribe(Collections.singletonList("demo"));
        // 这里因为是异步的操作，和kafka通信之后没有立即返回
        // 手动指定消费起始偏移量位置 代码执行到这里的时候再均衡还没完，所以这里指定分区是无效的，还没有为消费者分配分区
        consumer.seek(new TopicPartition("demo",0), 2);
 */
        // 既然要自己指定一个确定的起始消费位置，那么就不需要再进行消费者组的再均衡了，可以直接指定分区
        // 这里使用assign方法指定分区，然后seek方法指定偏移量
        consumer.assign(Collections.singletonList(new TopicPartition("demo",0)));
        consumer.seek(new TopicPartition("demo",0), 2);
        while (true) {
            // 拉取数据的时候，如果服务端没有数据响应，会保持连接等待响应，这个超时时长就是等待的最大时长
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
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

    }
}
