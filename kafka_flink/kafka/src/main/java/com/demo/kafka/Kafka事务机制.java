package com.demo.kafka;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * 从kafka的topic—a中读数据，处理，处理结果写回kafka的topic-b
 * 利用Kafka自身的事务机制来实现端到端的eos语句
 * 核心点： 让消费者的偏移量的更新和生产端的数据落地，绑定在一个事务中
 *
 * kafka内部端到端一致有两种：
 * 1. 生产数据到Kafka然后从Kafka读数据处理。需要保证生产者开启事务，消费者设置读隔离级别为 read-committed，这样能保证只读到已提交事务的消息。至于下游消费Kafka中的数据，关键在于偏移量的管理，在flink中保存为状态先写入检查点中。
 * 2. 从Kafka读数据然后写回Kafka。这种在Kafka内部来说是通过将consumer的消费偏移量绑定到事务上提交，与写入的生产者到新的topic中绑定在一个事务中，就可以实现
 */
public class Kafka事务机制 {
    public static void main(String[] args) {

        Properties props_p = new Properties();
        props_p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        props_p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 生产者序列化
        props_p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props_p.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tran_id_001");
        // acks
        props_p.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        // 生产者的重试次数
        props_p.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        // 飞行中的请求缓存最大数量
        props_p.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "3");
        // 开启幂等性
        props_p.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Properties props_c = new Properties();
        props_c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // 消费者反序列化
        props_c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props_c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        props_c.put(ConsumerConfig.GROUP_ID_CONFIG, "groupid01");
        props_c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 关闭消费者自动提交偏移量,如果需要自己提交偏移量，则设置为false，否则默认为true
        // props_c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 这个配合手动提交偏移量一起使用commitSync()和commitAsync()
        // 消费者事务隔离级别，默认为read_uncommitted。当下游消费者需要筛选已经提交事务的数据时，需要设置为read_committed
        props_c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // 构造生产者和消费者
        KafkaProducer<String, String> p = new KafkaProducer<String, String>(props_p);
        KafkaConsumer<String, String> c = new KafkaConsumer<String, String>(props_c);

        // 初始化事务
        p.initTransactions();
        // 创建一个自己记录最大消费位移的hashmap
        HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();

        c.subscribe(Arrays.asList("topic-a"));
        boolean flag = false;
        while (flag) {
            /*
             * 下面可以控制事务的粒度，可以在每条数据上开启事务，也可以在每个分区上开启事务，这样会减少事务的开销。
             */
            ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(1000));
            // 如果要对本次拉取的所有数据的处理绑定在一个事件中，则在此处开启事务
            p.beginTransaction();
            // 从获取到的数据中，获取本批数据包含哪些分区
            try {
                Set<TopicPartition> topicPartitionSet = records.partitions();
                // 遍历每一个分区
                for (TopicPartition topicPartition : topicPartitionSet) {
                    // 获取每个分区中的所有数据
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        // 处理数据
                        String value = record.value();
                        String result = value.toUpperCase();
                        // 将处理结果写回kafka的topic-b
                        p.send(new ProducerRecord<String, String>("topic-b", result));
                    }
                    // 自己手动记住每个分区的最大偏移量
                    // 获取分区中的最后一个数据得到分区中的最大偏移量
                    long offset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    // 将处理完的本分区对应的消费位移记录到 hashmap 中
                    map.put(topicPartition, new OffsetAndMetadata(offset + 1));
                }
                // 将消费偏移量的更新和发送数据落地绑定在一个事务中，需要开启自动提交偏移量
                p.sendOffsetsToTransaction(map ,"groupid01");
                // 也可以手动提交偏移量 需要关闭自动提交  他会自动计算出 每个分区中的最后一个偏移量+1
                // c.commitSync();
                // c.commitSync(map); // 或者自己维护每个分区的偏移量
                // 提交事务
                p.commitTransaction();
            } catch (Exception e) {
                // 如果出现异常，则放弃事务
                // 下游可以通过设置 ConsumerConfig.ISOLATION_LEVEL_CONFIG=read_committed 来避免本次的脏数据
                p.abortTransaction();
            }
            // 手动提交偏移量
        }
        c.close();
        p.close();
    }
}
