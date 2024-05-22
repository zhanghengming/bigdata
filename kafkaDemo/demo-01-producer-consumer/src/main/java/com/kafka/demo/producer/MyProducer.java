package com.kafka.demo.producer;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MyProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {

//        new一个map来设置配置信息
        Map<String, Object> configs = new HashMap<>();
        // 指定初始化连接的broker地址
//        如果是集群，可通过此初始化连接发现集群的其它brokers，指定bootstrap.servers
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"linux121:9092");
        // 设置key的序列化器
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class );
        // 设置value的序列化器
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

//        configs.put("acks","all");
//        configs.put("reties","3");
        // 创建producer对象
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs);


        // 创建封装消息的对象
        ProducerRecord<Integer, String> record = new ProducerRecord<>(
                "test", // 分区
                0, // partition num
                0, // key
                "message 0" //value
        );

        // 消息的同步确认
//        Future<RecordMetadata> future = producer.send(record);
//        RecordMetadata metadata = future.get();
//        System.out.println("消息的主题：" + metadata.topic());
//        System.out.println("消息的分区号：" + metadata.partition());
//        System.out.println("消息的偏移量:" + metadata.offset());

        // 消息的异步确认
        producer.send(record, new Callback() {
            @Override
            // 回调函数
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e == null){
                    System.out.println("消息的主题：" + metadata.topic());
                    System.out.println("消息的分区号：" + metadata.partition());
                    System.out.println("消息的偏移量:" + metadata.offset());
                }else {
                    System.out.println("异常消息：" + e.getMessage());
                }
            }
        });

        // 关闭生产者
        producer.close();

    }
}
