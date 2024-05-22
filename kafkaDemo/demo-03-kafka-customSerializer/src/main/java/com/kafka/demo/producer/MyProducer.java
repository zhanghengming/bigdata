package com.kafka.demo.producer;

import com.kafka.demo.entity.User;
import com.kafka.demo.serialization.UserSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class MyProducer {
    public static void main(String[] args) {
        Map<String,Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"linux121:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 设置自定义的序列化器
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class);
        // 设置自定义的分区器
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.kafka.demo.partitioner.MyPartitioner");
        // 如果有多个拦截器，则设置为多个拦截器类的全限定类名，中间用逗号隔开
        configs.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"com.lagou.kafka.demo.interceptor.InterceptorOne");

        // 保证等待确认的消息只有设置的这几个。如果设置为1，则只有一个请求在等待响应
        // 此时可以保证发送消息即使在重试的情况下也是有序的。
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);


        // 创建一个生产者对象
        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(configs);

        User user = new User();
        user.setUserId(1001);
        user.setUsername("zhang");

        // 创建一个消息的封装对象
        ProducerRecord<String, User> producerRecord = new ProducerRecord<>(
                "tp_user_01",
                user.getUsername(),// key
                user // value
        );

        // 异步回调
        producer.send(producerRecord,
                new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    System.out.println("消息发送异常");
                } else {
                    System.out.println("主题：" + recordMetadata.topic() + "\t"
                            + "分区：" + recordMetadata.partition() + "\t"
                            + "生产者偏移量：" + recordMetadata.offset());
                }
            }
        });

        //producer.close();
    }
}

