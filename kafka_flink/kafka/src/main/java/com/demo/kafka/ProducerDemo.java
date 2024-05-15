package com.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) throws InterruptedException {

        // 创建kafka生产者客户端
        // 泛型K：封装格式的key类型，V：封装格式的value类型
        // kafka中的消息是kv格式的，可以没有key
        Properties properties = new Properties();
        // 配置kafka集群地址
        properties.setProperty("bootstrap.servers", "linux121:9092");
        // 配置kafka客户端key和value的序列化器,因为发送的消息有各种格式的都必须变成序列化之后的字节数组，
        // 所以，kafka的producer需要一个针对用户要发送的数据类型的序列化工具类
        // 且这个序列化工具类，需要实现kafka所提供的序列工具接口:org,apache,kafka,common.serialization.Serializer
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092,linux122:9092,linux123:9092");
        // 获取类的全路径名
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 创建kafka生产者客户端
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 将业务数据封装成客户端所能发送的封装格式
        for(int i = 0; i < 10; i++) {
            // 调用客户端发送数据，异步发送，也就是发完之后继续执行之后的代码
            // 先放在producer的缓存中，攒一批，攒够一定量的数据之后，再发送给kafka集群
            producer.send(new ProducerRecord<String, String>("demo", "key" + i, "value" + i));
            Thread.sleep(100);
        }

        // 关闭客户端
        producer.close();
    }
}
