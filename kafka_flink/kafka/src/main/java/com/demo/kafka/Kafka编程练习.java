package com.demo.kafka;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产者不断生成数据写入kafka
 * 写一个消费者，不断地从kafka中消费如上“用户行为事件"数据，并做统计计算:每 5分钟，输出一次截止到当时的数据中出现过的用户总数
 */
public class Kafka编程练习 {
    public static void main(String[] args) throws InterruptedException {
        MyDataGen myDataGen = new MyDataGen();
        myDataGen.genData();
    }
}

// 业务数据生成器
class MyDataGen {
     KafkaProducer<String, String> producer;
    public MyDataGen() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }
    public void genData() throws InterruptedException {
        UserEvent userEvent = new UserEvent();
        while (true) {
            // 造随机的用户事件数据
            userEvent.setGuid(RandomUtils.nextInt(1,10000));
            userEvent.setEventId(RandomStringUtils.randomAlphabetic(5, 8));
            userEvent.setTimestamp(System.currentTimeMillis());

            // 转为json字符串
            String s = JSON.toJSONString(userEvent);

            // 将业务数据封装为ProducerRecord对象
            ProducerRecord<String, String> record = new ProducerRecord<>("demo", s);
            // 用producer写入kafka
            producer.send(record);

            Thread.sleep(RandomUtils.nextInt(500, 1000));
        }
    }
}
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
class UserEvent{
    private long guid;
    private String eventId;
    private long timestamp;
}
