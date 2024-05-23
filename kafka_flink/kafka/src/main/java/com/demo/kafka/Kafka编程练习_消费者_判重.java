package com.demo.kafka;

import com.alibaba.fastjson.JSON;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.roaringbitmap.RoaringBitmap;

import java.time.Duration;
import java.util.*;

/**
 * 需求2：给每条数据添加一个字段来标识该条数据是否是第一次出现，如果是标注1，否则标注0
 * {"guid":1,"eventId":"pageview","timeStamp":1637868346789,"flag":1}
 * <p>
 * 布隆过滤器只能用来判重，但是会牺牲真确性，底层是取的hashcode，判断存在是有一定的误差，但是能判断不存在
 */

public class Kafka编程练习_消费者_判重 {
    public static void main(String[] args) {


        // 要保证不同类之间数据共享，可以使用构造方法传入
        // 启动数据消费线程
        new Thread(new ConsumeRunnableBloomFilter()).start();

    }
}

/**
 * 消费拉取数据的线程runnable，将拉到的数据写入map中保证key不重复
 */
class ConsumeRunnableBloomFilter implements Runnable {
    // 构造方法传入hashmap
    // 自己搞个成员变量来接收传来的hashmap，传递的是引用，这个map指向的就是传进来的map
    BloomFilter bloomFilter;
    KafkaConsumer<String, String> consumer;

    public ConsumeRunnableBloomFilter() {
        // 创建布隆过滤器，参数是hash函数的入参类型，预计元素数量，误差率
        bloomFilter = BloomFilter.create(Funnels.longFunnel(), 1000000000, 0.01);
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("demo"));
        while (true) {
            // 如果5秒没有数据，则会执行接下来的代码
            // 否则，会阻塞在这里，等待新数据到来
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord<String, String> record : records) {
                String eventJson = record.value();
                UserEvent userEvent = JSON.parseObject(eventJson, UserEvent.class);
                boolean b = bloomFilter.mightContain(userEvent.getGuid());
                if (b) {
                    // 已经存在, 标志位为0
                    userEvent.setFlag(0);
                } else {
                    // 不存在，标志位为1, 添加到布隆过滤器中
                    userEvent.setFlag(1);
                    bloomFilter.put(userEvent.getGuid());
                }
                System.out.println(JSON.toJSONString(userEvent));
            }

        }
    }
}
