package com.demo.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.roaringbitmap.RoaringBitmap;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 每 5分钟，输出一次截止到当时的数据中出现过的用户总数，需要搞两个线程，一个负责源源不断拉取数据，另一个每5分钟输出一次
 * 要创建一个hashmap来让两个线程共享数据，所以写在main方法中
 *
 * 用hashset或者hashmap当数据量10亿的时候所占内存就会很大。所以需要使用bitmap来存储数据，只需要记录数据所在的位置，每一位是一个bit，1表示数据存在，0表示不存在。
 * roaringbitmap工具包就是基于bitmap开发的数据结构工具，底层会对最原始的bitmap存储进行优化，这样就避免了一开始指定最大长度导致的稀疏向量占空间的问题
 *
 * bitmap就是存整数，可以记录、判重整数，但不能记录字符串，所以需要将字符串转成整数，再存入bitmap中。
 * 布隆过滤器只能用来判重，但是会牺牲真确性，底层是取的hashcode，判断存在是有一定的误差，但是能判断不存在
 */

public class Kafka编程练习_消费者_bitmap{
    public static void main(String[] args) {

        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();

        // 要保证不同类之间数据共享，可以使用构造方法传入
        // 启动数据消费线程
        new Thread(new ConsumeRunnableBitmap(bitmap)).start();

        // 启动一个统计及输出结果的线程(每5秒输出一次结果)
        //优雅一点来实现定时调度，可以用各种定时调度器(有第三方的，也可以用idk自己的:Timer)
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new StatisticTaskBitmap(bitmap), 5, 10 * 1000);
    }
}

/**
 * 消费拉取数据的线程runnable，将拉到的数据写入map中保证key不重复
 */
class ConsumeRunnableBitmap implements Runnable{
    // 构造方法传入hashmap
    // 自己搞个成员变量来接收传来的hashmap，传递的是引用，这个map指向的就是传进来的map
    RoaringBitmap guidMap;
    public ConsumeRunnableBitmap(RoaringBitmap guidMap) {
        this.guidMap = guidMap;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("demo"));
        while (true) {
            // 如果5秒没有数据，则会执行接下来的代码
            // 否则，会阻塞在这里，等待新数据到来
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord<String, String> record : records) {
                String eventJson = record.value();
                UserEvent userEvent = JSON.parseObject(eventJson, UserEvent.class);
                // 这里可以用hashset来保证key不重复
                guidMap.add((int) userEvent.getGuid());
            }

        }
    }
}

/**
 *  定时统计线程
 */
class StatisticTaskBitmap extends TimerTask{
    RoaringBitmap guidMap;
    public StatisticTaskBitmap(RoaringBitmap guidMap) {
        this.guidMap = guidMap;
    }
    @Override
    public void run() {
        System.out.println(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss") + "，截止到当前的用户总数为：" + guidMap.getCardinality());
    }
}