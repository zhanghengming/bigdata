package com.kafka.demo.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {
/**
 * 为指定的消息记录计算分区值
 *
 * @param topic 主题名称
 * @param key 根据该key的值进⾏分区计算，如果没有则为null。
 * @param keyBytes key的序列化字节数组，根据该数组进⾏分区计算。如果没有key，则为null
 * @param value 根据value值进⾏分区计算，如果没有，则为null
 * @param valueBytes value的序列化字节数组，根据此值进⾏分区计算。如果没有，则为null
 * @param cluster 当前集群的元数据
 */

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
