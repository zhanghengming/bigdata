package com.demo.kafka;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClientDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        // 管理客户端
        AdminClient adminClient = KafkaAdminClient.create(properties);
        NewTopic test = new NewTopic("test", 3, (short) 2);
        // 创建主题
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(test));

        // 查看topic信息
        DescribeTopicsResult topicDescription = adminClient.describeTopics(Arrays.asList("test"));
        topicDescription.all().get().values().forEach(topic -> {
            System.out.println(topic.toString());
            String name = topic.name();
            for (TopicPartitionInfo partition : topic.partitions()) {
                int partition1 = partition.partition();
                List<Node> isr = partition.isr();
                Node leader = partition.leader();
                List<Node> replicas = partition.replicas();
            }
        });
    }
}
