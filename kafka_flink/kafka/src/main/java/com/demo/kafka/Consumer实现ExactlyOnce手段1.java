package com.demo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * 利用mysql的事务机制，米实现kafka consumer数据传输过程端到端的exactly once
 * 消费数据到mysql中，并更新offset到mysql中，实现exactly once。
 * 具体步骤如下：
 * 1. 创建kafka consumer，指定group.id，enable.auto.commit=false，并订阅topic。
 * 2. 创建jdbc连接，并关闭自动事务提交。
 * 3. 定义一个业务数据插入语句，并定义一个偏移量更新语句。
 * 4. 当出现问题的时候需要重新订阅主题，初始化偏移量到当前位置，然后接着消费
 * 5. 循环消费kafka消息，并执行业务数据插入和偏移量更新语句，如果出现异常，则回滚事务。
 * 6. 关闭jdbc连接和kafka consumer。
 */
public class Consumer实现ExactlyOnce手段1 {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux121:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 关闭自动提交

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // 创建jdbc连接
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3307/test?useSSL=false", "root", "");
        // 关闭自动事务提交，防止每个语句都提交一次事务
        conn.setAutoCommit(false);
        // 定义一个业务数据插入语句
        PreparedStatement pst = conn.prepareStatement("insert into user values(?, ?, ?, ?)");
        // 定义一个偏移量更新语句
        PreparedStatement pst2 = conn.prepareStatement("insert into t_offset values(?, ?) on duplicate key update offset = ?");

        PreparedStatement pst3 = conn.prepareStatement("select offset from t_offset where topic_partition = ?");

        // TODO: 需要把消费起始位置初始化成上一次的消费位置
        // TODO：当发生消费组再均衡的时候产生的问题，也需要重新初始化
        // 当发生消费组再均衡的时候，会触发onPartitionsRevoked和onPartitionsAssigned两个回调函数，
        // 这两个函数的作用是记录当前消费组的消费状态，并将消费组重新分配给其他消费者。
        // 因此，在这两个回调函数中，需要重新初始化偏移量到当前位置，然后接着消费。
        kafkaConsumer.subscribe(Arrays.asList("demo"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 如果没有事务，那么可以在这里自己实现偏移量的更新

            }

            // 被分配了新的分区消费权后调用的方法
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 重平衡之后的分配的分区信息
                for (TopicPartition topicPartition : partitions) {
                    try {
                        pst3.setString(1,topicPartition.topic()+"-"+topicPartition.partition());
                        ResultSet resultSet = pst3.executeQuery();
                        // 获取到每个分区最新的偏移量
                        long offset = resultSet.getLong("offset");
                        System.out.println("重平衡之后的分区信息：" + topicPartition.topic() + "-" + topicPartition.partition() + " 偏移量：" + offset);
                        // 指定分区和偏移量，从当前位置开始消费
                        kafkaConsumer.seek(topicPartition, offset);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });



        boolean isTransaction = false;
        while (isTransaction) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {

                try {
                    String data = record.value();
                    String[] split = data.split(",");

                    pst.setInt(1, Integer.parseInt(split[0]));
                    pst.setString(2, split[1]);
                    pst.setInt(3, Integer.parseInt(split[2]));
                    pst.setInt(4, Integer.parseInt(split[0]));

                    // 执行业务数据插入数据
                    pst.execute();

                    pst2.setString(1, record.topic()+"-"+record.partition());
                    pst2.setLong(2, record.offset() + 1);
                    pst2.setLong(3, record.offset() + 1);

                    // 更新mysql中记录的offset
                    pst2.execute();

                    // 事务提交  每次循环提交一次事务
                    conn.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    // 事务回滚
                    conn.rollback();
                }
            }
        }
        pst.close();
        conn.close();
        kafkaConsumer.close();
    }
}
