package com.common;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;

/**
 * @author ：zhm
 * @version ：1.0
 * @since ：2025/11/5 14:12
 * 获取相关source工具类
 */
public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.1.100:9092")
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        return kafkaSource;
    }

    // 获取mysql source
    public static MySqlSource<String> getMysqlSource(String database, String table) {
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("192.168.56.101")
                .port(3306)
                .databaseList(database)
                .tableList(database + "." + table)
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        return mysqlSource;
    }
}
