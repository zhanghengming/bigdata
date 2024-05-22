package com.kafka.demo.deserializer;

import com.kafka.demo.entity.User;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class UserDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public User deserialize(String topic, byte[] bytes) {
        // 申请空间
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        // 从buffer中获取对应的变量值
        int userId = buffer.getInt();
        int length = buffer.getInt();
        System.out.println(length);
        String username = new String(bytes, 8, length);
        return new User(userId,username);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
