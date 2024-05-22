package com.kafka.demo.serialization;

import com.kafka.demo.entity.User;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;


public class UserSerializer implements Serializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
        // 用于接收对序列化器的配置参数，并对当前序列化器进行配置和初始化的
    }

    @Override
    public byte[] serialize(String topic, User user) {
        try {
            if (user == null) {
                return  null;
            } else {
                Integer userId = user.getUserId();
                String username = user.getUsername();

                if (userId != null) {
                    byte[] bytes = username.getBytes("UTF-8");
                    int length = bytes.length;
                    // 第一个4个字节用于存储userId的值
                    // 第二个4个字节用于存储username字节数组的长度int值
                    // 第三个长度，用于存放username序列化之后的字节数组
                    ByteBuffer allocate = ByteBuffer.allocate(4 + 4 + length);
                    // 设置username字节数组长度
                    allocate.putInt(length);
                    // 设置userId
                    allocate.putInt(userId);
                    // 设置username字节数组
                    allocate.put(bytes);
                    // 以字节数组形式返回user对象的值
                    return allocate.array();
                }
            }

        } catch (Exception e) {
            throw new SerializationException("数据序列化失败");
        }

        return null;
    }


    @Override
    public void close() {
        // do nothing
        // 用于关闭资源等操作。需要幂等，即多次调用，效果是一样的。
    }
}
