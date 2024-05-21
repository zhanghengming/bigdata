package com.zkDemo;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class getDataChange {
    public static void main(String[] args) throws InterruptedException {
        ZkClient zkClient = new ZkClient("192.168.10.100:2181");
        //设置⾃定义的序列化类型
        zkClient.setZkSerializer(new ZkStrSerializer());

        boolean exists = zkClient.exists("/test1");
        if(!exists){
            zkClient.createEphemeral("/test1","123");
        }
        //注册监听器，节点数据改变的类型，接收通知后的处理逻辑定义
        zkClient.subscribeDataChanges("/test1", new IZkDataListener() {
            public void handleDataChange(String path, Object o) throws Exception {
                System.out.println(path + "data is changed ,new data" + o);
            }

            public void handleDataDeleted(String path) throws Exception {
                System.out.println(path + " is deleted!!");
            }
        });
        //读数据
        Object data = zkClient.readData("/test1");
        System.out.println(data);
        //写数据
        zkClient.writeData("/test1","new data");
        Thread.sleep(1000);

        //删除节点
        zkClient.delete("/test1");
        Thread.sleep(Integer.MAX_VALUE);
    }
}

class ZkStrSerializer implements ZkSerializer{
    //序列化
    public byte[] serialize(Object o) throws ZkMarshallingError {
        return String.valueOf(o).getBytes();
    }

    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return new String(bytes);
    }
}