package com.zkDemo;

import org.I0Itec.zkclient.ZkClient;

public class zkTest {
    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient("192.168.10.100:2181");
        System.out.println("successful");

        // 创建持久节点 值设为true，可以递归创建节点
//        zkClient.createPersistent("/test11");
//        System.out.println("123");

        // 删除节点
        zkClient.delete("/test11");
    }
}
