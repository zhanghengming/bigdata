package com.zkDemo;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;

public class Watcher {

    public static void main(String[] args) throws InterruptedException {
        ZkClient zkClient = new ZkClient("192.168.10.100:2181");
        //zkClient对指定⽬录进⾏监听(不存在⽬录:/lg-client)
        // 注册了监听器，一直在监听节点的变化 参数传接口，用匿名内部类去实现
        zkClient.subscribeChildChanges("/test1", new IZkChildListener() {
            // 该方法是接受到通知后执行的逻辑
            public void handleChildChange(String path, List<String> childs) throws Exception {
                System.out.println(path + childs);
                System.out.println(123);
            }
        });
        //创建一个节点，验证监听器
        zkClient.createPersistent("/test1");
        Thread.sleep(1000);
        zkClient.createPersistent("/test1/zk");
        Thread.sleep(1000);
        zkClient.delete("/test1/zk");
        Thread.sleep(1000);
        zkClient.delete("/test1");
        Thread.sleep(Integer.MAX_VALUE);
        /*
        * 客户端可以对不存在的节点进行监听
        * 监听目录下的子节点发生变化是，服务端会通知客户端，并将最新的子节点列表发给客户端
        * 节点本身的创建删除也会被监听到
        * */

    }

}
