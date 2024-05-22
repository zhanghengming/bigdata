package com.example.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

// 客户端和服务端建立通信  获取rpc的代理，设置通信协议 通过代理对象访问服务
public class NNClient {
    public static void main(String[] args) throws IOException {
        // 创建 RPC 代理对象
        RPCProtocol proxy = RPC.getProxy(
                RPCProtocol.class,
                RPCProtocol.versionID,
                new InetSocketAddress("localhost", 8888),
                new Configuration());

        // 打印客户端信息
        System.out.println("Client");

        // 调用代理对象的 mkdirs 方法
        proxy.mkdirs("/input");
    }
}
