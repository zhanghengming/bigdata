package com.example.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

// 创建服务端，提供基于协议的服务
public class NNServer implements RPCProtocol{
    @Override
    public void mkdirs(String path) {
        System.out.println("----------" + path);
    }

    public static void main(String[] args) throws IOException {
        // 创建服务对象，并设置rpc协议
        Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(RPCProtocol.class)
                .setInstance(new NNServer())
                .build();
        System.out.println("server");
        server.start();
    }

}