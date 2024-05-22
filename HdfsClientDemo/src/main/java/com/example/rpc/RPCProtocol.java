package com.example.rpc;

// 创建rpc协议 定义方法实现通信的功能
public interface RPCProtocol {

    long versionID = 666;


    void mkdirs(String path);
}
