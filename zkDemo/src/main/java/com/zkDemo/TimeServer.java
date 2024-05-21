package com.zkDemo;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

//服务端提供时间查询的线程类
public class TimeServer extends Thread {
    private int port = 0;
    public TimeServer(int port){
        this.port = port;
    }

    @Override
    public void run() {
        //启动serversocket监听⼀个端⼝
        try {
            ServerSocket socket = new ServerSocket(port);
            while (true){
                Socket accept = socket.accept();
                OutputStream outputStream = accept.getOutputStream();
                outputStream.write(new Date().toString().getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
