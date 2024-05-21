package com.zkDemo;

import org.I0Itec.zkclient.ZkClient;

//服务端
public class ServerMain {
    private ZkClient zkClient = null;

    //获取zk对象
    private void connectZk(){
         zkClient = new ZkClient("linux121:2181,linux122:2181,linux123:2181");
         if(!zkClient.exists("/servers")){
             zkClient.createPersistent("/servers");
         }
    }
    //注册服务端信息到zk
    private void registerServerInfo(String ip,String port){
        //创建临时顺序节点
        String path = zkClient.createEphemeralSequential("/servers/server", ip + ":" + port);
        System.out.println("服务器注册成功，ip=" + ip + ";port =" + port + ";节点路径信息=" + path);
    }

    public static void main(String[] args) {
        ServerMain serverMain = new ServerMain();
        serverMain.connectZk();
        serverMain.registerServerInfo("192.168.10.100","2181");
        new TimeServer(Integer.parseInt("2181")).start();
    }

}
