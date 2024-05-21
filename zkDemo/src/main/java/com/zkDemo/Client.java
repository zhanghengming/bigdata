package com.zkDemo;

import com.sun.xml.internal.ws.api.ha.StickyFeature;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// 注册监听zk指定⽬录，
public class Client {
    //获取zkclient
    ZkClient zkClient = null;
    //维护⼀个serversi 信息集合
    ArrayList<String> infos = new ArrayList<String>();

    private void connectZk(){
        zkClient = new ZkClient("linux121:2181,linux122:2181");
        //第⼀次获取服务器信息,所有的⼦节点
        List<String> children = zkClient.getChildren("/servers");
        for (String child : children) {
            Object o = zkClient.readData("/servers/" + child);
            infos.add(String.valueOf(o));
        }
        //对servers⽬录进⾏监听
        zkClient.subscribeChildChanges("/servers", new IZkChildListener() {
            public void handleChildChange(String s, List<String> children) throws Exception {
                //接收到通知，说明节点发⽣了变化，client需要更新infos集合中的数据
                ArrayList<String> list = new ArrayList<String>();
                //遍历更新过后的所有节点信息
                for (String path : list) {
                    Object o = zkClient.readData("/servers/" + path);
                    list.add(String.valueOf(o));
                }
                //最新数据覆盖⽼数据
                infos = list;
                System.out.println("--》接收到通知，最新服务器信息为：" + infos);
            }
        });
    }

    //发送时间查询的请求
    public void sendRequest() throws IOException {
        //⽬标服务器地址
        Random random = new Random();
        int i = random.nextInt(infos.size());
        String ipPort = infos.get(i);
        String[] split = ipPort.split(":");
        //建⽴socket连接
        Socket socket = new Socket(split[0], Integer.parseInt(split[1]));
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();
        //发送数据
        out.write("query time".getBytes());
        out.flush();
        //接收返回结果
        byte[] bytes = new byte[1024];
        in.read(bytes);
        System.out.println("client端接收到server:+" + ipPort + "+返回结果：" +
                new String(bytes));
        in.close();
        out.close();
        socket.close();
    }

    public static void main(String[] args) throws InterruptedException {
        Client client = new Client();
        client.connectZk();
        while (true){
            try {
                client.sendRequest();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Thread.sleep(1000);
        }
    }

}
