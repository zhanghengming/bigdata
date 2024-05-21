package com.lagou.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;


import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    // 创建目录
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {

        //1.获取文件系统
        Configuration configuration = new Configuration();
        //配置在集群上运行
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.10.100:9000"), configuration, "root");
        //2.创建目录
        fs.mkdirs(new Path("/test1"));
        //3.关闭资源
        fs.close();
    }

    // 上传文件
   @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");
        fs.copyFromLocalFile(new Path("C:\\Users\\95877\\Desktop\\cwb002.md"),new Path("/test"));
        fs.close();
    }

    // 下载文件
   @Test
    public void testCopyToLocalFile() throws IOException, URISyntaxException, InterruptedException {

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");
        fs.copyToLocalFile(false,new Path("/test/cwb002.md"),new Path("e:"),true);
        fs.close();
    }

    //删除文件、文件夹
   @Test
    public void testDelete() throws IOException, URISyntaxException, InterruptedException {

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");
        fs.delete(new Path("/test1"),true);
        fs.close();

    }

    // 查看文件名称，权限，长度，块信息
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");
        // 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            // 文件名
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String str : hosts){
                    System.out.println(str);
                }
            }
            System.out.println("-----------华丽的分割线----------");
        }
        fs.close();
    }

    //  I/O流操作HDFS
    //文件上传 把本地文件上传到HDFS根目录
    @Test
    public void putFileToHDFS() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/cwb002.md"));
        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/cwb002.md"));
        // 4 输入流数据拷贝到输出流 :数组的大小，以及是否关闭流底层有默认值
        IOUtils.copyBytes(fis,fos,configuration);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 文件下载
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");

        //获取输入流
        FSDataInputStream fis = fs.open(new Path("/cwb002.md"));
        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/cwb002_11.md"));
        //流的对拷
        IOUtils.copyBytes(fis,fos,configuration);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // seek 定位读取 将HDFS上的lagou.txt的内容在控制台输出两次
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");

        FSDataInputStream in = null;
        // 打开输入流,读取数据输出到控制台
        try {
             in = fs.open(new Path("/cwb002.md"));
            //实现流拷贝，输入流--》控制台输出
            IOUtils.copyBytes(in,System.out,4096,false);
            in.seek(0); //从头再次读取
            IOUtils.copyBytes(in, System.out, 4096, false);

        } finally {
            IOUtils.closeStream(in);
        }
    }
}
