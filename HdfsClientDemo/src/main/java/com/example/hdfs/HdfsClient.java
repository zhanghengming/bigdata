package com.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 客户端代码套路
 * 1.获取客户端对象
 * 2.执行相关操作命令
 * 3.释放资源
 * hdfs zk
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://linux121:8020");
        Configuration entries = new Configuration();
        String user = "zhm";
        fs = FileSystem.get(uri, entries, user);
    }
    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdir() throws IOException {
        fs.mkdirs(new Path("/huaguoshan"));
    }
}
