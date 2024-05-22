
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Test;
import org.junit.Before;


import javax.sound.midi.Soundbank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseClient {
    Configuration conf = null;
    Connection connection = null;
    HBaseAdmin hBaseAdmin = null;

    @Before
    public void init() throws IOException {
        // 获取配置文件对象
        conf = HBaseConfiguration.create();
        // hbase-site 里面配置的 + 主机的hosts
        conf.set("hbase.zookeeper.quorum", "linux121,linux122");
        // 指定端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // 通过conf获取到hbase集群的连接
        connection = ConnectionFactory.createConnection(conf);
    }

    // 创建表
    @Test
    public void createTable() throws IOException {

        // 获取hbaseadmin对象用来创建表
        hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        // 创建htable desc描述器，表描述器
        HTableDescriptor friends = new HTableDescriptor(TableName.valueOf("lagou_trade_orders"));
        // 指定列族
        friends.addFamily(new HColumnDescriptor("f1"));
        hBaseAdmin.createTable(friends);
        System.out.println("lagou_trade_orders table create success");

        hBaseAdmin.close();
        connection.close();

    }

    //判断表是否存在
    @Test
    public void isTableExists() throws IOException {
        hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        boolean tableExists = hBaseAdmin.tableExists(TableName.valueOf("lagou_trade_orders"));
        System.out.println(tableExists);
        hBaseAdmin.close();
        connection.close();
    }

    @Test
    // 删除表
    public void deleteTable() throws IOException {
        // 获取 admin 对象
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf("lagou_trade_orders"))) {
            System.out.println("表格不存在 无法删除");
        }
        // HBase 删除表格之前 一定要先标记表格为不可以
        TableName tableName1 = TableName.valueOf("lagou_trade_orders");
        admin.disableTable(tableName1);
        admin.deleteTable(tableName1);
        // 关闭 admin
        admin.close();
        System.out.println("删除成功！");
        isTableExists();
    }

    //扫描数据
    @Test
    public void scanRows() throws IOException {
        // 1. 获取 table
        Table table = connection.getTable(TableName.valueOf("lagou_trade_orders"));
        // 2. 创建 scan 对象
        Scan scan = new Scan();
        // 如果此时直接调用 会直接扫描整张表
        // 读取多行数据 获得 scanner
        ResultScanner scanner = table.getScanner(scan);
        // result 来记录一行数据 cell 数组
        // ResultScanner 来记录多行数据 result 的数组
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                        new String(CellUtil.cloneRow(cell))
                + "-" + new String(CellUtil.cloneFamily(cell))
                + "-" + new String(CellUtil.cloneQualifier(cell))
                + "-" + new String(CellUtil.cloneValue(cell))
                + "\t"
                );
            }
            System.out.println();
        }
        table.close();
        connection.close();
    }

    //插入数据
    @Test
    public void putData() throws IOException {
        //获取表对象
        Table friends = connection.getTable(TableName.valueOf("friends"));
        //设定rowkey 准备put对象
        Put put1 = new Put(Bytes.toBytes("uid1"));
        Put put2 = new Put(Bytes.toBytes("uid2"));
        List<Put> puts = new ArrayList<>();

        //列族，列，value
        put1.addColumn(Bytes.toBytes("friends_info"),Bytes.toBytes("uid"),Bytes.toBytes("uid2"));

        put2.addColumn(Bytes.toBytes("friends_info"),Bytes.toBytes("uid"),Bytes.toBytes("uid1"));
        //使用list批量插入数据
        puts.add(put1);
        puts.add(put2);
        // 插入数据 参数使put
        friends.put(puts);

        //关闭table对象
        friends.close();
        System.out.println("插入成功");
    }

    @Test
    // 删除数据
    public void deleteColumn() throws IOException {
        String rowKey = null;
        // 1.获取 table
        Table table = connection.getTable(TableName.valueOf("lagou_trade_orders"));
        // 2.创建 Delete 对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 删除列族
        delete.addFamily(Bytes.toBytes("f1"));
    }

    @After
    public void release(){
        if(hBaseAdmin != null){
            try {
                hBaseAdmin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void deleteData() throws IOException {
        Table friends = connection.getTable(TableName.valueOf("friends"));
        Delete uid1 = new Delete(Bytes.toBytes("uid1"));
        friends.delete(uid1);
        friends.close();
        System.out.println("删除成功");
    }
}
