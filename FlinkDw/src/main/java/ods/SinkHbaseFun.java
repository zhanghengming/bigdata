package ods;

import bean.AreaInfo;
import bean.DataInfo;
import bean.TableObject;
import com.alibaba.fastjson.JSON;
import myutils.ConnHBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 *自定义Hbase的下沉器
 * 输入的数据是每个json中的数组，里面包含了多个
 */
public class SinkHbaseFun extends RichSinkFunction<ArrayList<TableObject>> {

    private Connection connection;
    private Table table;


    // 实例化Hbase connection table
    @Override
    public void open(Configuration parameters) throws Exception {
         connection = new ConnHBase().connToHbase();
         table = connection.getTable(TableName.valueOf("lagou_trade_orders"));
    }

    @Override
    // 每来一条数据，会执行一次
    public void invoke(ArrayList<TableObject> value, Context context) throws Exception {
        // 遍历列表中的元素
        value.forEach(data -> {
            String database = data.getDatabase();
            String tableName = data.getTableName();
            String typeInfo = data.getTypeInfo();

            if (database.equalsIgnoreCase("dwshow") && tableName.equalsIgnoreCase("lagou_trade_orders")) {
                if (typeInfo.equalsIgnoreCase("insert")) {
                    // 将json字符串转为实体类
                    DataInfo dataInfo = JSON.parseObject(data.getDataInfo(), DataInfo.class);
                    insertTradeOrders(table,dataInfo);
                } else if (typeInfo.equalsIgnoreCase("update")) {

                } else if (typeInfo.equalsIgnoreCase("delete")) {

                }
            }
            if (database.equalsIgnoreCase("dwshow") && tableName.equalsIgnoreCase("lagou_area")) {
                if (typeInfo.equalsIgnoreCase("insert")) {
                    // 将json字符串转为实体类
                    AreaInfo areaInfo = JSON.parseObject(data.getDataInfo(), AreaInfo.class);
                    insertArea(table,areaInfo);
                } else if (typeInfo.equalsIgnoreCase("update")) {
                    // 更新操作就是插入一个新的版本
                    AreaInfo areaInfo = JSON.parseObject(data.getDataInfo(), AreaInfo.class);
                    insertArea(table,areaInfo);
                } else if (typeInfo.equalsIgnoreCase("delete")) {
                    AreaInfo areaInfo = JSON.parseObject(data.getDataInfo(), AreaInfo.class);
                    try {
                        deleteArea(table,areaInfo.id);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    public void deleteArea(Table table,String rowKey) throws IOException {
        //创建 Delete 对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除
        table.delete(delete);

    }

    public void insertArea(Table table, AreaInfo areaInfo) {
        Put put = new Put(areaInfo.id.getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), areaInfo.name.getBytes());
        put.addColumn("f1".getBytes(), "pid".getBytes(), areaInfo.pid.getBytes());
        put.addColumn("f1".getBytes(), "sname".getBytes(), areaInfo.sname.getBytes());
        put.addColumn("f1".getBytes(), "level".getBytes(), areaInfo.level.getBytes());
        put.addColumn("f1".getBytes(), "citycode".getBytes(), areaInfo.citycode.getBytes());
        put.addColumn("f1".getBytes(), "yzcode".getBytes(), areaInfo.yzcode.getBytes());
        put.addColumn("f1".getBytes(), "mername".getBytes(), areaInfo.mername.getBytes());
        put.addColumn("f1".getBytes(), "lng".getBytes(), areaInfo.Lng.getBytes());
        put.addColumn("f1".getBytes(), "lat".getBytes(), areaInfo.Lat.getBytes());
        put.addColumn("f1".getBytes(), "pinyin".getBytes(), areaInfo.pinyin.getBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 插入数据的方法
    public void insertTradeOrders(Table table, DataInfo dataInfo) {
        // put对象 传行号 设置 rowkey
        Put put = new Put(dataInfo.orderId.getBytes());
        // 列族，列名，具体的值
        put.addColumn("f1".getBytes(), "modifiedTime".getBytes(), dataInfo.modifiedTime.getBytes());
        put.addColumn("f1".getBytes(), "orderNo".getBytes(), dataInfo.orderNo.getBytes());
        put.addColumn("f1".getBytes(), "isPay".getBytes(), dataInfo.isPay.getBytes());
        put.addColumn("f1".getBytes(), "orderId".getBytes(), dataInfo.orderId.getBytes());
        put.addColumn("f1".getBytes(), "tradeSrc".getBytes(), dataInfo.tradeSrc.getBytes());
        put.addColumn("f1".getBytes(), "payTime".getBytes(), dataInfo.payTime.getBytes());
        put.addColumn("f1".getBytes(), "productMoney".getBytes(), dataInfo.productMoney.getBytes());
        put.addColumn("f1".getBytes(), "totalMoney".getBytes(), dataInfo.totalMoney.getBytes());
        put.addColumn("f1".getBytes(), "dataFlag".getBytes(), dataInfo.dataFlag.getBytes());
        put.addColumn("f1".getBytes(), "userId".getBytes(), dataInfo.userId.getBytes());
        put.addColumn("f1".getBytes(), "areaId".getBytes(), dataInfo.areaId.getBytes());
        put.addColumn("f1".getBytes(), "createTime".getBytes(), dataInfo.createTime.getBytes());
        put.addColumn("f1".getBytes(), "payMethod".getBytes(), dataInfo.payMethod.getBytes());
        put.addColumn("f1".getBytes(), "isRefund".getBytes(), dataInfo.isRefund.getBytes());
        put.addColumn("f1".getBytes(), "tradeType".getBytes(), dataInfo.tradeType.getBytes());
        put.addColumn("f1".getBytes(), "status".getBytes(), dataInfo.status.getBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
