package dim;

import myutils.ConnHBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

// 将流数据插入hbase
public class HBaseWriterSink extends RichSinkFunction<String> {
    private Connection connection;
    private Table table;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = new ConnHBase().connToHbase();
        table = connection.getTable(TableName.valueOf("dim_lagou_area"));

    }

    @Override
    public void close() throws Exception {
        if(table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println(value);
        insertDimArea(table,value);
    }

    public void insertDimArea(Table table, String value) throws IOException {
        String[] split = value.split(",");
        String areaId = split[0].trim();
        String aname = split[1].trim();
        String cid = split[2].trim();
        String city = split[3].trim();
        String proid = split[4].trim();
        String province = split[5].trim();

        Put put = new Put(areaId.getBytes());
        put.addColumn("f1".getBytes(), "aname".getBytes(), aname.getBytes());
        put.addColumn("f1".getBytes(), "cid".getBytes(), cid.getBytes());
        put.addColumn("f1".getBytes(), "city".getBytes(), city.getBytes());
        put.addColumn("f1".getBytes(), "proid".getBytes(), proid.getBytes());
        put.addColumn("f1".getBytes(), "province".getBytes(), province.getBytes());

        table.put(put);
    }
}
