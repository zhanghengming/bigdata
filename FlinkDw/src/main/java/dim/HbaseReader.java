package dim;

import myutils.ConnHBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;

/**
 * 从Hbase中读取数据
 * 对数据进行转化，转成指定的维表
 * 存在hbase中
 */
public class HbaseReader extends RichSourceFunction<Tuple2<String, String>> {
    private Connection connection;
    private Table table;
    private Scan scan;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        connection = new ConnHBase().connToHbase();
        TableName tableName = TableName.valueOf("lagou_area");
        table = connection.getTable(tableName);
        scan = new Scan();
        scan.addFamily(Bytes.toBytes("f1"));
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        // 读取多行数据 获得 scanner
        ResultScanner scanner = table.getScanner(scan);
        // ResultScanner 来记录多行数据 result 的数组
        for (Result result : scanner) {
            StringBuffer stringBuffer = new StringBuffer();
            // 获取每一行的rk
            String rowKey = Bytes.toString(result.getRow());
            // result 来记录一行数据 cell 数组
            Cell[] cells = result.rawCells();
            // 每一行包含多个字段，也就是多个cell
            for (Cell cell : cells) {
                String s = Bytes.toString(CellUtil.cloneValue(cell));
                stringBuffer.append(s).append("-");
            }
            stringBuffer.replace(stringBuffer.length() - 1, stringBuffer.length(), "");
            ctx.collect(new Tuple2<>(rowKey, stringBuffer.toString()));
        }

    }

    @Override
    public void cancel() {

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
}
