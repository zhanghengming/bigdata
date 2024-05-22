package dw.dws;

import myutils.ConnHBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadTradeOrder extends RichSourceFunction<Tuple2<String, String>> {
    private Connection conn;
    private Table table;
    private Scan scan;
    Boolean flag = false;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = new ConnHBase().connToHbase();
        table = conn.getTable(TableName.valueOf("lagou_trade_orders"));
        scan = new Scan();
        scan.addFamily(Bytes.toBytes("f1"));
    }

    @Override
    // 从hbase读取数据
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            StringBuffer stringBuffer = new StringBuffer();
            String rk = Bytes.toString(result.getRow());
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String field = Bytes.toString(CellUtil.cloneValue(cell));
                stringBuffer.append(field).append(",");
            }
            stringBuffer.replace(stringBuffer.length() - 1, stringBuffer.length(), "");
            ctx.collect(new Tuple2<>(rk, stringBuffer.toString()));
        }
    }

    @Override
    public void cancel() {
        flag = true;
    }

    @Override
    public void close() throws Exception {
        try{
            if(table != null) {
                table.close();
            }
            if(conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
