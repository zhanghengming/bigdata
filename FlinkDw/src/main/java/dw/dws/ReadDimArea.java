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
import java.util.Iterator;

public class ReadDimArea extends RichSourceFunction<Tuple2<String,String>> {
    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;
    Boolean flag = false;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = new ConnHBase().connToHbase();
        table = conn.getTable(TableName.valueOf("dim_lagou_area"));
        scan = new Scan();
        scan.addFamily(Bytes.toBytes("f1"));
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        if (!flag) {
            ResultScanner results = table.getScanner(scan);
            Iterator<Result> iterator = results.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                String rk = Bytes.toString(result.getRow());
                StringBuffer buffer = new StringBuffer();
                Cell[] cells = result.rawCells();
                for (Cell row : cells) {
                    String s = Bytes.toString(CellUtil.cloneValue(row));
                    buffer.append(s).append(",");
                }
                buffer.replace(buffer.length() - 1, buffer.length(), "");
                ctx.collect(new Tuple2<>(rk, buffer.toString()));
            }
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
