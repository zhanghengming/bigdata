package dim;

import bean.AreaDetail;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import scala.io.StdIn;

import static org.apache.flink.table.api.Expressions.$;

public class AreaDetailInfo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 读取hbase中的数据，形成流
        DataStreamSource<Tuple2<String, String>> source = env.addSource(new HbaseReader());
//        source.print();

        //获取几个必要的字段id name pid
        SingleOutputStreamOperator<AreaDetail> map = source.map(data -> {
            Integer id = Integer.parseInt(data.f0);
            String[] split = data.f1.split("-");
            String name = split[5].trim();
            Integer pid = Integer.parseInt(split[6].trim());
            // 转为bean类 必须是pojo类，有无参构造，所有的都是public的
//            return new Tuple3<Integer, String, Integer>(id, name, pid);
            return new AreaDetail(id, name, pid);
        });
                //.returns(new TypeHint<Tuple3<Integer, String, Integer>>(){});
//        map.print();

        // 将流转化成表操作
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.createTemporaryView("lagou_area", map);
//        Table table = tenv.sqlQuery("select * from lagou_area");
//        tenv.toRetractStream(table,Row.class).print();

        String sql = "select a.id as areaid,a.name as aname,a.pid as cid,b.name as city, c.id as proid,c.name as province\n" +
                "from lagou_area as a\n" +
                "inner join lagou_area as b on a.pid = b.id\n" +
                "inner join lagou_area as c on b.pid = c.id";
        Table table = tenv.sqlQuery(sql);
        // 将表转为流
        SingleOutputStreamOperator<String> maped = tenv.toRetractStream(table, Row.class)
                .map(data -> {
                    Row row = data.f1;
                    // 为null的话会报空指针异常
//                    row.getField(0).toString();
                    String areaId = String.valueOf(row.getField(0));
                    String aname = String.valueOf(row.getField(1));
                    String cid = String.valueOf(row.getField(2));
                    String city = String.valueOf(row.getField(3));
                    String proid = String.valueOf(row.getField(4));
                    String province = String.valueOf(row.getField(5));
                    return areaId + "," + aname + "," + cid + "," + city + "," + proid + "," + province;
                });
        // 将流sink到hbase
        maped.addSink(new HBaseWriterSink());

        env.execute();
    }
}
