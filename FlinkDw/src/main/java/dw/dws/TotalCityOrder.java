package dw.dws;

import bean.DimArea;
import bean.TradeOrder;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 *需求1 :  查询城市、省份、订单总额、订单总数----全量查询
 *  * 获取两部分数据
 *      * 1、dim_lagou_area  dim维表数据
 *      * 2、增量数据   lagou_trade_orders(HBase)
 *  * 进行计算
 *  *      1，2 统一到一起参与计算  sql
 */
public class TotalCityOrder {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // dim_lagou_area 从hbase读 dim维表数据
        DataStreamSource<Tuple2<String, String>> dimAreaStream = env.addSource(new ReadDimArea());
        // 增量数据   lagou_trade_orders(HBase) 从hbase读
        DataStreamSource<Tuple2<String, String>> tradeOrderStream = env.addSource(new ReadTradeOrder());
        // 那几条出来测试
//        SingleOutputStreamOperator<Tuple2<String, String>> test = tradeOrderStream.filter(new FilterFunction<Tuple2<String, String>>() {
//            private int count = 0;
//            private final int maxRecords = 5; // 打印前 n 条记录
//
//            @Override
//            public boolean filter(Tuple2<String, String> value) throws Exception {
//                count++;
//                if (count < maxRecords) {
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        });

        SingleOutputStreamOperator<DimArea> areaStream = dimAreaStream.map(data -> {
            Integer arearId = Integer.parseInt(data.f0);
            String[] datas = data.f1.split(",");
            String aname = datas[0].trim();
            String cid = datas[1].trim();
            String city = datas[2].trim();
            String proid = datas[3].trim();
            String province = datas[4].trim();

            return new DimArea(arearId, aname, cid, city, proid, province);

        });

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.createTemporaryView("dim_lagou_area", areaStream);

        SingleOutputStreamOperator<TradeOrder> orderStream = tradeOrderStream.map(data -> {
            Integer orderid = Integer.parseInt(data.f0);
            String[] datas = data.f1.split(",");
            String orderNo = datas[7].trim();
            Integer userId = Integer.parseInt(datas[15].trim());
            Integer status = Integer.parseInt(datas[11].trim());
            Double totalMoney = Double.parseDouble(datas[12]);
            Integer areaId = Integer.parseInt(datas[0].trim());
            return new TradeOrder(orderNo, orderid, totalMoney, userId, areaId, status);
        });

        tenv.createTemporaryView("lagou_orders", orderStream);

        String sql = "select f.city,f.province,sum(f.qusum) as orderMoney, sum(f.qucount) as orderCount from\n" +
                "(select r.aname as qu,r.city as city,r.province as province,sum(k.totalMoney) as qusum,count(k.totalMoney) as qucount\n" +
                "from lagou_orders as k\n" +
                "inner join dim_lagou_area as r\n" +
                "on k.areaId = r.areaId\n" +
                "group by r.aname,r.city,r.province) as f\n" +
                "group by f.city,f.province";
        Table table = tenv.sqlQuery(sql);
        DataStream<Tuple2<Boolean, Row>> result = tenv.toRetractStream(table, Row.class);
        result.filter(x -> x.f0).print();

        env.execute();
    }
}
