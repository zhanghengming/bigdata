package dw.dws;

import bean.CityOrder;
import bean.TableObject;
import bean.TradeOrder;
import com.alibaba.fastjson.JSON;
import myutils.ConnHBase;
import myutils.Json2Bean;
import ods.SourceKafka;
import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 *  每隔5分钟统计最近1小时内的订单交易情况，要求显示城市、省份、交易总金额、订单总数---增量统计
 *  订单统计
 *  省-市 订单总数 订单总额 时间
 */
public class OrderStatistics {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取批量数据
        DataSource<Tuple2<String, String>> hbaseData = batchEnv.createInput(new TableInputFormat<Tuple2<String, String>>() {
            @Override
            public void configure(Configuration parameters) {
                Connection conn = new ConnHBase().connToHbase();
                try {
                    table = (HTable) conn.getTable(TableName.valueOf("dim_lagou_area"));
                    scan = new Scan();
                    scan.addFamily(Bytes.toBytes("f1"));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            protected Scan getScanner() {
                return scan;
            }

            @Override
            protected String getTableName() {
                return "dim_lagou_area";
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                String rk = Bytes.toString(result.getRow());
                StringBuffer buffer = new StringBuffer();
                for (Cell cell : result.rawCells()) {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    buffer.append(value).append(",");
                }
                buffer.replace(buffer.length() - 1, buffer.length(), "");
                return new Tuple2<>(rk, buffer.toString());
            }
        });

        List<Tuple2<String, String>> collect = hbaseData.collect();

//        System.out.println(collect);
        //从kafka中获取增量数据,数据就是直接从canal同步来的json格式的数据
        FlinkKafkaConsumer<String> kafkaConsumer = new SourceKafka().getKafkaSource("test");
        DataStreamSource<String> kafkaSource = streamEnv.addSource(kafkaConsumer);

        Json2Bean json2Bean = new Json2Bean();
        // 将json转为tableObject类
        SingleOutputStreamOperator<TradeOrder> orderInfo = kafkaSource.map(x -> json2Bean.jsonToBeans(x))
                // 过滤出指定表的数据
                .filter(x -> x.getTableName().equals("lagou_trade_orders"))
                // 抽取需要的字段转为类
                .map(x -> JSON.parseObject(x.getDataInfo(), TradeOrder.class))
                .returns(new TypeHint<TradeOrder>(){});

        // TradeOrder{orderNo='23a0b124546', orderId=5, totalMoney=32757.0, userId=72, areaId=370203, status=2}
        // 根据地域分组
        SingleOutputStreamOperator<Tuple2<CityOrder, Long>> result = orderInfo.keyBy(x -> x.areaId)
                .map(x -> {
                    String str = null;
                    Double money = 0.0;
                    Integer count = 0;
                    // 循环批数据组成的集合来和另一个流的数据作比较
                    for (Tuple2<String, String> data : collect) {
                        // 判断是同一个地区的 累加
                        if (data.f0.equals(x.areaId.toString())) {
                            money += x.totalMoney;
                            count += 1;
                            str = data.f1;
                        }
                    }
                    String[] split = str.split(",");
                    return new Tuple2<String, Tuple2<Double, Integer>>(split[2] + "-" + split[4], new Tuple2<>(money, count));
                }).returns(new TypeHint<Tuple2<String, Tuple2<Double, Integer>>>(){})
                // 按照整个地域分组
                .keyBy(x -> x.f0)
                // 每隔5s统计10分钟内的窗口
                .timeWindow(Time.seconds(60 * 10), Time.seconds(5))
                //增量聚合函数和全窗口函数的结合，为的是附加窗口的其它信息
                .aggregate(new MyAggFunc(), new MyWindowFunc());
        result.print();

//        orderInfo.print();
        streamEnv.execute();

    }


    private static class MyAggFunc implements AggregateFunction<Tuple2<String, Tuple2<Double, Integer>>, Tuple2<Double, Long>, Tuple2<Double, Long>> {
        @Override
        public Tuple2<Double, Long> createAccumulator() {
            return new Tuple2<>(0.0, 0L);
        }

        @Override
        public Tuple2<Double, Long> add(Tuple2<String, Tuple2<Double, Integer>> value, Tuple2<Double, Long> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1.f0, accumulator.f1 + value.f1.f1);
        }

        @Override
        public Tuple2<Double, Long> getResult(Tuple2<Double, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
            return null;
        }
    }

    private static class MyWindowFunc extends ProcessWindowFunction<Tuple2<Double, Long>, Tuple2<CityOrder, Long>, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Tuple2<Double, Long>, Tuple2<CityOrder, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<Double, Long>> elements, Collector<Tuple2<CityOrder, Long>> out) throws Exception {
            Tuple2<Double, Long> info = elements.iterator().next();
            String[] split = s.split("-");
            String city = split[0];
            String province = split[1];
            out.collect(new Tuple2<>(new CityOrder(city, province, info.f0, info.f1), context.window().getEnd()));
        }
    }
}
