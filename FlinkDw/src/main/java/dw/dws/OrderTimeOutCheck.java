package dw.dws;

import bean.OrderDetail;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * 交易支付异常
 * 15分钟内，订单没有完成 测流输出支付异常的数据
 */
public class OrderTimeOutCheck {
    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
//        DataStreamSource<String> data = env.socketTextStream("linux121", 9999);
        DataStreamSource<String> data = env.fromElements(
                "9390,1,2020-07-28 00:15:11,295",
                "5990,1,2020-07-28 00:16:12,165",
                "9390,2,2020-07-28 00:18:11,295",
                "5990,2,2020-07-28 00:18:12,165",
                "9390,3,2020-07-29 08:06:11,295",
                "5990,4,2020-07-29 12:21:12,165",
                "8457,1,2020-07-30 00:16:15,132",
                "5990,5,2020-07-30 18:13:24,165",
                "1001,1,2020-10-20 11:05:15,132",
                "1001,2,2020-10-20 11:25:15,132",
                "8458,2,2020-10-20 11:00:15,132"
        );
        SingleOutputStreamOperator<OrderDetail> watermarks = data.map(x -> {
            String[] split = x.split(",");
            return new OrderDetail(split[0], split[1], split[2], Double.parseDouble(split[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        try {
                            Date parse = simpleDateFormat.parse(element.getOrderCreateTime());
                            return parse.getTime();
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }));
        // 按用户id分组,相同id分到一起
        KeyedStream<OrderDetail, String> keyedStream = watermarks.keyBy(OrderDetail::getOrderId);
        // 做出模式
        Pattern<OrderDetail, OrderDetail> pattern = Pattern.<OrderDetail>begin("start")
                .where(new IterativeCondition<OrderDetail>() {
                    @Override
                    public boolean filter(OrderDetail value, Context<OrderDetail> ctx) throws Exception {
                        return value.getStatus().equals("1");
                    }
                })
                .followedBy("second")
                .where(new IterativeCondition<OrderDetail>() {
                    @Override
                    public boolean filter(OrderDetail value, Context<OrderDetail> ctx) throws Exception {
                        return value.getStatus().equals("2");
                    }
                })
                .within(Time.minutes(15));
        // 匹配流中的模式
        PatternStream<OrderDetail> patternStream = CEP.pattern(keyedStream, pattern);
        // 侧流
        OutputTag<OrderDetail> outputTag = new OutputTag<OrderDetail>("orderTimeout"){};
        // 对匹配流进行选择
        SingleOutputStreamOperator<OrderDetail> select = patternStream.select(outputTag, new PatternTimeoutFunction<OrderDetail, OrderDetail>() {
            @Override
            public OrderDetail timeout(Map<String, List<OrderDetail>> pattern, long timeoutTimestamp) throws Exception {
                return pattern.get("start").iterator().next();
            }
        }, new PatternSelectFunction<OrderDetail, OrderDetail>() {
            @Override
            public OrderDetail select(Map<String, List<OrderDetail>> pattern) throws Exception {
                return pattern.get("second").iterator().next();
            }
        });
        // 侧流输出匹配超时的
        select.getSideOutput(outputTag).print("侧流");
        System.out.println("------------");
        // 输出匹配正常的
        select.print();
        env.execute();

    }
}
