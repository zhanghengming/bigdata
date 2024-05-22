package com.flink.cep;


import com.flink.cep.entity.UserAction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class CepLearn01 {

    private static final OutputTag<UserAction> overFiveTag = new OutputTag<UserAction>("overFive") {
    };

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Pattern<UserAction, ?> pattern = Pattern.<UserAction>begin("start").where(
                new SimpleCondition<UserAction>() {
                    @Override
                    public boolean filter(UserAction event) {
                        return event.action.equals("click");
                    }
                }
        ).next("middle").where(
                new SimpleCondition<UserAction>() {
                    @Override
                    public boolean filter(UserAction subEvent) {
                        return subEvent.action.equals("buy");
                    }
                }
        ).within(Time.seconds(5));


        SingleOutputStreamOperator<UserAction> input = env.fromElements(WORDS).map(new MapFunction<String, UserAction>() {
            @Override
            public UserAction map(String value) throws Exception {
                String[] split = value.toLowerCase().split(",");
                return new UserAction(split[0], split[1], split[2], split[3]);
            }
        }).keyBy(new KeySelector<UserAction, String>() {
            @Override
            public String getKey(UserAction value) throws Exception {

                return value.name;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserAction>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(UserAction element) {
                return Long.parseLong(element.timeStamp);
            }
        });

        PatternStream<UserAction> patternStream = CEP.pattern(input, pattern);

        //获取click-buy事件消息及超时消息
        SingleOutputStreamOperator<String> clickAndBuy = patternStream.select(overFiveTag, new PatternTimeoutFunction<UserAction, UserAction>() {
            @Override
            public UserAction timeout(Map<String, List<UserAction>> pattern, long timeoutTimestamp) throws Exception {

                UserAction click = pattern.get("start").iterator().next();
//                UserAction buy = pattern.get("middle").iterator().next();
//                System.out.println("click"+click);
//                System.out.println("buy"+buy);

                return click;
            }
        }, new PatternSelectFunction<UserAction, String>() {
            @Override
            public String select(Map<String, List<UserAction>> pattern) throws Exception {
                UserAction click = pattern.get("start").iterator().next();
                UserAction buy = pattern.get("middle").iterator().next();
                return "name:"+ click.name+"--click--"+click.timeStamp+"--buy--"+buy.timeStamp;
            }
        });
        clickAndBuy.print();
        DataStream<UserAction> sideOutput = clickAndBuy.getSideOutput(overFiveTag);
//        sideOutput.print();

//仅获取click-buy事件消息
//        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<UserAction, String>() {
//           @Override
//           public String select(Map<String, List<UserAction>> map) throws Exception {
//                UserAction click = map.get("start").iterator().next();
//                UserAction buy = map.get("middle").iterator().next();
//               return "name:"+ click.name+"--click--"+click.timeStamp+"--buy--"+buy.timeStamp;
//            }
//       });
//        select.print();

        env.execute("jjjj");
    }


    public static final String[] WORDS = new String[]{
            "176.168.50.26,1575600181000,john,click",
            "176.168.50.26,1575600182000,john,buy",
            "176.168.50.26,1575600183000,john,click",
            "176.168.50.26,1575600184000,john,click",
            "176.168.50.26,1575600190000,john,buy",
            "176.168.50.26,1575600191000,jerry,click",
            "176.168.50.26,1575600194000,jerry,buy",
            "176.168.50.26,1575600199000,jerry,click",
            "176.168.50.26,1575600200000,jerry,order",
            "176.168.50.26,1575600220000,jerry,buy"
    };


}
