package com.flink.watermark;

import com.flink.entity.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

// 在自定义数据源中发送水位线
// 在自定义数据源中发送了水位线以后，就不能再在程序中使用 assignTimestampsAndWatermarks 方法来生成水位线了

public class EmitWatermarkInSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSourceWithWatermark()).print();
        env.execute();
    }
    // 泛型是数据源中的类型
    public static class  ClickSourceWithWatermark implements SourceFunction<Event>{
        private boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                long currTs = Calendar.getInstance().getTimeInMillis();
                String username = userArr[random.nextInt(userArr.length)];
                String url      = urlArr[random.nextInt(urlArr.length)];
                Event event = new Event(username, url, currTs);
                // 使用collectWithTimestamp方法将数据发送出去，并指明数据中的时间戳的字段
                ctx.collectWithTimestamp(event, event.timestamp);
                // 发送水位线
                ctx.emitWatermark(new Watermark(event.timestamp -1L));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
