package com.flink.streamdatasource;

import com.flink.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义没有并行度的数据源
 */

public class CustomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> streamSource = env.addSource(new customSource()).setParallelism(2);
        streamSource.print("CustomSource");
        env.execute();
    }

    //    public static class customSource implements SourceFunction<Event> {
    // 多个并行度的数据源
    public static class customSource implements ParallelSourceFunction<Event> {
        // 声明一个布尔变量，作为控制数据生成的标识位
        private Boolean running = true;

        /**
         * 使用运行时上下文对象（SourceContext）向下游发送数据
         *
         * @param ctx The context to emit elements to and for accessing locks.
         * @throws Exception
         */
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 在指定的数据集中随机选取数据
            Random random = new Random();
            String[] users = {"Mary", "Alice", "Bob", "Cary"};
            String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
            while (running) {
                ctx.collect(new Event(users[random.nextInt(users.length)]
                        , urls[random.nextInt(urls.length)]
                        , Calendar.getInstance().getTimeInMillis()));
                // 隔 1 秒生成一个点击事件，方便观测
                Thread.sleep(1000);
            }
        }

        /**
         *
         */
        @Override
        public void cancel() {
            running = false;
        }
    }
}

