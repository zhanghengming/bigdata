package com.flink.transformation;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class RescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RichParallelSourceFunction<Integer>() {
            /**
             * @param ctx The context to emit elements to and for accessing locks. 
             * @throws Exception
             */
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 0; i < 8; i++) {
                    if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i + 1);
                        Thread.sleep(1000);
                    }
                }
            }

            /**
             * 
             */
            @Override
            public void cancel() {

            }
        }).setParallelism(2).rebalance().print().setParallelism(4);
        env.execute();
    }
}
