package com.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：zhm
 * @version ：1.0
 * @since ：2025/11/5 14:39
 */
public abstract class BaseApp {
    public void start() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink/checkpoints/");

        // 获取source
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("test", "test");
        /**
         * 模版方法设计模式：某个方法我不知道怎么去实现，只能提供方法的声明，具体的实现留给子类去完成。
         * 定义成一个抽象方法，一个类中有一个抽象方法，那么这个类就要定义为抽象类。
         */
        // 处理逻辑
        handle(env, kafkaSource);
        env.execute();

    }

    public abstract void handle(StreamExecutionEnvironment env, Object source);

}
