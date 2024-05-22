package com.flink.sink;

import com.flink.entity.Event;
import com.flink.streamdatasource.CustomClickSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new CustomClickSource());
        env.setParallelism(4);
        /**
         * Flink为此专门提供了一个流式文件系统的连接器：StreamingFileSink它继承自抽象类RichSinkFunction，而且集成了 Flink 的检查点
         * （checkpoint）机制，用来保证精确一次（exactly once）的一致性语义。StreamingFileSink 为批处理和流处理提供了一个统一的 Sink，
         * 它可以将分区文件写入 Flink支持的文件系统。它可以保证精确一次的状态一致性，大大改进了之前流式文件 Sink 的方式。它的主要操作是将
         * 数据写入桶（buckets），每个桶中的数据都可以分割成一个个大小有限的分区文件，这样一来就实现真正意义上的分布式文件存储。
         * 我们可以通过各种配置来控制“分桶”的操作；默认的分桶方式是基于时间的，我们每小时写入一个新的桶。换句话说，每个桶内保存的文件，记录的都是 1 小时的输出数据。
         *
         * StreamingFileSink 支持行编码（Row-encoded）和批量编码（Bulk-encoded，比如 Parquet）
         * 格式。这两种不同的方式都有各自的构建器（builder），调用方法也非常简单，可以直接调用
         * StreamingFileSink 的静态方法：
         * ⚫ 行编码：StreamingFileSink.forRowFormat（basePath，rowEncoder）。
         * ⚫ 批量编码：StreamingFileSink.forBulkFormat（basePath，bulkWriterFactory）。
         * 在创建行或批量编码 Sink 时，我们需要传入两个参数，用来指定存储桶的基本路径
         * （basePath）和数据的编码逻辑（rowEncoder 或 bulkWriterFactory）。
         */
        // 配置滚动策略默认的策略
        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                /**
                 * 在以下 3 种情况下，我们就会滚动分区文件：
                 * ⚫ 至少包含 15 分钟的数据
                 * ⚫ 最近 5 分钟没有收到新的数据
                 * ⚫ 文件大小已达到 1 GB
                 */
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())
                .build();

        source.map(Event::toString).addSink(fileSink);

        env.execute();
    }
}
