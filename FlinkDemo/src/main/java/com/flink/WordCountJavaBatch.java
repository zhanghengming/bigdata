package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;

/**
 * 1、读取数据源
 * <p>
 * 2、处理数据源
 * <p>
 * a、将读到的数据源文件中的每一行根据空格切分
 * <p>
 * b、将切分好的每个单词拼接1
 * <p>
 * c、根据单词聚合（将相同的单词放在一起）
 * <p>
 * d、累加相同的单词（单词后面的1进行累加）
 * <p>
 * 3、保存处理结果
 */
public class WordCountJavaBatch {
    public static void main(String[] args) throws Exception {
        String input = "D:\\新大数据\\data\\wc.txt";
        String output = "D:\\新大数据\\data\\outputLambda";
        // 获取flink的运行 环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        // 读取数据源数据
        DataSource<String> dataSource = executionEnvironment.readTextFile(input);

        // 处理数据  参数为接口 需要他的实现
        // 多个单词和1的组合
        // 方法一：创建接口的实现类的对象
        // FlatMapOperator<String, Tuple2<String, Integer>> wordAndOnes = dataSource.flatMap(new SplitLine());
        // 方法二：匿名内部类
//        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOnes = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] words = s.split("\\s+");
//                for (String word : words) {
//                    collector.collect(new Tuple2<String, Integer>(word, 1));
//                }
//            }
//        });
        // 方法三：lambda表达式
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOnes = dataSource.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
            String[] words = s.split("\\s+");
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        // （hello 1） （spark 1）  （spark 1） 根据第一个单词进行聚合
        UnsortedGrouping<Tuple2<String, Integer>> group = wordAndOnes.groupBy(0);
        // 将相同单词的聚合到一起 然后根据第二个进行 累加求和
        AggregateOperator<Tuple2<String, Integer>> out = group.sum(1);

        // 设置并行度
        out.writeAsCsv(output, "\n", " ").setParallelism(1);
        // 人为调用执行方法
        executionEnvironment.execute();

    }

    //输入的每一行数据源的类型为string 输出结果为每个单词和1的拼接 为元组,需要传递泛型
    static class SplitLine implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        // s就是每一行元素 collector为向下游算子发送出去的对象
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split("\\s+");
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
