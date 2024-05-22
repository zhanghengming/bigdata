package com.flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class UDFExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 注册函数
        tableEnv.createTemporarySystemFunction("HashFunction", HashFunction.class);
        // 在 SQL 里调用注册好的自定义标量函数
        tableEnv.sqlQuery("select HashFunction(myField) from MyTable");
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        // 重命名侧向表中的字段
        // 在 SQL 里调用注册好的自定义表函数
        tableEnv.sqlQuery("select  myField, newWord, newLength from MyTable left join " +
                "lateral table(SplitFunction(myField)) as T(newWord, newLength) on ture");
        // 注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("WeightedAvg", WeightedAvg.class);
        // 在 SQL 里调用注册好的聚合函数
        tableEnv.sqlQuery("select student,WeightedAvg(score, weight) FROM ScoreTable GROUP BY student");

        tableEnv.createTemporarySystemFunction("Top2", Top2.class);
        // 在 Table API 中调用函数
        tableEnv.from("MyTable")
                .groupBy($("myField"))
                // flatAggregate()方法它就是专门用来调用表聚合函数的接口
                .flatAggregate(call("Top2",$("value").as("value", "rank")))
                .select($("myField"), $("value"), $("rank"));
    }

    /**
     * 自定义的哈希（hash）函数 HashFunction
     */
    public static class HashFunction extends ScalarFunction {
        // 接受任意类型输入，返回 INT 型输出
        //  Table API 在对函数进行解析时需要提取求值方法参数的类型引用，所以
        //我们用 DataTypeHint(inputGroup = InputGroup.ANY)对输入参数的类型做了标注，表示 eval 的
        //参数可以是任意类型。
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.hashCode();
        }
    }

    /**
     * 表函数的输入参数也可以是 0 个、1 个或多个标量值；不同的是，它可
     * 以返回任意多行数据，“多行数据”事实上就构成了一个表
     * 一个分隔字符串的函数 SplitFunction
     */
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {
        public void eval(String str) {
            for (String s : str.split(" ")) {
                // 使用 collect()方法发送一行数据
                collect(Row.of(s, s.length()));
            }
        }
    }

    /**
     * 自定义聚合函数，加权平均值
     */
    // 累加器类型定义
    public static class WeightedAvgAccumulator {
        public long sum = 0; // 加权和
        public int count = 0; // 数据个数
    }

    // 自定义聚合函数，输出为长整型的平均值，累加器类型为 WeightedAvgAccumulator
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator> {

        @Override
        // 这是得到最终返回结果的方法
        public Long getValue(WeightedAvgAccumulator acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count; // 计算平均值并返回
            }
        }

        // 创建累加器
        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        // 累加计算方法，每来一行数据都会调用
        public void accumulate(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }
    }

    /**
     * 自定义表聚合函数（UDTAGG）可以把一行或多行数据（也就是一个表）聚合成另
     * 一张表，结果表中可以有多行多列
     * top2
     */
    public static class Top2Accumulator {
        public Integer first;
        public Integer second;
    }
    // 自定义表聚合函数，查询一组数中最大的两个，返回值为(数值，排名)的二元组
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            // 设置初值
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }
        // 每来一个数据调用一次，判断是否更新累加器
        public void accumulate(Top2Accumulator acc, Integer value) {
            if (value > acc.first) {
                // 每来一个数，如果比之前大的话赋值，将之前的赋值给第二个
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                // 和第二个比较 大的话更新
                acc.second = value;
            }
        }
        // 输出(数值，排名)的二元组，输出两行数据
        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }

}
