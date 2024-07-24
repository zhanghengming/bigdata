package com.demo.flinksql.demos;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**

 *   interval join 示例 间隔join， 左表数据按照时间戳升序排列，右表数据按照时间戳升序排列，左表数据和右表数据的时间戳相差2秒以上，则会被join。
 *   间隔join，两个留关联的条件一个流的时间在另一个流的某个时间段中。
 *
 **/
public class Demo18_IntervalJoin {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        // 设置table环境中的状态ttl时长
        tenv.getConfig().getConfiguration().setLong("table.exec.state.ttl",60*60*1000L);



        /**
         * 1,a,1000
         * 2,b,2000
         * 3,c,2500
         * 4,d,3000
         * 5,e,12000
         */
        DataStreamSource<String> s1 = env.socketTextStream("doitedu", 9998);
        SingleOutputStreamOperator<Tuple3<String, String,Long>> ss1 = s1.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1],Long.parseLong(arr[2]));
        }).returns(new TypeHint<Tuple3<String, String,Long>>() {
        });

        /**
         * 1,bj,1000
         * 2,sh,2000
         * 4,xa,2600
         * 5,yn,12000
         */
        DataStreamSource<String> s2 = env.socketTextStream("doitedu", 9999);
        SingleOutputStreamOperator<Tuple3<String, String,Long>> ss2 = s2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1],Long.parseLong(arr[2]));
        }).returns(new TypeHint<Tuple3<String, String,Long>>() {
        });


        // 创建两个表
        tenv.createTemporaryView("t_left",ss1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt","to_timestamp_ltz(f2,3)")
                .watermark("rt","rt - interval '0' second")
                .build());

        tenv.createTemporaryView("t_right",ss2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt","to_timestamp_ltz(f2,3)")
                .watermark("rt","rt - interval '0' second")
                .build());



        // interval  join
        tenv.executeSql("select a.f0,a.f1,a.f2,b.f0,b.f1   from t_left  a  join t_right b " +
                "on a.f0=b.f0  " +
                "and a.rt between b.rt  - interval '2' second and b.rt").print();



    }
}