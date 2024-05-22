package ods;

import bean.TableObject;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 1.从kafka的test这个topic获取数据-----FlinkKafkaConsumer
 * 2.把获取到的json格式的数据进行格式转化-----fastjson
 * type,database, table,data(jsonArray)
 * 3.把转化好的数据保存到HBase中
 */
public class KafkaToHbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(new SourceKafka().getKafkaSource("test"));
        source.print();
        SingleOutputStreamOperator<ArrayList<TableObject>> maped = source.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            String database = jsonObject.get("database").toString();
            String table = jsonObject.get("table").toString();
            String typeInfo = jsonObject.get("type").toString();
            JSONArray datas = jsonObject.getJSONArray("data");
            // datas是每个json的data的value
            ArrayList<TableObject> tableObjects = new ArrayList<TableObject>();
            for (Object o : datas) {
                tableObjects.add(new TableObject(database, table, typeInfo, o.toString()));
                System.out.println(o.toString());
            }
            return tableObjects;
            // lambda表达式的泛型擦除，需要指明返回类型
        }).returns(new TypeHint<ArrayList<TableObject>>(){});

        /**
         * 讲数据下沉到Hbase保存
         * 1.拿到当前数据
         * 2.addSink -- 自定义下沉器SinkHbase
         */
//        maped.addSink(new SinkHbaseFun());
        maped.print();
        env.execute();
    }

}
