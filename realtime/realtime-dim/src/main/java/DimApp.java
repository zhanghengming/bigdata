import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.BaseApp;
import com.common.Constant;
import com.common.FlinkSourceUtil;
import com.common.TableProcessDim;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author ：zhm
 * @version ：1.0
 * @since ：2025/11/4 19:41
 */
@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001,
                4,
                "dim_app",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 对消费的数据, 做数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
    }
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            String db = jsonObj.getString("database");
                            String type = jsonObj.getString("type");
                            String data = jsonObj.getString("data");

                            return "gmall".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2;

                        } catch (Exception e) {
                            log.warn("不是正确的 json 格式的数据: " + value);
                            return false;
                        }

                    }
                })
                .map(JSON::parseObject);
    }

    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream) {
    /*
    1. 有没有专门的 HBase 连接器
        没有
    2. sql 有专门的 HBase 连接器, 由于一次只能写到一个表中, 所以也不能把流转成表再写

    3. 自定义sink
     */
        resultStream.addSink(new HBaseSinkFunction());
    }
}

