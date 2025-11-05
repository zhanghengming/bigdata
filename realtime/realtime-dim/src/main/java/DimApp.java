import com.common.BaseApp;
import com.common.FlinkSourceUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author ：zhm
 * @version ：1.0
 * @since ：2025/11/4 19:41
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("ods_base_dic", "dim_app");

        env.execute();
    }

    @Override
    public void handle(StreamExecutionEnvironment env, Object source) {

    }
}

