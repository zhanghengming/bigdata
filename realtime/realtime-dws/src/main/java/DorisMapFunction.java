/**
 * @author ：zhm
 * @version ：1.0
 * @since ：2025/11/8 16:07
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

public class DorisMapFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T bean) throws Exception {
        SerializeConfig conf = new SerializeConfig();
        conf.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, conf);
    }
}
