package myutils;

import bean.TableObject;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

public class Json2Bean implements Serializable {
// 将json中的数据转化为实体类
    public TableObject jsonToBeans (String json) {
        JSONObject jsonObject = JSON.parseObject(json);
        String database = jsonObject.get("database").toString();
        String table = jsonObject.get("table").toString();
        String type = jsonObject.get("type").toString();
        // 获取json中的数组value，value值是数组，只有一个值
        JSONArray data = jsonObject.getJSONArray("data");
        String businessData = data.get(0).toString();
        return new TableObject(database, table, type, businessData);
    }
}
