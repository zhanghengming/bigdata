package com.dw.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.junit.Test;

import java.util.ArrayList;

public class ParseJsonArray extends UDF {

    // 解析json中的数组
    public ArrayList<String> evaluate(String jsonStr,String key){
        if (Strings.isNullOrEmpty(jsonStr)){
            return null;
        }
        try {

            // 从json串中获得json对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            // 获得json中的数组
            JSONArray jsonArray = jsonObject.getJSONArray(key);
            // 将json中的数组返回 以数组的形式
            ArrayList<String> objects = new ArrayList<>();
            for (Object str : jsonArray){
                objects.add(str.toString());
            }
            return objects;
        } catch (JSONException e){
            return null;
        }
    }


    @Test
    public void JunitParseJsonArray(){
        String str = " {\"lagou_event\":[{\"name\":\"notification\",\"json\":{\"action\":\"1\",\"type\":\"1\"},\"time\":1595333674179},{\"name\":\"favorites\",\"json\":{\"course_id\":8,\"id\":0,\"userid\":0},\"time\":1595344106919},{\"name\":\"praise\",\"json\":{\"id\":6,\"type\":3,\"add_time\":\"1597891993192\",\"userid\":9,\"target\":9},\"time\":1595329089797}],\"attr\":{\"area\":\"德州\",\"uid\":\"2F10092A6\",\"app_v\":\"1.1.2\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1006\",\"os_type\":\"0.71\",\"channel\":\"VY\",\"language\":\"chinese\",\"brand\":\"xiaomi-1\"}} ";
        ArrayList<String> evaluate = evaluate(str, "lagou_event");
        System.out.println(JSON.toJSONString(evaluate));
    }
}
