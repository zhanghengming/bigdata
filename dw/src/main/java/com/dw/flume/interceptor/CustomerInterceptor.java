package com.dw.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.compress.utils.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CustomerInterceptor implements Interceptor {

//        @Test
//        public void testStart(){
//
//            String str = "2020-08-02 18:19:32.959 [main] INFO com.lagou.ecommerce.AppStart - {\"app_active\":{\"name\":\"app_active\",\"json\":{\"entry\":\"1\",\"action\":\"0\",\"error_code\":\"0\"},\"time\":1596342840284},\"attr\":{\"area\":\"大庆\",\"uid\":\"2F10092A2\",\"app_v\":\"1.1.15\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1002\",\"os_type\":\"2.8\",\"channel\":\"TB\",\"language\":\"chinese\",\"brand\":\"iphone-8\"}}";
//
//            // 创建一个event对象，将消息封装为event
//            HashMap<String, String> hashMap = new HashMap<>();
//            Event event = new SimpleEvent();
//            hashMap.put("logtype","start");
//            event.setHeaders(hashMap);
//            event.setBody(str.getBytes(Charsets.UTF_8));
//
//            CustomerInterceptor customerInterceptor = new CustomerInterceptor();
//            Event intercept = customerInterceptor.intercept(event);
//            Map<String, String> headers = intercept.getHeaders();
//
//            System.out.println(JSON.toJSONString(hashMap));
//
//        }
//
//    @Test
//    public void testEvent(){
//
//        String str = "2020-08-02 18:19:32.959 [main] INFO com.lagou.ecommerce.AppEvent - {\"lagou_event\":[{\"name\":\"goods_detail_loading\",\"json\":{\"entry\":\"1\",\"goodsid\":\"0\",\"loading_time\":\"93\",\"action\":\"3\",\"staytime\":\"56\",\"showtype\":\"2\"},\"time\":1596343881690},{\"name\":\"loading\",\"json\":{\"loading_time\":\"15\",\"action\":\"3\",\"loading_type\":\"3\",\"type\":\"1\"},\"time\":1596356988428},{\"name\":\"notification\",\"json\":{\"action\":\"1\",\"type\":\"2\"},\"time\":1596374167278},{\"name\":\"favorites\",\"json\":{\"course_id\":1,\"id\":0,\"userid\":0},\"time\":1596350933962}],\"attr\":{\"area\":\"长治\",\"uid\":\"2F10092A4\",\"app_v\":\"1.1.14\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1004\",\"os_type\":\"0.5.0\",\"channel\":\"QL\",\"language\":\"chinese\",\"brand\":\"xiaomi-0\"}}";
//
//        // 创建一个event对象，将消息封装为event
//        HashMap<String, String> hashMap = new HashMap<>();
//        Event event = new SimpleEvent();
//        hashMap.put("logtype","event");
//        event.setHeaders(hashMap);
//        event.setBody(str.getBytes(Charsets.UTF_8));
//
//        CustomerInterceptor customerInterceptor = new CustomerInterceptor();
//        Event intercept = customerInterceptor.intercept(event);
//        Map<String, String> headers = intercept.getHeaders();
//
//        System.out.println(JSON.toJSONString(hashMap));
//
//    }
//


    @Override
    public void initialize() {

    }

    @Override
    // 逐条处理event
    public Event intercept(Event event) {
        // 获取 event 的 body
        String eventBody = new String(event.getBody(), Charsets.UTF_8);

        // 获取 event 的 header
        Map<String, String> headers = event.getHeaders();

        // 解析body获取json串
        String[] split = eventBody.split("\\s+");

        try {
            String jsonStr = split[6];
            // 解析json串获取时间戳
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            String timestampStr = "";
            // 根据日志类型的不同 选择不同的处理方式
            // 如果日志的类型为start
            if(headers.getOrDefault("logtype","").equals("start")){
                //获得json中的具体的键值
                timestampStr = jsonObject.getJSONObject("app_active").getString("time");
            } else if (headers.getOrDefault("logtype","").equals("event")){
                /*
                * {"lagou_event":[
                * {"name":"goods_detail_loading",
                * "json":{"entry":"1","goodsid":"0","loading_time":"93","action":"3","staytime":"56","showtype":"2"},
                * "time":1596343881690},
                * {"name":"loading","json":{"loading_time":"15","action":"3","loading_type":"3","type":"1"},
                * "time":1596356988428},{"name":"notification","json":{"action":"1","type":"2"},"time":1596374167278},
                * {"name":"favorites","json":{"course_id":1,"id":0,"userid":0},"time":1596350933962}],
                * "attr":{"area":"长治","uid":"2F10092A4","app_v":"1.1.14","event_type":"common",
                * "device_id":"1FB872-9A1004","os_type":"0.5.0","channel":"QL","language ":"chinese","brand":"xiaomi-0"}}
                * */
                // 此json串中lagou_event是一个数组，数组中包含着json对象
                JSONArray jsonArray = jsonObject.getJSONArray("lagou_event");
                if (jsonArray.size() > 0){

                    timestampStr = jsonArray
                            .getJSONObject(0).getString("time");
                }
            }
            // 设置日期格式
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            // 将时间戳转换为字符串 "yyyy-MM-dd"
            long timestamp = Long.parseLong(timestampStr);
            String date = simpleDateFormat.format(timestamp);
            // 将转换后的字符串放置header中
            headers.put("logtime",date);
            event.setHeaders(headers);
        } catch (Exception e){
            // 将解析不到的设为默认值
            headers.put("logtime","Unknown");
            event.setHeaders(headers);
        }
        return event;
    }

    @Override
    // 批量处理event
    public List<Event> intercept(List<Event> list) {
        List<Event> listEvent = new ArrayList<>();
        for (Event event : list) {
            Event intercept = intercept(event);
            if (intercept != null) {

                listEvent.add(intercept);
            }
        }
        return listEvent;
    }



    @Override
    public void close() {

    }

    // 创建一个构建的类
    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new CustomerInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
