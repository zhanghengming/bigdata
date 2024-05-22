package com.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {

    /**
     * 转为json格式
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建 JSON 对象用于存储数据信息
        JSONObject data = new JSONObject();

        //获取主题信息,包含着数据库和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        data.put("db",fields[1]);
        data.put("tableName",fields[2]);

        //获取before信息并转换为 Struct 类型 value=Struct{before=Struct{id=11,name=3}}
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        // 获取列信息
        if (before != null) {
            Schema schema = before.schema();
            List<Field> fieldsList = schema.fields();
            for (Field field : fieldsList) {
                beforeJson.put(field.name(),before.get(field));
            }
        }
        data.put("before",beforeJson);

        //获取after信息并转换为 Struct 类型 value=Struct{before=Struct{id=11,name=3}}
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        // 获取列信息
        if (after != null) {
            Schema schema = after.schema();
            List<Field> fieldsList = schema.fields();
            for (Field field : fieldsList) {
                afterJson.put(field.name(),after.get(field));
            }
        }
        data.put("after",afterJson);

        //获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        data.put("op",operation.toString().toLowerCase());

        //发送数据至下游
        collector.collect(data.toJSONString());

    }

    /**
     * @return 返回类型string
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
