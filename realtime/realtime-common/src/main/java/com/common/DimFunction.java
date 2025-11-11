package com.common;

/**
 * @author ：zhm
 * @version ：1.0
 * @since ：2025/11/9 11:05
 */
import com.alibaba.fastjson.JSONObject;
//创建模板方法设计模式模板接口DimFunction
public interface DimFunction<T> {
    String getRowKey(T bean);
    String getTableName();
    void addDims(T bean, JSONObject dim);
}
