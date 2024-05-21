package com.lagou.mr.join.reduce_join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoinReducer extends Reducer<Text,DeliverBean,DeliverBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<DeliverBean> values, Context context) throws IOException, InterruptedException {
        // 1准备投递行为数据的集合
        ArrayList<DeliverBean> list = new ArrayList<>();
        // 2 准备bean对象
        DeliverBean bean = new DeliverBean();
        for (DeliverBean de : values) {
            if ("deliver".equals(de.getFlag())){
                DeliverBean dbean = new DeliverBean();
                try {
                    BeanUtils.copyProperties(dbean,de);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                list.add(dbean);
            }else {
                try {
                    BeanUtils.copyProperties(bean,de);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        // 3 表的拼接
        for (DeliverBean bean1 : list){
            bean1.setPositionName(bean.getPositionName());
            // 4 数据写出去
            context.write(bean1,NullWritable.get());
        }
    }
}
