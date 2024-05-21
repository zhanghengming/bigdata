package com.lagou.mr.groupSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        //自定义分区，将相同订单id的数据发送到同一个reduce里面去,key为bean对象，判断对象中的一个变量相同即为该key相同
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) %i;
    }
}
