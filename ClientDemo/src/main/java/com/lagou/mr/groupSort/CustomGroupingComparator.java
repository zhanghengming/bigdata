package com.lagou.mr.groupSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomGroupingComparator extends WritableComparator {

    //将我们自定义的OrderBean注册到我们自定义的CustomGroupIngCompactor当中来
    // 表示我们的分组器在分组的时候，对OrderBean这一种类型的数据进行分组
    // 传入作为key的bean的class类型，以及制定需要让框架做反射获取实例对象
    public CustomGroupingComparator() {

        super(OrderBean.class,true);
    }

    // 比较的是两个bean对象
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //首先需要将该对象转为基本的bean对象
        OrderBean a1 = (OrderBean) a;
        OrderBean b1 = (OrderBean) b;
        int i = a1.getOrderId().compareTo(b1.getOrderId());
        if (i == 0){
            System.out.println(a1.getOrderId()+b1.getOrderId());
        }
        return i;
    }
}
