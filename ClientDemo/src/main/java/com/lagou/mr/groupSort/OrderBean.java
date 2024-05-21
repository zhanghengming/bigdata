package com.lagou.mr.groupSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {


    private String orderId;
    private Double price;

    public OrderBean() {
    }

    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public String toString() {
        return  orderId + '\'' + price ;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        // 比较订单id的排序顺序,string的比较
        int i = this.orderId.compareTo(o.orderId);
        if (i == 0){
            //如果订单id相同，则比较金额，金额大的排在前面,默认是升序
           i = -this.price.compareTo(o.price);
        }
        return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }
}
