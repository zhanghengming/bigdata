package com.flink.cep.entity;

public class UserBean {
    private String uid;
    private double money;
    private long ts;

    public UserBean(String uid, double money, long ts) {
        this.uid = uid;
        this.money = money;
        this.ts = ts;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public double getMoney() {
        return money;
    }

    public void setMoney(double money) {
        this.money = money;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserBean{" +
                "uid='" + uid + '\'' +
                ", money=" + money +
                ", ts=" + ts +
                '}';
    }
}

