package com.flink.cep.entity;

public class CEPLoginBean {
    private long id;
    private String state;
    private long ts;

    public CEPLoginBean(long id, String state, long ts) {
        this.id = id;
        this.state = state;
        this.ts = ts;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "CEPLoginBean{" +
                "id=" + id +
                ", state='" + state + '\'' +
                ", ts=" + ts +
                '}';
    }
}
