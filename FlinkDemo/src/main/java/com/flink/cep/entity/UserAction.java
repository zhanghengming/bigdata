package com.flink.cep.entity;

public class UserAction {
    public String ip;
    public String timeStamp;
    public String name;
    public String action;

    public UserAction(String ip, String timeStamp, String name, String action) {
        this.ip = ip;
        this.timeStamp = timeStamp;
        this.name = name;
        this.action = action;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "ip='" + ip + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", name='" + name + '\'' +
                ", action='" + action + '\'' +
                '}';
    }
}
