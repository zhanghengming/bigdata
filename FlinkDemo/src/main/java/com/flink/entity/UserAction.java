package com.flink.entity;

public class UserAction {
    private Long userId;
    private String userAction;

    public UserAction(Long userId, String action) {
        this.userId = userId;
        this.userAction = action;
    }

    public UserAction() {
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getAction() {
        return userAction;
    }

    public void setAction(String action) {
        this.userAction = action;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "userId=" + userId +
                ", action='" + userAction + '\'' +
                '}';
    }
}
