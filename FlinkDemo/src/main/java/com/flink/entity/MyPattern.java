package com.flink.entity;

public class MyPattern {
    private String firstAction;
    private String secondAction;

    public MyPattern(String firstAction, String secondAction) {
        this.firstAction = firstAction;
        this.secondAction = secondAction;
    }

    public MyPattern() {
    }

    public String getFirstAction() {
        return firstAction;
    }

    public void setFirstAction(String firstAction) {
        this.firstAction = firstAction;
    }

    public String getSecondAction() {
        return secondAction;
    }

    public void setSecondAction(String secondAction) {
        this.secondAction = secondAction;
    }

    @Override
    public String toString() {
        return "MyPattern{" +
                "firstAction='" + firstAction + '\'' +
                ", secondAction='" + secondAction + '\'' +
                '}';
    }
}
