package com.lagou.mr.writableComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SpeakBeanSort implements WritableComparable<SpeakBeanSort> {

    private Long selfDuration;
    private Long thirdPartDuration;//第三方内容时长
    private String deviceId;//设备id
    private Long sumDuration;//总时长

    public SpeakBeanSort() {
    }

    public SpeakBeanSort(Long selfDuration, Long thirdPartDuration, String deviceId) {
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.deviceId = deviceId;
        this.sumDuration = this.selfDuration + this.thirdPartDuration;
    }

    //重写compareTo方法，此处是比较一个字段如果比较2个字段就是二次排序
    @Override
    public int compareTo(SpeakBeanSort o) {
        int result;

        // 按照总流量大小，倒序排列
        if (sumDuration > o.getSumDuration()){
            result = -1;
        }else if (sumDuration < o.getSumDuration()){
            result = 1;
        }else {
            result = 0;
        }
        return result;
        //return this.getSumDuration() - o.getSumDuration();
    }

    //序列化方法:就是把内容输出到网络或者文本中
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeUTF(deviceId);
        out.writeLong(sumDuration);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        this.selfDuration  = in.readLong();
        this.thirdPartDuration = in.readLong();
        this.deviceId = in.readUTF();
        this.sumDuration = in.readLong();
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Long sumDuration) {
        this.sumDuration = sumDuration;
    }

    @Override
    public String toString() {
        return  selfDuration + "\t"
                 + thirdPartDuration + "\t"
                + deviceId + '\t' +
                 sumDuration ;
    }


}
