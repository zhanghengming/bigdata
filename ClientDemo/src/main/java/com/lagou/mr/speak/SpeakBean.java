package com.lagou.mr.speak;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/* 当你需要用对象作为key或者value时，就可以设计类，但是得实现writeable序列化接口，这是
Java中的序列化操作在Hadoop中优化后的一种实现，如果自定义Bean对象需要放在Mapper输出KV中的K,则该对象还需实现Comparable接口，因为因
为MapReduce框中的Shuffle过程要求对key必须能排序
 这个类型是map输出kv中value的类型，需要实现writeable序列化接口*/
public class SpeakBean implements Writable {

    //定义属性
    private Long selfDuration;//自由内容时常
    private Long thirdDuration;//第三方内容时常
    private String deviceId;//设备id
    private Long sumDuration;//总时长

    //反序列化时，需要反射调用空参构造函数，所以必须有
    public SpeakBean() {
    }

    public SpeakBean(Long selfDuration, Long thirdDuration) {
        this.selfDuration = selfDuration;
        this.thirdDuration = thirdDuration;
        this.sumDuration = this.selfDuration + this.thirdDuration;
    }

    //序列化方法：就是把内容输出到网络或文本中
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(selfDuration);
        out.writeLong(thirdDuration);
        out.writeLong(sumDuration);
    }

    //反序列化方法，反序列化的字段顺序和序列化字段的顺序必须完全一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.selfDuration = in.readLong();
        this.thirdDuration = in.readLong();
        this.sumDuration = in.readLong();
    }

    @Override
    public String toString() {
        return selfDuration +
                "\t" + thirdDuration +
                "\t" + sumDuration ;
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThirdDuration() {
        return thirdDuration;
    }

    public void setThirdDuration(Long thirdDuration) {
        this.thirdDuration = thirdDuration;
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


    public void set(long selfDuration, long thirdDuration) {
        this.selfDuration = selfDuration;
        this.thirdDuration = thirdDuration;
        this.sumDuration = this.selfDuration + this.thirdDuration;
    }
}
