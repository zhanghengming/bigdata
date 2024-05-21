package com.lagou.mr.speak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeakReducer extends Reducer<Text,SpeakBean, Text,SpeakBean> {

    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        long self_Duration = 0;
        long thirdPart_Duration = 0;
        // 遍历所用bean，将其中的自有，第三方时常分别累加
        for (SpeakBean speakBean : values
             ) {
            self_Duration += speakBean.getSelfDuration();
            thirdPart_Duration += speakBean.getThirdDuration();
        }
        //封装对象

        SpeakBean speakBean = new SpeakBean(self_Duration, thirdPart_Duration);
        // 写出
        context.write(key,speakBean);
    }
}
