package com.flink;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
    public static void main(String[] args) throws ParseException {
        String s = "123";
        // 将新的地址赋值给s，改变的是对象的指向，并不改变对象的内容，所有字符串的频繁创建会浪费空间。
        Date date = new Date();
        long currentTimeMillis = System.currentTimeMillis();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        // 将字符串转为时间戳
        Date parse = simpleDateFormat.parse("2020-07-28 00:15:11.295");
        System.out.println(parse.getTime());
//        System.out.println(parse);
//        String format = simpleDateFormat.format(date);
//        String format1 = simpleDateFormat.format(1597906239000L);
//        System.out.println(format);
//        System.out.println(format1);
//        System.out.println(currentTimeMillis);
    }
}
