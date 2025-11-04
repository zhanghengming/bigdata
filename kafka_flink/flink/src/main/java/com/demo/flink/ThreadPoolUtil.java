package com.demo.flink;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
    线程池工具类
    线程池参数：
    核心线程数：10
    最大线程数：20
    存活时间：1000ms
    单位：TimeUnit.SECONDS
    阻塞队列：ArrayBlockingQueue<50>
 */
public class ThreadPoolUtil {
    // volatile关键字保证可见性，也就是多线程看不见其他线程对成员变量的修改
    private static volatile ThreadPoolExecutor executor;
    // 懒汉式单例模式，双重检查锁 解决了懒汉式线程安全问题
    public static ThreadPoolExecutor getThreadPool() {
        if (executor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (executor == null) {
                    executor = new ThreadPoolExecutor(10, 20, 1000, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(50));
                }
            }
        }
        return executor;
    }
}
